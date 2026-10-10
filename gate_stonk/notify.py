"""Telegram notifier with a durable SQLite outbox (DESIGN §12, notes/ref_config_cli_ops.md §5).

Delivery model (ported from the reference ``spot_portfolio/notifications.py``):

* The store outbox is authoritative. Fill events (``fill:<fill_key>``) are written by
  ``Store.record_trades`` in the SAME transaction as the fill (text from
  ``store.fill_text``); every other event is queued with ``Notifier.event(key, text)``
  (``store.outbox_add``, idempotent by key). Nothing is ever sent synchronously from
  trading code; ``event`` never raises.
* A background daemon thread (``gate-stonk-telegram``) delivers due rows strictly in
  outbox order (``store.outbox_due`` stops at the first row in backoff, so newer events
  never overtake an older one) through ``sendMessage``. Each message carries
  ``[event <key>]``: Telegram has no idempotency key, so a response lost after a
  successful delivery can duplicate a message (at-least-once), never lose it.
* Long texts are split into chunks of at most 3900 characters (UTF-16 units, which is
  what Telegram counts), preferably at a line break; the event is acknowledged only after
  ALL chunks were delivered. Within one process a retry continues from the first
  undelivered chunk; after a restart the whole event is sent again.
* One HTTP call per ``send_interval_seconds`` (>= 1 s, Telegram's per-chat pace).
* A failure keeps the row: ``store.outbox_mark_failed`` schedules the next attempt after
  ``max(retry_after, min(60, 2**min(attempts, 6)))`` s (2, 4, 8, ... 60 s), durable across
  restarts. HTTP 429 ``parameters.retry_after`` additionally blocks every Telegram call of
  this process until it elapses.
* Secrets: the bot token appears only in the request URL handed to the HTTP session.
  Transport exceptions are replaced by their type name and raised outside the ``except``
  block (no ``__context__`` holding the URL); logs, ``last_error``, the outbox and
  ``status()`` contain only codes such as ``TELEGRAM_HTTP_429`` or ``ConnectionError``.

Health (``healthy()``): Telegram disabled -> True. Enabled -> True only when a
``sendMessage`` was confirmed in this process (the chat is verified) and the last good
contact is at most ``telegram.max_unhealthy_seconds`` old. Good contact = a confirmed
``sendMessage`` or a successful ``getMe`` healthcheck (every ``healthcheck_seconds``
while nothing is being sent) as long as no delivery failure is outstanding (``getMe``
proves the token, never the chat). A single timeout or 429 therefore never makes the
channel unhealthy; ``max_unhealthy_seconds`` without confirmed contact does. The run
command queues a start message so the chat gets verified right after start.

Telegram disabled: no thread traffic, no HTTP; events are still stored and immediately
marked ``skipped`` (status ``skipped`` in the outbox, counted in ``status()``); fill
events written directly by the store are marked skipped by ``pump()``.

Message helpers below build the Russian texts for the engine/CLI: start/stop, startup
sizes, state changes and resumes, unknown create result, reject, cancel, terminal
close, new order notice, position snapshot and the periodic summary
(``Notifier.maybe_summary`` with a durable ``summary_bucket`` meta key). Paper events
get the ``[PAPER] `` prefix in ``event`` unless the text already starts with
``[PAPER]``/``[LIVE]``.
"""
from __future__ import annotations

import itertools
import logging
import math
import threading
import time
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping, Sequence

from .dec import ZERO, D, fmt, usd
from .ledger import Position, edge_text, signed
from .store import fill_text, utc_text
from .types import Balance

__all__ = [
    'Notifier', 'TelegramAPI', 'TelegramError', 'split_text', 'event_key', 'fill_text',
    'started_text', 'stopped_text', 'startup_sizes_text', 'state_text', 'resumed_text', 'unknown_text',
    'reject_text', 'cancel_text', 'terminal_text', 'order_text', 'position_text', 'summary_text',
]

LOG = logging.getLogger(__name__)

API_BASE = 'https://api.telegram.org'
CHUNK_LIMIT = 3900
METHODS = frozenset({'getMe', 'sendMessage'})
LOOP_SECONDS = 0.2
FLUSH_SECONDS = 15.0
MAX_RETRY_AFTER = 3600.0
SUMMARY_META = 'summary_bucket'
_COUNTER = itertools.count(1)


# ----------------------------------------------------------------------------- Telegram API

class TelegramError(RuntimeError):
    """A Telegram failure described only by a safe code (never a URL or the token)."""

    def __init__(self, code: str, retry_after: float = 0.0):
        super().__init__(code)
        self.code = code
        self.retry_after = float(retry_after)


def _retry_after(data: Any) -> float:
    try:
        value = float(data['parameters']['retry_after'])
    except (KeyError, TypeError, ValueError):
        return 0.0
    if not math.isfinite(value) or value < 0:
        return 0.0
    return min(value, MAX_RETRY_AFTER)


def _utf16_units(text: str) -> int:
    return len(text.encode('utf-16-le')) // 2


def split_text(text: str, limit: int = CHUNK_LIMIT) -> list[str]:
    """Chunks of at most ``limit`` UTF-16 units whose concatenation is exactly ``text``;
    a chunk ends after a line break when one exists in its second half."""
    if limit < 2:
        raise ValueError('Chunk limit too small')
    if _utf16_units(text) <= limit:
        return [text]
    chunks: list[str] = []
    start, size = 0, len(text)
    while start < size:
        units, end = 0, start
        while end < size:
            width = 2 if ord(text[end]) > 0xFFFF else 1
            if units + width > limit:
                break
            units += width
            end += 1
        if end < size:
            cut = text.rfind('\n', start, end)
            if cut >= start + (end - start) // 2:
                end = cut + 1
        chunks.append(text[start:end])
        start = end
    return chunks


class TelegramAPI:
    """Minimal Bot API client (``getMe``, ``sendMessage``). ``repr`` never shows secrets."""

    def __init__(self, token: str | None, chat_id: str | None, *, timeout: float, session: Any = None):
        self._token = (token or '').strip()
        self.chat_id = (chat_id or '').strip()
        self.timeout = float(timeout)
        self._session = session
        self._own_session = session is None

    def __repr__(self) -> str:
        return (f'TelegramAPI(token={"set" if self._token else None}, '
                f'chat={"set" if self.chat_id else None}, timeout={self.timeout})')

    __str__ = __repr__

    def has_credentials(self) -> bool:
        token = self._token
        return bool(self.chat_id) and bool(token) and ':' in token and not any(
            c.isspace() or c in '/?#' for c in token)

    def _http(self) -> Any:
        if self._session is None:
            import requests  # local import: tests and disabled mode never need it
            self._session = requests.Session()
        return self._session

    def call(self, method: str, **fields: Any) -> Any:
        if method not in METHODS:
            raise ValueError('Unsupported Telegram method')
        token = self._token
        if not token or ':' not in token or any(c.isspace() or c in '/?#' for c in token):
            raise TelegramError('MISSING_TELEGRAM_BOT_TOKEN')
        failure: str | None = None
        response: Any = None
        try:
            response = self._http().post(f'{API_BASE}/bot{token}/{method}', json=fields, timeout=self.timeout)
            status = int(response.status_code)
        except Exception as exc:  # requests exceptions may embed the URL with the token
            failure = type(exc).__name__
        if failure is not None:
            raise TelegramError(failure)
        try:
            data = response.json()
        except Exception:
            data = None
        if status == 200 and isinstance(data, dict) and data.get('ok') is True:
            return data.get('result')
        code = 'TELEGRAM_BAD_REPLY' if status == 200 else f'TELEGRAM_HTTP_{status}'
        raise TelegramError(code, _retry_after(data))

    def send_message(self, text: str) -> None:
        """One ``sendMessage`` (the caller chunks long texts)."""
        if not self.chat_id:
            raise TelegramError('MISSING_TELEGRAM_CHAT_ID')
        self.call('sendMessage', chat_id=self.chat_id, text=text, link_preview_options={'is_disabled': True})

    def close(self) -> None:
        if self._own_session and self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None


def _error_code(exc: BaseException) -> str:
    return exc.code if isinstance(exc, TelegramError) else type(exc).__name__


def event_key(kind: str, ident: Any = None) -> str:
    """Outbox key ``kind:ident``; without ``ident`` a process-unique suffix is used
    (``time_ns`` plus a counter, so a frozen test clock still yields distinct keys)."""
    if not isinstance(kind, str) or not kind:
        raise ValueError('Event kind is required')
    if ident is None:
        ident = f'{time.time_ns()}-{next(_COUNTER)}'
    return f'{kind}:{ident}'


# ----------------------------------------------------------------------------- notifier

class Notifier:
    """Durable at-least-once Telegram delivery; never called synchronously by trading."""

    def __init__(self, cfg: Any, store: Any, *, session: Any = None, start_thread: bool = True):
        tg = cfg.telegram
        self.cfg, self.store = cfg, store
        self.enabled = bool(tg['enabled'])
        self.live = bool(cfg.live)
        self.max_unhealthy_seconds = float(tg['max_unhealthy_seconds'])
        self.healthcheck_seconds = float(tg['healthcheck_seconds'])
        self.send_interval = float(tg['send_interval_seconds'])
        self.summary_minutes = tg.get('summary_minutes')
        self.timeout = float(cfg.app['request_timeout_seconds'])
        secrets = cfg.secrets
        self.api: TelegramAPI | None = None
        if self.enabled:
            self.api = TelegramAPI(secrets.telegram_token, secrets.telegram_chat, timeout=self.timeout,
                                   session=session)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_good: float | None = None          # monotonic time of the last good contact
        self.chat_verified = False
        self.last_error: str | None = None
        self.last_sent_ms: int | None = None
        self._last_call = float('-inf')               # monotonic time of the last sendMessage attempt
        self._last_healthcheck = float('-inf')
        self._blocked_until: float | None = None      # 429 retry_after (monotonic)
        self._progress: tuple[str, int] | None = None # (event key, chunks delivered) in this process
        self.delivery_failed = False
        try:
            head = store.outbox_head()
            self.delivery_failed = bool(self.enabled and head and head['attempts'])
            if self.enabled and head and head.get('last_error'):
                self.last_error = str(head['last_error'])
            self.last_sent_ms = self._db_last_sent_ms()
        except Exception as exc:
            LOG.warning('Telegram outbox state unavailable: %s', type(exc).__name__)
        if start_thread:
            self.start()

    def __repr__(self) -> str:
        return f'Notifier(enabled={self.enabled}, live={self.live}, api={self.api!r})'

    # ----- queueing -----
    def _prefixed(self, text: str) -> str:
        if self.live or text.startswith(('[PAPER]', '[LIVE]')):
            return text
        return '[PAPER] ' + text

    def event(self, key: str, text: str, now_ms: int | None = None) -> bool:
        """Queue one event (idempotent by key). True if newly added. Never raises."""
        try:
            now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
            added = bool(self.store.outbox_add(key, self._prefixed(str(text)), now_ms))
            if added and not self.enabled:
                self.store.outbox_mark_skipped(key, now_ms)
            elif added:
                self._wake.set()
            return added
        except Exception as exc:
            LOG.warning('Telegram event not queued (%s): %s', key, type(exc).__name__)
            return False

    def summary_due(self, now_ms: int) -> int | None:
        """The summary bucket (``now_ms // period``) if a periodic summary is due."""
        minutes = self.summary_minutes
        if not minutes:
            return None
        bucket = int(now_ms) // (int(minutes) * 60_000)
        return None if self.store.meta(SUMMARY_META) == bucket else bucket

    def maybe_summary(self, now_ms: int, build_text: Callable[[], str]) -> bool:
        """Queue ``summary:<bucket>`` once per ``summary_minutes`` bucket (durable across
        restarts). ``build_text`` is called only when due. Never raises."""
        try:
            bucket = self.summary_due(now_ms)
            if bucket is None:
                return False
            text = build_text()
            self.store.set_meta(SUMMARY_META, bucket)
            return self.event(f'summary:{bucket}', text, now_ms)
        except Exception as exc:
            LOG.warning('Telegram summary skipped: %s', type(exc).__name__)
            return False

    # ----- delivery -----
    def healthy(self, now: float | None = None) -> bool:
        if not self.enabled:
            return True
        now = time.monotonic() if now is None else now
        return bool(self.chat_verified and self.last_good is not None
                    and 0 <= now - self.last_good <= self.max_unhealthy_seconds)

    def _blocked(self, now: float) -> bool:
        if self._blocked_until is None:
            return False
        if now >= self._blocked_until:
            self._blocked_until = None
            return False
        return True

    def _note_failure(self, exc: BaseException, what: str) -> tuple[str, float]:
        code = _error_code(exc)
        retry_after = exc.retry_after if isinstance(exc, TelegramError) else 0.0
        self.last_error = code
        if retry_after > 0:
            self._blocked_until = time.monotonic() + retry_after
        LOG.warning('Telegram %s failed (%s)', what, code)
        return code, retry_after

    def pump(self) -> None:
        """One delivery step: at most one HTTP call (a chunk or a healthcheck)."""
        with self._lock:
            now_ms = int(time.time() * 1000)
            if not self.enabled:
                self._skip_pending(now_ms)
                return
            now = time.monotonic()
            due = self.store.outbox_due(now_ms, limit=1)
            blocked = self._blocked(now)
            if due and not blocked and now - self._last_call >= self.send_interval:
                self._send(due[0], now, now_ms)
            elif not blocked and now - self._last_healthcheck >= self.healthcheck_seconds:
                self._healthcheck(now)

    def _skip_pending(self, now_ms: int) -> None:
        for row in self.store.outbox_rows(1000, pending_only=True):
            self.store.outbox_mark_skipped(row['key'], now_ms)

    def _send(self, row: Mapping[str, Any], now: float, now_ms: int) -> None:
        key = str(row['key'])
        chunks = split_text(f"{row['text']}\n[event {key}]")
        done = self._progress[1] if self._progress and self._progress[0] == key else 0
        done = min(done, len(chunks) - 1)
        self._last_call = now
        assert self.api is not None
        try:
            self.api.send_message(chunks[done])
        except Exception as exc:
            self.delivery_failed = True
            code, retry_after = self._note_failure(exc, 'delivery')
            self._progress = (key, done) if done else None
            self.store.outbox_mark_failed(key, now_ms, code, retry_after or None)
            return
        self.last_good = time.monotonic()
        self.chat_verified = True
        self.delivery_failed = False
        self.last_error = None
        done += 1
        if done < len(chunks):
            self._progress = (key, done)
            return
        self._progress = None
        self.store.outbox_mark_sent(key, now_ms)
        self.last_sent_ms = now_ms

    def _healthcheck(self, now: float) -> None:
        self._last_healthcheck = now
        assert self.api is not None
        try:
            self.api.call('getMe')
        except Exception as exc:
            # Keep last_good: healthy() expires it after max_unhealthy_seconds.
            self._note_failure(exc, 'healthcheck')
            return
        if not self.delivery_failed:
            # getMe proves the token, never a failed send or the chat permission.
            self.last_good = time.monotonic()
            self.last_error = None

    # ----- thread -----
    def _run(self) -> None:
        worker_error: str | None = None
        while not self._stop.is_set():
            try:
                self.pump()
                worker_error = None
            except Exception as exc:
                if self.enabled:
                    self.delivery_failed = True
                self.last_error = type(exc).__name__
                if worker_error != self.last_error:   # log once per error type, not every tick
                    LOG.error('Telegram worker cannot advance: %s', self.last_error)
                worker_error = self.last_error
            self._wake.wait(LOOP_SECONDS)
            self._wake.clear()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError('Notifier already started')
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name='gate-stonk-telegram', daemon=True)
        self._thread.start()

    def _stop_thread(self) -> bool:
        """Stop the worker; False if it is still inside a slow HTTP call."""
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=self.timeout + 2)
            if thread.is_alive():
                return False
            self._thread = None
        return True

    def flush(self, timeout: float = FLUSH_SECONDS) -> int:
        """Stop the worker and keep delivering queued events for at most ``timeout``
        seconds (exit path: stop/cancel/fill notices go out now, not at the next start).
        Gives up early without credentials or when the head event's backoff (or a 429
        block) ends after the deadline. Returns the number of events still queued."""
        if not self._stop_thread():
            return self._pending()
        if not self.enabled:
            try:
                self.pump()
            except Exception as exc:
                LOG.warning('Telegram flush stopped: %s', type(exc).__name__)
            return self._pending()
        timeout = max(0.0, float(timeout))
        # Real-time bound as well, so a frozen (test) monotonic clock cannot hang the exit.
        real_deadline = time.perf_counter() + timeout
        deadline = time.monotonic() + timeout
        while True:
            pending = self._pending()
            now = time.monotonic()
            remaining = min(deadline - now, real_deadline - time.perf_counter())
            if not pending or remaining <= 0:
                return pending
            if self.api is None or not self.api.has_credentials():
                return pending  # no credentials: nothing can be delivered
            waits = [0.0]
            if self._blocked_until is not None:
                waits.append(self._blocked_until - now)
            head = self._head()
            if head is not None:
                waits.append((int(head['next_ms']) - int(time.time() * 1000)) / 1000)
            if max(waits) > remaining:
                return pending
            try:
                self.pump()
            except Exception as exc:
                LOG.warning('Telegram flush stopped: %s', type(exc).__name__)
                return self._pending()
            pace = self._last_call + self.send_interval - time.monotonic()
            time.sleep(min(max(0.01, pace, max(waits)), max(0.01, remaining)))

    def close(self) -> None:
        self._stop_thread()
        if self.api is not None:
            self.api.close()

    # ----- status -----
    def _pending(self) -> int:
        try:
            return int(self.store.outbox_pending_count())
        except Exception:
            return -1

    def _head(self) -> dict | None:
        try:
            return self.store.outbox_head()
        except Exception:
            return None

    def _db_last_sent_ms(self) -> int | None:
        with self.store.read() as db:
            row = db.execute("SELECT MAX(sent_ms) FROM outbox WHERE status='sent'").fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def _skipped_count(self) -> int:
        with self.store.read() as db:
            return int(db.execute("SELECT COUNT(*) FROM outbox WHERE status='skipped'").fetchone()[0])

    def status(self) -> dict:
        """JSON-safe state for status.json (never raises)."""
        result: dict[str, Any] = {
            'enabled': self.enabled, 'healthy': self.healthy(), 'chat_verified': self.chat_verified,
            'delivery_failed': self.delivery_failed, 'delivery': 'at_least_once',
            'last_error_type': self.last_error, 'last_sent_ms': self.last_sent_ms,
            'pending': None, 'skipped': None, 'next_attempt_ms': None,
        }
        try:
            result['pending'] = int(self.store.outbox_pending_count())
            result['skipped'] = self._skipped_count()
            head = self.store.outbox_head()
            if head is not None and head['attempts']:
                result['next_attempt_ms'] = int(head['next_ms'])
        except Exception as exc:
            result['status_error'] = type(exc).__name__
        return result


# ----------------------------------------------------------------------------- message helpers (Russian)

def _slot(side: str, level: str) -> str:
    return f'{side.upper()} [{level}]'


def _clean(text: Any, limit: int = 200) -> str:
    return ' '.join(str(text).split())[:limit] if text is not None else ''


def _mode(live: bool) -> str:
    return '[LIVE]' if live else '[PAPER]'


def started_text(*, live: bool, pair: str, levels: Mapping[str, Sequence[str]] | None = None) -> str:
    parts = []
    for side in ('buy', 'sell'):
        ids = list((levels or {}).get(side, ()))
        if ids:
            parts.append(f"{side.upper()} {', '.join(ids)}")
    ladder = f", уровни {'; '.join(parts)}" if parts else ''
    return (f'{_mode(live)} gate_stonk: {pair}{ladder}. Проверка сохранённых заявок и баланса. '
            f'Доставка событий — минимум один раз.')


def stopped_text(reason: str, *, live: bool) -> str:
    tail = ('Сохранённые заявки отменяются; проверьте биржу. Неподтверждённые резервы и DMS сохраняются.'
            if live else 'Режим paper: реальных заявок на бирже нет; резервы сохранены в базе до следующего запуска.')
    return f'{_mode(live)} Процесс бота остановлен ({_clean(reason, 120) or "без причины"}). {tail}'


def startup_sizes_text(rows: Iterable[Mapping[str, Any]], *, base: str, quote: str) -> str:
    """Configured order sizes per level (before balance/limit caps).

    Row keys: ``side``, ``level`` and either ``qty``, ``price`` (+ optional ``index``,
    ``below_minimum``, ``minimum_text``) or ``error`` (estimate unavailable)."""
    lines = ['Размер заявок из конфига (до ограничений баланса и лимитов):']
    for row in rows:
        slot = _slot(row['side'], row['level'])
        if row.get('error'):
            lines.append(f"{slot}: оценка недоступна ({_clean(row['error'], 80)}); "
                         f'сообщу после восстановления котировок.')
            continue
        qty, price = D(row['qty']), D(row['price'])
        line = f'{slot}: {fmt(qty)} {base} × {fmt(price)} = {usd(qty * price)} {quote}'
        if row.get('index') is not None:
            line += f"; по индексу ≈ {usd(qty * D(row['index']))} {quote}"
        line += '.'
        if row.get('below_minimum'):
            line += f" МЕНЬШЕ МИНИМУМА БИРЖИ{(' (' + _clean(row['minimum_text'], 80) + ')') if row.get('minimum_text') else ''}."
        lines.append(line)
    return '\n'.join(lines)


REASON_HINTS_RU = {
    'STOP_TRADING': 'найден файл STOP_TRADING; удалите его, чтобы продолжить',
    'STOP_TRADING_FILE_UNREADABLE': 'не удаётся проверить файл STOP_TRADING',
    'STOP_BUY': 'найден файл STOP_BUY',
    'STOP_SELL': 'найден файл STOP_SELL',
    'STOP_ACCOUNT_REVIEW': 'обнаружена посторонняя активность на аккаунте; нужна ручная сверка',
    'TELEGRAM_UNAVAILABLE': 'Telegram недоступен дольше допустимого; заявки снимаются',
    'BAD_MARKET': 'индекс или стакан недоступен; заявки снимаются',
    'CLOCK_SKEW': 'часы компьютера расходятся со временем биржи',
    'GATE_AUTH': 'ошибка авторизации Gate (ключ, подпись, IP или права)',
    'DMS_OR_ACTION_UNKNOWN': 'результат действия на бирже неизвестен; заявки снимаются',
    'ACCOUNT_READ_FAILED': 'не удаётся прочитать состояние аккаунта',
    'ACCOUNT_MODE_MISMATCH': 'режим аккаунта Gate не совпадает с конфигом',
    'ICEBERG_POC_NOT_VERIFIED': 'iceberg вместе с post-only не проверен',
    'RECOVERY': 'восстановление после перезапуска: сохранённые заявки снимаются',
    'FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT': 'на аккаунте посторонняя заявка; нужен отдельный аккаунт',
    'CANCEL_ONLY': 'режим только отмены',
}


def _hint(reason: str) -> str:
    code = reason.split(':', 1)[0]
    return REASON_HINTS_RU.get(reason) or REASON_HINTS_RU.get(code) or ''


def state_text(reason: str, *, pair: str) -> str:
    reason = _clean(reason, 160)
    hint = _hint(reason)
    return f'{pair}: состояние {reason}' + (f' — {hint}.' if hint else '.')


def resumed_text(reason: str, *, pair: str) -> str:
    return f'{pair}: причина остановки устранена ({_clean(reason, 160)}); торговля возобновляется.'


def unknown_text(*, side: str, level: str, qty, price, base: str, quote: str, text: str | None = None) -> str:
    qty, price = D(qty), D(price)
    return (f'{base} {_slot(side, level)}: результат выставления неизвестен. {fmt(qty)} {base} по {fmt(price)} '
            f'{quote}, резерв сохранён; повтор запрещён до сверки.' + (f' text: {text}.' if text else ''))


def reject_text(*, side: str, level: str, label: str | None, message: str | None, qty, price, base: str,
                quote: str, retry_seconds: float | None = None) -> str:
    qty, price = D(qty), D(price)
    detail = _clean(label, 60) or 'без метки'
    if message:
        detail += ': ' + _clean(message, 200)
    retry = (f'Повтор этого уровня не раньше чем через {fmt(D(str(round(float(retry_seconds), 1))))} с.'
             if retry_seconds else 'Повтор на следующем шаге.')
    return (f'{base} {_slot(side, level)}: заявка отклонена ({detail}). Размер {fmt(qty)} {base}, '
            f'цена {fmt(price)}, сумма {usd(qty * price)} {quote}. {retry}')


def cancel_text(*, side: str, level: str, reason: str, result: str, base: str) -> str:
    return (f'{base} {_slot(side, level)}: отмена {_clean(reason, 120)}; ответ {_clean(result, 60)}. '
            f'Резерв сохраняется до сверки.')


def terminal_text(*, side: str, level: str, status: str, executed, qty, base: str, reason: str | None = None,
                  finish_as: str | None = None) -> str:
    state = _clean(status, 40) + (f' ({_clean(finish_as, 40)})' if finish_as else '')
    return (f'{base} {_slot(side, level)}: {state}; исполнено {fmt(D(executed))}/{fmt(D(qty))} {base}; '
            f'причина {_clean(reason, 120) or "биржа"}. Резерв освобождён после сверки.')


def order_text(*, side: str, level: str, qty, price, center, lower, upper, rule: str, base: str, quote: str,
               suppressed: int = 0) -> str:
    qty, price, center = D(qty), D(price), D(center)
    lines = [f'{base} {_slot(side, level)}: новая заявка {fmt(qty)} {base} по {fmt(price)} = {usd(qty * price)} {quote}.',
             f'Центр индекса {fmt(center, 8)}; цена {edge_text(side, price, center, "центра")}; '
             f'полоса {fmt(D(lower))}–{fmt(D(upper))}; правило {_clean(rule, 80)}.']
    if suppressed:
        lines.append(f'Перестановок без отдельного сообщения с прошлого уведомления: {int(suppressed)}.')
    return '\n'.join(lines)


def _balance(balances: Mapping[str, Balance], currency: str) -> Balance:
    return balances.get(currency) or Balance(ZERO, ZERO)


def _position_parts(position: Position, index: Decimal | None, quote: str,
                    initial_avg_price: Decimal | None = None) -> list[str]:
    parts = []
    avg = position.avg_cost
    if avg is not None:
        part = f'куплено ботом {fmt(position.qty)} по средней {fmt(avg, 8)}'
        if index is not None:
            part += f', нереализовано {signed(position.qty * index - position.cost)} {quote}'
        parts.append(part)
    if position.realized or position.qty:
        parts.append(f'реализовано {signed(position.realized)} {quote}')
    if position.unmatched_qty:
        total = position.realized_with(initial_avg_price)
        parts.append(f'продано из прежнего запаса {fmt(position.unmatched_qty)}'
                     + (f' (с учётом initial_avg_price всего {signed(total)} {quote})'
                        if initial_avg_price is not None and total is not None else ' (себестоимость не учтена)'))
    return parts


def position_text(*, now_ms: int, base: str, quote: str, balances: Mapping[str, Balance], position: Position,
                  index: Decimal | None, initial_avg_price: Decimal | None = None) -> str:
    """Position snapshot owed after a complete reconciliation that followed fills."""
    cash, held = _balance(balances, quote), _balance(balances, base)
    lines = [f'После сверки исполнений ({utc_text(now_ms)}): {quote} всего {usd(cash.total)}, '
             f'свободно {usd(cash.free)}.']
    line = f'{base}: остаток {fmt(held.total)}, свободно {fmt(held.free)}'
    line += (f'; оценка по индексу {usd(held.total * index)} {quote}' if index is not None
             else '; оценка по индексу недоступна')
    parts = _position_parts(position, index, quote, initial_avg_price)
    lines.append(line + ('; ' + '; '.join(parts) if parts else '') + '.')
    return '\n'.join(lines)


def summary_text(*, now_ms: int, pair: str, base: str, quote: str, state: str | None,
                 balances: Mapping[str, Balance], position: Position, index: Decimal | None,
                 orders: Sequence[Mapping[str, Any]] = (), slot_states: Mapping[str, str] | None = None,
                 day_buy: tuple[Decimal, Decimal] | None = None, day_sell: tuple[Decimal, Decimal] | None = None,
                 initial_avg_price: Decimal | None = None) -> str:
    """Periodic summary: balances, inventory value, PnL, active orders, waiting slots.

    ``orders`` are active store rows (``side, level, qty, price, filled_qty``);
    ``slot_states`` maps ``side:level`` to the engine's wait reason; ``day_*`` are
    ``store.totals(side, utc_day_start_ms(now))`` = (qty, notional)."""
    cash, held = _balance(balances, quote), _balance(balances, base)

    def remaining(o: Mapping[str, Any]) -> Decimal:
        return max(ZERO, D(o['qty']) - D(o.get('filled_qty') or ZERO))

    buy_reserved = sum((remaining(o) * D(o['price']) for o in orders if o['side'] == 'buy'), ZERO)
    lines = [f'Сводка {pair} на {utc_text(now_ms)}. Состояние: {_clean(state, 120) or "н/д"}.',
             f'{quote}: всего {usd(cash.total)}, свободно {usd(cash.free)}, в заявках BUY {usd(buy_reserved)}.']
    token_value = ZERO
    first = f'{base}: {fmt(held.total)} {base}'
    if index is not None:
        token_value = held.total * index
        first += f' ≈ {usd(token_value)} {quote} (индекс {fmt(index, 8)})'
    elif held.total:
        first += ' (индекс недоступен)'
    parts = [first] + _position_parts(position, index, quote, initial_avg_price)
    lines.append('; '.join(parts) + '.')
    if orders:
        legs = [f"{_slot(o['side'], o['level'])} {fmt(remaining(o))} по {fmt(D(o['price']))} "
                f"({usd(remaining(o) * D(o['price']))} {quote})"
                for o in sorted(orders, key=lambda o: (o['side'], o['level']))]
        lines.append('Заявки: ' + ', '.join(legs) + '.')
    else:
        lines.append('Активных заявок нет.')
    active = {f"{o['side']}:{o['level']}" for o in orders}
    waits = [f'{slot} — {_clean(reason, 80)}' for slot, reason in sorted((slot_states or {}).items())
             if slot not in active and reason]
    if waits:
        lines.append('Ожидание: ' + ', '.join(waits) + '.')
    unrealized = position.qty * index - position.cost if index is not None and position.qty else ZERO
    lines.append(f'Портфель ≈ {usd(cash.total + token_value)} {quote}'
                 + ('' if index is not None or not held.total else f' (без {base}: индекс недоступен)') + '.')
    lines.append(f'Реализовано ботом {signed(position.realized)} {quote}; '
                 f'нереализовано по купленному ботом {signed(unrealized)} {quote}.')
    if day_buy is not None or day_sell is not None:
        buy_notional = day_buy[1] if day_buy else ZERO
        sell_notional = day_sell[1] if day_sell else ZERO
        lines.append(f'Сегодня (UTC): куплено на {usd(buy_notional)} {quote}, продано на {usd(sell_notional)} {quote}.')
    return '\n'.join(lines)
