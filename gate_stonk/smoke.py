"""``smoke-test``: the stage-4 live smoke test as ONE guarded command (README «Шаг 5», API_NOTES §9.2).

``python -m gate_stonk smoke-test --config config.live.yaml --live [--edge-pct 10] [--max-notional 10]
[--index-seconds 30] [--dms-fire] [--probe-exptime]``

One minimal post-only BUY far from the market (price <= index -10 %), checked by reading,
cancelled, checked again, with the dead man's switch armed the whole time; the run also records
the venue facts still marked «НЕ ПОДТВЕРЖДЕНО» in API_NOTES (create reply fields, finish_as,
lookup by ``text`` before/after finish, ``countdown(0)`` reply, expired ``x-gate-exptime`` label,
DMS behaviour).

Safety contract (tests/test_smoke.py):

* the same local live gates as ``run`` in the same order (Telegram is not required), then the
  bot's process locks for the whole run (a running bot makes the smoke test refuse);
* read-only prechecks before ANY write: clock skew vs ``/spot/time``, pair tradable for BUY,
  account mode and key whitelist, no open order on the pair (``SMOKE_PAIR_NOT_EMPTY``) nor on
  other pairs (``SMOKE_ACCOUNT_NOT_EMPTY``), a valid index and Gate spot book, free USDT covering
  the order. Any failure is a refusal (exit 2): nothing is written;
* price = ``floor_price(min(index, center) * (1 - edge/100))`` with edge >= 10 (the centre carries
  ``index.basis_offset_pct``; the lower of the two keeps the BUY <= index -10 %), re-checked
  against a fresh index and book right before each create (price <= index * 0.9 and
  <= center * 0.9, below the best ask and bid and at least ``MIN_BID_GAP_PCT`` below the best
  bid); quantity = the pair minimum + 2 % rounded UP to the lot (5 USDT when the quote minimum
  is unknown); notional <= ``--max-notional``; ``poc``, no iceberg, ``text`` from ``make_text``;
* ``countdown_cancel_all`` is armed BEFORE the create; at most one order (a second one, after the
  first is verified gone, only with ``--dms-fire``); only orders carrying our ``text`` or order id
  are ever cancelled;
* cleanup in ``finally`` (also on Ctrl+C and, with ``deps.signals``, on SIGTERM / SIGBREAK —
  Ctrl+Break or a closed console on Windows): every smoke order that may still be open is
  cancelled by id (by ``text`` when no id is known) and counts as gone only when a direct read
  of it (``get_order``) says so; the DMS is disarmed only when the pair is verified empty (two
  empty open-orders reads ``safety.terminal_settle_seconds`` apart), otherwise it stays armed
  (Gate then cancels the pair's orders at triggerTime).

Every HTTP exchange is recorded by :class:`RecordingSession` (method, endpoint, query, request
body, HTTP status, label, rate-limit headers, latency, sanitized reply — never request headers,
keys or signatures) into ``<state_dir>/smoke/smoke_<utc>.json``; every JSON reply is also saved
as a candidate fixture under ``<state_dir>/smoke/fixtures/<utc>/`` (see the README written there).

Exit codes: 0 every mandatory step passed and the pair is verified empty; 2 refusal while no
order was ever sent (prechecks, the re-check right before the first create); 1 failure — also a
requested ``--dms-fire`` whose second order was not sent because the re-check failed after the
main flow (the summary keeps the main-flow result).
"""
from __future__ import annotations

import json
import logging
import math
import re
import signal
import threading
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl, urlsplit

from . import __version__
from .dec import ONE, ceil_to, fmt, usd
from .doctor import safe_error
from .errors import NotSent, Rejected, UnknownResult, Unsafe
from .gate_rest import AUTH_LABELS, EXPTIME_HEADER, endpoint_group
from .pricing import floor_price
from .types import Book, CancelRequest, IndexSnapshot, PairMeta, PlaceRequest, VenueOrder
from .venue import POST_ONLY_LABEL, is_post_only_rejection, make_text, side_allowed

LOG = logging.getLogger(__name__)

EXIT_PASS, EXIT_FAIL, EXIT_REFUSED = 0, 1, 2
LEVEL_ID = 'smk'                         # text: t-<client_prefix>-smk-<base36 ms><n>
MIN_EDGE_PCT = Decimal(10)               # price <= index - 10 % (customer requirement)
MAX_PRICE_FRACTION = ONE - MIN_EDGE_PCT / 100
MIN_BID_GAP_PCT = Decimal(5)             # and at least 5 % below the best spot bid ("far from the market")
DEFAULT_MAX_NOTIONAL = Decimal(10)
MAX_NOTIONAL_LIMIT = Decimal(100)        # hard upper bound of --max-notional (typo guard)
FALLBACK_QUOTE_MIN = Decimal(5)          # USDT when the pair's min_quote_amount is null/0
QTY_MARGIN = Decimal('1.02')             # +2 % over the pair minimums before rounding UP
DEFAULT_INDEX_SECONDS = 30.0
INDEX_POLL_SECONDS = 0.5
POLL_SECONDS = 1.0
CANCEL_VERIFY_SECONDS = 15.0
UNKNOWN_POLL_SECONDS = 2.0
CLEANUP_SECONDS = 30.0
CLEANUP_CANCEL_EVERY_SECONDS = 5.0       # re-send the cleanup cancel at most this often
DMS_FIRE_TIMEOUT = 10                    # countdown of the --dms-fire order (s)
DMS_FIRE_WAIT_SECONDS = 25.0
DMS_FIRE_POLL_SECONDS = 2.0
FINISHED_WINDOW_MARGIN_MS = 120_000
PROBE_ORDER_ID = '1'                     # --probe-exptime: cancel of a nonexistent order id
PROBE_EXPIRED_AGE_MS = 60_000            # x-gate-exptime one minute in the past
FINAL_STATES = frozenset({'finished', 'rejected', 'not_sent', 'not_found'})
BASE_NAMES = {'index': 'индекса', 'center': 'центра'}
STATUS_MARK = {'PASS': 'OK', 'FAIL': 'ОШИБКА', 'INFO': 'ИНФО', 'SKIP': 'ПРОПУЩЕН', 'REFUSED': 'ОТКАЗ',
               'RUNNING': 'ОШИБКА'}

MASK = '***'
_SENSITIVE_KEY_RE = re.compile(r'(?i)^(?:(?:.*_)?(?:user_?id|uid)|email|phone|mobile|ip|ips|ip_whitelist|ip_list)$')
_HEX_RUN_RE = re.compile(r'[0-9a-fA-F]{32,}')
_RATE_HEADER_RE = re.compile(r'(?i)^(x-gate-ratelimit-.*|x-ratelimit-.*|retry-after)$')
_SLUG_RE = re.compile(r'[^A-Za-z0-9]+')


# ============================================================================ sanitizing / recording

def sanitize(value: Any, secrets: tuple[str, ...] | list[str] = ()) -> Any:
    """JSON-safe deep copy with user-id-like fields and IP lists masked, secret values and long
    hex runs (signatures) replaced. Decimals become plain strings."""
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            name = str(key)
            if _SENSITIVE_KEY_RE.match(name):
                out[name] = [MASK] * len(item) if isinstance(item, (list, tuple)) else (None if item is None else MASK)
            else:
                out[name] = sanitize(item, secrets)
        return out
    if isinstance(value, (list, tuple)):
        return [sanitize(item, secrets) for item in value]
    if isinstance(value, str):
        for secret in secrets:
            if secret and secret in value:
                value = value.replace(secret, MASK)
        return _HEX_RUN_RE.sub('[REDACTED]', value)
    if isinstance(value, Decimal):
        return format(value, 'f') if value.is_finite() else str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return sanitize(str(value), secrets)


def _parse_json(content: Any) -> tuple[Any, bool]:
    try:
        if isinstance(content, (bytes, bytearray)):
            content = bytes(content).decode('utf-8')
        if not isinstance(content, str):
            return None, False
        return json.loads(content), True
    except Exception:
        return None, False


class RecordingSession:
    """``requests.Session`` stand-in used ONLY by the smoke test: forwards each request to the
    real session and records the exchange, sanitized. Request headers (KEY, SIGN, Timestamp)
    are never stored: only whether the request was signed and the ``x-gate-exptime`` offset."""

    def __init__(self, inner: Any = None, *, secrets: tuple[str, ...] | list[str] = ()) -> None:
        self._inner = inner
        self._owns = inner is None
        self._secrets = tuple(s for s in secrets if s)
        self.records: list[dict] = []
        self.step = 'setup'

    @property
    def inner(self) -> Any:
        if self._inner is None:
            import requests   # local import: tests inject a fake session
            self._inner = requests.Session()
        return self._inner

    def request(self, method: str, url: str, data: Any = None, headers: Any = None, timeout: Any = None,
                allow_redirects: bool = True) -> Any:
        parts = urlsplit(url)
        path = parts.path[len('/api/v4'):] if parts.path.startswith('/api/v4') else parts.path
        sent_ms = int(time.time() * 1000)
        headers = dict(headers or {})
        body, parsed = _parse_json(data) if data is not None else (None, False)
        record: dict[str, Any] = {
            'seq': len(self.records) + 1, 'step': self.step, 'method': str(method).upper(), 'path': path,
            'endpoint': endpoint_group(path), 'query': sanitize(dict(parse_qsl(parts.query)), self._secrets),
            'request_body': sanitize(body, self._secrets) if parsed else None, 'signed': 'SIGN' in headers,
            'exptime_offset_ms': None, 'sent_at_ms': sent_ms, 'http_status': None, 'label': None,
            'message': None, 'rate_limit_headers': {}, 'latency_ms': None, 'transport_error': None,
            'reply': None, 'reply_text': None,
        }
        exptime = headers.get(EXPTIME_HEADER)
        if isinstance(exptime, str) and exptime.isdigit():
            record['exptime_offset_ms'] = int(exptime) - sent_ms
        started = time.monotonic()
        try:
            response = self.inner.request(method, url, data=data, headers=headers, timeout=timeout,
                                          allow_redirects=allow_redirects)
        except BaseException as exc:
            record['latency_ms'] = round((time.monotonic() - started) * 1000, 1)
            record['transport_error'] = type(exc).__name__
            self.records.append(record)
            raise
        record['latency_ms'] = round((time.monotonic() - started) * 1000, 1)
        try:
            status = getattr(response, 'status_code', None)
            record['http_status'] = status if isinstance(status, int) and not isinstance(status, bool) else None
            rate: dict[str, str] = {}
            for name, value in dict(getattr(response, 'headers', None) or {}).items():
                if _RATE_HEADER_RE.match(str(name)):
                    rate[str(name).lower()] = str(value)[:64]
            record['rate_limit_headers'] = rate
            content = getattr(response, 'content', None)
            if content is None:
                content = getattr(response, 'text', None)
            reply, ok = _parse_json(content)
            if ok:
                record['reply'] = sanitize(reply, self._secrets)
                if isinstance(reply, Mapping) and not (isinstance(status, int) and 200 <= status < 300):
                    label, message = reply.get('label'), reply.get('message')
                    record['label'] = label if isinstance(label, str) else None
                    record['message'] = sanitize(message, self._secrets)[:200] if isinstance(message, str) else None
            elif content:
                text = content.decode('utf-8', 'replace') if isinstance(content, (bytes, bytearray)) else str(content)
                record['reply_text'] = sanitize(' '.join(text.split())[:300], self._secrets)
        except Exception as exc:     # recording never changes the request's outcome
            record['transport_error'] = f'RECORDING_FAILED:{type(exc).__name__}'
        self.records.append(record)
        return response

    def get(self, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Any:
        return self.inner.get(url, params=params, headers=headers, timeout=timeout)

    def close(self) -> None:
        inner, self._inner = self._inner, None
        if inner is not None and self._owns:
            try:
                inner.close()
            except Exception:
                pass


# ============================================================================ options / pure helpers

@dataclass(frozen=True)
class SmokeOptions:
    edge_pct: Decimal = MIN_EDGE_PCT
    max_notional: Decimal = DEFAULT_MAX_NOTIONAL
    index_seconds: float = DEFAULT_INDEX_SECONDS
    dms_fire: bool = False
    probe_exptime: bool = False

    def as_dict(self) -> dict:
        return sanitize(asdict(self))


def option_refusal(options: SmokeOptions) -> tuple[str, str] | None:
    """Local argument checks (before the live gates)."""
    edge, cap, seconds = options.edge_pct, options.max_notional, options.index_seconds
    if not isinstance(edge, Decimal) or not edge.is_finite() or edge < MIN_EDGE_PCT:
        return ('SMOKE_EDGE_TOO_SMALL', f'--edge-pct должен быть не меньше {fmt(MIN_EDGE_PCT)}: '
                                        'заявка ставится не выше индекс −10%.')
    if edge >= 100:
        return ('SMOKE_EDGE_INVALID', '--edge-pct должен быть меньше 100.')
    if not isinstance(cap, Decimal) or not cap.is_finite() or not 0 < cap <= MAX_NOTIONAL_LIMIT:
        return ('SMOKE_MAX_NOTIONAL_INVALID', f'--max-notional должен быть в (0, {fmt(MAX_NOTIONAL_LIMIT)}] USDT.')
    if not (isinstance(seconds, (int, float)) and math.isfinite(seconds) and 1 <= seconds <= 600):
        return ('SMOKE_INDEX_SECONDS_INVALID', '--index-seconds должно быть 1..600.')
    return None


def price_base(center: Decimal, index: Decimal | None) -> tuple[str, Decimal]:
    """The base the BUY price is measured from: the LOWER of the index and the centre.

    The centre is ``index * (1 + basis_offset_pct/100)``; with a positive basis offset a price
    from the centre alone could sit closer than −10 % to the index (or even above it), which
    breaks the customer rule «BUY не выше индекс −10%». Returns ``('index' | 'center', value)``.
    """
    if not isinstance(index, Decimal) or not index.is_finite() or index <= 0:
        raise ValueError('index must be positive')
    return ('index', index) if index <= center else ('center', center)


def smoke_price(base: Decimal, edge_pct: Decimal, meta: PairMeta) -> Decimal:
    """``floor_price(base * (1 - edge/100))`` on the pair tick (BUY: rounding only lowers it);
    ``base`` is :func:`price_base` = min(centre, index)."""
    return floor_price(base * (ONE - edge_pct / 100), meta)


def smoke_quantity(meta: PairMeta, price: Decimal) -> Decimal:
    """Smallest quantity meeting ``min_base_amount`` and ``min_quote_amount`` with a 2 % margin,
    rounded UP to the lot; an unknown (null/0) quote minimum counts as 5 USDT."""
    if price <= 0:
        raise ValueError('price must be positive')
    needs = [FALLBACK_QUOTE_MIN / price if not meta.min_quote_amount else meta.min_quote_amount * QTY_MARGIN / price]
    if meta.min_base_amount:
        needs.append(meta.min_base_amount * QTY_MARGIN)
    qty = ceil_to(max(needs), meta.amount_precision)
    return qty if qty > 0 else meta.lot


def price_guard(price: Decimal, center: Decimal, index: Decimal | None, book: Book) -> str | None:
    """None when the BUY price is far enough from the index AND from the market: <= index * 0.9,
    <= center * 0.9, below the best ask and bid and at least ``MIN_BID_GAP_PCT`` below the best bid."""
    if price <= 0:
        return 'PRICE_NOT_POSITIVE'
    if not isinstance(index, Decimal) or not index.is_finite() or index <= 0 or price > index * MAX_PRICE_FRACTION:
        return 'PRICE_ABOVE_INDEX_MINUS_10_PCT'
    if price > center * MAX_PRICE_FRACTION:
        return 'PRICE_ABOVE_CENTER_MINUS_10_PCT'
    if price >= book.ask:
        return 'PRICE_NOT_BELOW_BEST_ASK'
    if price >= book.bid:
        return 'PRICE_NOT_BELOW_BEST_BID'
    if price > book.bid * (ONE - MIN_BID_GAP_PCT / 100):
        return 'PRICE_TOO_CLOSE_TO_BEST_BID'
    return None


def _utc_text(ms: int | float | None) -> str:
    if ms is None:
        return 'н/д'
    return datetime.fromtimestamp(float(ms) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def _utc_stamp(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def _now_ms() -> int:
    return int(time.time() * 1000)


class SmokeRefused(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code)
        self.code = code
        self.message = message


class _Abort(Exception):
    """A mandatory step failed: stop the flow (cleanup still runs)."""


@dataclass(frozen=True)
class SmokePlan:
    text: str
    price: Decimal
    qty: Decimal
    notional: Decimal
    center: Decimal
    index: Decimal | None
    edge_pct: Decimal
    actual_edge_pct: Decimal                 # 1 - price / index, % (the customer rule: >= 10)
    bid: Decimal
    ask: Decimal
    price_base: str = 'index'                # which of min(index, centre) the price was taken from
    edge_vs_center_pct: Decimal = Decimal(0)
    bid_gap_pct: Decimal = Decimal(0)        # 1 - price / best bid, % (>= MIN_BID_GAP_PCT)


# ============================================================================ the test

class SmokeTest:
    """One smoke-test run over a live venue (``GateVenue`` over a :class:`RecordingSession`)."""

    def __init__(self, cfg: Any, venue: Any, recorder: RecordingSession | None, hub_factory: Callable[[Any], Any], *,
                 options: SmokeOptions, out: Callable[[str], None], err: Callable[[str], None] | None = None,
                 sleep: Callable[[float], None] | None = None, signals: bool = False) -> None:
        self.cfg = cfg
        self.signals = signals                   # install SIGTERM/SIGBREAK handlers for the run (deps.signals)
        self._old_signals: dict[int, Any] = {}
        self._stop_exc: type[BaseException] | None = None
        self._in_cleanup = False
        self.stop_signal: str | None = None
        self.interrupt_reason: str | None = None     # 'Ctrl+C' or the signal name that stopped the flow
        self.venue = venue
        self.rec = recorder if recorder is not None else RecordingSession()
        self.hub_factory = hub_factory
        self.opt = options
        self.say = out
        self.err = err or out
        self.sleep = sleep or time.sleep
        self.pair = cfg.currency_pair
        self.account = cfg.market['account']
        self.prefix = cfg.app['client_prefix']
        self.dms_timeout = int(cfg.safety['dms_timeout_seconds'])
        self.unknown_window_ms = int((float(cfg.app['request_expiry_seconds'])
                                      + float(cfg.safety['unknown_create_margin_seconds'])) * 1000)
        self.started_ms = _now_ms()
        self.steps: list[dict] = []
        self.prechecks: dict[str, Any] = {}
        self.facts: dict[str, Any] = {
            'post_only_rejection_path': 'не наблюдался',
            'create_reply': None, 'amend_text_default': None, 'finish_as_values': {},
            'get_order_by_text_while_open': None, 'get_order_by_text_after_finish': None,
            'finished_orders_window_contains_order': None, 'cancel_batch_item': None,
            'countdown_reply': [], 'countdown_zero_reply': [], 'exptime_expired': {'probed': False},
            'dms_fire': {'tested': False}, 'rate_limit_headers': None, 'balances_reply': None,
            'account': None, 'clock': None,
        }
        self.plan: SmokePlan | None = None
        self.meta: PairMeta | None = None
        self.hub: Any = None
        self.builder: Any = None
        self.orders: dict[str, dict] = {}
        self.counter = 0
        self.dms_armed = False
        self.dms_state = 'never_armed'
        self.dms_trigger_ms: int | None = None
        self.pair_verified_empty = False
        self._pair_empty_now = False             # last open-orders read of the flow returned []
        self.main_passed = False
        self.dms_fire_skipped: str | None = None     # --dms-fire requested but the second order was not sent
        self.refusal: dict | None = None
        self.fatal: str | None = None
        self.interrupted = False
        self.cleanup_failed = False
        self.exit_code: int | None = None
        self.finished_ms: int | None = None

    # ------------------------------------------------------------------ steps / output
    def _begin(self, name: str) -> dict:
        step = {'n': len(self.steps) + 1, 'name': name, 'title': name, 'status': 'RUNNING', 'mandatory': True,
                'detail': '', 'data': {}, 'started_ms': _now_ms()}
        self.steps.append(step)
        self.rec.step = f"{step['n']:02d}_{name}"
        return step

    def _end(self, step: dict, status: str, title: str, detail: str = '', *, mandatory: bool = True,
             data: Mapping | None = None) -> dict:
        step.update(status=status, title=title, detail=detail, mandatory=mandatory, finished_ms=_now_ms())
        if data:
            step['data'].update(sanitize(dict(data)))
        line = f"{step['n']:>2}. {title}" + (f': {detail}' if detail else '') + f' — {STATUS_MARK[status]}'
        (self.err if status in ('FAIL', 'REFUSED') else self.say)(line)
        log = LOG.warning if status in ('FAIL', 'REFUSED') else LOG.info
        log('SMOKE step %s %s: %s %s', step['n'], step['name'], status, detail)
        return step

    def _refuse(self, step: dict, title: str, code: str, message: str, data: Mapping | None = None) -> None:
        self._end(step, 'REFUSED', title, f'{code}: {message}', data=data)
        raise SmokeRefused(code, message)

    def _step_records(self, step: dict) -> list[dict]:
        key = f"{step['n']:02d}_{step['name']}"
        return [r for r in self.rec.records if r['step'] == key]

    def _last_record(self, step: dict, method: str | None = None, endpoint: str | None = None) -> dict | None:
        for record in reversed(self._step_records(step)):
            if (method is None or record['method'] == method) and (endpoint is None or record['endpoint'] == endpoint):
                return record
        return None

    # ------------------------------------------------------------------ market data
    def _sample(self) -> tuple[IndexSnapshot | None, Book | None, str | None]:
        hub = self.hub
        try:
            snapshots = {s.id: hub.source_snapshot(s.id) for s in self.cfg.sources()}
            fx = hub.fx_snapshot()
            book = hub.spot_book()
            book_error = hub.spot_book_error()
            snap = self.builder.compute(snapshots, fx, time.monotonic(), _now_ms())
        except Exception as exc:
            return None, None, f'INDEX_UNAVAILABLE:{type(exc).__name__}'
        if book is None and book_error is None:
            book_error = 'NO_DATA'
        if book is not None and book_error is None:
            try:
                book.check(time.monotonic(), _now_ms(), float(self.cfg.index['max_age_seconds']), None)
            except Unsafe as exc:
                book_error = str(exc)
        return snap, book, book_error

    @staticmethod
    def _sample_problem(snap: IndexSnapshot | None, book: Book | None, book_error: str | None) -> str | None:
        if snap is None:
            return book_error or 'INDEX_UNAVAILABLE'
        if (not snap.ok or snap.center is None or snap.center <= 0 or not isinstance(snap.index, Decimal)
                or not snap.index.is_finite() or snap.index <= 0):
            return f'INDEX:{snap.reason or "INVALID"}'
        if book is None or book_error is not None:
            return f'SPOT_BOOK:{book_error or "NO_DATA"}'
        return None

    def _next_text(self) -> str:
        text = make_text(self.prefix, LEVEL_ID, _now_ms(), self.counter)
        self.counter += 1
        return text

    # ------------------------------------------------------------------ run
    # ------------------------------------------------------------------ signals (SIGTERM / SIGBREAK)
    def _on_signal(self, signum: int, frame: Any) -> None:
        """Like ``Runner._on_signal``: record the stop and raise ``StopRequested`` for the FIRST
        signal only, and never once the cleanup has started (a repeated Ctrl+Break or a console
        close must not abort the cancel, its verification or the report)."""
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = str(signum)
        first = self.stop_signal is None
        if first:
            self.stop_signal = name
            LOG.warning('SMOKE stop signal %s (cleanup started: %s)', name, self._in_cleanup)
        if first and not self._in_cleanup and self._stop_exc is not None:
            raise self._stop_exc(name)

    def _install_signals(self) -> None:
        if not self.signals or threading.current_thread() is not threading.main_thread():
            return
        for name in ('SIGTERM', 'SIGBREAK'):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                self._old_signals[signum] = signal.signal(signum, self._on_signal)
            except (ValueError, OSError):
                pass

    def _restore_signals(self) -> None:
        for signum, handler in self._old_signals.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError, TypeError):
                pass
        self._old_signals.clear()

    # ------------------------------------------------------------------ run
    def run(self) -> int:
        from .cli import StopRequested      # local import: cli imports this module lazily

        self._stop_exc = StopRequested
        self._install_signals()
        try:
            try:
                self._prechecks()
                if self.opt.probe_exptime:
                    self._probe_exptime()
                self._main_flow()
                if self.opt.dms_fire:
                    self._dms_fire()
            except SmokeRefused as exc:
                self.refusal = {'code': exc.code, 'message': exc.message}
            except _Abort:
                pass
            except KeyboardInterrupt:
                self._in_cleanup = True
                self.interrupted = True
                self.interrupt_reason = 'Ctrl+C'
                self.err('Прервано (Ctrl+C): снимаю smoke-заявку, если она есть, и проверяю пару чтением.')
            except StopRequested as exc:
                self._in_cleanup = True
                self.interrupted = True
                self.interrupt_reason = str(exc) or 'SIGTERM'
                self.err(f'Остановлено сигналом {exc}: снимаю smoke-заявку, если она есть, и проверяю пару '
                         'чтением.')
            except Exception as exc:
                self.fatal = safe_error(exc)
                LOG.error('SMOKE aborted: %s', self.fatal, exc_info=not isinstance(exc, Unsafe))
                self.err(f'Ошибка smoke-теста: {self.fatal}')
            finally:
                self._in_cleanup = True     # from here on a SIGTERM/SIGBREAK is only recorded
                try:
                    self._cleanup()
                finally:
                    self._stop_hub()
        finally:
            self._restore_signals()
        self.finished_ms = _now_ms()
        self.exit_code = self._exit_code()
        return self.exit_code

    def _exit_code(self) -> int:
        for step in self.steps:
            if step['status'] == 'RUNNING':
                step.update(status='FAIL', detail=(step['detail'] or 'шаг не завершён'))
        # 2 only for a refusal while NO order was ever sent (prechecks, the first pre-create guard)
        if self.refusal is not None and not self.cleanup_failed and not self.orders:
            return EXIT_REFUSED
        mandatory_ok = all(s['status'] == 'PASS' for s in self.steps if s['mandatory'])
        if (mandatory_ok and self.refusal is None and self.fatal is None and not self.interrupted
                and self.pair_verified_empty and self.dms_state == 'disarmed' and self.main_passed):
            return EXIT_PASS
        return EXIT_FAIL

    def _stop_hub(self) -> None:
        if self.hub is not None:
            try:
                self.hub.stop()
            except Exception as exc:
                LOG.warning('smoke: feed hub stop failed: %s', type(exc).__name__)

    # ------------------------------------------------------------------ prechecks (read only)
    def _prechecks(self) -> None:
        cfg, venue = self.cfg, self.venue
        # clock
        step = self._begin('clock')
        title = 'Часы Gate (/spot/time)'
        try:
            t0 = time.time()
            server_ms = venue.server_time_ms()
            t1 = time.time()
        except Exception as exc:
            self._refuse(step, title, 'SMOKE_CLOCK_READ_FAILED', safe_error(exc))
        skew = server_ms - (t0 + t1) / 2 * 1000
        limit = int(cfg.safety['max_clock_skew_ms'])
        data = {'server_time_ms': server_ms, 'skew_ms': round(skew), 'max_clock_skew_ms': limit}
        self.prechecks['clock'] = data
        self.facts['clock'] = data
        if abs(skew) > limit:
            self._refuse(step, title, 'CLOCK_SKEW', f'расхождение часов {skew:+.0f} мс больше допуска {limit} мс',
                         data)
        self._end(step, 'PASS', title, f'расхождение {skew:+.0f} мс (допуск {limit} мс)', data=data)

        # pair metadata
        step = self._begin('pair_meta')
        title = f'Пара {self.pair}: метаданные и trade_status'
        try:
            meta = venue.pair_meta()
        except Exception as exc:
            self._refuse(step, title, 'PAIR_METADATA_UNAVAILABLE', safe_error(exc))
        self.meta = meta
        data = {'precision': meta.precision, 'amount_precision': meta.amount_precision,
                'min_base_amount': meta.min_base_amount, 'min_quote_amount': meta.min_quote_amount,
                'max_base_amount': meta.max_base_amount, 'max_quote_amount': meta.max_quote_amount,
                'trade_status': meta.trade_status}
        self.prechecks['pair_meta'] = sanitize(data)
        if not side_allowed(meta.trade_status, 'buy'):
            self._refuse(step, title, f'PAIR_NOT_TRADABLE:{meta.trade_status}', 'BUY на паре сейчас запрещён', data)
        self._end(step, 'PASS', title, f'trade_status {meta.trade_status}, шаг цены {fmt(meta.tick)}, лот '
                                       f'{fmt(meta.lot)}, минимумы {fmt(meta.min_base_amount)} {meta.base} / '
                                       f'{fmt(meta.min_quote_amount)} {meta.quote}', data=data)

        # account mode / key whitelist
        step = self._begin('account_check')
        title = 'Аккаунт: режим и белый список пар ключа'
        try:
            info = venue.account_check()
        except Rejected as exc:
            code = f'GATE_AUTH:{exc.label}' if exc.label in AUTH_LABELS else f'ACCOUNT_CHECK_FAILED:{exc.label}'
            self._refuse(step, title, code, safe_error(exc))
        except Exception as exc:
            self._refuse(step, title, 'ACCOUNT_CHECK_FAILED', safe_error(exc))
        info = info if isinstance(info, Mapping) else {'problems': ['INVALID_ACCOUNT_CHECK']}
        data = {key: info.get(key) for key in ('key_mode', 'unified_mode', 'unified_mode_error', 'detected_account',
                                               'configured_account', 'account_ok', 'pair_whitelist_ok', 'tier',
                                               'problems')}
        data['ip_whitelist_entries'] = len(info.get('ip_whitelist') or [])
        self.prechecks['account'] = sanitize(data)
        self.facts['account'] = self.prechecks['account']
        problems = list(info.get('problems') or [])
        if problems:
            self._refuse(step, title, str(problems[0]), f'market.account: {self.account}; проблемы: '
                                                        f"{', '.join(map(str, problems))}", data)
        self._end(step, 'PASS', title, f"режим {info.get('detected_account')} = market.account {self.account}, "
                                       'пара разрешена ключом', data=data)

        # open orders on the pair
        step = self._begin('pair_open_orders')
        title = f'Открытые заявки {self.pair}'
        try:
            opens = venue.open_orders()
        except Exception as exc:
            self._refuse(step, title, 'OPEN_ORDERS_READ_FAILED', safe_error(exc))
        data = {'count': len(opens), 'order_ids': [o.order_id for o in opens][:20]}
        self.prechecks['pair_open_orders'] = data
        if opens:
            self._refuse(step, title, 'SMOKE_PAIR_NOT_EMPTY',
                         f'на паре {len(opens)} открытых заявок; smoke-тест идёт только на пустой паре '
                         '(снимите их: cancel-only, или проверьте аккаунт)', data)
        self._end(step, 'PASS', title, 'нет', data=data)

        # open orders on other pairs
        step = self._begin('account_open_orders')
        title = 'Открытые заявки других пар аккаунта'
        try:
            others = venue.account_open_orders()
        except Exception as exc:
            self._refuse(step, title, 'ACCOUNT_OPEN_ORDERS_READ_FAILED', safe_error(exc))
        pairs = sorted({o.currency_pair for o in others})
        data = {'count': len(others), 'pairs': pairs}
        self.prechecks['account_open_orders'] = data
        if others:
            self._refuse(step, title, 'SMOKE_ACCOUNT_NOT_EMPTY',
                         f"на аккаунте {len(others)} открытых заявок других пар ({', '.join(pairs)}); нужен "
                         'отдельный (суб)аккаунт только для бота', data)
        self._end(step, 'PASS', title, 'нет', data=data)

        # fees (information only)
        step = self._begin('fees')
        title = 'Комиссии (справочно)'
        try:
            fees = venue.fee_info()
            data = {k: fees.get(k) for k in ('maker_fee_pct', 'taker_fee_pct', 'gt_deduction_enabled', 'debit_fee',
                                             'debit_fee_error')}
            gt = {True: 'да', False: 'нет'}.get(fees.get('gt_deduction_enabled'), 'н/д')
            detail = (f"maker {fmt(fees.get('maker_fee_pct'))}%, taker {fmt(fees.get('taker_fee_pct'))}%, "
                      f'списание в GT: {gt}')
        except Exception as exc:
            data, detail = {'error': safe_error(exc)}, f'не прочитаны ({safe_error(exc)})'
        self.prechecks['fees'] = sanitize(data)
        self._end(step, 'INFO', title, detail, mandatory=False, data=data)

        # index and spot book
        step = self._begin('index')
        title = 'Индекс и стакан спота Gate'
        from .index import IndexBuilder

        self.builder = IndexBuilder(cfg.index_settings())
        try:
            self.hub = self.hub_factory(cfg)
            self.hub.start()
        except Exception as exc:
            self._refuse(step, title, 'SMOKE_INDEX_INVALID', f'фиды не запущены: {safe_error(exc)}')
        started = time.monotonic()
        deadline = started + float(self.opt.index_seconds)
        rounds = int(math.ceil(float(self.opt.index_seconds) / INDEX_POLL_SECONDS)) + 1
        problem = 'INDEX_UNAVAILABLE'
        snap = book = None
        for _ in range(rounds):
            snap, book, book_error = self._sample()
            problem = self._sample_problem(snap, book, book_error)
            if problem is None or time.monotonic() >= deadline:
                break
            self.sleep(INDEX_POLL_SECONDS)
        waited = round(time.monotonic() - started, 1)
        data = {'waited_seconds': waited, 'problem': problem}
        if snap is not None:
            data.update(index=snap.index, center=snap.center, reason=snap.reason, n_included=snap.n_included,
                        components=[{'source': c.source_id, 'included': c.included, 'reason': c.reason,
                                     'usdt_price': c.usdt_price} for c in snap.components])
        if book is not None:
            data.update(spot_bid=book.bid, spot_ask=book.ask)
        self.prechecks['index'] = sanitize(data)
        if problem is not None:
            self._refuse(step, title, 'SMOKE_INDEX_INVALID',
                         f'за {self.opt.index_seconds:g} с нет валидного индекса и стакана ({problem})', data)
        self._end(step, 'PASS', title, f'центр {fmt(snap.center)} ({snap.n_included} источн.), спот '
                                       f'{fmt(book.bid)} / {fmt(book.ask)}, ожидание {waited:g} с', data=data)

        # order plan
        step = self._begin('plan')
        title = 'Расчёт заявки'
        center, index = snap.center, snap.index
        base_name, base = price_base(center, index)
        price = smoke_price(base, self.opt.edge_pct, meta)
        try:
            qty = smoke_quantity(meta, price) if price > 0 else Decimal(0)
        except ValueError:
            qty = Decimal(0)
        notional = qty * price
        guard = price_guard(price, center, index, book)
        actual_edge = (ONE - price / index) * 100
        edge_center = (ONE - price / center) * 100
        bid_gap = (ONE - price / book.bid) * 100 if book.bid > 0 else Decimal(0)
        text = self._next_text()
        data = {'center': center, 'index': index, 'price_base': base_name, 'base_value': base,
                'edge_pct': self.opt.edge_pct, 'price': price, 'qty': qty, 'notional': notional,
                'actual_edge_pct': actual_edge, 'edge_vs_center_pct': edge_center, 'bid_gap_pct': bid_gap,
                'max_price_by_index': index * MAX_PRICE_FRACTION, 'max_price_by_center': center * MAX_PRICE_FRACTION,
                'max_price_by_bid_gap': book.bid * (ONE - MIN_BID_GAP_PCT / 100),
                'bid': book.bid, 'ask': book.ask, 'text': text, 'max_notional': self.opt.max_notional, 'guard': guard}
        self.prechecks['plan'] = sanitize(data)
        if guard is not None:
            self._refuse(step, title, 'SMOKE_PRICE_GUARD', f'цена {fmt(price)} не прошла проверку ({guard})', data)
        if qty <= 0:
            self._refuse(step, title, 'SMOKE_QTY_INVALID', 'не удалось рассчитать объём', data)
        if ((meta.max_base_amount is not None and qty > meta.max_base_amount)
                or (meta.max_quote_amount is not None and notional > meta.max_quote_amount)):
            self._refuse(step, title, 'SMOKE_ORDER_ABOVE_PAIR_MAXIMUM', 'минимальный объём выше максимума пары', data)
        if notional > self.opt.max_notional:
            self._refuse(step, title, 'SMOKE_NOTIONAL_CAP',
                         f'сумма {usd(notional)} {meta.quote} больше --max-notional {fmt(self.opt.max_notional)}', data)
        self.plan = SmokePlan(text=text, price=price, qty=qty, notional=notional, center=center, index=index,
                              edge_pct=self.opt.edge_pct, actual_edge_pct=actual_edge, bid=book.bid, ask=book.ask,
                              price_base=base_name, edge_vs_center_pct=edge_center, bid_gap_pct=bid_gap)
        self._end(step, 'PASS', title, f'BUY poc {fmt(qty)} {meta.base} по {fmt(price)} ≈ {usd(notional)} {meta.quote} '
                                       f'(от {BASE_NAMES[base_name]} {fmt(base)}: индекс {fmt(index)}, центр '
                                       f'{fmt(center)}; край к индексу {fmt(actual_edge, 3)}%, до bid спота '
                                       f'{fmt(bid_gap, 3)}%), text {text}', data=data)

        # balance
        step = self._begin('balance')
        title = f'Баланс {meta.quote}'
        try:
            balances = venue.balances()
        except Exception as exc:
            self._refuse(step, title, 'BALANCES_READ_FAILED', safe_error(exc))
        quote = balances.get(meta.quote)
        free = quote.free if quote is not None else Decimal(0)
        locked = quote.hold if quote is not None else Decimal(0)
        base = balances.get(meta.base)
        data = {'quote_free': free, 'quote_locked': locked, 'base_total': None if base is None else base.total,
                'needed': notional}
        self.prechecks['balance'] = sanitize(data)
        record = self._last_record(step, 'GET')
        if record is not None:
            reply = record.get('reply')
            self.facts['balances_reply'] = {
                'endpoint': record['endpoint'], 'http_status': record['http_status'],
                'rows': len(reply) if isinstance(reply, list) else None,
                'keys': sorted(reply.keys())[:30] if isinstance(reply, dict) else None,
                'quote_row_present': quote is not None}
        if free < notional:
            self._refuse(step, title, 'SMOKE_INSUFFICIENT_USDT',
                         f'свободно {usd(free)} {meta.quote}, нужно {usd(notional)} {meta.quote}', data)
        self._end(step, 'PASS', title, f'свободно {usd(free)}, заблокировано {usd(locked)}, нужно {usd(notional)}',
                  data=data)

    # ------------------------------------------------------------------ optional exptime probe
    def _probe_exptime(self) -> None:
        """Two cancels of a NONEXISTENT order id while the account has no open order (verified by
        the prechecks): one with x-gate-exptime in the past, one control with a valid exptime."""
        step = self._begin('probe_exptime')
        title = 'Проба x-gate-exptime в прошлом (DELETE несуществующей заявки)'
        params = [('currency_pair', self.pair), ('account', self.account)]
        client = getattr(self.venue, 'client', None)
        result: dict[str, Any] = {'probed': True, 'order_id': PROBE_ORDER_ID}
        if client is None or not callable(getattr(client, 'probe_expired_exptime', None)):
            self.facts['exptime_expired'] = {'probed': False, 'reason': 'NO_REST_CLIENT'}
            self._end(step, 'SKIP', title, 'нет REST-клиента Gate', mandatory=False)
            return
        for name, call in (('expired', lambda: client.probe_expired_exptime(
                                PROBE_ORDER_ID, params, expired_ms=_now_ms() - PROBE_EXPIRED_AGE_MS)),
                           ('control', lambda: client.private('DELETE', f'/spot/orders/{PROBE_ORDER_ID}', params))):
            entry: dict[str, Any] = {}
            try:
                reply = call()
                entry.update(outcome='accepted', reply_type=type(reply).__name__)
            except Rejected as exc:
                entry.update(outcome='rejected', label=exc.label, http_status=exc.status)
            except NotSent as exc:
                entry.update(outcome='not_sent', error=safe_error(exc))
            except Exception as exc:
                entry.update(outcome='unknown', error=safe_error(exc))
            record = self._last_record(step, 'DELETE')
            if record is not None and entry['outcome'] != 'not_sent':
                entry.update(http_status=record['http_status'], label=record['label'], message=record['message'],
                             exptime_offset_ms=record['exptime_offset_ms'])
            result[name] = entry
        self.facts['exptime_expired'] = result
        expired, control = result['expired'], result['control']
        detail = (f"просроченный: {expired.get('label') or expired.get('outcome')} HTTP {expired.get('http_status')}; "
                  f"контроль: {control.get('label') or control.get('outcome')} HTTP {control.get('http_status')}")
        self._end(step, 'INFO', title, detail, mandatory=False, data=result)

    # ------------------------------------------------------------------ main flow
    def _arm_dms(self, name: str, title: str, timeout: int) -> int | None:
        step = self._begin(name)
        sent_ms = _now_ms()
        try:
            trigger = self.venue.countdown(timeout)
        except NotSent as exc:
            self._end(step, 'FAIL', title, f'не отправлено ({safe_error(exc)})')
            raise _Abort() from None
        except Exception as exc:
            self.dms_armed = True                      # it may be armed: disarm only after verifying
            self.dms_state = 'unknown'
            record = self._last_record(step, 'POST')
            self.facts['countdown_reply'].append(sanitize({'timeout': timeout, 'error': safe_error(exc),
                                                           'http_status': record and record['http_status'],
                                                           'reply': record and record['reply']}))
            self._end(step, 'FAIL', title, f'не подтверждено ({safe_error(exc)})')
            raise _Abort() from None
        self.dms_armed = True
        self.dms_state = 'armed'
        self.dms_trigger_ms = trigger
        record = self._last_record(step, 'POST')
        fact = {'timeout': timeout, 'trigger_time_ms': trigger, 'trigger_minus_sent_ms': trigger - sent_ms,
                'http_status': record and record['http_status'], 'reply': record and record['reply']}
        self.facts['countdown_reply'].append(sanitize(fact))
        self._end(step, 'PASS', title, f'countdown {timeout} с, triggerTime {_utc_text(trigger)}', data=fact)
        return trigger

    def _guard_before_create(self, kind: str) -> None:
        """Fresh index and book right before a create. Before the FIRST create a failure is a
        refusal (exit 2: no order was ever sent). Before the ``--dms-fire`` second create a real
        order has already been placed and cancelled, so it is NOT a refusal: the requested
        DMS-fire part is recorded as not performed (exit 1) and the main-flow result is kept.

        The price is recomputed from the fresh sample and may only go DOWN (further from the
        market): an ordinary index tick between the plan and the create would otherwise refuse
        the run. The planned quantity is kept, so a lower price must still meet the pair's
        ``min_quote_amount``; the notional only shrinks, so the cap and the balance still hold."""
        plan = self.plan
        snap, book, book_error = self._sample()
        guard = self._sample_problem(snap, book, book_error)
        price = plan.price
        if guard is None:
            fresh = smoke_price(price_base(snap.center, snap.index)[1], plan.edge_pct, self.meta)
            if fresh < price:
                price = fresh
                min_quote = self.meta.min_quote_amount
                if price <= 0 or (min_quote and plan.qty * price < min_quote):
                    guard = 'QTY_BELOW_PAIR_MINIMUM_AT_LOWER_PRICE'
            guard = guard or price_guard(price, snap.center, snap.index, book)
        if guard is None:
            if price != plan.price:
                fact = {'kind': kind, 'planned_price': plan.price, 'price': price, 'index': snap.index,
                        'center': snap.center, 'bid': book.bid, 'ask': book.ask}
                self.facts.setdefault('price_lowered_before_create', []).append(sanitize(fact))
                LOG.info('SMOKE price lowered before the %s create: %s -> %s', kind, plan.price, price)
                self.plan = replace(plan, price=price, notional=plan.qty * price, center=snap.center,
                                    index=snap.index, actual_edge_pct=(ONE - price / snap.index) * 100,
                                    edge_vs_center_pct=(ONE - price / snap.center) * 100,
                                    bid_gap_pct=(ONE - price / book.bid) * 100, bid=book.bid, ask=book.ask)
            return
        data = {'center': None if snap is None else snap.center, 'index': None if snap is None else snap.index,
                'planned_price': plan.price, 'price': price, 'bid': None if book is None else book.bid,
                'ask': None if book is None else book.ask, 'guard': guard}
        if kind == 'main':
            step = self._begin('pre_create_guard')
            self._refuse(step, 'Повторная проверка цены перед созданием', 'SMOKE_PRICE_GUARD',
                         f'{guard}; заявка не отправлялась', data)
        step = self._begin('dms_fire_guard')
        self.facts['dms_fire'] = {'tested': False, 'not_tested_reason': f'SMOKE_PRICE_GUARD:{guard}'}
        self.dms_fire_skipped = f'SMOKE_PRICE_GUARD: {guard}'
        self._end(step, 'SKIP', 'Повторная проверка цены перед второй заявкой (--dms-fire)',
                  f'{guard}: вторая заявка не отправлялась, проверка DMS не выполнена', data=data)
        raise _Abort()

    def _create(self, kind: str, text: str) -> VenueOrder | None:
        name = 'create' if kind == 'main' else 'dms_fire_create'
        title = 'Создание post-only BUY' + ('' if kind == 'main' else ' (вторая заявка, --dms-fire)')
        self._guard_before_create(kind)
        plan = self.plan                         # after the re-check: it may have lowered the price
        step = self._begin(name)
        req = PlaceRequest(text=text, side='buy', price=plan.price, amount=plan.qty, iceberg=None, time_in_force='poc')
        entry = {'text': text, 'order_id': None, 'kind': kind, 'state': 'sending', 'sent_ms': _now_ms(),
                 'price': plan.price, 'qty': plan.qty}
        self.orders[text] = entry
        try:
            order = self.venue.place(req)
        except NotSent as exc:
            entry['state'] = 'not_sent'
            self._end(step, 'FAIL', title, f'не отправлено ({safe_error(exc)})')
            return None
        except Rejected as exc:
            entry['state'] = 'rejected'
            record = self._last_record(step, 'POST', '/spot/orders')
            fact = {'outcome': 'rejected', 'label': exc.label, 'http_status': exc.status,
                    'message': record and record['message']}
            self.facts.setdefault('create_rejections', []).append(sanitize(fact))
            if is_post_only_rejection(label=exc.label):
                self.facts['post_only_rejection_path'] = (f'ошибка {POST_ONLY_LABEL} (HTTP {exc.status}): заявка '
                                                          'не создана')
            self._end(step, 'FAIL', title, f'отклонена Gate: {exc.label} (HTTP {exc.status})', data=fact)
            return None
        except UnknownResult as exc:
            entry['state'] = 'unknown'
            record = self._last_record(step, 'POST', '/spot/orders')
            if record is not None and isinstance(record.get('reply'), Mapping) and record['http_status'] is not None:
                self.facts.setdefault('create_replies_unparsed', []).append(sanitize({
                    'http_status': record['http_status'], 'error': safe_error(exc), 'reply': record['reply']}))
            found = self._search_unknown(entry, step)
            detail = f'результат неизвестен ({safe_error(exc)}); '
            if found is None:
                detail += 'по text не найдена до конца окна x-gate-exptime: считается не созданной'
            else:
                detail += f'найдена по text: id {found.order_id}, status {found.status}'
            self._end(step, 'FAIL', title, detail, data={'error': safe_error(exc), 'found': found is not None})
            if found is None:
                return None
            order = found
        else:
            self._create_facts(step, order)
            if order.status == 'open':
                self._end(step, 'PASS', title, f'принята: id {order.order_id}, status open, finish_as '
                                               f'{order.finish_as}', data={'order_id': order.order_id})
            elif is_post_only_rejection(order):
                self.facts['post_only_rejection_path'] = ('ответ 2xx с status=cancelled, finish_as=poc: заявка '
                                                          'создана и сразу снята как taker')
                self._end(step, 'FAIL', title, f'post-only отказ: id {order.order_id}, status cancelled, finish_as poc '
                                               '(неожиданно для дальней цены)', data={'order_id': order.order_id})
            else:
                self._end(step, 'FAIL', title, f'неожиданное состояние: id {order.order_id}, status {order.status}, '
                                               f'finish_as {order.finish_as}, исполнено {fmt(order.filled_amount)}',
                          data={'order_id': order.order_id})
        entry['order_id'] = order.order_id
        entry['state'] = 'open' if order.is_open else 'finished'
        self.facts['finish_as_values'][f'{kind}_create'] = order.finish_as
        return order

    def _create_facts(self, step: dict, order: VenueOrder) -> None:
        record = self._last_record(step, 'POST', '/spot/orders')
        reply = record.get('reply') if record else None
        fact: dict[str, Any] = {'http_status': record and record['http_status'], 'status': order.status,
                                'finish_as': order.finish_as}
        if isinstance(reply, Mapping):
            fact.update(fields=sorted(reply.keys()), account_echo=reply.get('account'),
                        time_in_force_echo=reply.get('time_in_force'), iceberg_echo=reply.get('iceberg'),
                        amend_text=reply.get('amend_text'), type_echo=reply.get('type'),
                        create_time_ms_type=type(reply.get('create_time_ms')).__name__)
            self.facts['amend_text_default'] = reply.get('amend_text')
        if self.facts.get('create_reply') is None:
            self.facts['create_reply'] = sanitize(fact)
        else:
            self.facts.setdefault('create_reply_dms_fire', sanitize(fact))

    def _search_unknown(self, entry: dict, step: dict) -> VenueOrder | None:
        """UnknownResult: look for our text in open orders, by text, in the finished window; until
        the x-gate-exptime window (request_expiry + unknown_create_margin) has passed."""
        text = entry['text']
        deadline_ms = entry['sent_ms'] + self.unknown_window_ms
        rounds = int(math.ceil(self.unknown_window_ms / 1000 / UNKNOWN_POLL_SECONDS)) + 2
        notes: list[str] = []
        for _ in range(rounds):
            try:
                for order in self.venue.open_orders():
                    if order.text == text:
                        return order
            except Exception as exc:
                notes.append(f'open_orders:{safe_error(exc)}')
            try:
                order = self.venue.get_order(text=text)
                if order is not None:
                    return order
            except Exception as exc:
                notes.append(f'get_order(text):{safe_error(exc)}')
            try:
                for order in self.venue.finished_orders(max(1, entry['sent_ms'] - FINISHED_WINDOW_MARGIN_MS),
                                                        _now_ms() + 1000):
                    if order.text == text:
                        return order
            except Exception as exc:
                notes.append(f'finished_orders:{safe_error(exc)}')
            if _now_ms() >= deadline_ms:
                entry['state'] = 'not_found'
                break
            self.sleep(UNKNOWN_POLL_SECONDS)
        step['data']['search_errors'] = notes[-10:]
        return None

    def _main_flow(self) -> None:
        plan = self.plan
        self._arm_dms('dms_arm', f'DMS включён до создания (countdown {self.dms_timeout} с)', self.dms_timeout)
        order = self._create('main', plan.text)
        if order is None or not order.is_open:
            raise _Abort()
        got = self._verify_open(order)
        self._lookup_text(order, 'get_order_by_text_while_open', 'Поиск по text, пока заявка открыта')
        self._status_block(got or order)
        self._renew_dms()
        self._cancel(order, 'cancel', 'Отмена по order_id')
        ok = self._verify_cancelled(order)
        self._lookup_text(order, 'get_order_by_text_after_finish', 'Поиск по text после отмены')
        self._finished_window(order)
        if not ok:
            raise _Abort()
        self._disarm('dms_disarm', 'DMS выключен (countdown 0)')
        self._final_read('final_read', 'Итоговое чтение: на паре нет открытых заявок')
        self.main_passed = all(s['status'] == 'PASS' for s in self.steps if s['mandatory'])

    def _verify_open(self, order: VenueOrder) -> VenueOrder | None:
        plan, venue = self.plan, self.venue
        step = self._begin('verify_open')
        title = 'Проверка чтением: заявка открыта'
        problems: list[str] = []
        data: dict[str, Any] = {}
        got = None
        try:
            got = venue.get_order(order_id=order.order_id)
        except Exception as exc:
            problems.append(f'get_order(id): {safe_error(exc)}')
        if got is None and not problems:
            problems.append('get_order(id): не найдена')
        if got is not None:
            data['get_order'] = {'status': got.status, 'left': got.left, 'amount': got.amount, 'price': got.price,
                                 'time_in_force': got.time_in_force, 'text': got.text, 'account': got.account or None,
                                 'finish_as': got.finish_as, 'filled_amount': got.filled_amount}
            self.facts['order_account_echo_get'] = got.account or None
            for ok, what in ((got.status == 'open', f'status {got.status}'),
                             (got.left == plan.qty and got.amount == plan.qty, f'left {fmt(got.left)} / amount '
                                                                               f'{fmt(got.amount)}'),
                             (got.filled_amount == 0, f'исполнено {fmt(got.filled_amount)}'),
                             (got.time_in_force == 'poc', f'time_in_force {got.time_in_force}'),
                             (got.text == order.text, 'text не совпал'),
                             (got.price == plan.price, f'цена {fmt(got.price)}'),
                             (got.account in ('', self.account), f'account {got.account}')):
                if not ok:
                    problems.append(what)
        try:
            opens = venue.open_orders()
            present = any(o.order_id == order.order_id for o in opens)
            data['open_orders'] = {'count': len(opens), 'present': present}
            if not present:
                problems.append('нет в списке открытых заявок')
            if len(opens) > 1:
                problems.append(f'на паре {len(opens)} открытых заявок (ожидалась одна)')
        except Exception as exc:
            problems.append(f'open_orders: {safe_error(exc)}')
        try:
            quote = venue.balances().get(self.meta.quote)
            locked = quote.hold if quote is not None else Decimal(0)
            data['quote_locked'] = locked
            if locked < plan.notional:
                problems.append(f'заблокировано {usd(locked)} < {usd(plan.notional)} {self.meta.quote}')
        except Exception as exc:
            problems.append(f'balances: {safe_error(exc)}')
        if problems:
            self._end(step, 'FAIL', title, '; '.join(problems), data=data)
        else:
            self._end(step, 'PASS', title, f'get_order(id): open, left = amount = {fmt(plan.qty)}, poc, цена и text '
                                           f'совпали; в open_orders; заблокировано {usd(data["quote_locked"])} '
                                           f'{self.meta.quote}', data=data)
        return got

    def _lookup_text(self, order: VenueOrder, fact: str, title: str) -> None:
        step = self._begin(fact)
        result: dict[str, Any] = {}
        try:
            found = self.venue.get_order(text=order.text)
            result.update(found=found is not None, status=None if found is None else found.status,
                          order_id_matches=None if found is None else found.order_id == order.order_id)
        except Exception as exc:
            result.update(found=None, error=safe_error(exc))
        record = self._last_record(step, 'GET')
        if record is not None:
            result.update(http_status=record['http_status'], label=record['label'])
        self.facts[fact] = sanitize(result)
        if result.get('found'):
            detail = f"найдена (status {result['status']})"
        elif result.get('found') is False:
            detail = f"не найдена ({result.get('label')}, HTTP {result.get('http_status')})"
        else:
            detail = f"ошибка ({result.get('error')}, label {result.get('label')})"
        self._end(step, 'INFO', title, detail, mandatory=False, data=result)

    def _status_block(self, order: VenueOrder) -> None:
        plan, meta = self.plan, self.meta
        step = self._begin('status')
        remaining = None if self.dms_trigger_ms is None else round((self.dms_trigger_ms - _now_ms()) / 1000)
        lines = [
            '--- Статус smoke-заявки ---',
            f'Пара {self.pair}, аккаунт {self.account}',
            f'Заявка: id {order.order_id}, text {order.text}, BUY post-only (poc), без iceberg',
            f'Цена {fmt(plan.price)} от {BASE_NAMES.get(plan.price_base, plan.price_base)}: индекс '
            f'{fmt(plan.index)}, центр {fmt(plan.center)}; край к индексу {fmt(plan.actual_edge_pct, 3)}% (не выше '
            f'индекс × 0.9 = {fmt(plan.index * MAX_PRICE_FRACTION)} и центр × 0.9 = '
            f'{fmt(plan.center * MAX_PRICE_FRACTION)})',
            f'Спот bid {fmt(plan.bid)} / ask {fmt(plan.ask)}: цена ниже bid на {fmt(plan.bid_gap_pct, 3)}% '
            f'(не меньше {fmt(MIN_BID_GAP_PCT)}%)',
            f'Объём {fmt(plan.qty)} {meta.base} ≈ {usd(plan.notional)} {meta.quote}',
            f'Состояние: {order.status}, осталось {fmt(order.left)}, finish_as {order.finish_as}',
            f'DMS: при остановке продлений Gate снимет заявки пары в {_utc_text(self.dms_trigger_ms)}'
            + ('' if remaining is None else f' (через {remaining} с)'),
        ]
        for line in lines:
            self.say('    ' + line)
        self._end(step, 'INFO', 'Статус заявки', 'выведен выше', mandatory=False,
                  data={'order_id': order.order_id, 'status': order.status, 'dms_remaining_seconds': remaining})

    def _renew_dms(self) -> None:
        try:
            self._arm_dms('dms_renew', f'Продление DMS (countdown {self.dms_timeout} с)', self.dms_timeout)
        except _Abort:
            pass      # recorded as FAIL; the cancel and its verification still run

    def _cancel(self, order: VenueOrder, name: str, title: str) -> str:
        step = self._begin(name)
        entry = self.orders.get(order.text)
        try:
            results = self.venue.cancel_many([CancelRequest(text=order.text, order_id=order.order_id)])
            outcome = results.get(order.text, 'unknown')
        except Exception as exc:
            outcome = f'error:{safe_error(exc)}'
        if entry is not None and entry['state'] == 'open':
            entry['state'] = 'cancel_sent'
        record = self._last_record(step, 'POST', '/spot/cancel_batch_orders')
        item = None
        if record is not None and isinstance(record.get('reply'), list):
            item = next((i for i in record['reply'] if isinstance(i, Mapping)
                         and str(i.get('id')) in (order.order_id, order.text)), None)
        fact = {'outcome': outcome, 'http_status': record and record['http_status'], 'item': item,
                'item_fields': sorted(item.keys()) if isinstance(item, Mapping) else None}
        if name == 'cancel':
            self.facts['cancel_batch_item'] = sanitize(fact)
        self._end(step, 'PASS' if outcome == 'sent' else 'FAIL', title,
                  f'id {order.order_id}: результат {outcome}', data=fact)
        return outcome

    def _wait_finished(self, order: VenueOrder, seconds: float, poll: float) -> tuple[VenueOrder | None, list[str]]:
        errors: list[str] = []
        got = None
        deadline = time.monotonic() + seconds
        for _ in range(int(math.ceil(seconds / poll)) + 1):
            try:
                got = self.venue.get_order(order_id=order.order_id)
            except Exception as exc:
                errors.append(safe_error(exc))
                got = None
            if got is not None and got.status != 'open':
                entry = self.orders.get(order.text)
                if entry is not None:
                    entry['state'] = 'finished'
                return got, errors
            if time.monotonic() >= deadline:
                break
            self.sleep(poll)
        return got, errors

    def _verify_cancelled(self, order: VenueOrder) -> bool:
        plan, venue = self.plan, self.venue
        step = self._begin('verify_cancel')
        title = 'Проверка отмены чтением'
        problems: list[str] = []
        data: dict[str, Any] = {}
        got, errors = self._wait_finished(order, CANCEL_VERIFY_SECONDS, POLL_SECONDS)
        if errors:
            data['read_errors'] = errors[-5:]
        if got is None:
            problems.append('get_order(id): нет ответа' + (f' ({errors[-1]})' if errors else ''))
        else:
            data['get_order'] = {'status': got.status, 'finish_as': got.finish_as, 'left': got.left,
                                 'filled_amount': got.filled_amount, 'update_time_ms': got.update_time_ms}
            if got.status != 'open':
                self.facts['finish_as_values']['after_cancel'] = got.finish_as
            if got.status != 'cancelled':
                problems.append(f'status {got.status}')
            if got.left != plan.qty or got.filled_amount != 0:
                problems.append(f'left {fmt(got.left)}, исполнено {fmt(got.filled_amount)}')
        empty = False
        try:
            opens = venue.open_orders()
            present = any(o.order_id == order.order_id for o in opens)
            empty = not opens
            data['open_orders'] = {'count': len(opens), 'present': present}
            if present:
                problems.append('всё ещё в open_orders')
            elif opens:
                problems.append(f'на паре {len(opens)} посторонних открытых заявок')
        except Exception as exc:
            problems.append(f'open_orders: {safe_error(exc)}')
        try:
            trades = venue.order_trades(order.order_id)
            data['order_trades'] = len(trades)
            if trades:
                problems.append(f'у заявки {len(trades)} сделок')
        except Exception as exc:
            problems.append(f'order_trades: {safe_error(exc)}')
        self._pair_empty_now = empty
        if problems:
            self._end(step, 'FAIL', title, '; '.join(problems), data=data)
            return False
        self._end(step, 'PASS', title, f"status cancelled, finish_as {got.finish_as}, left = {fmt(got.left)}, нет в "
                                       'open_orders, сделок нет', data=data)
        return True

    def _finished_window(self, order: VenueOrder) -> None:
        step = self._begin('finished_orders')
        entry = self.orders.get(order.text) or {}
        start = max(1, int(entry.get('sent_ms') or order.create_time_ms) - FINISHED_WINDOW_MARGIN_MS)
        result: dict[str, Any] = {'from_ms': start}
        try:
            rows = self.venue.finished_orders(start, _now_ms() + 1000)
            match = next((o for o in rows if o.order_id == order.order_id), None)
            result.update(rows=len(rows), contains=match is not None,
                          finish_as=None if match is None else match.finish_as)
        except Exception as exc:
            result.update(contains=None, error=safe_error(exc))
        self.facts['finished_orders_window_contains_order'] = sanitize(result)
        detail = ('заявка есть в списке finished' if result.get('contains')
                  else 'заявки нет в списке finished' if result.get('contains') is False
                  else f"ошибка чтения ({result.get('error')})")
        self._end(step, 'INFO', 'Список finished за окно', detail, mandatory=False, data=result)

    def _disarm(self, name: str, title: str) -> bool:
        """countdown(0), only while the pair is verified empty by the last open-orders read."""
        step = self._begin(name)
        if not self._pair_empty_now:
            self.dms_state = 'left_armed' if self.dms_armed else self.dms_state
            self._end(step, 'FAIL', title, 'пара не подтверждена пустой: DMS оставлен включённым')
            return False
        result: dict[str, Any] = {}
        ok = False
        try:
            value = self.venue.countdown(0)
            result.update(outcome='acknowledged', returned=value)
            ok = True
        except Exception as exc:
            result.update(outcome='not_acknowledged', error=safe_error(exc))
        record = self._last_record(step, 'POST')
        if record is not None:
            reply = record.get('reply')
            result.update(http_status=record['http_status'], reply=reply,
                          trigger_time_present=isinstance(reply, Mapping) and 'triggerTime' in reply,
                          trigger_time_value=reply.get('triggerTime') if isinstance(reply, Mapping) else None)
            if not ok and isinstance(record['http_status'], int) and 200 <= record['http_status'] < 300:
                ok = True        # Gate accepted the request; only the reply shape differs from timeout > 0
                result['outcome'] = 'accepted_2xx_without_valid_trigger_time'
        self.facts['countdown_zero_reply'].append(sanitize(result))
        if ok:
            self.dms_state = 'disarmed'
            self._end(step, 'PASS', title, f"HTTP {result.get('http_status')}, ответ {json.dumps(result.get('reply'))}",
                      data=result)
        else:
            self._end(step, 'FAIL', title, f"не подтверждено ({result.get('error')}); DMS может остаться включённым "
                                           '(пара пуста — он ничего не снимет)', data=result)
        return ok

    def _final_read(self, name: str, title: str) -> bool:
        step = self._begin(name)
        try:
            opens = self.venue.open_orders()
        except Exception as exc:
            self.pair_verified_empty = False
            self._end(step, 'FAIL', title, f'чтение не удалось ({safe_error(exc)})')
            return False
        self.pair_verified_empty = not opens
        if opens:
            self._end(step, 'FAIL', title, f'открыто {len(opens)}', data={'count': len(opens)})
            return False
        self._end(step, 'PASS', title, 'открыто 0', data={'count': 0})
        return True

    # ------------------------------------------------------------------ --dms-fire
    def _dms_fire(self) -> None:
        if not (self.main_passed and self.pair_verified_empty and self.dms_state == 'disarmed'):
            step = self._begin('dms_fire')
            self._end(step, 'SKIP', 'Проверка срабатывания DMS (--dms-fire)', 'основная проверка не пройдена',
                      mandatory=False)
            return
        self.err(DMS_FIRE_WARNING)
        fire: dict[str, Any] = {'tested': True}
        self.facts['dms_fire'] = fire
        self.pair_verified_empty = False
        self._arm_dms('dms_fire_arm', f'DMS включён до создания второй заявки (countdown {self.dms_timeout} с)',
                      self.dms_timeout)
        order = self._create('dms_fire', self._next_text())
        if order is None or not order.is_open:
            fire['result'] = 'order_not_open'
            raise _Abort()
        fire['order_id'] = order.order_id
        trigger = self._arm_dms('dms_fire_countdown', f'DMS {DMS_FIRE_TIMEOUT} с без продления', DMS_FIRE_TIMEOUT)
        armed_ms = _now_ms()
        fire.update(timeout_seconds=DMS_FIRE_TIMEOUT, trigger_time_ms=trigger)
        step = self._begin('dms_fire_wait')
        title = f'Ожидание снятия заявки по DMS (до {DMS_FIRE_WAIT_SECONDS:g} с)'
        got, errors = self._wait_finished(order, DMS_FIRE_WAIT_SECONDS, DMS_FIRE_POLL_SECONDS)
        observed_ms = _now_ms()
        if got is not None and got.status != 'open':
            fire.update(status=got.status, finish_as=got.finish_as, left=got.left, filled_amount=got.filled_amount,
                        observed_ms=observed_ms, observed_after_arm_ms=observed_ms - armed_ms,
                        observed_after_trigger_ms=None if trigger is None else observed_ms - trigger,
                        update_time_minus_trigger_ms=None if trigger is None else got.update_time_ms - trigger)
            self.facts['finish_as_values']['after_dms'] = got.finish_as
            ok = got.status == 'cancelled' and got.filled_amount == 0
            self._end(step, 'PASS' if ok else 'FAIL', title,
                      f'снята: status {got.status}, finish_as {got.finish_as}, через '
                      f'{(observed_ms - armed_ms) / 1000:.1f} с после countdown (update_time − triggerTime = '
                      f"{fire['update_time_minus_trigger_ms']} мс)", data=fire)
        else:
            fire.update(result='not_cancelled_by_dms', read_errors=errors[-5:])
            self._end(step, 'FAIL', title, f'за {DMS_FIRE_WAIT_SECONDS:g} с DMS не снял заявку; она будет снята '
                                           'отменой', data=fire)
            raise _Abort()
        self._pair_empty_now = False
        verify = self._begin('dms_fire_verify')
        try:
            opens = self.venue.open_orders()
            self._pair_empty_now = not opens
            if opens:
                self._end(verify, 'FAIL', 'Пара пуста после DMS', f'открыто {len(opens)}')
                raise _Abort()
            self._end(verify, 'PASS', 'Пара пуста после DMS', 'открыто 0')
        except _Abort:
            raise
        except Exception as exc:
            self._end(verify, 'FAIL', 'Пара пуста после DMS', f'чтение не удалось ({safe_error(exc)})')
            raise _Abort() from None
        self._disarm('dms_fire_disarm', 'DMS выключен (countdown 0)')
        self._final_read('dms_fire_final_read', 'Итоговое чтение: на паре нет открытых заявок')

    # ------------------------------------------------------------------ cleanup
    def _entry_for(self, order: Any) -> dict | None:
        """Our smoke entry for a listed order (by text, else by bound order id); None = not ours."""
        entry = self.orders.get(order.text)
        if entry is not None:
            return entry
        return next((e for e in self.orders.values() if e['order_id'] and e['order_id'] == str(order.order_id)),
                    None)

    def _is_ours(self, order: Any) -> bool:
        return self._entry_for(order) is not None

    def _cleanup(self) -> None:
        """Cancel every smoke order that may still be open and verify the pair by reading.

        * A bound smoke order (id known) that is not confirmed finished is ALWAYS cancelled by
          ``order_id`` (harmless when it is already finished), re-sent at most every
          ``CLEANUP_CANCEL_EVERY_SECONDS``; an unbound one (create result unknown) is cancelled by
          ``text`` and looked up by ``text`` (works only while open), then in the finished list by
          ``text``; once found it is bound to its id. Only our own ids and texts are ever cancelled.
        * A smoke order counts as gone only when a direct read of it (``get_order(order_id)``, or
          its row found by text) shows a status other than ``open``: absence from the open list
          alone proves nothing (the list may lag). An unbound order never seen counts as not
          created only after its ``x-gate-exptime`` window has passed.
        * The pair counts as verified empty only after TWO empty open-orders reads at least
          ``safety.terminal_settle_seconds`` apart while every smoke order is confirmed gone. A
          disagreement (``get_order`` says open, the list is empty) or an unreadable ``get_order``
          keeps it unverified. The DMS is disarmed only then; otherwise it stays armed.
        """
        targets = [e for e in self.orders.values() if e['state'] not in FINAL_STATES]
        needs_dms = self.dms_armed and self.dms_state != 'disarmed'
        if not targets and not needs_dms and (self.pair_verified_empty or not self.dms_armed):
            return
        step = self._begin('cleanup')
        title = 'Уборка: снять smoke-заявки и проверить пару чтением'
        settle = float(self.cfg.safety['terminal_settle_seconds'])
        data: dict[str, Any] = {'targets': [{'text': e['text'], 'order_id': e['order_id'], 'state': e['state']}
                                            for e in targets], 'settle_seconds': settle}
        verified_empty = False
        foreign = 0
        cancels: list[dict] = []
        checks: dict[str, dict] = {}            # text -> last direct read of that smoke order
        disagreements: list[str] = []
        empty_reads: list[int] = []             # wall ms of the consecutive clean open-orders reads
        last_error = None
        first_clean: float | None = None
        deadline = time.monotonic() + CLEANUP_SECONDS + settle
        next_cancel = 0.0
        for _ in range(int((CLEANUP_SECONDS + settle) / POLL_SECONDS) + 3):
            pending = [e for e in self.orders.values() if e['state'] not in FINAL_STATES]
            # 1) cancel every smoke order that may be open: by order_id, by text only without one
            if pending and time.monotonic() >= next_cancel:
                next_cancel = time.monotonic() + CLEANUP_CANCEL_EVERY_SECONDS
                requests = [CancelRequest(text=e['text'], order_id=e['order_id']) for e in pending]
                try:
                    results = self.venue.cancel_many(requests)
                except Exception as exc:
                    results = {r.text: f'error:{safe_error(exc)}' for r in requests}
                for e in pending:
                    if e['state'] == 'open':
                        e['state'] = 'cancel_sent'
                cancels.append(sanitize(results))
            # 2) direct read of each smoke order: the only proof that it is gone
            for e in pending:
                by = 'order_id' if e['order_id'] else 'text'
                try:
                    got = (self.venue.get_order(order_id=e['order_id']) if e['order_id']
                           else self.venue.get_order(text=e['text']))
                    if got is None and not e['order_id']:   # by text works only while open: finished list
                        by = 'finished_orders'
                        got = next((o for o in self.venue.finished_orders(
                            max(1, e['sent_ms'] - FINISHED_WINDOW_MARGIN_MS), _now_ms() + 1000)
                            if o.text == e['text']), None)
                except Exception as exc:
                    last_error = safe_error(exc)
                    checks[e['text']] = {'by': by, 'error': last_error}
                    continue
                if got is None:
                    checks[e['text']] = {'by': by, 'found': False}
                    continue
                checks[e['text']] = {'by': by, 'found': True, 'order_id': got.order_id, 'status': got.status,
                                     'finish_as': got.finish_as, 'left': got.left}
                if not e['order_id']:
                    e['order_id'] = got.order_id            # found by text: bound from now on
                if got.status != 'open':
                    e['state'] = 'finished'
                    self.facts['finish_as_values'].setdefault(f"{e['kind']}_cleanup", got.finish_as)
                elif e['state'] in ('sending', 'unknown'):
                    e['state'] = 'open'
                    next_cancel = 0.0                       # open and now bound: cancel it by id at once
            # 3) the pair's open orders
            try:
                opens = self.venue.open_orders()
            except Exception as exc:
                last_error = safe_error(exc)
                opens = None
            clean = False
            if opens is not None:
                ours = [o for o in opens if self._is_ours(o)]
                foreign = len(opens) - len(ours)
                listed = set()
                for o in ours:
                    entry = self._entry_for(o)
                    listed.add(entry['text'])
                    if not entry['order_id']:
                        entry['order_id'] = o.order_id
                    if entry['state'] in FINAL_STATES and entry['state'] != 'finished':
                        entry['state'] = 'open'             # e.g. a late create once taken as not created
                        next_cancel = 0.0
                now_ms = _now_ms()
                for e in self.orders.values():
                    if e['state'] in FINAL_STATES or e['text'] in listed:
                        continue
                    check = checks.get(e['text']) or {}
                    if check.get('status') == 'open' and e['text'] not in disagreements:
                        disagreements.append(e['text'])     # get_order says open, the list does not show it
                    if (not e['order_id'] and e['state'] in ('sending', 'unknown') and check.get('found') is False
                            and now_ms >= e['sent_ms'] + self.unknown_window_ms):
                        e['state'] = 'not_found'            # never seen until the x-gate-exptime window ended
                clean = not opens and all(e['state'] in FINAL_STATES for e in self.orders.values())
            if clean:
                empty_reads.append(_now_ms())
                if first_clean is None:
                    first_clean = time.monotonic()
                elif time.monotonic() - first_clean >= settle:
                    verified_empty = True
                    break
            else:
                first_clean = None
                empty_reads.clear()
            if time.monotonic() >= deadline:
                break
            self.sleep(max(POLL_SECONDS, settle) if clean else POLL_SECONDS)
        unresolved = [e['text'] for e in self.orders.values() if e['state'] not in FINAL_STATES]
        data.update(verified_empty=verified_empty, foreign_open=foreign, cancels=cancels, last_error=last_error,
                    unresolved=unresolved, order_reads=checks, open_but_not_listed=disagreements,
                    empty_reads_ms=list(empty_reads))
        self.pair_verified_empty = verified_empty and not unresolved
        detail = []
        if self.dms_armed and self.dms_state != 'disarmed':
            if self.pair_verified_empty:
                self._pair_empty_now = True
                disarm = self._disarm_inline(step, data)
                detail.append('DMS выключен' if disarm else 'DMS выключить не удалось (пара пуста)')
            else:
                self.dms_state = 'left_armed'
                detail.append(f'DMS ОСТАВЛЕН ВКЛЮЧЁННЫМ: Gate снимет все заявки пары не позже '
                              f'{_utc_text(self.dms_trigger_ms)}')
        if self.pair_verified_empty:
            head = (f'пара проверена чтением: smoke-заявки сняты (get_order), открытых заявок нет в двух чтениях '
                    f'с интервалом {settle:g} с')
        else:
            head = 'пара НЕ подтверждена пустой'
            if unresolved:
                head += f'; не подтверждены снятыми smoke-заявки: {", ".join(unresolved)}'
            if disagreements:
                head += f'; get_order показывает open, а в списке открытых заявки нет: {", ".join(disagreements)}'
            if foreign:
                head += f'; посторонних заявок {foreign}'
            if last_error:
                head += f'; ошибка чтения {last_error}'
        detail.insert(0, head)
        ok = self.pair_verified_empty and self.dms_state in ('disarmed', 'never_armed')
        self.cleanup_failed = not ok
        self._end(step, 'PASS' if ok else 'FAIL', title, '; '.join(detail), data=data)

    def _disarm_inline(self, step: dict, data: dict) -> bool:
        try:
            self.venue.countdown(0)
            ok = True
        except Exception as exc:
            data['disarm_error'] = safe_error(exc)
            ok = False
        record = self._last_record(step, 'POST', '/spot/countdown_cancel_all')
        if record is not None:
            reply = record.get('reply')
            fact = {'http_status': record['http_status'], 'reply': reply, 'outcome': 'cleanup',
                    'trigger_time_present': isinstance(reply, Mapping) and 'triggerTime' in reply}
            self.facts['countdown_zero_reply'].append(sanitize(fact))
            if not ok and isinstance(record['http_status'], int) and 200 <= record['http_status'] < 300:
                ok = True
        if ok:
            self.dms_state = 'disarmed'
        return ok

    # ------------------------------------------------------------------ report
    def report(self, *, fixtures_dir: Path | None = None) -> dict:
        records = self.rec.records
        rate = [r['rate_limit_headers'] for r in records if r.get('rate_limit_headers')]
        self.facts['rate_limit_headers'] = {'seen': bool(rate), 'sample': rate[-1] if rate else None}
        steps = []
        for step in self.steps:
            row = dict(step)
            row['requests'] = self._step_records(step)       # full sanitized exchanges of this step
            steps.append(row)
        result = 'REFUSED' if self.exit_code == EXIT_REFUSED else 'PASS' if self.exit_code == EXIT_PASS else 'FAIL'
        cfg = self.cfg
        return sanitize({
            'kind': 'gate_stonk_smoke_test', 'version': __version__, 'result': result, 'exit_code': self.exit_code,
            'started_utc': _utc_text(self.started_ms), 'finished_utc': _utc_text(self.finished_ms),
            'started_ms': self.started_ms, 'finished_ms': self.finished_ms,
            'config': {'path': str(cfg.path), 'identity': cfg.identity(), 'state_dir': str(cfg.state_dir),
                       'rest_url': cfg.market['rest_url'], 'dms_timeout_seconds': self.dms_timeout,
                       'max_clock_skew_ms': cfg.safety['max_clock_skew_ms'],
                       'request_expiry_seconds': cfg.app['request_expiry_seconds'],
                       'request_timeout_seconds': cfg.app['request_timeout_seconds']},
            'options': self.opt.as_dict(),
            'refusal': self.refusal, 'error': self.fatal, 'interrupted': self.interrupted,
            'interrupt_reason': self.interrupt_reason, 'stop_signal': self.stop_signal,
            'main_passed': self.main_passed,
            'dms_fire_not_performed': self.dms_fire_skipped,
            'pair_verified_empty': self.pair_verified_empty, 'dms_state': self.dms_state,
            'dms_trigger_ms': self.dms_trigger_ms,
            'prechecks': self.prechecks, 'plan': None if self.plan is None else asdict(self.plan),
            'orders': list(self.orders.values()),
            'steps': steps, 'facts': self.facts, 'requests': records,
            'fixtures_dir': None if fixtures_dir is None else str(fixtures_dir),
        })

    def summary_lines(self, report_path: Path | None, fixtures_dir: Path | None) -> list[str]:
        lines = []
        failed = [s for s in self.steps if s['mandatory'] and s['status'] not in ('PASS',)]
        if self.exit_code == EXIT_PASS:
            lines.append('Итог smoke-теста: ПРОЙДЕН — все обязательные шаги OK, на паре нет открытых заявок, '
                         'DMS выключен.')
        elif self.exit_code == EXIT_REFUSED:
            refusal = self.refusal or {}
            lines.append(f"Итог smoke-теста: ОТКЛОНЁН ({refusal.get('code')}): {refusal.get('message')}. "
                         + ('Заявки не отправлялись.' if not self.orders else ''))
        elif (self.dms_fire_skipped and self.main_passed and not self.fatal and not self.interrupted
              and not [s for s in failed if s['name'] != 'dms_fire_guard']):
            lines.append('Итог smoke-теста: основная проверка ПРОЙДЕНА (одна заявка выставлена, проверена, снята и '
                         'проверена чтением); проверка DMS (--dms-fire) НЕ ВЫПОЛНЕНА — '
                         f'{self.dms_fire_skipped}: вторая заявка не отправлялась. Код 1: запрошенная проверка не '
                         'сделана; повторите smoke-test с --dms-fire.')
        else:
            names = ', '.join(f"{s['n']} ({s['title']})" for s in failed) or 'н/д'
            lines.append(f'Итог smoke-теста: НЕ ПРОЙДЕН — шаги с ошибкой: {names}'
                         + (f'; ошибка {self.fatal}' if self.fatal else '')
                         + (f'; прервано ({self.interrupt_reason or "Ctrl+C"})' if self.interrupted else ''))
        if self.orders or self.dms_armed:
            lines.append('Пара: ' + ('проверена чтением, открытых заявок нет.' if self.pair_verified_empty
                                     else 'НЕ подтверждена пустой — проверьте веб-интерфейс Gate и выполните '
                                          'cancel-only.'))
            state = {'disarmed': 'выключен', 'left_armed': 'ОСТАВЛЕН ВКЛЮЧЁННЫМ (снимет заявки пары в '
                                                          f'{_utc_text(self.dms_trigger_ms)})',
                     'armed': 'включён', 'unknown': 'состояние неизвестно', 'never_armed': 'не включался'}
            lines.append(f'DMS: {state.get(self.dms_state, self.dms_state)}.')
        facts = self.facts
        probed = facts['exptime_expired'].get('probed')
        if self.orders or probed:
            lines.append('Факты для API_NOTES:')
        if self.orders:
            lines.append(f"  путь отказа post-only: {facts['post_only_rejection_path']}")
            create = facts.get('create_reply') or {}
            if create:
                lines.append(f"  ответ создания: HTTP {create.get('http_status')}, account "
                             f"{create.get('account_echo')!r}, time_in_force {create.get('time_in_force_echo')!r}, "
                             f"amend_text {create.get('amend_text')!r}, finish_as {create.get('finish_as')!r}")
            lines.append(f"  finish_as: {json.dumps(facts['finish_as_values'], ensure_ascii=False)}")
            for key, label in (('get_order_by_text_while_open', 'get_order по text (открыта)'),
                               ('get_order_by_text_after_finish', 'get_order по text (после отмены)')):
                value = facts.get(key)
                if value:
                    lines.append(f"  {label}: {'найдена' if value.get('found') else 'не найдена'}"
                                 f" (HTTP {value.get('http_status')}, label {value.get('label')})")
            window = facts.get('finished_orders_window_contains_order')
            if window:
                lines.append(f"  список finished за окно содержит заявку: {window.get('contains')}")
            if facts['countdown_zero_reply']:
                zero = facts['countdown_zero_reply'][0]
                lines.append(f"  countdown(0): HTTP {zero.get('http_status')}, ответ "
                             f"{json.dumps(zero.get('reply'), ensure_ascii=False)}")
            if facts['dms_fire'].get('tested'):
                fire = facts['dms_fire']
                lines.append(f"  DMS-fire: status {fire.get('status')}, finish_as {fire.get('finish_as')}, "
                             f"update_time − triggerTime {fire.get('update_time_minus_trigger_ms')} мс")
        if probed:
            expired = facts['exptime_expired'].get('expired') or {}
            control = facts['exptime_expired'].get('control') or {}
            lines.append(f"  просроченный x-gate-exptime: label {expired.get('label')} HTTP "
                         f"{expired.get('http_status')} (контроль: {control.get('label')} HTTP "
                         f"{control.get('http_status')})")
        rate = facts.get('rate_limit_headers') or {}
        if self.orders or probed:
            lines.append(f"  заголовки X-Gate-RateLimit-*: {'есть' if rate.get('seen') else 'не замечены'}")
        if report_path is not None:
            lines.append(f'JSON отчёта: {report_path}')
        if fixtures_dir is not None:
            lines.append(f'Сырые ответы (кандидаты фикстур): {fixtures_dir}')
        return lines


DMS_FIRE_WARNING = ('ВНИМАНИЕ (--dms-fire): сейчас будет выставлена ВТОРАЯ такая же дальняя post-only BUY, DMS '
                    f'взведён на {DMS_FIRE_TIMEOUT} с и НЕ продлевается: Gate должен сам снять заявку. Если за '
                    f'{DMS_FIRE_WAIT_SECONDS:g} с этого не произойдёт, заявка будет снята отменой по order_id.')

FIXTURES_README = """# Кандидаты фикстур из smoke-теста gate_stonk

Каждый файл — один ответ Gate, записанный командой `smoke-test`, в формате тестовых фикстур
(`tests/fixtures/gate/README.md`): `{"_comment": ..., "_schema_source": ..., "_request": ..., "data": <ответ>}`.
Поля, похожие на идентификатор пользователя (`user_id`, `uid`, ...), и списки IP заменены на `***`;
заголовки запросов (ключ, подпись, Timestamp) не записываются вообще.

Как перенести в тесты:

1. Просмотрите файл глазами: в `data` не должно остаться личных данных (балансы, id аккаунта, IP).
   Значения id заявок и время можно оставить.
2. Скопируйте нужные файлы в `tests/fixtures/gate/recorded/` (папку создать при первом переносе),
   имя — по эндпоинту и случаю, например `order_create_open.json`, `countdown_zero_reply.json`.
3. Оставьте `_comment`, начинающийся с `RECORDED`: синтетические фикстуры в `tests/fixtures/gate/`
   помечены `SYNTHETIC`, и тесты различают их по этой метке.
4. Добавьте строку в таблицу `tests/fixtures/gate/README.md` и тест, который читает фикстуру через
   настоящий разбор (`parse_order`, `GateVenue` на FakeSession), затем прогоните `pytest -q`.
5. Если записанный ответ расходится с синтетической фикстурой той же формы, поправьте синтетику и
   отметьте факт в API_NOTES_RU.md (раздел 9.2, «Что записать по итогам этапа 5»).
"""


def write_fixtures(records: list[dict], directory: Path, stamp: str) -> int:
    """One candidate fixture per recorded JSON reply (already sanitized by the recorder)."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory.parent / 'README.md').write_text(FIXTURES_README, encoding='utf-8')
    written = 0
    for record in records:
        if record.get('reply') is None:
            continue
        slug = _SLUG_RE.sub('_', record['endpoint']).strip('_').lower() or 'root'
        name = f"{record['seq']:03d}_{record['step']}_{record['method'].lower()}_{slug}.json"
        payload = {
            '_comment': (f'RECORDED reply from Gate (gate_stonk smoke-test {stamp}, step {record["step"]}); '
                         'sanitized: user-id-like fields and IP lists masked. Review before promoting to '
                         'tests/fixtures/gate/recorded/.'),
            '_schema_source': f"smoke-test request #{record['seq']}: {record['method']} {record['endpoint']} -> "
                              f"HTTP {record['http_status']}",
            '_request': {'method': record['method'], 'endpoint': record['endpoint'], 'query': record['query'],
                         'body': record['request_body'], 'http_status': record['http_status'],
                         'label': record['label'], 'latency_ms': record['latency_ms'],
                         'rate_limit_headers': record['rate_limit_headers']},
            'data': record['reply'],
        }
        (directory / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        written += 1
    return written


# ============================================================================ entry point

def run_smoke(cfg: Any, options: SmokeOptions, deps: Any) -> int:
    """Build the venue over a :class:`RecordingSession`, run, write the report and fixtures.

    Called by ``cli.cmd_smoke_test`` AFTER the live gates, inside the process locks.
    """
    from .cli import _default_hub_factory, _default_live_venue_factory, _secret_values
    from .ratelimit import RateLimiter
    from .status import write_status

    secrets = tuple(_secret_values(cfg))
    say, error = deps.say, deps.error
    say(f"SMOKE-ТЕСТ LIVE {cfg.currency_pair}, аккаунт {cfg.market['account']}: одна post-only BUY не выше "
        f'индекс −{fmt(options.edge_pct)}%, минимальный объём (не больше {fmt(options.max_notional)} USDT), DMS '
        'включается до создания; затем проверка чтением, отмена, проверка, выключение DMS.')
    if options.dms_fire:
        error(DMS_FIRE_WARNING)
    LOG.warning('SMOKE-TEST LIVE %s account %s options %s', cfg.currency_pair, cfg.market['account'],
                options.as_dict())
    recorder = RecordingSession(deps.http, secrets=secrets)
    venue = None
    smoke: SmokeTest | None = None
    try:
        venue = (deps.live_venue_factory or _default_live_venue_factory)(cfg, RateLimiter.from_config(cfg), recorder)
        smoke = SmokeTest(cfg, venue, recorder, deps.hub_factory or _default_hub_factory, options=options,
                          out=say, err=error, sleep=deps.sleep, signals=bool(getattr(deps, 'signals', False)))
        try:
            code = smoke.run()
        except BaseException as exc:      # run() handles the flow; this is a failure INSIDE its cleanup
            smoke.interrupted = smoke.interrupted or isinstance(exc, KeyboardInterrupt)
            smoke.fatal = smoke.fatal or safe_error(exc)
            smoke.cleanup_failed = True
            smoke.pair_verified_empty = False
            smoke.finished_ms = _now_ms()
            smoke.exit_code = code = EXIT_FAIL
            LOG.error('SMOKE cleanup interrupted: %s', safe_error(exc))
            error(f'Уборка прервана ({safe_error(exc)}): пара НЕ подтверждена пустой; DMS мог остаться '
                  'включённым. Проверьте веб-интерфейс Gate и выполните cancel-only.')
    finally:
        if venue is not None and callable(getattr(venue, 'close', None)):
            try:
                venue.close()
            except Exception:
                pass
        recorder.close()
    stamp = _utc_stamp(smoke.started_ms)
    smoke_dir = Path(cfg.state_dir) / 'smoke'
    report_path: Path | None = smoke_dir / f'smoke_{stamp}.json'
    fixtures_dir: Path | None = smoke_dir / 'fixtures' / stamp
    try:
        count = write_fixtures(recorder.records, fixtures_dir, stamp) if recorder.records else 0
        if not count:
            fixtures_dir = None
    except OSError as exc:
        error(f'Фикстуры не записаны: {type(exc).__name__}')
        fixtures_dir = None
    if not write_status(report_path, smoke.report(fixtures_dir=fixtures_dir)):
        error(f'JSON отчёта НЕ записан: {report_path}')
        report_path = None
        if code == EXIT_PASS:
            code = EXIT_FAIL
    for line in smoke.summary_lines(report_path, fixtures_dir):
        (say if code == EXIT_PASS else error)(line)
    return code
