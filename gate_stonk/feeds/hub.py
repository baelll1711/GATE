"""FeedHub: owns feed workers, immutable snapshots, validation, the trade tape (DESIGN §16).

Port of the reference ``Feeds`` ingestion/threading model (12-chagpt
``spot_portfolio/feeds.py``) without the index computation (that is
``gate_stonk.index``):

* latest ``Book``/``Ticker`` per feed key under an ``RLock``; frozen snapshots, so
  readers copy references under the lock and validate outside it;
* ingestion validation: structure + event age (``Book.check(received, wall_ms,
  max_age, None)``: spread is an index-time check), then regressions
  (``BOOK_EVENT_REGRESSION``; ``BOOK_UPDATE_REGRESSION`` except Bybit ``u == 1``,
  which marks a service restart; equal ids are heartbeat repeats and allowed);
  a rejected update drops the key's previous data too (never reused);
* per-key errors (stored reason codes), transport states ``CONNECTING`` /
  ``DISCONNECTED`` / ``STOPPED``, ``connected`` flags, a ``wake`` event set on every
  change;
* public trade tape of the traded pair (deque 5000, dedup by trade id, local
  strictly increasing ``seq``);
* ``QuoteHealth`` (bounded fault logs, bounded stale-stream reconnects);
* REST refresher: for keys with a ``rest_refresh_after_seconds`` threshold, when the
  newest book/ticker receipt age exceeds it, one ``adapter.rest_snapshot`` at most
  once per ``max(1.0, adapter.rest_min_interval)``, published through the same path.

Cross-transport ordering (deliberate addition): WS and REST snapshots of one key
are separate streams. Update ids are compared only within one transport, and a
snapshot OLDER (event time) than the stored one from the other transport is
dropped without invalidating the newer data. Within one transport any regression
invalidates the key (reference behaviour).
"""
from __future__ import annotations

import dataclasses
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..dec import D
from ..errors import ConfigError, Unsafe
from ..types import SIDES, Book, FxSnapshot, PublicTrade, SourceSnapshot, Ticker
from .base import FX_KEY, GATE_SPOT_KEY, Update
from .fx import FixedFx, fx_needed
from .health import LOG_INTERVAL_SECONDS, QuoteHealth
from .ws import STOP_JOIN_SECONDS, WsWorker, adapter_keys, safe_reason

LOG = logging.getLogger(__name__)

TRADE_HISTORY = 5000
NO_DATA = 'NO_DATA'
UNKNOWN_SOURCE = 'UNKNOWN_SOURCE'
BYBIT_VENUE = 'bybit_linear'
TRANSPORT_STATES = frozenset({'CONNECTING', 'DISCONNECTED'})
TRANSPORTS = ('ws', 'rest')
REFRESH_TICK_SECONDS = 0.25
MIN_REST_INTERVAL_SECONDS = 1.0
# Per-venue REST poll floors (DESIGN §16: Lighter 2 s), applied even when an adapter
# does not declare ``rest_min_interval`` itself.
VENUE_REST_MIN_INTERVAL_SECONDS = {'lighter': 2.0}
TICKER_PRICE_KINDS = ('mark', 'last')


@dataclass(frozen=True)
class KeySpec:
    """Static validation rules of one feed key."""

    key: str
    role: str                         # 'source' | 'spot' | 'fx'
    venue: str
    symbol: str
    max_age: float                    # receipt/event age limit (incl. heartbeat allowance)
    max_spread_pct: Decimal | None    # diagnostics only; ingestion never checks spread
    price_kind: str | None = None     # index sources: mid | microprice | mark | last
    rest_refresh_after: float | None = None   # from config (sources); None -> disabled
    rest_configured: bool = False     # True when the config decided (even None)


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_seconds(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _positive_price(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def effective_max_age(source: Any) -> float:
    """Index freshness limit of a source (DESIGN §4): quiet-book heartbeat allowance."""
    base = float(source.max_age_seconds)
    heartbeat = float(getattr(source, 'heartbeat_seconds', 0) or 0)
    return max(base, heartbeat + 1.0) if heartbeat > 0 else base


class FeedHub:
    """Market data hub. ``start()`` launches workers only when ``start_threads``."""

    TRADE_HISTORY = TRADE_HISTORY

    def __init__(self, cfg: Any, adapters: Sequence[Any] | None = None, start_threads: bool = True, *,
                 connect: Any = None, http: Any = None) -> None:
        self.cfg = cfg
        settings = cfg.index_settings()
        self.settings = settings
        self.fx_settings = settings.fx
        self.timeout = float(cfg.app['request_timeout_seconds'])
        self.base_max_age = float(settings.max_age_seconds)
        self.start_threads = bool(start_threads)
        self._connect = connect
        self._http = http
        self._own_http: Any = None
        market = cfg.market
        self.currency_pair = str(market['currency_pair'])
        self.needs_fx = fx_needed(settings.sources)
        fx_mode = getattr(self.fx_settings, 'mode', None)
        self._fixed_fx = FixedFx(self.fx_settings) if self.needs_fx and fx_mode == 'fixed' else None

        specs: dict[str, KeySpec] = {}
        for source in settings.sources:
            if not getattr(source, 'enabled', True):
                continue
            if source.id in (GATE_SPOT_KEY, FX_KEY) or source.id in specs:
                raise ConfigError(f'index.sources[{source.id}].id: reserved or duplicate feed key')
            specs[source.id] = KeySpec(
                key=source.id, role='source', venue=source.venue, symbol=source.symbol,
                max_age=effective_max_age(source), max_spread_pct=source.max_spread_pct, price_kind=source.price,
                rest_refresh_after=(None if source.rest_refresh_after_seconds is None
                                    else float(source.rest_refresh_after_seconds)),
                rest_configured=True)
        specs[GATE_SPOT_KEY] = KeySpec(key=GATE_SPOT_KEY, role='spot', venue='gate_spot', symbol=self.currency_pair,
                                       max_age=self.base_max_age, max_spread_pct=None)
        if self.needs_fx and fx_mode is not None and fx_mode != 'fixed':
            specs[FX_KEY] = KeySpec(key=FX_KEY, role='fx', venue=self.fx_settings.mode, symbol='USDC/USDT',
                                    max_age=float(self.fx_settings.max_age_seconds),
                                    max_spread_pct=self.fx_settings.max_spread_pct)
        self.specs = specs

        self._adapters: dict[str, Any] = {}
        self._adapter_list: list[Any] = []
        for adapter in adapters or ():
            keys = adapter_keys(adapter)
            for key in keys:
                if key not in specs:
                    raise ValueError(f'adapter key {key!r} is not a configured feed key')
                if key in self._adapters:
                    raise ValueError(f'duplicate adapter for feed key {key!r}')
                self._adapters[key] = adapter
            self._adapter_list.append(adapter)

        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.quote_health = QuoteHealth()
        self._books: dict[str, Book] = {}
        self._tickers: dict[str, Ticker] = {}
        self._origin: dict[tuple[str, str], str] = {}          # (key, 'book'|'ticker') -> transport
        self._updates: dict[tuple[str, str], int] = {}         # (key, transport) -> last update id
        self._errors: dict[str, str] = {}
        self._connected: set[str] = set()
        self._counters: dict[str, dict[str, int]] = {
            key: {'accepted': 0, 'rejected': 0, 'dropped_out_of_order': 0} for key in specs}
        self._rest: dict[str, dict[str, Any]] = {}
        self._tape: deque[PublicTrade] = deque(maxlen=TRADE_HISTORY)
        self._tape_ids: deque[str] = deque()
        self._tape_id_set: set[str] = set()
        # Monotonic across restarts: paper orders persist the tape position at placement.
        self._tape_seq = time.time_ns()
        self._tape_rejected = 0
        self._warned: dict[str, float] = {}
        self._workers: list[WsWorker] = []
        self._refresher: threading.Thread | None = None
        self._stop = threading.Event()
        self._running = False
        self._stopped = False

    # ================================================================ lifecycle
    def start(self) -> None:
        """Idempotent. Launches one WS worker per adapter and the REST refresher
        (only when constructed with ``start_threads=True``)."""
        with self.lock:
            if self._running:
                return
            self._running = True
            self._stopped = False
            self._stop.clear()
        if not self.start_threads:
            return
        workers = [WsWorker(adapter, self, timeout=self.timeout, connect=self._connect)
                   for adapter in self._adapter_list]
        self._workers = workers
        for worker in workers:
            worker.start()
        if self._refresh_plan():
            self._refresher = threading.Thread(target=self._refresh_loop, name='feed-rest-refresher', daemon=True)
            self._refresher.start()

    def stop(self) -> None:
        """Stop workers (closing sockets unblocks recv), then mark every key STOPPED."""
        with self.lock:
            self._stopped = True
            self._running = False
        self._stop.set()
        self.wake.set()
        workers, self._workers = self._workers, []
        for worker in workers:
            worker.request_stop()
        for worker in workers:
            worker.join(STOP_JOIN_SECONDS)
        refresher, self._refresher = self._refresher, None
        if refresher is not None and refresher is not threading.current_thread():
            refresher.join(STOP_JOIN_SECONDS)
        own, self._own_http = self._own_http, None
        if own is not None:
            try:
                own.close()
            except Exception:
                pass
        with self.lock:
            for key in self.specs:
                self._drop(key, 'STOPPED')
            self._connected.clear()
        self.wake.set()

    # ============================================================ sink (workers)
    def publish(self, update: Update) -> None:
        """Validate and store one update. Raises ``Unsafe(code)`` when a book/ticker
        is rejected (the key is invalidated with that code before raising)."""
        if not isinstance(update, Update):
            raise TypeError('FeedHub.publish expects an Update')
        spec = self.specs.get(update.key)
        if spec is None:
            self._warn('unknown_key', 'feed update for an unknown key dropped')
            return
        if update.source not in TRANSPORTS:
            raise ValueError('Update.source must be ws or rest')
        kind = update.kind
        if kind == 'book':
            self._publish_book(spec, update.book, update.source)
        elif kind == 'ticker':
            self._publish_ticker(spec, update.ticker, update.source)
        elif kind == 'trades':
            self._publish_trades(spec, update.trades)
        elif kind == 'error':
            self._publish_error(spec, update.error)
        elif kind in ('subscribed', 'ignore'):
            return
        else:
            raise ValueError('Unknown update kind')

    def invalidate(self, key: str, reason: str) -> None:
        """Drop the key's data and store ``reason`` (transport states, data errors)."""
        if key not in self.specs:
            return
        with self.lock:
            if self._stopped and reason in TRANSPORT_STATES:
                reason = 'STOPPED'
            self._drop(key, reason)
        self.wake.set()

    def set_connected(self, key: str, connected: bool) -> None:
        if key not in self.specs:
            return
        with self.lock:
            if connected and not self._stopped:
                self._connected.add(key)
            else:
                self._connected.discard(key)

    def connection_reset(self, keys: Iterable[str]) -> None:
        self.quote_health.connection_reset(tuple(keys))

    def should_reconnect(self, stream: str, now: float, keys: Iterable[str]) -> bool:
        return self.quote_health.should_reconnect(stream, now, tuple(keys))

    # ================================================================ readers
    def source_snapshot(self, source_id: str) -> SourceSnapshot:
        spec = self.specs.get(source_id)
        if spec is None or spec.role != 'source':
            return SourceSnapshot(source_id, None, None, UNKNOWN_SOURCE, False)
        with self.lock:
            book = self._books.get(source_id)
            ticker = self._tickers.get(source_id)
            error = self._errors.get(source_id)
            connected = source_id in self._connected
        if book is None and ticker is None:
            error = error or NO_DATA
        return SourceSnapshot(source_id, book, ticker, error, connected)

    def fx_snapshot(self) -> FxSnapshot | None:
        """USDT per 1 USDC; None when no enabled source quotes USDC.

        Live modes: the rate is the FX book mid, set only when the book passes
        ``Book.check(now, wall_ms, fx.max_age_seconds, fx.max_spread_pct)``; otherwise
        ``rate`` is None and ``error`` holds the reason (fail closed).
        """
        if not self.needs_fx or self.fx_settings is None:
            return None     # the index builder reports FX_UNAVAILABLE for USDC sources
        if self._fixed_fx is not None:
            return self._fixed_fx.snapshot()
        mode = self.fx_settings.mode
        with self.lock:
            book = self._books.get(FX_KEY)
            error = self._errors.get(FX_KEY)
        if book is None:
            return FxSnapshot(rate=None, mode=mode, book=None, error=error or NO_DATA)
        # Sample the clock AFTER copying the immutable book (reference race rule).
        now, wall_ms = time.monotonic(), int(time.time() * 1000)
        try:
            book.check(now, wall_ms, float(self.fx_settings.max_age_seconds), self.fx_settings.max_spread_pct)
        except Unsafe as exc:
            return FxSnapshot(rate=None, mode=mode, book=book, error=str(exc))
        return FxSnapshot(rate=book.mid, mode=mode, book=book, error=None)

    def spot_book(self) -> Book | None:
        """Latest structurally validated traded book (may be stale: check it)."""
        with self.lock:
            return self._books.get(GATE_SPOT_KEY)

    def spot_book_error(self) -> str | None:
        """Why the traded book is unusable right now (None = fresh and valid)."""
        with self.lock:
            book = self._books.get(GATE_SPOT_KEY)
            error = self._errors.get(GATE_SPOT_KEY)
        if book is None:
            return error or NO_DATA
        now, wall_ms = time.monotonic(), int(time.time() * 1000)
        try:
            book.check(now, wall_ms, self.base_max_age, None)
        except Unsafe as exc:
            return str(exc)
        return None

    def trades_since(self, seq: int) -> list[PublicTrade]:
        """Tape entries with ``seq`` strictly greater than ``seq`` (oldest first)."""
        with self.lock:
            result = []
            for trade in reversed(self._tape):
                if trade.seq <= seq:
                    break
                result.append(trade)
        result.reverse()
        return result

    def last_trade_seq(self) -> int:
        """Tape boundary now: every trade already received has ``seq <=`` this."""
        with self.lock:
            return self._tape_seq

    def diagnostics(self) -> dict:
        """Local per-key health: no I/O, no invalidation, no trading decisions.

        ``connected`` reports the presence of a transport, not fresh quotes.
        Cross-source skew, outliers and FX depeg remain index checks.
        """
        with self.lock:
            books = dict(self._books)
            tickers = dict(self._tickers)
            errors = dict(self._errors)
            origin = dict(self._origin)
            connected = set(self._connected)
            counters = {key: dict(row) for key, row in self._counters.items()}
            rest = {key: dict(row) for key, row in self._rest.items()}
            tape_size, tape_seq, tape_rejected = len(self._tape), self._tape_seq, self._tape_rejected
            workers = {worker.name: worker for worker in self._workers}
        now, wall_ms = time.monotonic(), int(time.time() * 1000)
        try:
            faults = self.quote_health.snapshot()
        except Exception as exc:
            faults = {'diagnostic_error': type(exc).__name__}
        result: dict[str, dict] = {}
        for key in sorted(self.specs):
            spec = self.specs[key]
            row: dict[str, Any] = {'ok': False, 'error': None, 'received_age_seconds': None,
                                   'event_age_seconds': None, 'max_age_seconds': spec.max_age,
                                   'connected': key in connected, 'source': None}
            book, ticker = books.get(key), tickers.get(key)
            use_ticker = spec.price_kind in TICKER_PRICE_KINDS
            primary = ticker if use_ticker else book
            if primary is None:
                row['error'] = errors.get(key, NO_DATA)
            else:
                row['received_age_seconds'] = round(now - primary.received, 6)
                row['event_age_seconds'] = round((wall_ms - primary.event_ms) / 1000, 6)
                row['source'] = origin.get((key, 'ticker' if use_ticker else 'book'))
                row['error'] = self._check(spec, book, ticker, now, wall_ms, errors.get(key))
                row['ok'] = row['error'] is None
            row['counters'] = counters.get(key, {})
            if key in faults:
                row['quote_diagnostics'] = faults[key]
            if key in rest:
                row['rest_refresh'] = rest[key]
            worker = workers.get(key)
            if worker is not None:
                try:
                    row['stream'] = worker.status()
                except Exception as exc:
                    row['stream'] = {'diagnostic_error': type(exc).__name__}
            if spec.role == 'spot':
                row['trade_tape'] = {'size': tape_size, 'last_seq': tape_seq, 'rejected_rows': tape_rejected}
            result[key] = row
        return result

    # ============================================================ REST refresher
    def refresh_once(self, http: Any = None) -> list[str]:
        """One refresher pass (the thread calls it every 0.25 s). Returns polled keys."""
        polled: list[str] = []
        for key, adapter, after, min_interval in self._refresh_plan():
            with self.lock:
                if self._stopped:
                    break
                newest = self._newest_receipt(key)
                state = self._rest.setdefault(key, {'after_seconds': after, 'min_interval_seconds': min_interval,
                                                    'polls': 0, 'failures': 0, 'last_poll_monotonic': None,
                                                    'last_error': None})
            now = time.monotonic()
            if newest is not None and now - newest <= after:
                continue
            last = state['last_poll_monotonic']
            if last is not None and now - last < min_interval:
                continue
            with self.lock:
                state['last_poll_monotonic'] = now
                state['polls'] += 1
            polled.append(key)
            client = http if http is not None else self._http_client()
            try:
                updates = adapter.rest_snapshot(client, self.timeout, now, int(time.time() * 1000))
            except Exception as exc:
                reason = safe_reason(exc)
                with self.lock:
                    state['failures'] += 1
                    state['last_error'] = reason
                self._warn('rest:' + key, f'{key} REST refresh failed: {reason}')
                continue
            error = None
            for update in updates or ():
                if not isinstance(update, Update) or update.key != key:
                    error = 'INVALID_ADAPTER_OUTPUT'
                    continue
                try:
                    self.publish(dataclasses.replace(update, source='rest'))
                except Unsafe as exc:
                    error = str(exc)
            with self.lock:
                state['last_error'] = error
                if error is not None:
                    state['failures'] += 1
        return polled

    def _refresh_plan(self) -> list[tuple[str, Any, float, float]]:
        plan = []
        for key, adapter in self._adapters.items():
            if not callable(getattr(adapter, 'rest_snapshot', None)):
                continue
            spec = self.specs[key]
            after = spec.rest_refresh_after if spec.rest_configured else getattr(adapter, 'rest_refresh_after_seconds', None)
            if after is None:
                continue
            try:
                after = float(after)
                min_interval = max(MIN_REST_INTERVAL_SECONDS, float(getattr(adapter, 'rest_min_interval', 0) or 0),
                                   VENUE_REST_MIN_INTERVAL_SECONDS.get(spec.venue, 0.0))
            except (TypeError, ValueError):
                continue
            if not after > 0 or not math.isfinite(after) or not math.isfinite(min_interval):
                continue
            plan.append((key, adapter, after, min_interval))
        return plan

    def _refresh_loop(self) -> None:
        while not self._stop.wait(REFRESH_TICK_SECONDS):
            try:
                self.refresh_once()
            except Exception as exc:
                self._warn('refresher', f'REST refresher pass failed: {type(exc).__name__}')

    def _http_client(self) -> Any:
        if self._http is not None:
            return self._http
        if self._own_http is None:
            import requests
            self._own_http = requests.Session()
        return self._own_http

    def _newest_receipt(self, key: str) -> float | None:
        times = [item.received for item in (self._books.get(key), self._tickers.get(key)) if item is not None]
        return max(times) if times else None

    # ================================================================ ingestion
    def _publish_book(self, spec: KeySpec, book: Any, transport: str) -> None:
        wall_ms = int(time.time() * 1000)
        try:
            self._validate_book(book, wall_ms, spec.max_age)
        except Unsafe as exc:
            self._reject(spec, str(exc), wall_ms, getattr(book, 'event_ms', None))
            raise
        except (TypeError, ValueError, AttributeError, ArithmeticError) as exc:
            self._reject(spec, 'INVALID_BOOK_SCHEMA', wall_ms, None)
            raise Unsafe('INVALID_BOOK_SCHEMA') from exc
        key = spec.key
        code = None
        with self.lock:
            if self._stopped:
                return
            previous = self._books.get(key)
            if previous is not None and book.event_ms < previous.event_ms:
                if self._origin.get((key, 'book')) != transport:
                    # Older than the other transport's snapshot: keep the newer one.
                    self._counters[key]['dropped_out_of_order'] += 1
                    return
                code = 'BOOK_EVENT_REGRESSION'
            if code is None and book.update_id is not None:
                last = self._updates.get((key, transport))
                # Heartbeat snapshots may repeat an id. A Bybit snapshot with u=1
                # marks a service restart that resets the update sequence.
                if last is not None and book.update_id < last and not (spec.venue == BYBIT_VENUE and book.update_id == 1):
                    code = 'BOOK_UPDATE_REGRESSION'
            if code is None:
                self._books[key] = book
                self._origin[(key, 'book')] = transport
                if book.update_id is not None:
                    self._updates[(key, transport)] = book.update_id
                self._errors.pop(key, None)
                self._counters[key]['accepted'] += 1
        if code is not None:
            self._reject(spec, code, wall_ms, book.event_ms)
            raise Unsafe(code)
        self._observe(spec, None, book.received, wall_ms, book.event_ms)
        self.wake.set()

    def _publish_ticker(self, spec: KeySpec, ticker: Any, transport: str) -> None:
        wall_ms = int(time.time() * 1000)
        try:
            self._validate_ticker(ticker, wall_ms, spec.max_age)
        except Unsafe as exc:
            self._reject(spec, str(exc), wall_ms, getattr(ticker, 'event_ms', None))
            raise
        except (TypeError, ValueError, AttributeError, ArithmeticError) as exc:
            self._reject(spec, 'INVALID_TICKER', wall_ms, None)
            raise Unsafe('INVALID_TICKER') from exc
        key = spec.key
        code = None
        with self.lock:
            if self._stopped:
                return
            previous = self._tickers.get(key)
            if previous is not None and ticker.event_ms < previous.event_ms:
                if self._origin.get((key, 'ticker')) != transport:
                    self._counters[key]['dropped_out_of_order'] += 1
                    return
                code = 'TICKER_EVENT_REGRESSION'
            if code is None:
                self._tickers[key] = ticker
                self._origin[(key, 'ticker')] = transport
                self._errors.pop(key, None)
                self._counters[key]['accepted'] += 1
        if code is not None:
            self._reject(spec, code, wall_ms, ticker.event_ms)
            raise Unsafe(code)
        self._observe(spec, None, ticker.received, wall_ms, ticker.event_ms)
        self.wake.set()

    def _publish_error(self, spec: KeySpec, error: Any) -> None:
        code = error if isinstance(error, str) and error else 'INVALID_UPDATE'
        self._reject(spec, code, int(time.time() * 1000), None)

    def _publish_trades(self, spec: KeySpec, rows: Any) -> None:
        if spec.role != 'spot':
            self._warn('trades:' + spec.key, 'public trades are only recorded for the traded pair')
            return
        received = time.monotonic()
        rejected = 0
        with self.lock:
            if self._stopped:
                return
            for row in rows if isinstance(rows, (tuple, list)) else ():
                try:
                    trade_id, price, amount, side, create_ms = self._trade_fields(row)
                except (KeyError, TypeError, ValueError, AttributeError, ArithmeticError):
                    rejected += 1
                    continue
                if trade_id in self._tape_id_set:
                    continue     # resubscription snapshots repeat recent trades
                self._remember_trade_id(trade_id)
                self._tape_seq += 1
                self._tape.append(PublicTrade(trade_id=trade_id, price=price, amount=amount, side=side,
                                              create_time_ms=create_ms, received=received, seq=self._tape_seq))
            if not isinstance(rows, (tuple, list)):
                rejected += 1
            self._tape_rejected += rejected
        if rejected:
            # The tape only drives paper simulation: bad rows never invalidate books.
            self._warn('trade_rows', 'Ignoring malformed public trade rows')
        self.wake.set()

    @staticmethod
    def _trade_fields(row: Any) -> tuple[str, Decimal, Decimal, str, int]:
        if not isinstance(row, dict):
            raise TypeError('trade row')
        raw_id = row['id']
        if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)):
            raise TypeError('trade id')
        trade_id = str(raw_id).strip()
        if not trade_id:
            raise ValueError('trade id')
        price, amount = D(row['price']), D(row['amount'])
        if price <= 0 or amount <= 0:
            raise ValueError('trade size')
        side = row['side']
        if side not in SIDES:
            raise ValueError('trade side')
        raw_ms = row['create_time_ms']
        if isinstance(raw_ms, bool):
            raise TypeError('trade time')
        number = D(raw_ms)          # Gate: decimal string "1648725035923.0" -> floor ms
        if number < 0:
            raise ValueError('trade time')
        create_ms = int(number)
        return trade_id, price, amount, side, create_ms

    def _remember_trade_id(self, trade_id: str) -> None:
        self._tape_ids.append(trade_id)
        self._tape_id_set.add(trade_id)
        while len(self._tape_ids) > TRADE_HISTORY:
            self._tape_id_set.discard(self._tape_ids.popleft())

    @staticmethod
    def _validate_book(book: Any, wall_ms: int, max_age: float) -> None:
        if not isinstance(book, Book):
            raise Unsafe('INVALID_BOOK_SCHEMA')
        for side in (book.bids, book.asks):
            if not isinstance(side, tuple):
                raise Unsafe('INVALID_BOOK_SCHEMA')
            for level in side:
                if not isinstance(level, tuple) or len(level) != 2:
                    raise Unsafe('INVALID_BOOK_SCHEMA')
        if (not _is_count(book.event_ms) or not _is_seconds(book.received)
                or (book.update_id is not None and not _is_count(book.update_id))):
            raise Unsafe('INVALID_BOOK_SCHEMA')
        # Receipt age is zero by construction: structure, crossing and event age only.
        book.check(book.received, wall_ms, max_age, None)

    @staticmethod
    def _validate_ticker(ticker: Any, wall_ms: int, max_age: float) -> None:
        if not isinstance(ticker, Ticker):
            raise Unsafe('INVALID_TICKER')
        if ticker.mark is None and ticker.last is None:
            raise Unsafe('INVALID_TICKER')
        for value in (ticker.mark, ticker.last):
            if value is not None and not _positive_price(value):
                raise Unsafe('INVALID_TICKER')
        if not _is_count(ticker.event_ms) or not _is_seconds(ticker.received):
            raise Unsafe('INVALID_TICKER')
        ticker.check(ticker.received, wall_ms, max_age)

    # ================================================================ helpers
    def _check(self, spec: KeySpec, book: Book | None, ticker: Ticker | None, now: float, wall_ms: int,
               stored: str | None) -> str | None:
        """The first failing freshness/structure/spread check (diagnostics only)."""
        try:
            if spec.price_kind in TICKER_PRICE_KINDS:
                if ticker is None:
                    return stored or NO_DATA
                ticker.check(now, wall_ms, spec.max_age)
                if getattr(ticker, spec.price_kind) is None:
                    return 'MISSING_PRICE:' + spec.price_kind
                if spec.max_spread_pct is not None:
                    if book is None:
                        return stored or 'MISSING_BOOK'
                    book.check(now, wall_ms, spec.max_age, spec.max_spread_pct)
                return None
            if book is None:
                return stored or NO_DATA
            book.check(now, wall_ms, spec.max_age, spec.max_spread_pct)
        except Unsafe as exc:
            return str(exc)
        return None

    def _drop(self, key: str, reason: str) -> None:
        """Under the lock: forget data and sequence state of ``key``; store ``reason``."""
        self._books.pop(key, None)
        self._tickers.pop(key, None)
        for kind in ('book', 'ticker'):
            self._origin.pop((key, kind), None)
        for transport in TRANSPORTS:
            self._updates.pop((key, transport), None)
        self._errors[key] = reason

    def _reject(self, spec: KeySpec, code: str, wall_ms: int, event_ms: Any) -> None:
        with self.lock:
            if self._stopped:
                return
            self._drop(spec.key, code)
            self._counters[spec.key]['rejected'] += 1
        if not _is_count(event_ms):
            event_ms = None
        self._observe(spec, code, time.monotonic(), wall_ms, event_ms)
        self.wake.set()

    def _observe(self, spec: KeySpec, reason: str | None, now: float, wall_ms: int, event_ms: int | None) -> None:
        try:
            self.quote_health.observe(spec.key, reason, now, wall_ms, event_ms, spec.max_age, symbol=spec.symbol)
        except Exception:
            pass  # diagnostics never change quote validity

    def _warn(self, topic: str, message: str) -> None:
        now = time.monotonic()
        last = self._warned.get(topic)
        if last is not None and now - last < LOG_INTERVAL_SECONDS:
            return
        self._warned[topic] = now
        LOG.warning('%s', message)
