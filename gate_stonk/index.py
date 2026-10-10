"""Index builder: per-source validation and aggregation (DESIGN §4).

The index is built from STONK perpetual futures on several venues. Every
configured (enabled) source produces an ``IndexComponent`` with an exclusion
reason or ``included=True``; the aggregate is a weighted median (default) or a
weighted mean of the included components, all normalised to USDT first.

Validation order (reason codes verbatim, first failure wins per source):

1. data: ``MISSING_SOURCE:<id>[:<stored error>]``, ``MISSING_PRICE:<kind>``,
   ``INVALID_SOURCE_PRICE``, ``INVALID_SOURCE_TIMESTAMP`` and the ``Book.check`` /
   ``Ticker.check`` codes (``EMPTY_BOOK`` ... ``STALE_QUOTE``,
   ``STALE_EVENT_OR_CLOCK_SKEW``, ``WIDE_SOURCE_SPREAD``). A source with
   ``heartbeat_seconds > 0`` (Bybit ``orderbook.1`` re-sends an unchanged L1
   snapshot every 3 s) gets ``max_age = max(max_age_seconds, heartbeat + 1)``.
2. FX (only USDC-quoted sources): ``FX_UNAVAILABLE:<error>`` or ``FX_DEPEG``.
   ``usdt = native * rate`` where rate is USDT per 1 USDC (multiply).
3. cross-source timestamp skew with the heartbeat allowance:
   ``SOURCE_TIMESTAMP_SKEW``.
4. unweighted-median outlier test: ``SOURCE_OUTLIER`` (two disagreeing sources
   may both drop out; a price disagreement is never voted away silently).
5. rejoin hysteresis: ``REJOIN_PENDING:<n>/<N>``.

Blocking codes of the snapshot: ``REQUIRED_SOURCE_INVALID:<id>:<reason>``,
``INSUFFICIENT_SOURCES:<n>/<min>``, ``NONPOSITIVE_INDEX``.

With ``weighted_median`` any source holding more than 50% of the included
weight IS the index (the outlier test still uses the plain median).

The builder is pure except for the small per-source rejoin state; it never
reads clocks or sockets (``now``/``wall_ms`` are passed in by the caller, who
samples them after copying the immutable snapshots).
"""
from __future__ import annotations

import logging
import math
import statistics
import threading
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from .config import FX_MODES, INDEX_METHODS, PRICE_KINDS, IndexSettings, SourceConfig
from .dec import HUNDRED, ONE, ZERO, D
from .errors import ConfigError, Unsafe
from .types import Book, FxSnapshot, IndexComponent, IndexSnapshot, SourceSnapshot, Ticker

LOG = logging.getLogger(__name__)

QUOTES = ('USDT', 'USDC')
BOOK_PRICE_KINDS = ('mid', 'microprice')
TICKER_PRICE_KINDS = ('mark', 'last')
HEARTBEAT_TRANSPORT_MARGIN_SECONDS = 1.0

# Data/arithmetic problems inside one source's payload exclude only that source.
_DATA_ERRORS = (TypeError, ValueError, AttributeError, ArithmeticError, IndexError)


# --------------------------------------------------------------------------------------------
# Aggregation helpers (pure)
# --------------------------------------------------------------------------------------------

def weighted_median(entries: Iterable[tuple[str, Decimal, Decimal]]) -> Decimal:
    """Weighted median of ``(source_id, price, weight)`` entries.

    Entries are sorted by price (ties by source_id). Walking the cumulative
    normalised weight, the first price where ``cum >= 0.5`` is the median; when
    ``cum == 0.5`` exactly at element k and k is not the last element, the
    result is the mean of elements k and k+1. Comparisons are done as
    ``2 * cum_raw`` vs ``total`` so the exact-half test never suffers from
    rounding of normalised weights (e.g. 1/3).
    """
    ordered = sorted(entries, key=lambda item: (item[1], item[0]))
    if not ordered:
        raise Unsafe('INSUFFICIENT_SOURCES')
    total = sum((weight for _, _, weight in ordered), ZERO)
    if total <= 0:
        raise Unsafe('NONPOSITIVE_WEIGHT')
    cumulative = ZERO
    for k, (_, price, weight) in enumerate(ordered):
        cumulative += weight
        twice = cumulative * 2
        if twice >= total:
            if twice == total and k + 1 < len(ordered):
                return (price + ordered[k + 1][1]) / 2
            return price
    return ordered[-1][1]  # unreachable: the cumulative weight ends at total


def weighted_mean(entries: Iterable[tuple[str, Decimal, Decimal]]) -> Decimal:
    """``sum(w_i * p_i) / sum(w_i)`` over ``(source_id, price, weight)`` entries."""
    items = list(entries)
    if not items:
        raise Unsafe('INSUFFICIENT_SOURCES')
    total = sum((weight for _, _, weight in items), ZERO)
    if total <= 0:
        raise Unsafe('NONPOSITIVE_WEIGHT')
    return sum((weight * price for _, price, weight in items), ZERO) / total


AGGREGATORS: dict[str, Callable[[Iterable[tuple[str, Decimal, Decimal]]], Decimal]] = {
    'weighted_median': weighted_median,
    'weighted_mean': weighted_mean,
}


def effective_max_age(max_age_seconds: float, heartbeat_seconds: float) -> float:
    """Freshness window of a source: a quiet-but-live heartbeat book stays valid until
    its next heartbeat is due plus a transport margin (reference quiet-Bybit rule)."""
    if heartbeat_seconds > 0:
        return max(max_age_seconds, heartbeat_seconds + HEARTBEAT_TRANSPORT_MARGIN_SECONDS)
    return max_age_seconds


# --------------------------------------------------------------------------------------------
# Internal records
# --------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class _Source:
    cfg: SourceConfig
    weight: Decimal
    max_spread_pct: Decimal | None
    max_age: float
    heartbeat_ms: int


@dataclass
class _Work:
    """Mutable per-compute evaluation of one source (converted to IndexComponent at the end)."""

    src: _Source
    reason: str | None = None
    native: Decimal | None = None
    usdt: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    spread_pct: Decimal | None = None
    event_ms: int | None = None
    received_age_s: float | None = None
    effective_ms: int | None = None
    fingerprint: tuple[int, float] | None = None

    def exclude(self, reason: str) -> None:
        if self.reason is None:
            self.reason = reason


@dataclass
class _RejoinState:
    pending: bool = False                      # excluded in the previous compute
    count: int = 0                             # distinct validated snapshots since the last failure
    last_fingerprint: tuple[int, float] | None = None
    included: bool | None = None               # outcome of the previous compute (None = never computed)
    reason: str | None = None                  # reason of the previous compute
    ever_included: bool = False                # hysteresis applies only after the source was once in the index


def _safely(fn: Callable[[], Any]) -> None:
    """Diagnostics/logging never change an index decision."""
    try:
        fn()
    except Exception:  # noqa: BLE001 - logging must never break pricing
        pass


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_seconds(value: Any) -> bool:
    return (isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)
            and math.isfinite(float(value)))


def _positive_decimal(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def _missing(src: _Source, snapshot: SourceSnapshot) -> str:
    error = snapshot.error
    return f'MISSING_SOURCE:{src.cfg.id}:{error}' if error else f'MISSING_SOURCE:{src.cfg.id}'


def _check_clock_fields(data: Book | Ticker) -> None:
    if not _is_int(data.event_ms) or not _is_seconds(data.received):
        raise Unsafe('INVALID_SOURCE_TIMESTAMP')


def _book_top(book: Book) -> tuple[Decimal, Decimal, Decimal] | None:
    """(bid, ask, spread_pct) when the top of book is structurally sound, else None (diagnostics)."""
    try:
        if not book.bids or not book.asks:
            return None
        bid, bid_size = book.bids[0]
        ask, ask_size = book.asks[0]
        if not all(_positive_decimal(x) for x in (bid, bid_size, ask, ask_size)) or bid >= ask:
            return None
        return bid, ask, (ask - bid) / ((bid + ask) / 2) * HUNDRED
    except _DATA_ERRORS:
        return None


# --------------------------------------------------------------------------------------------
# Settings validation
# --------------------------------------------------------------------------------------------

def _seconds(value: Any, key: str, *, positive: bool = True) -> float:
    if not _is_seconds(value):
        raise ConfigError(f'{key}: must be a finite number of seconds')
    value = float(value)
    if (value <= 0) if positive else (value < 0):
        raise ConfigError(f'{key}: must be {"> 0" if positive else ">= 0"}')
    return value


def _decimal(value: Any, key: str) -> Decimal:
    try:
        return D(value)
    except ValueError as exc:
        raise ConfigError(f'{key}: must be a finite number') from exc


def _prepare_sources(settings: IndexSettings) -> tuple[_Source, ...]:
    result: list[_Source] = []
    seen: set[str] = set()
    for i, cfg in enumerate(settings.sources):
        key = f'index.sources[{i}]'
        if not isinstance(cfg, SourceConfig):
            raise ConfigError(f'{key}: must be a SourceConfig')
        if not isinstance(cfg.id, str) or not cfg.id:
            raise ConfigError(f'{key}.id: must be a non-empty string')
        if cfg.id in seen:
            raise ConfigError(f'{key}.id: duplicate source id {cfg.id!r}')
        seen.add(cfg.id)
        if not cfg.enabled:
            if cfg.required:
                raise ConfigError(f'{key}.required: a required source must be enabled')
            continue
        if cfg.price not in PRICE_KINDS:
            raise ConfigError(f'{key}.price: must be one of {", ".join(PRICE_KINDS)}')
        if cfg.quote not in QUOTES:
            raise ConfigError(f'{key}.quote: must be USDT or USDC')
        if not isinstance(cfg.required, bool):
            raise ConfigError(f'{key}.required: must be true or false')
        weight = _decimal(cfg.weight, f'{key}.weight')
        if weight <= 0:
            raise ConfigError(f'{key}.weight: must be > 0')
        if cfg.max_spread_pct is None:
            if cfg.price in BOOK_PRICE_KINDS:
                raise ConfigError(f'{key}.max_spread_pct: required for price {cfg.price}')
            max_spread = None
        else:
            max_spread = _decimal(cfg.max_spread_pct, f'{key}.max_spread_pct')
            if max_spread <= 0:
                raise ConfigError(f'{key}.max_spread_pct: must be > 0')
        max_age = _seconds(cfg.max_age_seconds, f'{key}.max_age_seconds')
        heartbeat = _seconds(cfg.heartbeat_seconds, f'{key}.heartbeat_seconds', positive=False)
        result.append(_Source(cfg=cfg, weight=weight, max_spread_pct=max_spread,
                              max_age=effective_max_age(max_age, heartbeat),
                              heartbeat_ms=int(round(heartbeat * 1000))))
    return tuple(result)


# --------------------------------------------------------------------------------------------
# IndexBuilder
# --------------------------------------------------------------------------------------------

class IndexBuilder:
    """Validates index sources and aggregates the included ones (DESIGN §4)."""

    def __init__(self, settings: IndexSettings):
        if not isinstance(settings, IndexSettings):
            raise ConfigError('index: IndexSettings required')
        if settings.method not in AGGREGATORS or settings.method not in INDEX_METHODS:
            raise ConfigError(f'index.method: must be one of {", ".join(INDEX_METHODS)}')
        if not _is_int(settings.min_sources) or settings.min_sources < 1:
            raise ConfigError('index.min_sources: must be an integer >= 1')
        if not _is_int(settings.rejoin_snapshots) or settings.rejoin_snapshots < 0:
            raise ConfigError('index.rejoin_snapshots: must be an integer >= 0')
        skew = _seconds(settings.max_timestamp_skew_seconds, 'index.max_timestamp_skew_seconds')
        self._skew_ms = int(round(skew * 1000))
        self._max_deviation = _decimal(settings.max_source_deviation_pct, 'index.max_source_deviation_pct')
        if self._max_deviation <= 0:
            raise ConfigError('index.max_source_deviation_pct: must be > 0')
        self._basis = _decimal(settings.basis_offset_pct, 'index.basis_offset_pct')
        self.settings = settings
        self._sources = _prepare_sources(settings)
        self._needs_fx = any(src.cfg.quote == 'USDC' for src in self._sources)
        fx = settings.fx
        self._fx_mode = 'fixed'
        self._fx_fixed_rate = ONE
        self._fx_max_depeg = ZERO
        self._fx_max_spread: Decimal | None = None
        self._fx_max_age = 0.0
        if fx is None:
            if self._needs_fx:
                raise ConfigError('index.fx: required when a source quotes USDC')
        else:
            if fx.mode not in FX_MODES:
                raise ConfigError(f'index.fx.mode: must be one of {", ".join(FX_MODES)}')
            self._fx_mode = fx.mode
            self._fx_fixed_rate = _decimal(fx.usdc_to_usdt, 'index.fx.usdc_to_usdt')
            if self._fx_fixed_rate <= 0:
                raise ConfigError('index.fx.usdc_to_usdt: must be > 0')
            self._fx_max_depeg = _decimal(fx.max_depeg_pct, 'index.fx.max_depeg_pct')
            if self._fx_max_depeg < 0:
                raise ConfigError('index.fx.max_depeg_pct: must be >= 0')
            if fx.max_spread_pct is not None:
                self._fx_max_spread = _decimal(fx.max_spread_pct, 'index.fx.max_spread_pct')
            if fx.mode != 'fixed':
                self._fx_max_age = _seconds(fx.max_age_seconds, 'index.fx.max_age_seconds')
        self._lock = threading.Lock()
        self._state: dict[str, _RejoinState] = {src.cfg.id: _RejoinState() for src in self._sources}
        self._computes = 0
        self._last_at_ms: int | None = None
        self._last_ok: bool | None = None
        self._last_reason: str | None = None

    # ---------------------------------------------------------------- public API

    def compute(self, snapshots: Mapping[str, SourceSnapshot], fx: FxSnapshot | None,
                now: float, wall_ms: int) -> IndexSnapshot:
        """Validate every enabled source and aggregate the included ones.

        ``now`` is ``time.monotonic()`` and ``wall_ms`` is ``int(time.time()*1000)``,
        both sampled by the caller after copying the snapshots.
        """
        snapshots = snapshots or {}
        works = [self._evaluate_source(src, snapshots.get(src.cfg.id), now, wall_ms)
                 for src in self._sources]
        fx_rate, fx_reason = self._evaluate_fx(fx, now, wall_ms) if self._needs_fx else (None, None)
        self._apply_fx(works, fx_rate, fx_reason)
        self._apply_skew(works)
        self._apply_outliers(works)
        with self._lock:
            self._apply_rejoin(works)
            snapshot = self._finish(works, fx_rate, fx_reason, wall_ms)
            self._record(snapshot)
        return snapshot

    def status(self) -> dict:
        """JSON-safe rejoin state per source plus the last result summary."""
        with self._lock:
            sources = {}
            for source_id, state in self._state.items():
                fingerprint = None
                if state.last_fingerprint is not None:
                    fingerprint = {'event_ms': int(state.last_fingerprint[0]),
                                   'received': float(state.last_fingerprint[1])}
                sources[source_id] = {
                    'included': state.included,
                    'reason': state.reason,
                    'rejoin_pending': state.pending,
                    'rejoin_count': int(state.count),
                    'rejoin_required': int(self.settings.rejoin_snapshots),
                    'last_fingerprint': fingerprint,
                }
            return {
                'method': self.settings.method,
                'min_sources': int(self.settings.min_sources),
                'rejoin_snapshots': int(self.settings.rejoin_snapshots),
                'computes': int(self._computes),
                'last_at_ms': self._last_at_ms,
                'last_ok': self._last_ok,
                'last_reason': self._last_reason,
                'sources': sources,
            }

    # ---------------------------------------------------------------- step 1: per-source data

    def _evaluate_source(self, src: _Source, snapshot: Any, now: float, wall_ms: int) -> _Work:
        work = _Work(src)
        cfg = src.cfg
        if not isinstance(snapshot, SourceSnapshot):
            work.reason = f'MISSING_SOURCE:{cfg.id}'
            return work
        if snapshot.source_id != cfg.id:
            work.reason = f'MISSING_SOURCE:{cfg.id}:SOURCE_ID_MISMATCH'
            return work
        try:
            self._describe(work, snapshot.book)
            if cfg.price in BOOK_PRICE_KINDS:
                self._validate_book_price(work, snapshot, now, wall_ms)
            else:
                self._validate_ticker_price(work, snapshot, now, wall_ms)
        except Unsafe as exc:
            work.exclude(str(exc) or 'UNSAFE')
        except _DATA_ERRORS as exc:
            work.exclude(f'INVALID_SOURCE_DATA:{type(exc).__name__}')
        return work

    @staticmethod
    def _describe(work: _Work, book: Book | None) -> None:
        """Diagnostic top of book (bid/ask/spread) even for sources that end up excluded."""
        if book is None:
            return
        top = _book_top(book)
        if top is not None:
            work.bid, work.ask, work.spread_pct = top

    @staticmethod
    def _stamp(work: _Work, data: Book | Ticker, now: float) -> None:
        _check_clock_fields(data)
        work.event_ms = int(data.event_ms)
        work.received_age_s = round(now - float(data.received), 6)
        work.fingerprint = (int(data.event_ms), float(data.received))

    def _validate_book_price(self, work: _Work, snapshot: SourceSnapshot, now: float, wall_ms: int) -> None:
        src = work.src
        book = snapshot.book
        if book is None:
            raise Unsafe(_missing(src, snapshot))
        self._stamp(work, book, now)
        if work.bid is not None:  # structurally sound top: price shown even if the check fails
            work.native = book.mid if src.cfg.price == 'mid' else book.microprice
        book.check(now, wall_ms, src.max_age, src.max_spread_pct)
        if not _positive_decimal(work.native):
            raise Unsafe('INVALID_SOURCE_PRICE')

    def _validate_ticker_price(self, work: _Work, snapshot: SourceSnapshot, now: float, wall_ms: int) -> None:
        src = work.src
        ticker = snapshot.ticker
        if ticker is None:
            raise Unsafe(_missing(src, snapshot))
        self._stamp(work, ticker, now)
        value = ticker.mark if src.cfg.price == 'mark' else ticker.last
        if value is None:
            raise Unsafe(f'MISSING_PRICE:{src.cfg.price}')
        if not _positive_decimal(value):
            raise Unsafe('INVALID_SOURCE_PRICE')
        work.native = value
        ticker.check(now, wall_ms, src.max_age)
        if src.max_spread_pct is not None:
            book = snapshot.book
            if book is None:
                raise Unsafe(_missing(src, snapshot))
            _check_clock_fields(book)
            book.check(now, wall_ms, src.max_age, src.max_spread_pct)

    # ---------------------------------------------------------------- step 2: FX

    def _evaluate_fx(self, fx: FxSnapshot | None, now: float, wall_ms: int) -> tuple[Decimal | None, str | None]:
        """Rate (USDT per 1 USDC) and the failure code (None when usable)."""
        mode = self._fx_mode
        if mode == 'fixed':
            rate: Any = self._fx_fixed_rate
        else:
            if not isinstance(fx, FxSnapshot):
                return None, 'FX_UNAVAILABLE:MISSING_FX'
            if fx.mode != mode:
                return None, 'FX_UNAVAILABLE:FX_MODE_MISMATCH'
            if fx.error:
                return None, f'FX_UNAVAILABLE:{fx.error}'
            if fx.book is not None:
                try:
                    _check_clock_fields(fx.book)
                    fx.book.check(now, wall_ms, self._fx_max_age, self._fx_max_spread)
                except Unsafe as exc:
                    return None, f'FX_UNAVAILABLE:{exc}'
                except _DATA_ERRORS as exc:
                    return None, f'FX_UNAVAILABLE:INVALID_FX_DATA:{type(exc).__name__}'
            rate = fx.rate
            if rate is None:
                return None, 'FX_UNAVAILABLE:MISSING_RATE'
        if not _positive_decimal(rate):
            return None, 'FX_UNAVAILABLE:INVALID_RATE'
        if abs(rate - ONE) * HUNDRED > self._fx_max_depeg:
            return rate, 'FX_DEPEG'
        return rate, None

    @staticmethod
    def _apply_fx(works: list[_Work], rate: Decimal | None, fx_reason: str | None) -> None:
        for work in works:
            if work.src.cfg.quote == 'USDT':
                work.usdt = work.native
                continue
            # USDC: MULTIPLY by USDT-per-USDC.
            if fx_reason is None and rate is not None:
                if work.native is not None:
                    work.usdt = work.native * rate
            else:
                work.exclude(fx_reason or 'FX_UNAVAILABLE:MISSING_RATE')

    # ---------------------------------------------------------------- step 3: timestamp skew

    def _apply_skew(self, works: list[_Work]) -> None:
        survivors = [work for work in works if work.reason is None]
        if not survivors:
            return
        newest = max(work.event_ms for work in survivors)
        for work in survivors:
            work.effective_ms = min(work.event_ms + work.src.heartbeat_ms, newest)
            if newest - work.effective_ms > self._skew_ms:
                work.exclude('SOURCE_TIMESTAMP_SKEW')

    # ---------------------------------------------------------------- step 4: outliers

    def _apply_outliers(self, works: list[_Work]) -> None:
        survivors = [work for work in works if work.reason is None]
        if not survivors:
            return
        center = statistics.median([work.usdt for work in survivors])
        if not _positive_decimal(center):
            for work in survivors:
                work.exclude('SOURCE_OUTLIER')
            return
        # One pass against one median: every disagreeing source drops out together.
        flagged = [work for work in survivors
                   if abs(work.usdt / center - ONE) * HUNDRED > self._max_deviation]
        for work in flagged:
            work.exclude('SOURCE_OUTLIER')

    # ---------------------------------------------------------------- step 5: rejoin hysteresis

    def _apply_rejoin(self, works: list[_Work]) -> None:
        required = self.settings.rejoin_snapshots
        for work in works:
            state = self._state[work.src.cfg.id]
            if work.reason is not None:
                # A source that has never been part of the index (e.g. still CONNECTING at start) joins on its
                # first validated snapshot; rejoin hysteresis protects against a flapping source only.
                state.pending = state.ever_included
                state.count = 0
                state.last_fingerprint = None
                continue
            if not state.pending:
                state.ever_included = True
                continue
            if work.fingerprint != state.last_fingerprint:
                state.last_fingerprint = work.fingerprint
                state.count += 1
            if state.count >= required:
                state.pending = False
                state.count = 0
                state.last_fingerprint = None
            else:
                work.reason = f'REJOIN_PENDING:{state.count}/{required}'

    # ---------------------------------------------------------------- steps 6-8: blocking, aggregate

    def _finish(self, works: list[_Work], fx_rate: Decimal | None, fx_reason: str | None,
                wall_ms: int) -> IndexSnapshot:
        components = tuple(self._component(work) for work in works)
        included = [work for work in works if work.reason is None]
        reason: str | None = None
        index: Decimal | None = None
        center: Decimal | None = None
        for work in works:
            if work.src.cfg.required and work.reason is not None:
                reason = f'REQUIRED_SOURCE_INVALID:{work.src.cfg.id}:{work.reason}'
                break
        if reason is None and len(included) < self.settings.min_sources:
            reason = f'INSUFFICIENT_SOURCES:{len(included)}/{self.settings.min_sources}'
        if reason is None:
            entries = [(work.src.cfg.id, work.usdt, work.src.weight) for work in included]
            try:
                value = AGGREGATORS[self.settings.method](entries)
                shifted = value * (ONE + self._basis / HUNDRED)
            except Unsafe as exc:
                reason = str(exc) or 'INDEX_AGGREGATION_ERROR'
            except _DATA_ERRORS as exc:
                reason = f'INDEX_AGGREGATION_ERROR:{type(exc).__name__}'
            else:
                if _positive_decimal(value) and _positive_decimal(shifted):
                    index, center = value, shifted
                else:
                    reason = 'NONPOSITIVE_INDEX'
        return IndexSnapshot(ok=reason is None, index=index, center=center, reason=reason,
                             components=components, fx_rate=fx_rate, fx_reason=fx_reason,
                             method=self.settings.method, at_ms=wall_ms, n_included=len(included))

    @staticmethod
    def _component(work: _Work) -> IndexComponent:
        cfg = work.src.cfg
        return IndexComponent(
            source_id=cfg.id, venue=cfg.venue, symbol=cfg.symbol, quote=cfg.quote, price_kind=cfg.price,
            weight=work.src.weight, required=cfg.required, included=work.reason is None, reason=work.reason,
            native_price=work.native, usdt_price=work.usdt, bid=work.bid, ask=work.ask,
            spread_pct=work.spread_pct, event_ms=work.event_ms, received_age_s=work.received_age_s,
            effective_ms=work.effective_ms)

    # ---------------------------------------------------------------- bookkeeping / logging

    def _record(self, snapshot: IndexSnapshot) -> None:
        """Update per-source outcome and log transitions (never affects the result)."""
        self._computes += 1
        previous_ok, previous_reason = self._last_ok, self._last_reason
        self._last_at_ms, self._last_ok, self._last_reason = snapshot.at_ms, snapshot.ok, snapshot.reason
        for component in snapshot.components:
            state = self._state[component.source_id]
            was = state.included
            state.included, state.reason = component.included, component.reason
            if was is not False and not component.included:
                _safely(lambda c=component: LOG.warning(
                    'Источник индекса %s исключён: %s', c.source_id, c.reason))
            elif was is False and component.included:
                _safely(lambda c=component: LOG.info('Источник индекса %s снова в индексе', c.source_id))
        if previous_ok is not False and not snapshot.ok:
            _safely(lambda: LOG.warning('Индекс заблокирован: %s', snapshot.reason))
        elif previous_ok is False and snapshot.ok:
            _safely(lambda: LOG.info('Индекс восстановлен (было: %s)', previous_reason))


def index_snapshot_to_dict(snapshot: IndexSnapshot) -> dict:
    """JSON-safe view of an IndexSnapshot (Decimal -> str) for status/store/watch."""

    def text(value: Decimal | None) -> str | None:
        return None if value is None else format(value, 'f')

    return {
        'ok': snapshot.ok,
        'index': text(snapshot.index),
        'center': text(snapshot.center),
        'reason': snapshot.reason,
        'fx_rate': text(snapshot.fx_rate),
        'fx_reason': snapshot.fx_reason,
        'method': snapshot.method,
        'at_ms': int(snapshot.at_ms),
        'n_included': int(snapshot.n_included),
        'components': [{
            'source_id': c.source_id, 'venue': c.venue, 'symbol': c.symbol, 'quote': c.quote,
            'price_kind': c.price_kind, 'weight': text(c.weight), 'required': c.required,
            'included': c.included, 'reason': c.reason, 'native_price': text(c.native_price),
            'usdt_price': text(c.usdt_price), 'bid': text(c.bid), 'ask': text(c.ask),
            'spread_pct': text(c.spread_pct), 'event_ms': c.event_ms,
            'received_age_s': c.received_age_s, 'effective_ms': c.effective_ms,
        } for c in snapshot.components],
    }
