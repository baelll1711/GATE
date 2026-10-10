"""Trading engine: one Gate spot pair, a ladder of post-only slots per side (DESIGN §11, §17).

Port of the reference ``spot_portfolio.engine.Coordinator`` with these changes:

* one pair; the reference "leg" (symbol, side) becomes a ladder SLOT ``side:level``
  with its own client id, reservation, tolerance, reprice settings, reject backoff;
* prices come from :class:`gate_stonk.index.IndexBuilder` (STONK perpetual futures)
  and :func:`gate_stonk.pricing.ladder_targets`; ``safety.min_new_edge_pct`` is the
  buffer for new orders, ``safety.hard_min_edge_pct`` the fast guard of resting ones;
* Gate venue semantics (``text`` client ids, ``finish_as`` / ``POC_FILL_IMMEDIATELY``
  post-only rejections, ``countdown_cancel_all`` DMS, optional amend keeping the
  same order id, iceberg orders);
* several creates of one pass go out as one ``place_batch`` (chunks of 10).

Money invariants (unchanged from the reference):

* a reservation is the DB order row written BEFORE anything is sent and is released
  ONLY by the terminal protocol of the reconciliation (:mod:`gate_stonk.reconcile`);
  cancel acks, cancel rejects and amend acks never release money;
* ``NotSent`` closes a reservation at once (nothing can exist), ``Rejected`` too;
  anything else after a request may have left keeps it (``unknown``) and the slot is
  never re-sent until a reconciliation binds the order or proves its absence;
* one USDT amount is never reserved twice, tokens reserved for a SELL are never sold
  twice and SELL proceeds never fund BUYs (``quantity``);
* the DMS (``countdown_cancel_all``) is renewed only after a complete reconciliation
  in the same step and never while cancels are unconfirmed or new orders unreconciled.

Diagnostics, Telegram and status never change trading decisions (``_safely``).
"""
from __future__ import annotations

import dataclasses
import json
import logging
import time
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping, Sequence

from .config import Config, LevelConfig
from .dec import HUNDRED, ZERO, D, floor_qty, fmt, usd
from .errors import (BudgetWait, NotSent, RateLimited, ReadError, ReadInterrupted, Rejected, TransientUnsafe,
                     UnknownResult, Unsafe)
from .index import IndexBuilder, index_snapshot_to_dict
from .ledger import signed
from .marks import IndexTrail, resolve_marks
from .pricing import (adverse_boundary, coupled_buy_ceiling, coupled_sell_floor, edge_pct, hard_guard_violation,
                      ladder_targets, level_bounds, maker_limit, post_only_ok, profit_floor_price)
from .ratelimit import ORDER, PRIVATE_READ
from .reconcile import Reconciler
from .sizing import iceberg_visible, meets_minimums, requested_size
from .store import ACTIVE_STATES, FINAL_STATES, Store, utc_day_start_ms, utc_text
from .types import (SIDES, Balance, Book, CancelRequest, IndexSnapshot, LevelTarget, PairMeta, PlaceRequest,
                    PlaceResult, VenueOrder)
from .venue import (CANCEL_OUTCOMES, NOT_SENT_LABEL, POST_ONLY_LABEL, BudgetNotSent, base36, is_post_only_rejection,
                    json_safe, make_text, side_allowed)

LOG = logging.getLogger(__name__)

VERSION = '0.1.0'


class UnknownCreate(Unsafe):
    """A create (or amend) may exist on the exchange: keep its reservation and every
    other slot, stop creating in this step and let the next complete reconciliation
    bind it by text (or prove its absence after the request expiry)."""


ROUTINE = frozenset({'REPRICE', 'PREMIUM_RANGE_CHANGED', 'LIMIT_REDUCTION', 'MODIFY', 'AMEND'})
RETRY = (5, 10, 20, 40, 60)                     # cancel retry schedule, seconds
ACTION_RESERVE = 20                              # signed actions kept free under the hourly cap
INDEX_CACHE_SECONDS = 0.05                       # index recomputed at most every 50 ms
OBSERVATION_MIN_MS = 150                         # index_obs sampling while orders exist
ACCOUNT_CHECK_SECONDS = 600.0                    # live: account mode / whitelist re-check
PRUNE_EVERY_SECONDS = 3600.0
INDEX_RETENTION_MS = 27 * 3_600_000              # keep >= 26 h of index observations
BATCH_CREATE_MAX = 10                            # Gate: <= 10 orders per batch create
POC_FINISH_AS = 'poc'
AUTH_LABELS = frozenset({'INVALID_KEY', 'INVALID_SIGNATURE', 'REQUEST_EXPIRED', 'IP_FORBIDDEN', 'READ_ONLY',
                         'FORBIDDEN', 'ACCOUNT_LOCKED', 'INVALID_CREDENTIALS', 'AUTHENTICATION_FAILED',
                         'MISSING_REQUIRED_HEADER'})
RATE_LIMIT_LABELS = frozenset({'TOO_MANY_REQUESTS', 'TOO_FAST'})
REVIEW_CODES = ('FOREIGN_FILL', 'FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT')
AMEND_OPEN = ('sent', 'unknown')


def _priority(reason: str | None) -> int:
    """Cancel-reason priority: stops 2, routine 0, everything else 1 (reference)."""
    if reason is None:
        return -1
    if reason.startswith(('STOP_', 'PROCESS_SHUTDOWN', 'CANCEL_ONLY')):
        return 2
    return 0 if reason in ROUTINE else 1


def _opposite(side: str) -> str:
    return 'sell' if side == 'buy' else 'buy'


class Engine(Reconciler):
    """Single-pair ladder engine; paper and live share all logic (DESIGN §11)."""

    def __init__(self, cfg: Config, store: Store, venue: Any, hub: Any, index_builder: IndexBuilder,
                 notifier: Any = None, *, live: bool, meta: PairMeta, limiter: Any = None):
        live = bool(live)
        if bool(getattr(venue, 'live', live)) != live:
            raise ValueError('VENUE_MODE_MISMATCH')
        if bool(cfg.live) != live:
            raise ValueError('CONFIG_MODE_MISMATCH')
        if not isinstance(meta, PairMeta):
            raise ValueError('PairMeta required')
        self.cfg, self.store, self.venue, self.hub = cfg, store, venue, hub
        self.index_builder = index_builder
        self.notifier = notifier
        self.live = live
        self.meta = meta
        self.limiter = limiter if limiter is not None else getattr(getattr(venue, 'client', None), 'limiter', None)
        self.pair = cfg.currency_pair
        self.base = cfg.market['base']
        self.quote = cfg.market['quote']
        if (meta.currency_pair, meta.base, meta.quote) != (self.pair, self.base, self.quote):
            raise Unsafe('SPOT_PAIR_MAPPING_MISMATCH')
        store.initialize(cfg.identity(), self._now_ms())
        self._levels: dict[str, tuple[LevelConfig, ...]] = {side: cfg.levels(side) for side in SIDES}
        self._level_map = {side: {level.id: level for level in self._levels[side]} for side in SIDES}
        self._sources = tuple(cfg.sources())
        self._max_age = float(cfg.index['max_age_seconds'])
        self._settle_seconds = float(cfg.safety['terminal_settle_seconds'])
        self._hard_min_edge = D(cfg.safety['hard_min_edge_pct'])
        self._min_new_edge = D(cfg.safety['min_new_edge_pct'])
        self._fee_pct = D(cfg.portfolio['fee_estimate_pct'])

        self.recovering = bool(store.orders())
        self.stop_reason: str | None = None
        self.last_reason = 'STARTING'
        self.state_changed_ms = self._now_ms()
        self.index: IndexSnapshot | None = None
        self._index_mono = -1e18
        self.balances: dict[str, Balance] = {}
        self.open_snapshot: dict[str, VenueOrder] = {}
        self.last_sync_ms = 0
        self.dms_deadline_ms = int(store.meta('dms_deadline_ms', 0) or 0)
        self.last_dms = 0.0
        self.targets: dict[str, LevelTarget] = {}
        self.pricing: dict[str, dict] = {}
        self.slot_states: dict[str, str] = {}
        self._slot_notes: dict[str, str] = {}
        self.startup_sizes: dict[str, dict] = {}
        self.clock_skew_ms: int | None = None
        self.account_info: dict | None = None
        self.counters = {key: 0 for key in (
            'steps', 'orders_sent', 'creates_accepted', 'creates_unknown', 'rejects', 'post_only_rejects',
            'not_sent', 'cancel_requests', 'cancels_sent', 'cancels_rejected', 'cancels_unknown', 'closed',
            'fills', 'amends', 'amends_unknown', 'dms_renewals', 'dms_disarms', 'order_trades_reads')}
        self._account_complete = False
        self._new_since_sync = False
        self._unsynced_creates = False
        self._read_failed_since: float | None = None
        self._budget_wait_since: float | None = None
        self._in_checkpoint = False
        self._cancel_sync_generation = 0
        self._cancel_sync_covered = 0
        self._cancel_sync_due: float | None = None
        self._terminal_recheck: dict[str, float] = {}
        self._reject_backoff: dict[str, tuple[int, float, int]] = {}
        self._poc_backoff_applied: set[str] = set()
        self._last_closed_slot: dict[str, tuple[str, float]] = {}
        self._mismatch: tuple[tuple[Decimal, int], float] | None = None
        self._trail = IndexTrail()
        self._pending_marks: list[tuple[str, int, tuple[int, ...]]] = []
        self._last_obs_ms = 0
        self._last_obs_at: int | None = None
        self._guard_seen: dict[str, tuple[float, int]] = {}
        self._text_counter = 0
        self._sync_seq = 0
        self._fills_watermark: int | None = None
        self._last_clock_check: float | None = None
        self._last_account_check: float | None = None
        self._last_prune: float | None = None
        self._last_step_mono = 0.0
        self._startup_sizes_done = False
        self._order_notices: dict[str, tuple[float, int]] = {}
        self._errors: dict[str, str] = {}
        venue.before_read = self._before_read

    # =================================================================== small helpers
    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _q(self) -> float:
        return float(self.cfg.app['request_timeout_seconds'])

    def _e(self) -> float:
        return float(self.cfg.app['request_expiry_seconds'])

    def _cap(self) -> int:
        return int(self.cfg.safety['max_signed_actions_per_hour'])

    def _fee_factor(self) -> Decimal:
        return 1 + D(self.cfg.portfolio['fee_reserve_pct']) / HUNDRED

    def _level(self, side: str, level_id: str) -> LevelConfig | None:
        return self._level_map.get(side, {}).get(level_id)

    @staticmethod
    def _amend_open(o: Mapping) -> bool:
        return o.get('amend_state') in AMEND_OPEN

    def _prices(self, o: Mapping) -> list[Decimal]:
        """Every price the order may rest at (old and amend price while unresolved)."""
        prices = [o['price']]
        if self._amend_open(o) and o.get('amend_price') is not None and o['amend_price'] != o['price']:
            prices.append(o['amend_price'])
        return prices

    def _reserve_price(self, o: Mapping) -> Decimal:
        """BUY reservations use the highest possible price; SELLs reserve base only."""
        prices = self._prices(o)
        return max(prices) if o['side'] == 'buy' else min(prices)

    @staticmethod
    def _remaining(o: Mapping) -> Decimal:
        return max(ZERO, o['qty'] - (o.get('filled_qty') or ZERO))

    def _version_ms(self, o: Mapping) -> int:
        """Creation time of the current price version (an amend starts a new one)."""
        stamps = [int(o['created_ms'])] + [int(until) for _price, until in o.get('prior_prices') or ()]
        return max(stamps)

    def _safely(self, function: Callable, *args: Any, **kwargs: Any) -> Any:
        """Reporting, diagnostics and simulation never stop trading or safety logic."""
        try:
            return function(*args, **kwargs)
        except (BudgetWait, ReadInterrupted):
            raise
        except Exception as exc:
            name = getattr(function, '__name__', 'helper')
            self._errors[name] = type(exc).__name__
            try:
                LOG.warning('%s failed: %s', name, type(exc).__name__)
            except Exception:
                pass
            return None

    def _diag(self, event: str, **fields: Any) -> None:
        """Local observation only (log line): never Telegram, never a decision."""
        try:
            LOG.info('%s %s', event, json.dumps(json_safe(dict(mode='live' if self.live else 'paper', **fields)),
                                               ensure_ascii=True, separators=(',', ':'), allow_nan=False))
        except Exception:
            pass

    def event(self, key: str, text: str) -> None:
        """Telegram event (Russian); paper events are prefixed. Never raises."""
        text = ('' if self.live else '[PAPER] ') + text
        try:
            if self.notifier is not None:
                self.notifier.event(key, text)
            else:
                self.store.outbox_add(key, text, self._now_ms())
        except Exception as exc:
            try:
                LOG.warning('notification %s failed: %s', key.split(':', 1)[0], type(exc).__name__)
            except Exception:
                pass

    def _state(self, reason: str, *, notify: bool = False) -> str:
        if reason != 'REST_BUDGET_WAIT':
            self._budget_wait_since = None
        if reason != self.last_reason:
            LOG.info('ENGINE_STATE %s', reason)
            if notify:
                self.event('state:' + str(time.time_ns()), f'{self.pair}: состояние бота {reason}.')
            self.last_reason = reason
            self.state_changed_ms = self._now_ms()
        return reason

    def request_stop(self, reason: str = 'STOP_TRADING') -> None:
        self.stop_reason = reason
        self._diag('STOP_REQUESTED', reason=reason)

    def clear_stop(self, reason: str) -> bool:
        """Lift an internal stop that has been resolved (e.g. a transient startup check)."""
        if self.stop_reason != reason:
            return False
        self.stop_reason = None
        self.event('resumed:' + str(time.time_ns()),
                   f'{self.pair}: причина остановки устранена ({reason}); торговля возобновляется.')
        return True

    # =================================================================== stops
    def _global_stop(self) -> str | None:
        if self.store.external_activity():
            return 'STOP_ACCOUNT_REVIEW'
        if self.stop_reason:
            return self.stop_reason
        try:
            if self.cfg.stop_trading_path().exists():
                return 'STOP_TRADING'
        except OSError:
            return 'STOP_TRADING_FILE_UNREADABLE'
        return None

    def _side_stop(self, side: str) -> str | None:
        name = 'STOP_' + side.upper()
        try:
            if self.cfg.stop_side_path(side).exists():
                return name
        except OSError:
            return name + '_FILE_UNREADABLE'
        if not self.cfg.side_enabled(side):
            return 'SIDE_DISABLED'
        return None

    def _row_stop(self, o: Mapping) -> str | None:
        return self._side_stop(o['side']) or (None if self._level(o['side'], o['level']) else 'LEVEL_REMOVED')

    def _working(self) -> list[dict]:
        return [o for o in self.store.orders() if not o['cancel_ack'] and not o['terminal_status']]

    def _unknown_cancels(self) -> list[dict]:
        return [o for o in self.store.orders() if o['cancel_reason'] and not o['cancel_ack']]

    def retry_cancels_now(self) -> None:
        """Startup only: make every unconfirmed cancel due again immediately."""
        for o in self._unknown_cancels():
            self.store.update_order(o['text'], next_cancel_ms=0)

    def _telegram_ok(self) -> bool:
        if self.notifier is None:
            return False
        try:
            return bool(self.notifier.healthy())
        except Exception:
            return False

    # =================================================================== market data
    def _index(self, force: bool = False) -> IndexSnapshot:
        mono = time.monotonic()
        if (not force and self.index is not None and 0 <= mono - self._index_mono <= INDEX_CACHE_SECONDS):
            return self.index
        try:
            snapshots = {source.id: self.hub.source_snapshot(source.id) for source in self._sources}
            fx = self.hub.fx_snapshot()
            snap = self.index_builder.compute(snapshots, fx, time.monotonic(), self._now_ms())
        except Exception as exc:
            snap = IndexSnapshot(False, None, None, 'INDEX_UNAVAILABLE:' + type(exc).__name__, (), None, None,
                                 str(self.cfg.index.get('method')), self._now_ms(), 0)
        self.index, self._index_mono = snap, mono
        return snap

    def _spot(self) -> tuple[Book | None, str | None]:
        """Traded book and its blocking code (``SPOT_BOOK:<code>``) or None."""
        try:
            book = self.hub.spot_book()
        except Exception as exc:
            return None, 'SPOT_BOOK:' + type(exc).__name__
        if book is None:
            try:
                error = self.hub.spot_book_error()
            except Exception:
                error = None
            return None, 'SPOT_BOOK:' + str(error or 'NO_DATA')
        try:
            book.check(time.monotonic(), self._now_ms(), self._max_age, None)
        except Unsafe as exc:
            return book, 'SPOT_BOOK:' + str(exc)
        except Exception as exc:
            return book, 'SPOT_BOOK:' + type(exc).__name__
        return book, None

    def _observe(self, snap: IndexSnapshot | None, book: Book | None = None, *, force: bool = False) -> None:
        """Compact index observation (SQLite + in-memory trail for execution marks)."""
        if snap is None:
            return
        now_ms = self._now_ms()
        if not force and now_ms - self._last_obs_ms < OBSERVATION_MIN_MS:
            return
        if snap.at_ms == self._last_obs_at:
            return
        self._last_obs_ms = now_ms
        self._last_obs_at = snap.at_ms
        if snap.ok and snap.index is not None:
            self._safely(self._trail.observe, int(snap.at_ms), snap.index)
        bid = ask = None
        if book is not None and book.bids and book.asks:
            bid, ask = book.bid, book.ask
        components = None
        try:
            components = index_snapshot_to_dict(snap)['components']
        except Exception:
            components = None
        self._safely(self.store.record_index, int(snap.at_ms), snap.index if snap.ok else None,
                     snap.center if snap.ok else None, bool(snap.ok), snap.reason, components=components,
                     spot_bid=bid, spot_ask=ask)

    def sample_index(self) -> None:
        """Loop tick: sample the index every >= 150 ms while an own order exists and
        settle due execution marks / markouts. Never stops the loop."""
        try:
            now_ms = self._now_ms()
            if (self.store.orders() or self._pending_marks) and now_ms - self._last_obs_ms >= OBSERVATION_MIN_MS:
                snap = self._index()
                book, _error = self._spot()
                self._observe(snap, book)
            if self._pending_marks:
                self._safely(self._resolve_marks)
            mono = time.monotonic()
            if self._last_prune is None or mono - self._last_prune >= PRUNE_EVERY_SECONDS:
                self._last_prune = mono
                self._safely(self.store.prune_index, max(0, now_ms - INDEX_RETENTION_MS))
        except (BudgetWait, ReadInterrupted):
            raise
        except Exception as exc:
            LOG.warning('sample_index failed: %s', type(exc).__name__)

    def _resolve_marks(self) -> None:
        now_ms = self._now_ms()
        rows: list[tuple] = []
        keep = []
        for fill_key, at_ms, horizons in self._pending_marks:
            found, remaining = resolve_marks(self._trail.at, fill_key, at_ms, now_ms, horizons)
            rows.extend(found)
            if remaining:
                keep.append((fill_key, at_ms, remaining))
        self._pending_marks = keep
        if rows:
            self.store.record_marks(rows)

    # =================================================================== cancellation
    def cancel_orders(self, orders: Iterable[Mapping], reason: str | Mapping[str, str], *,
                      context: Mapping[str, Any] | None = None) -> bool:
        """One cancel batch for every due order; retries 5/10/20/40/60 s with priority
        escalation. Results never release a reservation."""
        now = self._now_ms()
        due: list[dict] = []
        for original in orders:
            o = self.store.order(original['text'])
            if o is None or o['state'] in FINAL_STATES or o['cancel_ack']:
                continue
            row_reason = reason.get(o['text']) if isinstance(reason, Mapping) else reason
            if not row_reason:
                continue
            old = o['cancel_reason']
            if now < int(o['next_cancel_ms'] or 0) and _priority(row_reason) <= _priority(old):
                continue
            chosen = row_reason if _priority(row_reason) >= _priority(old) else old
            attempts = int(o['cancel_attempts'] or 0) + 1
            self.store.update_order(o['text'], state='cancel_pending', cancel_reason=chosen,
                                    cancel_attempts=attempts, last_cancel_ms=now,
                                    next_cancel_ms=now + RETRY[min(attempts - 1, len(RETRY) - 1)] * 1000)
            due.append(self.store.order(o['text']))
        if not due:
            return False
        self.store.action('cancel_batch', now, self._cap(), emergency=True)
        for o in due:
            guard = self._guard_seen.get(o['text'])
            self._diag('ORDER_CANCEL_REQUEST', text=o['text'], slot=o['slot'], order_id=o['order_id'],
                       reason=o['cancel_reason'], attempt=o['cancel_attempts'], price=o['price'],
                       remaining=self._remaining(o), target=(context or {}).get(o['text']),
                       first_adverse_observed_ms=guard[1] if guard else None,
                       age_seconds=round((now - int(o['created_ms'])) / 1000, 3))
        try:
            results = self.venue.cancel_many([CancelRequest(o['text'], o['order_id']) for o in due])
        except Exception as exc:     # never crash: the batch outcome is simply unknown
            LOG.warning('cancel batch failed: %s', type(exc).__name__)
            results = {}
        if not isinstance(results, Mapping):
            results = {}
        self.counters['cancel_requests'] += len(due)
        for o in due:
            result = results.get(o['text'], 'unknown')
            if result not in CANCEL_OUTCOMES:
                result = 'unknown'
            self.counters['cancels_' + result] += 1
            self.store.set_meta('cancel_rejected:' + o['text'], result == 'rejected')
            if result == 'sent':
                self.store.update_order(o['text'], cancel_ack=True)
                self._cancel_sync_generation += 1
                if self._cancel_sync_due is None:
                    self._cancel_sync_due = time.monotonic()
            self._diag('ORDER_CANCEL_RESULT', text=o['text'], reason=o['cancel_reason'], result=result,
                       attempt=o['cancel_attempts'], reservation_released=False)
            if result != 'sent' or o['cancel_reason'] not in ROUTINE:
                self.event(f"cancel:{o['text']}:{o['cancel_attempts']}",
                           f"{self.base} {o['side'].upper()} [{o['level']}]: отмена {o['cancel_reason']}; "
                           f"ответ {result}. Резерв сохраняется до сверки.")
        return True

    # =================================================================== preflight
    def _guard(self, o: Mapping, center: Decimal, origin: str) -> str | None:
        """Fast price guard of a resting order (no reads): hard min edge first, then the
        level's adverse band-tolerance boundary (capped by the hard guard)."""
        side = o['side']
        prices = self._prices(o)
        code = None
        if any(hard_guard_violation(side, price, center, self._hard_min_edge) for price in prices):
            code = 'HARD_MIN_EDGE'
        else:
            level = self._level(side, o['level'])
            if level is not None:
                try:
                    boundary = adverse_boundary(side, center, self.meta, level, self._hard_min_edge)
                except Unsafe:
                    boundary = None     # band undefined: hard guard only; manage decides (§19)
                if boundary is not None and any((price > boundary) if side == 'buy' else (price < boundary)
                                                for price in prices):
                    code = 'PREMIUM_RANGE_CHANGED'
        if code is not None and o['text'] not in self._guard_seen:
            self._guard_seen[o['text']] = (time.monotonic(), self._now_ms())
            self._diag('ORDER_PRICE_GUARD', text=o['text'], slot=o['slot'], code=code, price=o['price'],
                       center=center, origin=origin)
        return code

    def preflight(self, origin: str = 'loop') -> str | None:
        """Local guard every tick and before every REST read: STOP files, index, spot
        book, hard guard, adverse band, pending cancel retries, DMS margin. No account
        reads; one cancel batch. Returns the dominant reason or None."""
        active = self.store.orders()
        live_texts = {o['text'] for o in active}
        for text in list(self._guard_seen):
            if text not in live_texts:
                self._guard_seen.pop(text, None)
        reason = self._global_stop()
        if reason:
            self.cancel_orders(active, reason)
            return reason
        if self.recovering and active:
            self.cancel_orders(active, 'RECOVERY')
            return 'RECOVERY'
        reasons: dict[str, str] = {}
        context: dict[str, Any] = {}
        for o in active:
            stop = self._row_stop(o)
            if stop:
                reasons[o['text']] = stop
        working = [o for o in active if not o['cancel_ack'] and not o['terminal_status']
                   and not o['cancel_reason'] and o['text'] not in reasons]
        if working:
            snap = self._index()
            book, book_error = self._spot()
            if not snap.ok:
                for o in working:
                    reasons[o['text']] = 'BAD_MARKET:' + str(snap.reason)
            elif book_error:
                for o in working:
                    reasons[o['text']] = 'BAD_MARKET:' + book_error
            else:
                self._observe(snap, book)
                for o in working:
                    code = self._guard(o, snap.center, origin)
                    if code:
                        reasons[o['text']] = code
                        context[o['text']] = {'center': snap.center, 'origin': origin}
        for o in active:
            if o['cancel_reason'] and not o['cancel_ack'] and not o['terminal_status']:
                reasons.setdefault(o['text'], o['cancel_reason'])
        deadline = False
        exposed = [o for o in active if not o['cancel_ack'] and not o['terminal_status']]
        if exposed and self.dms_deadline_ms:
            margin = (self._q() + max(self._q(), self._e()) + 0.5) * 1000
            if self.dms_deadline_ms - self._now_ms() <= margin:
                for o in exposed:
                    if _priority(reasons.get(o['text'])) < 1:
                        reasons[o['text']] = 'REST_READ_DMS_DEADLINE'
                deadline = True
        if reasons:
            self.cancel_orders([o for o in active if o['text'] in reasons], reasons, context=context)
        if deadline:
            return 'REST_READ_DMS_DEADLINE'
        if reasons:
            return max(reasons.values(), key=_priority)
        return None

    def _before_read(self) -> None:
        """Checkpoint installed on the venue: runs before EVERY physical read."""
        if self._in_checkpoint:
            return
        self._in_checkpoint = True
        try:
            self.preflight('before_rest_read')
            now = self._now_ms()
            for o in self.store.orders():
                if (o['cancel_reason'] and not o['cancel_ack'] and not o['terminal_status']
                        and not self.store.meta('cancel_rejected:' + o['text'], False)
                        and self.dms_deadline_ms > now):
                    raise ReadInterrupted(o['cancel_reason'])
        finally:
            self._in_checkpoint = False

    # =================================================================== step
    def _renewal_due(self) -> bool:
        refresh = float(self.cfg.safety['dms_refresh_seconds'])
        return bool(self._working()) and (not self.last_dms or time.monotonic() - self.last_dms >= refresh)

    def _admit_reads(self) -> None:
        """Whole-step admission for the planned reads (urgent steps are not delayed)."""
        if self.limiter is None:
            return
        if self._global_stop() or self.recovering or self._unknown_cancels() or self._renewal_due():
            return
        accounts = '/spot/accounts' if self.cfg.market['account'] == 'spot' else '/unified/accounts'
        for group, cost in (('/spot/orders', 2), ('/spot/my_trades', 1), (accounts, 1)):
            bucket = f'{PRIVATE_READ}:{group}'
            try:
                wait = self.limiter.wait(bucket, cost)
            except ValueError:
                continue
            if wait > 0:
                raise BudgetWait(wait, bucket)

    def _clock_check(self) -> None:
        mono = time.monotonic()
        every = float(self.cfg.safety['clock_check_seconds'])
        if self._last_clock_check is not None and mono - self._last_clock_check < every:
            return
        before = self._now_ms()
        server = int(self.venue.server_time_ms())
        after = self._now_ms()
        skew = server - (before + after) // 2
        self.clock_skew_ms = skew
        if abs(skew) > int(self.cfg.safety['max_clock_skew_ms']):
            raise Unsafe(f'CLOCK_SKEW:{skew}')
        self._last_clock_check = mono

    def _account_check(self) -> None:
        mono = time.monotonic()
        if self._last_account_check is not None and mono - self._last_account_check < ACCOUNT_CHECK_SECONDS:
            return
        info = self.venue.account_check()
        self.account_info = info if isinstance(info, dict) else None
        problems = (info or {}).get('problems') if isinstance(info, dict) else None
        if problems:
            raise Unsafe(str(problems[0]))
        self._last_account_check = mono

    def _paper_tick(self) -> None:
        """Paper only: let the simulator consume the public tape if it exposes a hook."""
        if self.live:
            return
        hook = getattr(self.venue, 'simulate', None)
        if callable(hook):
            hook()

    def step(self) -> str:
        """One full cycle: preflight, reconciliation, management, creation."""
        self._last_step_mono = time.monotonic()
        self._account_complete = False
        self._slot_notes = {}
        self.counters['steps'] += 1
        try:
            self._index(force=True)
            self.preflight('step')
            self._admit_reads()
            self._clock_check()
            if self.live:
                self._account_check()
            self._safely(self._paper_tick)
            self._sync(settlement_only=bool(self._global_stop()))
            self._read_failed_since = None
            if not self.store.external_activity():
                self._safely(self._startup_size_notice)
                self._safely(self._maybe_summary)
            stop = self._global_stop()
            if stop:
                self._disarm_if_empty()
                if stop == 'STOP_ACCOUNT_REVIEW' and not self.store.orders() and self._account_complete:
                    self.event('account_review_settled',
                               'Собственные заявки сверены и закрыты. Обнаружена посторонняя торговля: учёт позиции '
                               'требует проверки, новые заявки заблокированы. Состояние заявок и DMS — в статусе.')
                return self._state(stop, notify=True)
            if self.recovering:
                return self._state('RECOVERY')
            if self.live and self.cfg.safety['require_telegram'] and not self._telegram_ok():
                self.cancel_orders(self.store.orders(), 'TELEGRAM_UNAVAILABLE')
                self._disarm_if_empty()
                return self._state('TELEGRAM_UNAVAILABLE', notify=True)
            self._manage_existing()
            if self._unknown_cancels():
                return self._state('CANCEL_RECONCILIATION')
            if self._working():
                self._renew()
            self._create_all()
            self._disarm_if_empty()
            return self._state(self._global_stop() or 'RUNNING')
        except BudgetWait as exc:
            if self._budget_wait_since is None:
                self._budget_wait_since = time.monotonic()
            try:
                self.preflight('budget_wait')
            except Exception:
                pass
            self._diag('REST_BUDGET_WAIT', bucket=exc.bucket, wait_seconds=exc.wait_seconds)
            return self._state('REST_BUDGET_WAIT')
        except ReadInterrupted as exc:
            return self._state(exc.reason)
        except UnknownCreate as exc:
            # No ladder-wide cancel: the DMS still covers the slot and is extended only
            # after a complete reconciliation that includes it.
            return self._state(str(exc), notify=True)
        except (ReadError, TransientUnsafe) as exc:
            return self._read_failure(exc)
        except Rejected as exc:            # an explicit refusal of a READ (labelled 4xx)
            if exc.label in AUTH_LABELS:
                self.request_stop('GATE_AUTH:' + str(exc.label))
            return self._read_failure(exc)
        except Unsafe as exc:
            code = str(exc) or type(exc).__name__
            if code in REVIEW_CODES:
                self.store.require_account_review(code, self._now_ms())
            self.cancel_orders(self.store.orders(), code)
            if self._account_complete:
                self._safely(self._disarm_if_empty)
            return self._state(code, notify=True)
        except UnknownResult:
            self.cancel_orders(self.store.orders(), 'DMS_OR_ACTION_UNKNOWN')
            return self._state('DMS_OR_ACTION_UNKNOWN', notify=True)
        except NotSent as exc:             # a read refused locally before sending
            return self._read_failure(exc)

    def _read_failure(self, exc: BaseException) -> str:
        if isinstance(exc, RateLimited) and self.limiter is not None:
            try:
                self.limiter.rate_limited(PRIVATE_READ)
            except Exception:
                pass
        self._diag('ACCOUNT_READ_PROBLEM', error=str(exc)[:120] or type(exc).__name__)
        if self._read_failed_since is None:
            self._read_failed_since = time.monotonic()
        if time.monotonic() - self._read_failed_since >= float(self.cfg.app['read_error_grace_seconds']):
            self.cancel_orders(self.store.orders(), 'ACCOUNT_READ_FAILED')
            return self._state('ACCOUNT_READ_FAILED', notify=True)
        return self._state('WAIT_ACCOUNT_RECONCILIATION')

    def next_reconciliation_deadline(self) -> float | None:
        """Monotonic time of the next due reconciliation (local wakeup only)."""
        if self._read_failed_since is not None or self._budget_wait_since is not None:
            return None
        due: list[float] = []
        if self._cancel_sync_generation > self._cancel_sync_covered and self._cancel_sync_due is not None:
            due.append(self._cancel_sync_due)
        now, wall_ms = time.monotonic(), self._now_ms()
        for o in self.store.orders():
            since = o['terminal_since_ms']
            if not o['terminal_status'] or since is None:
                continue
            remaining = max(0.0, self._settle_seconds - (wall_ms - int(since)) / 1000)
            due.append(max(self._terminal_recheck.get(o['text'], now), now + remaining))
        return min(due) if due else None

    def next_step_seconds(self) -> float:
        poll = float(self.cfg.app['poll_seconds'])
        deadline = self.next_reconciliation_deadline()
        if deadline is not None:
            return min(poll, max(0.0, deadline - time.monotonic()))
        if any(o['terminal_status'] or o['cancel_ack'] for o in self.store.orders()):
            return min(poll, 2.5)
        return poll

    # =================================================================== DMS
    def _renew(self, *, creating: bool = False) -> bool:
        if not self.cfg.safety['dead_man_switch']:
            raise Unsafe('DMS_REQUIRED')
        if self._global_stop() or self.recovering or not self._account_complete or self._unknown_cancels():
            return False
        refresh = float(self.cfg.safety['dms_refresh_seconds'])
        due = not self.dms_deadline_ms or not self.last_dms or time.monotonic() - self.last_dms >= refresh
        if not due:
            return True
        if self._new_since_sync:
            return False       # every newly sent order must be reconciled before extending its lifetime
        self.preflight('before_dms')
        if self._global_stop() or self._unknown_cancels():
            return False
        timeout = int(self.cfg.safety['dms_timeout_seconds'])
        now = self._now_ms()
        self.store.action('dms', now, self._cap(), emergency=True)
        began = time.monotonic()
        try:
            trigger = self.venue.countdown(timeout)
        except NotSent as exc:
            self._diag('DMS_NOT_SENT', error=str(exc)[:80])
            return False
        deadline = now + timeout * 1000
        if isinstance(trigger, int) and not isinstance(trigger, bool) and 0 < trigger < deadline:
            deadline = trigger
        self.last_dms = began
        self.dms_deadline_ms = deadline
        self.store.set_meta('dms_deadline_ms', deadline)
        self.counters['dms_renewals'] += 1
        self._diag('DMS_RENEWED', deadline_ms=deadline, creating=creating)
        # STOP or a stale feed may have arrived while the request was in flight.
        self.preflight('after_dms')
        return not self._global_stop() and not self._unknown_cancels()

    def _disarm_if_empty(self) -> None:
        """Clear the countdown only after a complete reconciliation of an empty account."""
        if (not self.dms_deadline_ms or not self._account_complete or self._new_since_sync
                or self.store.orders() or self.open_snapshot
                or getattr(self.venue, 'cancel_only', False) is True
                or getattr(self.venue, '_cancel_only', False) is True):
            return
        self.store.action('dms_clear', self._now_ms(), self._cap(), emergency=True)
        try:
            self.venue.countdown(0)
        except NotSent as exc:
            self._diag('DMS_CLEAR_NOT_SENT', error=str(exc)[:80])
            return
        self.dms_deadline_ms = 0
        self.last_dms = 0.0
        self.store.set_meta('dms_deadline_ms', 0)
        self.counters['dms_disarms'] += 1
        self._diag('DMS_DISARMED')

    # =================================================================== limits / sizing
    def _active_plus(self, pending: Sequence[Mapping] = ()) -> list[Mapping]:
        return list(self.store.orders()) + list(pending)

    def _available(self, side: str, pending: Sequence[Mapping] = ()) -> Decimal:
        """Balance minus own reservations minus holds not explained by own visible orders.
        Deficits are kept (not clamped) so withdrawals and fee shortfalls stay visible."""
        token = self.quote if side == 'buy' else self.base
        balance = self.balances.get(token) or Balance(ZERO, ZERO)
        factor = self._fee_factor()
        reservations = known_hold = ZERO
        for o in self._active_plus(pending):
            if o['side'] != side:
                continue
            remaining = self._remaining(o)
            if side == 'buy':
                reservations += remaining * self._reserve_price(o) * factor
            else:
                reservations += remaining
            order_id = o.get('order_id')
            seen = self.open_snapshot.get(order_id) if order_id else None
            if seen is not None:
                known_hold += seen.left * seen.price if side == 'buy' else seen.left
        other_hold = max(ZERO, balance.hold - known_hold)
        return balance.total - reservations - other_hold

    def _totals_caps(self, side: str, active: Sequence[Mapping]) -> list[tuple[str, Decimal]]:
        settings = self.cfg.side(side)
        same = [o for o in active if o['side'] == side]
        pending_qty = sum((self._remaining(o) for o in same), ZERO)
        pending_notional = sum((self._remaining(o) * self._reserve_price(o) for o in same), ZERO)
        caps: list[tuple[str, Decimal]] = []
        totals = None
        if settings.get('max_total_qty') is not None:
            totals = self.store.totals(side)
            caps.append(('qty', max(ZERO, D(settings['max_total_qty']) - totals[0] - pending_qty)))
        if settings.get('max_total_notional_usdt') is not None:
            totals = totals or self.store.totals(side)
            caps.append(('notional', max(ZERO, D(settings['max_total_notional_usdt']) - totals[1] - pending_notional)))
        if settings.get('max_daily_notional_usdt') is not None:
            day = self.store.totals(side, since_ms=utc_day_start_ms(self._now_ms()))
            caps.append(('notional', max(ZERO, D(settings['max_daily_notional_usdt']) - day[1] - pending_notional)))
        return caps

    def quantity(self, side: str, level: LevelConfig, price: Decimal, center: Decimal, *,
                 exclude_text: str | None = None, apply_min: bool = True, slack: Decimal = ZERO,
                 pending: Sequence[Mapping] = (), size_cap: bool = True,
                 size_price: Decimal | None = None) -> Decimal:
        """Largest permitted size of one slot at ``price`` with every reservation counted.

        ``exclude_text`` + ``apply_min=False`` + ``slack`` serve the resize check of a
        resting order (its own reservation is added back; index-valued inventory caps
        get ``slack`` of tolerance; a remainder below the minimum is kept).
        ``pending`` are pseudo rows of intents not reserved yet (same pass).
        ``size_cap=False`` drops the configured level size (amend fund/limit check: the
        amount does not change, only the money it needs).
        ``size_price`` sizes the configured level size at that price instead of ``price``
        (resize check of an amended order: its amount was sized at its original price and
        a price-only amend keeps it; money and notional limits still use ``price``)."""
        rows = self.store.orders()
        own = next((o for o in rows if o['text'] == exclude_text), None) if exclude_text else None
        active = [o for o in rows if o['text'] != exclude_text] + list(pending)
        settings = self.cfg.side(side)
        size = requested_size(level, side, price if size_price is None else size_price, self.meta, self._fee_pct,
                              bool(settings.get('compensate_base_fee')) if side == 'buy' else False)
        limits = [size['requested_gross_qty']] if size_cap else []
        pair = self.cfg.pair
        if side == 'buy':
            factor = self._fee_factor()
            available = self._available('buy', pending)
            if own is not None:
                available += self._remaining(own) * self._reserve_price(own) * factor
            available = max(ZERO, available - D(self.cfg.portfolio['cash_reserve_usdt']))
            limits.append(available / (price * factor))
            holding = (self.balances.get(self.base) or Balance(ZERO, ZERO)).total
            pending_qty = sum((self._remaining(o) for o in active if o['side'] == 'buy'), ZERO)
            if pair.get('max_inventory_qty') is not None:
                limits.append(max(ZERO, D(pair['max_inventory_qty']) - holding - pending_qty))
            if pair.get('max_inventory_usdt') is not None:
                cap = D(pair['max_inventory_usdt']) * (1 + slack)
                limits.append(max(ZERO, cap / center - holding - pending_qty))
        else:
            available = self._available('sell', pending)
            if own is not None:
                available += self._remaining(own)
            limits.append(max(ZERO, available - D(pair.get('inventory_reserve_qty') or ZERO)))
        for kind, amount in self._totals_caps(side, active):
            limits.append(amount if kind == 'qty' else amount / price)
        qty = floor_qty(min(limits), self.meta.amount_precision)
        if not apply_min:
            return qty
        if meets_minimums(qty, price, self.meta):
            return qty
        if not size_cap:
            return ZERO
        # Name the cause when the configured size alone is below the exchange minimum.
        size_qty = size['requested_gross_qty']
        others = limits[1:]
        if not meets_minimums(size_qty, price, self.meta) and (
                not others or meets_minimums(floor_qty(min(others), self.meta.amount_precision), price, self.meta)):
            self._size_reason = 'ORDER_SIZE_BELOW_MINIMUM'
        return ZERO

    # =================================================================== profit floor / ceiling
    def _min_profit(self) -> Decimal | None:
        value = self.cfg.side('sell').get('min_profit_pct')
        return None if value is None else D(value)

    def _profit_floor(self, pending: Sequence[Mapping] = ()) -> Decimal | None:
        """SELL floor: cost of what a SELL may deliver + coupling with every own BUY."""
        profit = self._min_profit()
        if profit is None:
            return None
        position = self.store.position()
        initial = self.cfg.pair.get('initial_avg_price')
        holding = (self.balances.get(self.base) or Balance(ZERO, ZERO)).total
        costs = []
        if position.avg_cost is not None:
            costs.append(position.avg_cost)
        if initial is not None and (position.avg_cost is None or holding > position.qty):
            costs.append(D(initial))
        floors = []
        if costs:
            floors.append(profit_floor_price(max(costs), profit, self._fee_pct))
        buys = [max(self._prices(o)) for o in self._active_plus(pending)
                if o['side'] == 'buy' and self._remaining(o) > 0]
        if buys:
            floors.append(coupled_sell_floor(max(buys), profit, self._fee_pct))
        return max(floors) if floors else None

    def _profit_ceiling(self, pending: Sequence[Mapping] = ()) -> Decimal | None:
        """BUY ceiling implied by the lowest own SELL (every level, intents included)."""
        profit = self._min_profit()
        if profit is None:
            return None
        sells = [min(self._prices(o)) for o in self._active_plus(pending)
                 if o['side'] == 'sell' and self._remaining(o) > 0]
        if not sells:
            return None
        return coupled_buy_ceiling(min(sells), profit, self._fee_pct)

    # =================================================================== pricing
    def _own_prices(self, side: str, pending: Sequence[Mapping] = (), ignore: str | None = None) -> list[Decimal]:
        prices: list[Decimal] = []
        for o in self._active_plus(pending):
            if o['side'] == side and (ignore is None or o.get('text') != ignore):
                prices.extend(self._prices(o))
        return prices

    def _post_only_ok(self, side: str, price: Decimal, book: Book | None, *, ignore: str | None = None,
                      pending: Sequence[Mapping] = ()) -> bool:
        return post_only_ok(side, price, book, self._own_prices(_opposite(side), pending, ignore))

    def _own_levels(self, side: str, snapshot_ms: int) -> dict[Decimal, Decimal]:
        """Live only: price -> own VISIBLE qty (iceberg: min(display, remaining); normal:
        original qty) incl. orders closed within 2 s before the book snapshot."""
        active = "('" + "','".join(sorted(ACTIVE_STATES)) + "')"
        with self.store.read() as db:
            rows = [dict(r) for r in db.execute(
                'SELECT text,price,qty,iceberg,filled_qty,amend_state,amend_price FROM orders WHERE side=? AND '
                f"(state IN {active} OR (state='closed' AND terminal_since_ms>=?))",
                (side, int(snapshot_ms) - 2000)).fetchall()]
        result: dict[Decimal, Decimal] = {}
        for row in rows:
            qty = D(row['qty'])
            if row['iceberg'] is not None:
                visible = min(D(row['iceberg']), max(ZERO, qty - D(row['filled_qty'] or 0)))
            else:
                visible = qty
            prices = {D(row['price'])}
            if row['amend_state'] in AMEND_OPEN and row['amend_price'] is not None:
                prices.add(D(row['amend_price']))
            for price in prices:
                result[price] = result.get(price, ZERO) + visible
        return result

    def _side_targets(self, side: str, snap: IndexSnapshot, book: Book,
                      pending: Sequence[Mapping] = ()) -> dict[str, LevelTarget]:
        """Ladder targets of one side (DESIGN §5) against ALL own opposite orders."""
        levels = self._levels[side]
        if not levels:
            return {}

        def refused(code: str) -> dict[str, LevelTarget]:
            result = {level.id: LevelTarget(side, level.id, None, code, False) for level in levels}
            for level_id, target in result.items():
                self.targets[f'{side}:{level_id}'] = target
            return result

        try:
            limit = maker_limit(side, book, self._own_prices(_opposite(side), pending))
        except Unsafe as exc:
            return refused(str(exc))
        floor = ceiling = None
        try:
            if side == 'sell':
                floor = self._profit_floor(pending)
            else:
                ceiling = self._profit_ceiling(pending)
        except Unsafe as exc:
            return refused(str(exc))
        own = self._own_levels(side, book.event_ms) if self.live else {}
        targets = ladder_targets(snap.center, book, self.meta, levels, side, own_levels=own, floor=floor,
                                 ceiling=ceiling, maker_limit=limit,
                                 profit_floor_mode=self.cfg.side('sell').get('profit_floor_mode', 'hold'),
                                 min_new_edge_pct=self._min_new_edge)
        result = {target.level: target for target in targets}
        for target in targets:
            self.targets[f'{side}:{target.level}'] = target
        self.pricing[side] = {'at_ms': snap.at_ms, 'index': snap.index, 'center': snap.center,
                              'maker_limit': limit, 'profit_floor': floor, 'profit_ceiling': ceiling,
                              'book_bid': book.bid, 'book_ask': book.ask}
        self._observe(snap, book, force=True)
        return result

    # =================================================================== management
    def _keep_above_floor(self, o: Mapping, level: LevelConfig, center: Decimal) -> bool:
        """Hold mode: a resting SELL at/above the floor and inside the tolerated band keeps
        its place when the floor moved just above the band."""
        try:
            floor = self._profit_floor()
            _lower, upper = level_bounds(center, self.meta, level)
        except Unsafe:
            return False
        tolerance = D(level.band_tolerance_pct) / HUNDRED
        return floor is not None and o['price'] >= floor and o['price'] <= upper * (1 + tolerance)

    def _action_headroom(self) -> int:
        return self._cap() - self.store.action_count(self._now_ms())

    def _reprice_pace_ok(self) -> bool:
        return self.store.action_count(self._now_ms(), 600_000) < self._cap() / 6

    def _order_rate_room(self, requests: int = 1) -> bool:
        if self.limiter is None:
            return True
        try:
            return self.limiter.wait(ORDER, max(1, int(requests))) <= 0
        except Exception:
            return False

    def _manage_existing(self) -> None:
        """Per resting order (reference order of checks, first hit wins), then amend
        attempts and ONE cancel batch."""
        now = self._now_ms()
        candidates: dict[str, str] = {}
        context: dict[str, Any] = {}
        snap = self._index()
        book, book_error = self._spot()
        side_targets: dict[str, dict[str, LevelTarget]] = {}
        tolerance_slack = D(self.cfg.portfolio['inventory_cap_tolerance_pct']) / HUNDRED
        for o in self.store.orders():
            if o['cancel_reason'] or o['terminal_status']:
                continue
            text, side, slot = o['text'], o['side'], o['slot']
            level = self._level(side, o['level'])
            stop = self._side_stop(side) or (None if level is not None else 'LEVEL_REMOVED')
            if stop:
                candidates[text] = stop
                continue
            if not snap.ok:
                candidates[text] = 'BAD_MARKET:' + str(snap.reason)
                continue
            if book_error:
                candidates[text] = 'BAD_MARKET:' + book_error
                continue
            center = snap.center
            # Limits first (lesson N1): no pricing path may bypass a reduced limit.
            try:
                allowed = self.quantity(side, level, self._reserve_price(o), center, exclude_text=text,
                                        apply_min=False, slack=tolerance_slack,
                                        size_price=o['prior_prices'][0][0] if o['prior_prices'] else o['price'])
            except Unsafe as exc:
                candidates[text] = str(exc)
                continue
            if self._remaining(o) > allowed:
                candidates[text] = 'LIMIT_REDUCTION'
                continue
            prices = self._prices(o)
            if side == 'sell':
                try:
                    floor = self._profit_floor()
                except Unsafe as exc:
                    candidates[text] = str(exc)
                    continue
                if floor is not None and min(prices) < floor:
                    candidates[text] = 'BELOW_PROFIT_FLOOR'
                    continue
            else:
                try:
                    ceiling = self._profit_ceiling()
                except Unsafe as exc:
                    candidates[text] = str(exc)
                    continue
                if ceiling is not None and max(prices) > ceiling:
                    candidates[text] = 'ABOVE_PROFIT_CEILING'
                    continue
            if self._amend_open(o):
                # Quantity and profit protections (and the preflight guard) still ran.
                self._slot_notes[slot] = 'AMEND_RECONCILIATION'
                continue
            if side not in side_targets:
                side_targets[side] = self._side_targets(side, snap, book)
            target = side_targets[side].get(level.id)
            if target is None or target.decision is None:
                code = target.reason if target is not None else 'NO_TARGET'
                if code == 'PROFIT_FLOOR_ABOVE_RANGE' and self._keep_above_floor(o, level, center):
                    continue
                candidates[text] = (code if code in ('PROFIT_FLOOR_ABOVE_RANGE', 'PROFIT_CEILING_BELOW_RANGE')
                                    else 'BAD_MARKET:' + code)
                continue
            decision = target.decision
            tolerance = D(level.band_tolerance_pct) / HUNDRED
            threshold = D(level.reprice_threshold_pct)
            deeper = level.deeper_tolerance_pct
            context[text] = {'price': decision.price, 'center': center, 'lower': decision.lower,
                             'upper': decision.upper, 'reason': target.reason, 'placeable': target.placeable,
                             'change_pct': (decision.price / o['price'] - 1) * HUNDRED}
            if deeper is None:
                if o['price'] < decision.lower * (1 - tolerance) or o['price'] > decision.upper * (1 + tolerance):
                    candidates[text] = 'PREMIUM_RANGE_CHANGED'
                    continue
            else:
                worse = (o['price'] > decision.upper * (1 + tolerance) if side == 'buy'
                         else o['price'] < decision.lower * (1 - tolerance))
                if worse:
                    candidates[text] = 'PREMIUM_RANGE_CHANGED'
                    continue
                if decision.price > o['price'] if side == 'buy' else decision.price < o['price']:
                    threshold = max(threshold, D(deeper))
            moved = (decision.price != o['price']
                     and abs(decision.price / o['price'] - 1) * HUNDRED >= threshold)
            old_enough = now - self._version_ms(o) >= D(level.min_reprice_seconds) * 1000
            # Never withdraw a valid resting order for a target that cannot be sent now.
            if moved and old_enough and target.placeable and self._post_only_ok(side, decision.price, book,
                                                                                ignore=text):
                if self._action_headroom() <= 2 * ACTION_RESERVE or not self._reprice_pace_ok():
                    self._slot_notes[slot] = 'REPRICE_PAUSED_ACTION_CAP'
                elif not self._order_rate_room(1):
                    self._slot_notes[slot] = 'REPRICE_PAUSED_RATE'
                else:
                    candidates[text] = 'REPRICE'
        unknown_amend = False
        if candidates and self.cfg.execution.get('reprice_via_amend'):
            for text, reason in list(candidates.items()):
                if reason in ('REPRICE', 'PREMIUM_RANGE_CHANGED'):
                    outcome = self._try_amend(text)
                    if outcome == 'amended':
                        del candidates[text]
                    elif outcome == 'unknown':
                        unknown_amend = True      # the order is cancelled below; never re-amended
        if candidates:
            self.cancel_orders([o for o in self.store.orders() if o['text'] in candidates], candidates,
                               context=context)
        if unknown_amend:
            raise UnknownCreate('UNKNOWN_AMEND_RESULT')

    # =================================================================== amend (opt-in)
    def _amend_allowed(self, o: Mapping | None) -> bool:
        if (o is None or o['state'] != 'open' or o['order_id'] is None or o['cancel_reason']
                or o['terminal_status'] or o['amend_state'] is not None):
            return False
        if not self._account_complete or self._unsynced_creates:
            return False
        if self._action_headroom() <= 2 * ACTION_RESERVE or not self._reprice_pace_ok():
            return False
        if self.live and self.cfg.safety['require_telegram'] and not self._telegram_ok():
            return False
        if o['side'] == 'sell' and self._mismatch is not None and self._min_profit() is not None:
            return False
        if self._global_stop() or self._side_stop(o['side']) or self._unknown_cancels():
            return False
        return self._level(o['side'], o['level']) is not None

    def _amend_target(self, o: Mapping) -> tuple[Decimal, Decimal] | None:
        """(new price, center) of a placeable, post-only-safe, fundable amend, else None."""
        snap = self._index()
        book, book_error = self._spot()
        if not snap.ok or book_error:
            return None
        level = self._level(o['side'], o['level'])
        target = self._side_targets(o['side'], snap, book).get(level.id)
        if target is None or target.decision is None or not target.placeable:
            return None
        price = target.decision.price
        if price == o['price'] or not self._post_only_ok(o['side'], price, book, ignore=o['text']):
            return None
        if price > o['price']:
            # More money at stake (BUY: the reservation of an unresolved amend uses
            # max(old, new); either side: notional caps). The amount itself is unchanged.
            allowed = self.quantity(o['side'], level, price, snap.center, exclude_text=o['text'],
                                    apply_min=False, size_cap=False)
            if self._remaining(o) > allowed:
                return None
        return price, snap.center

    def _try_amend(self, text: str) -> str:
        """'amended' | 'declined' (caller cancels instead) | 'unknown' (order is cancelled,
        UnknownCreate('UNKNOWN_AMEND_RESULT') is raised by the caller)."""
        o = self.store.order(text)
        if not self._amend_allowed(o) or self._amend_target(o) is None:
            return 'declined'
        if not self._renew(creating=True):
            return 'declined'
        self.preflight('before_amend')
        o = self.store.order(text)
        if not self._amend_allowed(o):
            return 'declined'
        self._index(force=True)
        planned = self._amend_target(o)
        if planned is None:
            return 'declined'
        price, _center = planned
        now = self._now_ms()
        if self.dms_deadline_ms - now <= (self._q() + self._e() + 1) * 1000 or not self._order_rate_room(1):
            return 'declined'
        self.store.action('amend', now, self._cap())
        amend_text = 'a' + base36(now)
        self.store.begin_amend(text, price=price, amend_text=amend_text, now_ms=now)
        self._new_since_sync = True
        self._cancel_sync_generation += 1
        self._cancel_sync_due = time.monotonic()
        self._diag('ORDER_AMEND_REQUEST', text=text, order_id=o['order_id'], old_price=o['price'], price=price)
        try:
            order = self.venue.amend(o['order_id'], text, price=price, amount=None, amend_text=amend_text)
        except (Rejected, NotSent) as exc:
            self.store.clear_amend(text)
            self._diag('ORDER_AMEND_RESULT', text=text, result='refused', error=str(exc)[:120])
            return 'declined'
        except Exception as exc:
            self.store.amend_unknown(text)
            self.counters['amends_unknown'] += 1
            self._diag('ORDER_AMEND_RESULT', text=text, result='unknown', error=type(exc).__name__)
            self.event('amend_unknown:' + text,
                       f"{self.base} {o['side'].upper()} [{o['level']}]: результат изменения цены неизвестен "
                       f"({fmt(o['price'])} → {fmt(price)}); заявка снимается, резерв по большей цене до сверки.")
            return 'unknown'
        if (not isinstance(order, VenueOrder) or order.order_id != o['order_id'] or order.price != price
                or order.text != text):
            self.store.amend_unknown(text)
            self.counters['amends_unknown'] += 1
            self._diag('ORDER_AMEND_RESULT', text=text, result='unknown', error='AMEND_REPLY_MISMATCH')
            return 'unknown'
        self.store.confirm_amend(text, self._now_ms(), price=price)
        self.counters['amends'] += 1
        self._slot_notes[o['slot']] = 'AMEND_SENT'
        self._diag('ORDER_AMEND_RESULT', text=text, result='acknowledged', price=price)
        self.preflight('after_amend')
        return 'amended'

    # =================================================================== creation
    def _apply_backoff(self, slot: str, *, post_only: bool) -> int:
        """Per-slot reject backoff; the attempt counter is shared by every kind of
        refusal and reset only by an accepted order."""
        previous = self._reject_backoff.get(slot)
        attempts = (previous[0] if previous else 0) + 1
        streak = ((previous[2] if previous else 0) + 1) if post_only else 0
        if post_only:
            delay = 0 if attempts <= 2 else min(60, 5 * 2 ** (attempts - 3))
        else:
            delay = min(300, 15 * 2 ** (attempts - 1))
        self._reject_backoff[slot] = (attempts, time.monotonic() + delay, streak)
        return delay

    def _pending_row(self, side: str, level: LevelConfig, price: Decimal, qty: Decimal) -> dict:
        return {'text': None, 'side': side, 'level': level.id, 'slot': f'{side}:{level.id}', 'price': price,
                'qty': qty, 'filled_qty': ZERO, 'order_id': None, 'amend_state': None, 'amend_price': None,
                'pending': True}

    def _evaluate_slot(self, side: str, level: LevelConfig, snap: IndexSnapshot, book: Book | None,
                       book_error: str | None, pending: list[dict], targets: dict[str, dict[str, LevelTarget]],
                       *, recheck: bool = False) -> tuple[str | None, dict | None]:
        """Gate codes of one slot (first hit wins) or an intent."""
        slot = f'{side}:{level.id}'
        stop = self._global_stop()
        if stop:
            return stop, None
        stop = self._side_stop(side)
        if stop:
            return stop, None
        if not side_allowed(self.meta.trade_status, side):
            return f'PAIR_NOT_TRADABLE:{self.meta.trade_status}', None
        if self.store.slot_order(side, level.id) is not None:
            return self._slot_notes.get(slot, 'SLOT_ACTIVE'), None
        if not recheck:
            if self.store.possible_open_order_count() + len(pending) >= int(self.cfg.safety['max_open_orders']):
                return 'MAX_OPEN_ORDERS', None
            backoff = self._reject_backoff.get(slot)
            if backoff and time.monotonic() < backoff[1]:
                return 'REJECT_BACKOFF', None
            if side == 'sell' and self._mismatch is not None and self._min_profit() is not None:
                return 'POSITION_RECONCILIATION_WAIT', None
            if self._action_headroom() - len(pending) <= ACTION_RESERVE:
                return 'ACTION_CAP_WAIT', None
            batch = bool(self.cfg.execution.get('use_batch', True))
            requests = (len(pending) // BATCH_CREATE_MAX + 1) if batch else len(pending) + 1
            if not self._order_rate_room(requests):
                return 'ORDER_RATE_WAIT', None
        if not snap.ok:
            return 'INDEX_BLOCKED:' + str(snap.reason), None
        if book_error or book is None:
            return book_error or 'SPOT_BOOK:NO_DATA', None
        verified = bool(self.cfg.execution.get('iceberg_poc_verified'))
        if level.has_iceberg and self.live and not verified:
            return 'ICEBERG_POC_NOT_VERIFIED', None
        if side not in targets:
            targets[side] = self._side_targets(side, snap, book, pending)
        target = targets[side].get(level.id)
        if target is None:
            return 'NO_TARGET', None
        if not target.placeable or target.decision is None:
            return target.reason, None
        price = target.decision.price
        if not self._post_only_ok(side, price, book, pending=pending):
            return 'POST_ONLY_OR_SELF_CROSS_WAIT', None
        self._size_reason = None
        try:
            qty = self.quantity(side, level, price, snap.center, pending=pending)
        except Unsafe as exc:
            return str(exc), None
        if qty <= 0:
            return self._size_reason or 'BALANCE_OR_LIMIT', None
        try:
            iceberg = iceberg_visible(level, qty, self.meta, price=price) if level.has_iceberg else None
        except Unsafe as exc:
            return str(exc), None
        return None, {'side': side, 'level': level, 'slot': slot, 'target': target, 'price': price, 'qty': qty,
                      'iceberg': iceberg}

    def _create_all(self) -> None:
        """SELL levels then BUY levels, shallow -> deep; one DMS renewal, one batch."""
        snap = self._index()
        book, book_error = self._spot()
        intents: list[dict] = []
        pending: list[dict] = []
        targets: dict[str, dict[str, LevelTarget]] = {}
        for side in ('sell', 'buy'):
            for level in self._levels[side]:
                code, intent = self._evaluate_slot(side, level, snap, book, book_error, pending, targets)
                slot = f'{side}:{level.id}'
                if intent is None:
                    self.slot_states[slot] = str(code)
                    continue
                self.slot_states[slot] = 'AWAITING_DECISION'
                intents.append(intent)
                pending.append(self._pending_row(side, level, intent['price'], intent['qty']))
        if not intents:
            return
        if not self._renew(creating=True):
            for intent in intents:
                self.slot_states[intent['slot']] = 'DMS_NOT_CONFIRMED'
            return
        # Quotes, STOP files, prices and balances may have changed while the DMS call waited.
        self.preflight('before_create')
        stop = self._global_stop()
        if stop or self._unknown_cancels():
            for intent in intents:
                self.slot_states[intent['slot']] = stop or 'CANCEL_RECONCILIATION'
            return
        snap = self._index(force=True)
        book, book_error = self._spot()
        now = self._now_ms()
        if self.dms_deadline_ms - now <= (self._q() + self._e() + 1) * 1000:
            self.cancel_orders(self.store.orders(), 'DMS_TOO_CLOSE_FOR_CREATE')
            for intent in intents:
                self.slot_states[intent['slot']] = 'DMS_TOO_CLOSE_FOR_CREATE'
            return
        # Recompute every intent and reserve sequentially: later slots see earlier rows.
        reserved: list[tuple[dict, PlaceRequest]] = []
        try:
            self._reserve_intents(intents, snap, book, book_error, reserved)
        except BaseException:
            # Nothing of this pass was sent yet: the reservations provably cannot exist.
            for _intent, req in reserved:
                self._safely(self.store.close_order, req.text, 'rejected', self._now_ms())
            raise
        if not reserved:
            return
        unknown = False
        requests = [req for _intent, req in reserved]
        if len(requests) == 1 or not self.cfg.execution.get('use_batch', True):
            for req in requests:
                unknown |= self._place_one(req)
        else:
            for start in range(0, len(requests), BATCH_CREATE_MAX):
                unknown |= self._place_chunk(requests[start:start + BATCH_CREATE_MAX])
        if unknown:
            raise UnknownCreate('UNKNOWN_CREATE_RESULT')

    def _reserve_intents(self, intents: list[dict], snap: IndexSnapshot, book: Book | None, book_error: str | None,
                         reserved: list[tuple[dict, PlaceRequest]]) -> None:
        targets: dict[str, dict[str, LevelTarget]] = {}
        for intent in intents:
            side, level, slot = intent['side'], intent['level'], intent['slot']
            code, fresh = self._evaluate_slot(side, level, snap, book, book_error, [], targets, recheck=True)
            if fresh is None:
                self.slot_states[slot] = str(code)
                continue
            now = self._now_ms()
            self.store.action('create', now, self._cap())
            self._text_counter += 1
            text = make_text(self.cfg.app['client_prefix'], level.id, now, self._text_counter)
            decision = fresh['target'].decision
            diagnostics = {'at_ms': now, 'decision': dataclasses.asdict(decision), 'reason': fresh['target'].reason,
                           'center': snap.center, 'index': snap.index,
                           'edge_pct': edge_pct(side, fresh['price'], snap.center)}
            self.store.reserve_order(text=text, side=side, level=level.id, qty=fresh['qty'], price=fresh['price'],
                                     now_ms=now, reason=fresh['target'].reason, index_price=snap.index,
                                     center_price=snap.center, iceberg=fresh['iceberg'], diagnostics=diagnostics)
            self._new_since_sync = True
            self._unsynced_creates = True
            req = PlaceRequest(text, side, fresh['price'], fresh['qty'], fresh['iceberg'],
                               str(self.cfg.execution.get('time_in_force', 'poc')))
            reserved.append((fresh, req))
            self._diag('ORDER_CREATE_REQUEST', text=text, slot=slot, price=fresh['price'], qty=fresh['qty'],
                       iceberg=fresh['iceberg'], center=snap.center, reason=fresh['target'].reason,
                       notional=fresh['qty'] * fresh['price'])

    def _place_one(self, req: PlaceRequest) -> bool:
        try:
            order = self.venue.place(req)
        except Rejected as exc:
            self.counters['orders_sent'] += 1
            self._create_rejected(req, exc.label, str(exc))
            return False
        except NotSent as exc:
            self._create_not_sent(req, exc)
            return False
        except BudgetWait as exc:
            self._create_not_sent(req, exc)
            return False
        except Exception as exc:          # may have reached the exchange
            self.counters['orders_sent'] += 1
            self._create_unknown(req, exc)
            return True
        self.counters['orders_sent'] += 1
        return self._create_accepted(req, order)

    def _place_chunk(self, chunk: list[PlaceRequest]) -> bool:
        try:
            results = self.venue.place_batch(chunk)
        except (NotSent, BudgetWait) as exc:
            for req in chunk:
                self._create_not_sent(req, exc)
            return False
        except Exception as exc:          # after sending: every item may exist
            self.counters['orders_sent'] += len(chunk)
            for req in chunk:
                self._create_unknown(req, exc)
            return True
        self.counters['orders_sent'] += len(chunk)
        by_text: dict[str, PlaceResult] = {}
        if isinstance(results, (list, tuple)):
            for result in results:
                if isinstance(result, PlaceResult) and result.text not in by_text:
                    by_text[result.text] = result
        unknown = False
        for req in chunk:
            result = by_text.get(req.text)
            if result is None:
                self._create_unknown(req, UnknownResult('BATCH_ITEM_MISSING'))
                unknown = True
            elif result.outcome == 'accepted' and result.order is not None:
                unknown |= self._create_accepted(req, result.order)
            elif result.outcome == 'rejected' and result.label == NOT_SENT_LABEL:
                self._create_not_sent(req, NotSent(result.message or NOT_SENT_LABEL))
            elif result.outcome == 'rejected':
                self._create_rejected(req, result.label, result.message or '')
            else:
                self._create_unknown(req, UnknownResult(result.message or 'BATCH_ITEM_UNKNOWN'))
                unknown = True
        return unknown

    def _row_label(self, req: PlaceRequest) -> str:
        row = self.store.order(req.text)
        level = row['level'] if row else '?'
        return f'{self.base} {req.side.upper()} [{level}]'

    def _create_accepted(self, req: PlaceRequest, order: Any) -> bool:
        """Returns True when the reply cannot be trusted (treated as unknown)."""
        if (not isinstance(order, VenueOrder) or order.text != req.text or order.side != req.side
                or order.price != req.price or order.amount != req.amount or order.currency_pair != self.pair):
            self._create_unknown(req, UnknownResult('CREATE_REPLY_MISMATCH'))
            return True
        row = self.store.order(req.text)
        slot = row['slot']
        self.store.update_order(req.text, order_id=order.order_id, state='open')
        self.counters['creates_accepted'] += 1
        if is_post_only_rejection(order):
            # Created and immediately cancelled as a taker: a post-only rejection for the
            # slot; the reservation is still released only by the terminal protocol.
            self.counters['post_only_rejects'] += 1
            delay = self._apply_backoff(slot, post_only=True)
            self._poc_backoff_applied.add(req.text)
            self.slot_states[slot] = 'POST_ONLY_REJECTED'
            if delay:
                self.event('reject:' + req.text, f'{self._row_label(req)}: заявка снята биржей как post-only '
                                                 f'(finish_as poc). Повтор не раньше чем через {delay} с.')
        else:
            self._reject_backoff.pop(slot, None)
            self.slot_states[slot] = 'ORDER_SENT'
        previous = self._last_closed_slot.pop(slot, None)
        self._diag('ORDER_CREATE_RESULT', text=req.text, slot=slot, order_id=order.order_id, status=order.status,
                   finish_as=order.finish_as, reservation_released=False,
                   since_previous_close_seconds=round(time.monotonic() - previous[1], 3) if previous else None)
        self._safely(self._order_notice, row, req)
        return False

    def _create_rejected(self, req: PlaceRequest, label: str | None, message: str) -> None:
        row = self.store.order(req.text)
        slot = row['slot']
        self.store.close_order(req.text, 'rejected', self._now_ms())
        self.counters['rejects'] += 1
        label = label or 'REJECTED'
        post_only = label == POST_ONLY_LABEL
        if post_only:
            self.counters['post_only_rejects'] += 1
        if label in AUTH_LABELS:
            self.request_stop('GATE_AUTH:' + label)
        if label in RATE_LIMIT_LABELS and self.limiter is not None:
            try:
                self.limiter.rate_limited(ORDER)
            except Exception:
                pass
        delay = self._apply_backoff(slot, post_only=post_only)
        self.slot_states[slot] = 'REJECTED:' + label
        self._diag('ORDER_CREATE_RESULT', text=req.text, slot=slot, result='rejected', label=label, delay=delay,
                   reservation_released=True)
        if not post_only or delay:
            self.event('reject:' + req.text,
                       f'{self._row_label(req)}: заявка отклонена ({label}: {message[:200]}). '
                       f'Размер {fmt(req.amount)} {self.base}, цена {fmt(req.price)}, '
                       f'сумма {usd(req.amount * req.price)} {self.quote}. '
                       + (f'Повтор этого уровня не раньше чем через {delay} с.' if delay
                          else 'Повтор на следующем шаге.'))

    def _create_not_sent(self, req: PlaceRequest, exc: BaseException) -> None:
        row = self.store.order(req.text)
        slot = row['slot']
        self.store.close_order(req.text, 'rejected', self._now_ms())
        self.counters['not_sent'] += 1
        if isinstance(exc, (BudgetNotSent, BudgetWait)):
            # The local limiter refused: no reject is counted; the slot waits for room.
            self.slot_states[slot] = 'ORDER_RATE_WAIT'
        else:
            previous = self._reject_backoff.get(slot)
            self._reject_backoff[slot] = ((previous[0] if previous else 0) + 1, time.monotonic() + 5, 0)
            self.slot_states[slot] = 'NOT_SENT:' + (str(exc)[:80] or type(exc).__name__)
        self._diag('ORDER_CREATE_RESULT', text=req.text, slot=slot, result='not_sent',
                   error=str(exc)[:80] or type(exc).__name__, reservation_released=True)

    def _create_unknown(self, req: PlaceRequest, exc: BaseException) -> None:
        row = self.store.order(req.text)
        slot = row['slot'] if row else '?'
        self.store.update_order(req.text, state='unknown')
        self.counters['creates_unknown'] += 1
        self.slot_states[slot] = 'UNKNOWN_CREATE_RESULT'
        self._diag('ORDER_CREATE_RESULT', text=req.text, slot=slot, result='unknown',
                   error=str(exc)[:80] or type(exc).__name__, reservation_released=False)
        self.event('unknown:' + req.text,
                   f'{self._row_label(req)}: результат выставления неизвестен. {fmt(req.amount)} @ {fmt(req.price)}, '
                   f'резерв сохранён; повтор запрещён до сверки.')

    # =================================================================== startup / shutdown
    def startup(self) -> None:
        """Recovery first: persisted active rows are cancelled (RECOVERY) before any slow
        check; lost cancel replies are re-sent immediately (up to 3 attempts)."""
        self.store.initialize(self.cfg.identity(), self._now_ms())
        self.recovering = bool(self.store.orders())
        if self.recovering:
            for _attempt in range(3):
                try:
                    self.preflight('startup')
                except (BudgetWait, ReadInterrupted):
                    pass
                if not self.recovering or not self._unknown_cancels():
                    break
                self.retry_cancels_now()
        self._safely(self.store.sync_position, base=self.base, quote=self.quote)

    def shutdown(self) -> None:
        """Cancel everything; keep the server-side DMS (never clear it with unresolved results)."""
        self.request_stop('PROCESS_SHUTDOWN')
        self.cancel_orders(self.store.orders(), 'PROCESS_SHUTDOWN')

    # =================================================================== notifications
    def _order_notice(self, row: Mapping, req: PlaceRequest) -> None:
        telegram = self.cfg.telegram
        if not telegram.get('notify_orders'):
            return
        key = req.side
        now = time.monotonic()
        last, suppressed = self._order_notices.get(key, (None, 0))
        interval = float(telegram.get('order_notify_interval_seconds', 300))
        if last is not None and now - last < interval:
            self._order_notices[key] = (last, suppressed + 1)
            return
        self._order_notices[key] = (now, 0)
        center = row.get('center_price')
        lines = [f'{self._row_label(req)}: новая заявка {fmt(req.amount)} {self.base} по {fmt(req.price)} = '
                 f'{usd(req.amount * req.price)} {self.quote}.']
        if center:
            lines.append(f'Центр индекса {fmt(center, 8)}; отступ {fmt(edge_pct(req.side, req.price, center), 3)}%.')
        if suppressed:
            lines.append(f'Новых заявок без отдельного сообщения с прошлого уведомления: {suppressed}.')
        self.event(f'order:{req.side}:{time.time_ns()}', '\n'.join(lines))

    def _startup_size_notice(self) -> None:
        """Configured sizes once per start (before balance/limit caps); retried until quotes exist."""
        if self._startup_sizes_done:
            return
        snap = self._index()
        book, book_error = self._spot()
        if not snap.ok or book_error or book is None:
            return
        lines = ['Размер заявок из конфига (до ограничений баланса и лимитов):']
        for side in SIDES:
            if not self.cfg.side_enabled(side):
                continue
            targets = self._side_targets(side, snap, book)
            for level in self._levels[side]:
                slot = f'{side}:{level.id}'
                target = targets.get(level.id)
                if target is None or target.decision is None:
                    lines.append(f'{self.base} {side.upper()} [{level.id}]: цена недоступна '
                                 f'({target.reason if target else "NO_TARGET"}).')
                    continue
                price = target.decision.price
                try:
                    size = requested_size(level, side, price, self.meta, self._fee_pct,
                                          bool(self.cfg.side(side).get('compensate_base_fee')) and side == 'buy')
                except Unsafe as exc:
                    lines.append(f'{self.base} {side.upper()} [{level.id}]: размер недоступен ({exc}).')
                    continue
                qty = size['requested_gross_qty']
                below = not meets_minimums(qty, price, self.meta)
                self.startup_sizes[slot] = {'qty': qty, 'price': price, 'notional': qty * price,
                                            'index_notional': qty * snap.index, 'below_minimum': below}
                lines.append(f'{self.base} {side.upper()} [{level.id}]: {fmt(qty)} {self.base} × {fmt(price)} = '
                             f'{usd(qty * price)} {self.quote}; по индексу ≈ {usd(qty * snap.index)} {self.quote}.'
                             + (' МЕНЬШЕ МИНИМУМА БИРЖИ (min_base_amount / min_quote_amount).' if below else ''))
        self._startup_sizes_done = True
        self.event('startup_sizes:' + str(time.time_ns()), '\n'.join(lines))

    def _maybe_summary(self) -> None:
        minutes = self.cfg.telegram.get('summary_minutes')
        if not minutes:
            return
        period = int(minutes) * 60_000
        bucket = self._now_ms() // period
        if self.store.meta('summary_bucket') == bucket:
            return
        self.store.set_meta('summary_bucket', bucket)
        self.event(f'summary:{bucket}', self.summary_text())

    def summary_text(self) -> str:
        now = self._now_ms()
        cash = self.balances.get(self.quote, Balance(ZERO, ZERO))
        held = self.balances.get(self.base, Balance(ZERO, ZERO))
        orders = self.store.orders()
        buy_reserved = sum((self._remaining(o) * self._reserve_price(o) for o in orders if o['side'] == 'buy'), ZERO)
        snap = self.index
        index = snap.index if snap is not None and snap.ok else None
        position = self.store.position()
        lines = [f'Сводка {self.pair} на {utc_text(now)}. Состояние: {self.last_reason}.',
                 f'{self.quote}: всего {usd(cash.total)}, свободно {usd(cash.free)}, в заявках BUY {usd(buy_reserved)}.']
        token = f'{self.base}: {fmt(held.total)}'
        if index is not None:
            token += f' ≈ {usd(held.total * index)} {self.quote} (индекс {fmt(index, 8)})'
        if position.avg_cost is not None:
            token += f'; куплено ботом {fmt(position.qty)} по средней {fmt(position.avg_cost, 8)}'
            if index is not None:
                token += f'; нереализовано {signed(position.qty * index - position.cost)} {self.quote}'
        token += f'; реализовано {signed(position.realized)} {self.quote}'
        lines.append(token + '.')
        if orders:
            lines.append('Заявки: ' + ', '.join(
                f"{o['side'].upper()} [{o['level']}] {fmt(self._remaining(o))} по {fmt(o['price'])}" for o in orders)
                + '.')
        waiting = [f'{slot}: {code}' for slot, code in sorted(self.slot_states.items())
                   if code not in ('ORDER_SENT', 'SLOT_ACTIVE', 'AWAITING_DECISION')]
        if waiting:
            lines.append('Ожидание: ' + ', '.join(waiting) + '.')
        day = utc_day_start_ms(now)
        bought, sold = self.store.totals('buy', since_ms=day), self.store.totals('sell', since_ms=day)
        lines.append(f'Сегодня (UTC): куплено на {usd(bought[1])} {self.quote}, продано на {usd(sold[1])} {self.quote}.')
        return '\n'.join(lines)

    # =================================================================== status
    def _section(self, name: str, function: Callable[[], Any]) -> Any:
        try:
            return function()
        except Exception as exc:
            return {'diagnostic_error': type(exc).__name__}

    def _status_index(self) -> dict:
        snap = self.index or self._index()
        return {
            'ok': snap.ok, 'index': snap.index, 'center': snap.center, 'reason': snap.reason,
            'at_ms': snap.at_ms, 'n_included': snap.n_included, 'method': snap.method,
            'fx_rate': snap.fx_rate, 'fx_reason': snap.fx_reason,
            'components': [{'source_id': c.source_id, 'included': c.included, 'reason': c.reason,
                            'usdt_price': c.usdt_price, 'spread_pct': c.spread_pct, 'weight': c.weight,
                            'required': c.required} for c in snap.components],
        }

    def _status_spot(self) -> dict:
        book, error = self._spot()
        result: dict[str, Any] = {'error': error, 'bid': None, 'ask': None, 'basis_pct': None}
        if book is not None and book.bids and book.asks:
            result['bid'], result['ask'] = book.bid, book.ask
            snap = self.index
            if snap is not None and snap.ok and snap.index:
                result['basis_pct'] = (book.mid / snap.index - 1) * HUNDRED
        return result

    def _status_order(self, o: Mapping) -> dict:
        return {'text': o['text'], 'order_id': o['order_id'], 'price': o['price'], 'qty': o['qty'],
                'filled': o['filled_qty'], 'remaining': self._remaining(o), 'iceberg': o['iceberg'],
                'state': o['state'], 'created_ms': o['created_ms'], 'cancel_reason': o['cancel_reason'],
                'cancel_ack': o['cancel_ack'], 'cancel_attempts': o['cancel_attempts'],
                'terminal_status': o['terminal_status'], 'finish_as': o['finish_as'],
                'amend_state': o['amend_state'], 'amend_price': o['amend_price']}

    def _status_slots(self) -> dict:
        active = {o['slot']: o for o in self.store.orders()}
        slots: dict[str, Any] = {}
        names = [f'{side}:{level.id}' for side in SIDES for level in self._levels[side]]
        names += [slot for slot in active if slot not in names]
        now = time.monotonic()
        for slot in names:
            o = active.get(slot)
            if o is None:
                state = 'ABSENT'
                reason = self.slot_states.get(slot) or 'NOT_EVALUATED'
            else:
                if o['cancel_ack'] or o['terminal_status']:
                    state = 'SETTLING'
                elif o['cancel_reason']:
                    state = 'CANCEL_PENDING'
                elif o['state'] != 'open':
                    state = 'UNCONFIRMED'
                else:
                    state = 'OPEN'
                reason = o['cancel_reason'] or self._slot_notes.get(slot) or self.slot_states.get(slot)
            target = self.targets.get(slot)
            backoff = self._reject_backoff.get(slot)
            slots[slot] = {
                'state': state, 'reason': reason, 'note': self._slot_notes.get(slot),
                'target': None if target is None else {
                    'price': target.decision.price if target.decision else None, 'reason': target.reason,
                    'placeable': target.placeable,
                    'lower': target.decision.lower if target.decision else None,
                    'upper': target.decision.upper if target.decision else None},
                'order': None if o is None else self._status_order(o),
                'backoff': None if backoff is None else {
                    'attempts': backoff[0], 'remaining_seconds': round(max(0.0, backoff[1] - now), 3),
                    'post_only_streak': backoff[2]},
            }
        return slots

    def _status_reservations(self) -> dict:
        orders = self.store.orders()
        factor = self._fee_factor()
        usdt = sum((self._remaining(o) * self._reserve_price(o) * factor for o in orders if o['side'] == 'buy'), ZERO)
        base = sum((self._remaining(o) for o in orders if o['side'] == 'sell'), ZERO)
        return {'usdt': usdt, 'base': base, 'fee_factor': factor,
                'available_usdt': self._available('buy') if self.balances else None,
                'available_base': self._available('sell') if self.balances else None}

    def _status_limits(self) -> dict:
        now = self._now_ms()
        day = utc_day_start_ms(now)
        orders = self.store.orders()
        result: dict[str, Any] = {}
        for side in SIDES:
            settings = self.cfg.side(side)
            total_qty, total_notional = self.store.totals(side)
            _day_qty, day_notional = self.store.totals(side, since_ms=day)
            pending = sum((self._remaining(o) * self._reserve_price(o) for o in orders if o['side'] == side), ZERO)
            result[side] = {
                'daily_notional': day_notional, 'daily_cap': settings.get('max_daily_notional_usdt'),
                'total_qty': total_qty, 'total_qty_cap': settings.get('max_total_qty'),
                'total_notional': total_notional, 'total_notional_cap': settings.get('max_total_notional_usdt'),
                'pending_notional': pending}
        holding = (self.balances.get(self.base) or Balance(ZERO, ZERO)).total
        pending_buy = sum((self._remaining(o) for o in orders if o['side'] == 'buy'), ZERO)
        snap = self.index
        center = snap.center if snap is not None and snap.ok else None
        result['inventory'] = {
            'holding': holding, 'pending_buy_qty': pending_buy,
            'value_usdt': (holding + pending_buy) * center if center is not None else None,
            'max_inventory_usdt': self.cfg.pair.get('max_inventory_usdt'),
            'max_inventory_qty': self.cfg.pair.get('max_inventory_qty')}
        return result

    def _status_position(self) -> dict:
        position = self.store.position()
        return {'qty': position.qty, 'avg_cost': position.avg_cost, 'cost': position.cost,
                'realized': position.realized, 'unmatched_qty': position.unmatched_qty,
                'unmatched_proceeds': position.unmatched_proceeds, 'fees_base': position.fees_base,
                'fees_quote': position.fees_quote, 'fees_other': dict(position.fees_other),
                'bought_qty': position.bought_qty, 'bought_cost': position.bought_cost,
                'sold_qty': position.sold_qty, 'sold_proceeds': position.sold_proceeds, 'fills': position.fills}

    def status(self) -> dict:
        """JSON-safe snapshot (Decimal -> str); diagnostic failures never raise."""
        now = self._now_ms()
        external = self._section('external', self.store.external_activity)
        orders = self._section('orders', self.store.orders)
        result = {
            'version': VERSION,
            'mode': 'live' if self.live else 'paper',
            'currency_pair': self.pair,
            'state': self.last_reason,
            'state_changed_ms': self.state_changed_ms,
            'stop_reason': self.stop_reason,
            'global_stop': self._section('global_stop', self._global_stop),
            'updated_ms': now,
            'last_sync_ms': self.last_sync_ms,
            'recovering': self.recovering,
            'account_complete': self._account_complete,
            'account_review_required': bool(external) if not isinstance(external, dict) or
            'diagnostic_error' not in external else None,
            'external_activity': external,
            'own_orders_settled': self._account_complete and not orders,
            'exchange_orders_empty': self._account_complete and not self.open_snapshot,
            'dms_deadline_ms': self.dms_deadline_ms,
            'dms_remaining_seconds': round((self.dms_deadline_ms - now) / 1000, 3) if self.dms_deadline_ms else None,
            'clock_skew_ms': self.clock_skew_ms,
            'index': self._section('index', self._status_index),
            'spot': self._section('spot', self._status_spot),
            'pricing': self.pricing,
            'slots': self._section('slots', self._status_slots),
            'balances': {k: {'total': v.total, 'hold': v.hold, 'free': v.free} for k, v in self.balances.items()},
            'reservations': self._section('reservations', self._status_reservations),
            'limits': self._section('limits', self._status_limits),
            'position': self._section('position', self._status_position),
            'startup_sizes': self.startup_sizes,
            'rate_limiter': self._section('rate_limiter', lambda: self.limiter.status() if self.limiter else None),
            'telegram': self._section('telegram', lambda: self.notifier.status() if self.notifier is not None
                                      else {'enabled': False}),
            'feeds': self._section('feeds', self.hub.diagnostics),
            'index_builder': self._section('index_builder', self.index_builder.status),
            'signed_actions_60min': self._section('actions', lambda: self.store.action_count(now)),
            'outbox_pending': self._section('outbox', self.store.outbox_pending_count),
            'counters': dict(self.counters),
            'errors': dict(self._errors),
            'open_snapshot': sorted(self.open_snapshot),
        }
        return json_safe(result)
