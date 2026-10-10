"""Reconciliation pass of the trading engine (DESIGN §17 ``_sync`` (A)-(H), §19).

Ported from the reference ``Coordinator._sync`` / ``_sync_impl`` with Gate semantics.
REST is the only accounting authority and a reservation (the local order row) is
released ONLY here, by the terminal protocol:

1. every active row gets one observation per pass: the open-orders snapshot, an
   individual ``get_order`` for a bound order that left the snapshot, or the
   finished-orders window for an order whose create reply was lost (unbound row);
2. ONE ``my_trades`` traversal after all observations (plus the §19 per-order
   ``order_trades`` fallback when the window read misses executions the order
   status already proves);
3. a row whose executions disagree with its status aborts the pass
   (``ORDER_AND_FILLS_NOT_YET_CONSISTENT``) and nothing is released;
4. a terminal row needs two identical fingerprints at least
   ``terminal_settle_seconds`` apart and then a control open-orders read that
   does not show any terminal-observed row; the whole mature batch is closed at
   once or not at all;
5. the fills watermark (meta ``last_sync_ms``) advances only after a complete,
   consistent pass outside account review.

Foreign open orders / foreign executions put the account under permanent review
(``STOP_ACCOUNT_REVIEW``); in settlement mode they are recorded as evidence and the
bot's own orders still settle. The mixin relies on attributes and helpers defined
by :class:`gate_stonk.engine.Engine`.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Mapping

from .dec import ZERO, fmt, usd
from .errors import ReadError, TransientUnsafe, Unsafe
from .ledger import signed
from .marks import MARK_HORIZONS
from .store import utc_text
from .types import Balance, VenueOrder

LOG = logging.getLogger(__name__)

# Observation of one row in one pass:
# (status, left, stamp_ms, finish_as, amount, venue_order_or_None)
#   status: 'open' | 'filled' | 'cancelled' | 'rejected' (never accepted)
Observation = tuple[str, Decimal, int, Any, Decimal, Any]

UNBOUND_SEARCH_BACK_MS = 60_000      # finished-orders window starts this long before the oldest unbound row
FUTURE_STATUS_MS = 2000              # an order status stamped further in the future is invalid
ROUTINE_REASONS = frozenset({'REPRICE', 'PREMIUM_RANGE_CHANGED', 'LIMIT_REDUCTION', 'MODIFY', 'AMEND'})
AMEND_OPEN = ('sent', 'unknown')


class Reconciler:
    """Mixin with the reconciliation pass (used by Engine only)."""

    # ------------------------------------------------------------------ wrapper
    def _sync(self, *, settlement_only: bool = False) -> list[dict]:
        generation = self._cancel_sync_generation
        self._sync_seq += 1
        started = time.monotonic()
        completed = False
        self._diag('RECONCILIATION_START', sync_id=self._sync_seq, cancel_generation=generation,
                   settlement_only=settlement_only, active_orders=len(self.store.orders()))
        try:
            result = self._sync_impl(settlement_only=settlement_only)
            # A cancel acknowledged AFTER this pass started needs another complete
            # snapshot; failed passes never consume a wakeup.
            self._cancel_sync_covered = generation
            if self._cancel_sync_generation == generation:
                self._cancel_sync_due = None
            completed = True
            return result
        finally:
            self._diag('RECONCILIATION_END', sync_id=self._sync_seq, completed=completed,
                       elapsed_ms=round((time.monotonic() - started) * 1000, 3),
                       active_orders=len(self.store.orders()))

    # ------------------------------------------------------------------ pass
    def _sync_impl(self, *, settlement_only: bool) -> list[dict]:
        now = self._now_ms()
        self._fills_watermark = None
        start_ms = self.store.start_ms() or now
        watermark = int(self.store.meta('last_sync_ms', start_ms) or start_ms)
        window_from = max(0, start_ms - 2000, watermark - 60_000)

        # (A) open orders of the pair (complete paging inside the venue).
        opens, by_id = self._read_opens()
        # (B) bind rows whose create reply was lost by their client text.
        by_text: dict[str, list[VenueOrder]] = {}
        for order in opens:
            if order.text:
                by_text.setdefault(order.text, []).append(order)
        for row in self.store.orders():
            if row['order_id'] is None:
                matches = by_text.get(row['text']) or []
                if len(matches) > 1:
                    raise Unsafe('DUPLICATE_OPEN_ORDER_TEXT')
                if matches:
                    self.store.update_order(row['text'], order_id=matches[0].order_id)
                    self._diag('ORDER_BOUND_BY_TEXT', text=row['text'], order_id=matches[0].order_id,
                               source='open_orders')
        # (C) every open order of the pair must be an own ACTIVE row.
        own = {row['order_id'] for row in self.store.orders() if row['order_id'] is not None}
        self._check_foreign_opens(by_id, own, settlement_only)

        # (D) one observation per active row (creation order).
        observations: dict[str, Observation] = {}
        unbound: list[dict] = []
        for row in self.store.orders():
            text = row['text']
            if row['order_id'] is not None and row['order_id'] in by_id:
                order = by_id[row['order_id']]
                row = self._validate_own(row, order)
                observations[text] = ('open', order.left, now, None, order.amount, order)
            elif row['order_id'] is not None:
                observations[text] = self._order_status(row)
            else:
                unbound.append(row)
        if unbound:
            found = self._search_unbound(unbound, self._now_ms())
            for row in unbound:
                text = row['text']
                order = found.get(text)
                if order is not None:
                    self.store.update_order(text, order_id=order.order_id)
                    self._diag('ORDER_BOUND_BY_TEXT', text=text, order_id=order.order_id, source='finished_orders')
                    row = self._validate_own(self.store.order(text), order)
                    observations[text] = self._terminal_observation(row, order)
                elif self._create_expired(row) and self.store.fill_qty(text) == 0:
                    # x-gate-exptime + margin passed and no trace anywhere: the exchange
                    # cannot accept it any more. A stable fingerprint lets the normal
                    # settle rules release it as NOT accepted.
                    observations[text] = ('rejected', row['qty'], int(row['created_ms']), None, row['qty'], None)
                    self._unknown_absent_notice(row)
                else:
                    raise Unsafe('UNKNOWN_ORDER_KEEP_RESERVATION:' + text)
        terminal = [text for text, obs in observations.items() if obs[0] != 'open']

        # (E) ONE history traversal after all observations; its upper bound is the
        # time of the call so executions that ended the orders above are inside.
        to_ms = self._now_ms()
        trades = self.venue.my_trades(window_from, to_ms)
        new = self._ingest(trades, settlement_only)
        self._fills_watermark = to_ms
        new += self._order_trades_fallback(observations, settlement_only)

        # (F) consistency + terminal state machine (creation order).
        now = self._now_ms()
        settle = self._settle_seconds
        to_close: list[tuple[dict, str, Decimal, Any]] = []
        for row in self.store.orders():
            text = row['text']
            status, left, stamp, finish_as, amount, _order = observations[text]
            executed = self.store.fill_qty(text)
            expected = amount if status == 'filled' else amount - left
            if executed != expected:
                self._terminal_recheck.pop(text, None)
                self.store.update_order(text, terminal_status=None, terminal_since_ms=None, terminal_observations=0)
                self._diag('ORDER_FILLS_NOT_CONSISTENT', text=text, status=status, executed=executed,
                           expected=expected)
                raise TransientUnsafe('ORDER_AND_FILLS_NOT_YET_CONSISTENT')
            if status == 'open':
                self._terminal_recheck.pop(text, None)
                if left <= 0:
                    raise Unsafe('OPEN_ORDER_ZERO_SIZE')
                self.store.update_order(text, state='cancel_pending' if row['cancel_reason'] else 'open',
                                        terminal_status=None, terminal_since_ms=None, terminal_observations=0)
                continue
            if status == 'rejected' and executed:
                raise Unsafe('REJECTED_ORDER_HAS_FILLS')
            if row['order_id'] is not None and row['order_id'] in by_id:
                raise TransientUnsafe('TERMINAL_ORDER_STILL_OPEN')
            fp = [row['order_id'], status, finish_as, str(left), str(executed), int(stamp)]
            key = 'terminal_fp:' + text
            if self.store.meta(key) != fp or row['terminal_since_ms'] is None:
                self._terminal_recheck[text] = time.monotonic() + settle
                self.store.set_meta(key, fp)
                self.store.update_order(text, terminal_status=status, finish_as=finish_as, terminal_since_ms=now,
                                        terminal_observations=1, last_terminal_ms=now)
                self._diag('ORDER_TERMINAL_OBSERVATION', text=text, status=status, finish_as=finish_as,
                           executed=executed, left=left, first_consistent_terminal_ms=now,
                           earliest_confirmation_ms=now + int(settle * 1000))
            elif (now - int(row['terminal_since_ms']) >= settle * 1000
                  and now > int(row['last_terminal_ms'] or 0)):
                to_close.append((row, status, executed, finish_as))

        # (G) control read before ANY release (once per batch, only when mature).
        if to_close:
            _opens2, by_id2 = self._read_opens()
            own_now = {row['order_id'] for row in self.store.orders() if row['order_id'] is not None}
            self._check_foreign_opens(by_id2, own_now, settlement_only)
            reappeared = []
            for text in terminal:
                row = self.store.order(text)
                if row is not None and row['order_id'] is not None and row['order_id'] in by_id2:
                    reappeared.append(text)
            if reappeared:
                for text in reappeared:
                    self._terminal_recheck.pop(text, None)
                    self.store.update_order(text, terminal_status=None, terminal_since_ms=None,
                                            terminal_observations=0, last_terminal_ms=None)
                    self.store.set_meta('terminal_fp:' + text, None)
                raise TransientUnsafe('TERMINAL_ORDER_STILL_OPEN')
            by_id = by_id2
            now = self._now_ms()
            for row, status, executed, finish_as in to_close:
                self._close_settled(row, status, executed, finish_as, now)

        # (H) account state.
        self.open_snapshot = dict(by_id)
        self.balances = self._read_balances()
        self.last_sync_ms = self._now_ms()
        self._account_complete = True
        self._new_since_sync = False
        self._unsynced_creates = False
        if self.recovering and not self.store.orders():
            self.recovering = False
            self._diag('RECOVERY_COMPLETE')
        if self.store.external_activity():
            # Manual activity is not a withdrawal: never rewrite bot inventory, keep
            # the history window (watermark) frozen and skip position snapshots.
            return new
        if self._fills_watermark is not None:
            previous = int(self.store.meta('last_sync_ms', 0) or 0)
            if self._fills_watermark > previous:
                self.store.set_meta('last_sync_ms', self._fills_watermark)
        self._reconcile_positions()
        if self.store.pending_position():
            self._safely(self._position_event)
            self.store.clear_pending_position()
        return new

    # ------------------------------------------------------------------ reads
    def _read_opens(self) -> tuple[list[VenueOrder], dict[str, VenueOrder]]:
        opens = self.venue.open_orders()
        if not isinstance(opens, list):
            raise ReadError('INVALID_OPEN_ORDERS')
        by_id: dict[str, VenueOrder] = {}
        for order in opens:
            if not isinstance(order, VenueOrder):
                raise ReadError('INVALID_OPEN_ORDERS')
            if order.currency_pair != self.pair:
                raise Unsafe('UNEXPECTED_ORDER_PAIR')
            if order.status != 'open':
                raise ReadError('INVALID_OPEN_ORDER_STATUS')
            by_id[order.order_id] = order
        if len(by_id) != len(opens):
            raise Unsafe('DUPLICATE_OPEN_ORDER')
        return opens, by_id

    def _read_balances(self) -> dict[str, Balance]:
        balances = self.venue.balances()
        if not isinstance(balances, Mapping):
            raise ReadError('INVALID_BALANCES')
        result: dict[str, Balance] = {}
        for currency, balance in balances.items():
            if not isinstance(balance, Balance):
                raise ReadError('INVALID_BALANCES')
            total, hold = balance.total, balance.hold
            if (not isinstance(total, Decimal) or not isinstance(hold, Decimal) or not total.is_finite()
                    or not hold.is_finite() or not ZERO <= hold <= total):
                raise Unsafe('INVALID_SPOT_BALANCE')
            result[str(currency)] = balance
        for currency in (self.base, self.quote):
            result.setdefault(currency, Balance(ZERO, ZERO))
        return result

    def _order_status(self, row: dict) -> Observation:
        """Individual status of a bound order that is not in the open snapshot."""
        order = self.venue.get_order(order_id=row['order_id'])
        if order is None:
            raise Unsafe('OWN_ORDER_NOT_FOUND:' + row['text'])
        if not isinstance(order, VenueOrder):
            raise ReadError('INVALID_ORDER')
        if order.order_id != row['order_id']:
            raise Unsafe('ORDER_ID_MISMATCH')
        row = self._validate_own(row, order)
        if order.status == 'open':
            raise TransientUnsafe('ORDER_STATUS_OPEN_BUT_NOT_LISTED')
        return self._terminal_observation(row, order)

    def _terminal_observation(self, row: dict, order: VenueOrder) -> Observation:
        if order.update_time_ms > self._now_ms() + FUTURE_STATUS_MS:
            raise Unsafe('FUTURE_ORDER_STATUS')
        self._safely(self.store.record_order_rebate, order, self._now_ms())
        if order.status == 'closed':
            if order.left != 0:
                raise Unsafe('FILLED_ORDER_HAS_REMAINING')
            return ('filled', ZERO, int(order.update_time_ms), order.finish_as, order.amount, order)
        if order.status == 'cancelled':
            return ('cancelled', order.left, int(order.update_time_ms), order.finish_as, order.amount, order)
        if order.status == 'open':
            return ('open', order.left, self._now_ms(), None, order.amount, order)
        raise Unsafe('UNSUPPORTED_ORDER_STATUS:' + str(order.status)[:32])

    def _search_unbound(self, rows: list[dict], now: int) -> dict[str, VenueOrder]:
        """Finished orders around the unbound rows' reservations, matched by text (one read)."""
        low = max(0, min(int(row['created_ms']) for row in rows) - UNBOUND_SEARCH_BACK_MS)
        orders = self.venue.finished_orders(low, max(now, low))
        if not isinstance(orders, list):
            raise ReadError('INVALID_FINISHED_ORDERS')
        texts = {row['text'] for row in rows}
        found: dict[str, VenueOrder] = {}
        for order in orders:
            if not isinstance(order, VenueOrder):
                raise ReadError('INVALID_FINISHED_ORDERS')
            if order.text not in texts:
                continue
            previous = found.get(order.text)
            if previous is not None and previous.order_id != order.order_id:
                raise Unsafe('DUPLICATE_ORDER_TEXT_ON_EXCHANGE')
            found[order.text] = order
        return found

    def _create_expired(self, row: dict) -> bool:
        """The create's x-gate-exptime (signing time + request_expiry) has surely passed."""
        app, safety = self.cfg.app, self.cfg.safety
        margin = (float(app['request_expiry_seconds']) + 2 * float(app['request_timeout_seconds'])
                  + float(safety['unknown_create_margin_seconds']))
        return self._now_ms() > int(row['created_ms']) + int(margin * 1000)

    # ------------------------------------------------------------------ validation
    def _validate_own(self, row: dict, order: VenueOrder) -> dict:
        """Check an exchange order against its reservation; resolve a pending amend.

        Returns the (possibly refreshed) row.
        """
        if order.currency_pair != self.pair or order.side != row['side']:
            raise Unsafe('ORDER_MARKET_OR_SIDE_MISMATCH')
        if order.text != row['text']:
            raise Unsafe('ORDER_TEXT_MISMATCH')
        if row['order_id'] is not None and order.order_id != row['order_id']:
            raise Unsafe('ORDER_ID_MISMATCH')
        if order.type != 'limit' or order.time_in_force != 'poc':
            raise Unsafe('UNEXPECTED_ORDER_TYPE')
        amend_open = row.get('amend_state') in AMEND_OPEN
        prices = {row['price']}
        amounts = {row['qty']}
        if amend_open:
            if row.get('amend_price') is not None:
                prices.add(row['amend_price'])
            if row.get('amend_qty') is not None:
                amounts.add(row['amend_qty'])
        if order.price not in prices or order.amount not in amounts:
            raise Unsafe('ORDER_MODIFIED_OUTSIDE_BOT')
        if not ZERO <= order.left <= order.amount:
            raise Unsafe('INVALID_ORDER_REMAINING')
        expected_iceberg = row['iceberg'] if row['iceberg'] is not None else ZERO
        if order.iceberg != expected_iceberg:
            raise Unsafe('ORDER_ICEBERG_MISMATCH')
        if amend_open:
            row = self._resolve_amend(row, order)
        return row

    def _resolve_amend(self, row: dict, order: VenueOrder) -> dict:
        """An unresolved amend is applied when the exchange shows the new price; it is
        proven not applied when the order is terminal at the old price or its request
        expiry has passed."""
        text = row['text']
        new_price = row.get('amend_price')
        now = self._now_ms()
        if new_price is not None and order.price == new_price and new_price != row['price']:
            self.store.confirm_amend(text, now, price=order.price, qty=order.amount)
            self._diag('AMEND_CONFIRMED_BY_RECONCILIATION', text=text, price=order.price)
            return self.store.order(text)
        sent = int(row.get('amend_sent_ms') or row['created_ms'])
        expired = now > sent + int((float(self.cfg.app['request_expiry_seconds'])
                                    + 2 * float(self.cfg.app['request_timeout_seconds'])
                                    + float(self.cfg.safety['unknown_create_margin_seconds'])) * 1000)
        if order.status != 'open' or expired:
            self.store.clear_amend(text)
            self._diag('AMEND_NOT_APPLIED', text=text, price=order.price, status=order.status)
            return self.store.order(text)
        return row

    def _check_foreign_opens(self, by_id: Mapping[str, VenueOrder], own: set, settlement_only: bool) -> None:
        foreign = [order for order_id, order in by_id.items() if order_id not in own]
        if not foreign:
            return
        reappeared = []
        for order in foreign:
            row = self.store.order_by_id(order.order_id)
            if row is None and order.text:
                row = self.store.order(order.text)
            if row is not None:
                reappeared.append(order)
        self._diag('FOREIGN_OPEN_ORDERS', count=len(foreign), closed_own=len(reappeared),
                   order_ids=[order.order_id for order in foreign][:20])
        if not settlement_only:
            if reappeared:
                raise Unsafe('CLOSED_ORDER_REAPPEARED')
            raise Unsafe('FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT')
        self.store.require_account_review('FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT', self._now_ms())
        self.store.record_external_orders(foreign, self._now_ms())

    # ------------------------------------------------------------------ executions
    def _ingest(self, trades: Any, settlement_only: bool) -> list[dict]:
        if not isinstance(trades, list):
            raise ReadError('INVALID_FILL_HISTORY')
        if not trades:
            return []
        new = self.store.record_trades(
            trades, base=self.base, quote=self.quote, now_ms=self._now_ms(), quarantine_foreign=settlement_only,
            amount_precision=self.meta.amount_precision, initial_avg_price=self.cfg.pair.get('initial_avg_price'))
        for fill in new:
            self._pending_marks.append((fill['fill_key'], int(fill['time_ms']), MARK_HORIZONS))
            self.counters['fills'] += 1
            self._diag('FILL_RECORDED', text=fill['text'], level=fill['level'], side=fill['side'], qty=fill['qty'],
                       price=fill['price'], time_ms=fill['time_ms'], late=fill.get('late'))
        if new:
            self._safely(self._resolve_marks)
        return new

    def _order_trades_fallback(self, observations: Mapping[str, Observation], settlement_only: bool) -> list[dict]:
        """§19: the window read may filter by order END time; read the trades of every
        order whose observed execution exceeds the ledger (at most one read per row)."""
        fetch = getattr(self.venue, 'order_trades', None)
        if not callable(fetch):
            return []
        new: list[dict] = []
        for row in self.store.orders():
            obs = observations.get(row['text'])
            if obs is None or row['order_id'] is None:
                continue
            status, left, _stamp, _finish, amount, _order = obs
            if status == 'rejected':
                continue
            observed = amount if status == 'filled' else amount - left
            if observed > self.store.fill_qty(row['text']):
                self.counters['order_trades_reads'] += 1
                new += self._ingest(fetch(row['order_id']), settlement_only)
        return new

    # ------------------------------------------------------------------ release
    def _close_settled(self, row: dict, status: str, executed: Decimal, finish_as: Any, now: int) -> None:
        text, slot = row['text'], row['slot']
        routine = (status == 'cancelled' and executed == 0 and row['cancel_reason'] in ROUTINE_REASONS
                   and row['cancel_ack'] and int(row['cancel_attempts'] or 0) == 1)
        self.store.close_order(text, status, now, finish_as=finish_as if isinstance(finish_as, str) and finish_as
                               else None)
        self._terminal_recheck.pop(text, None)
        self._last_closed_slot[slot] = (text, time.monotonic())
        self._safely(self.store.delete_meta, 'cancel_rejected:' + text)
        self.counters['closed'] += 1
        self._diag('ORDER_CLOSED', text=text, slot=slot, status=status, finish_as=finish_as, executed=executed,
                   qty=row['qty'], cancel_reason=row['cancel_reason'],
                   terminal_confirmation_seconds=round((now - int(row['terminal_since_ms'] or now)) / 1000, 3),
                   local_lifetime_seconds=round((now - int(row['created_ms'])) / 1000, 3),
                   reservation_released=True)
        side = row['side'].upper()
        label = f"{self.base} {side} [{row['level']}]"
        if finish_as == 'poc' and executed == 0:
            # Accepted, then cancelled by the exchange as a taker: a post-only rejection.
            if text in self._poc_backoff_applied:
                self._poc_backoff_applied.discard(text)
                return
            self.counters['post_only_rejects'] += 1
            delay = self._apply_backoff(slot, post_only=True)
            if delay:
                self.event('reject:' + text, f"{label}: заявка снята биржей как post-only (finish_as poc). "
                                             f"Повтор этого уровня не раньше чем через {delay} с.")
            return
        if status == 'rejected':
            self.event('terminal:' + text,
                       f"{label}: заявка не принята биржей (не найдена после окна x-gate-exptime); "
                       f"цена {fmt(row['price'])}, размер {fmt(row['qty'])} {self.base}. Резерв освобождён после сверки.")
            return
        if not routine:
            self.event('terminal:' + text,
                       f"{label}: {status}; исполнено {fmt(executed)}/{fmt(row['qty'])}; "
                       f"причина {row['cancel_reason'] or 'биржа'}. Резерв освобождён после сверки.")

    def _unknown_absent_notice(self, row: dict) -> None:
        self.event('unknown_absent:' + row['text'],
                   f"{self.base} {row['side'].upper()} [{row['level']}]: заявка с неизвестным результатом не найдена "
                   f"на бирже после окна x-gate-exptime; освобождение резерва после стандартной сверки.")

    # ------------------------------------------------------------------ withdrawals / snapshots
    def _reconcile_positions(self) -> None:
        """Same shortfall (bot qty above the base balance) with no new fills in two complete
        reconciliations >= terminal_settle_seconds apart -> booked as a withdrawal."""
        position = self.store.position()
        balance = self.balances.get(self.base, Balance(ZERO, ZERO))
        shortfall = position.qty - balance.total
        if position.qty <= 0 or shortfall < self.meta.lot:
            self._mismatch = None
            return
        marker = (shortfall, position.fills)
        if self._mismatch is None or self._mismatch[0] != marker:
            self._mismatch = (marker, time.monotonic())
            self._diag('POSITION_SHORTFALL_OBSERVED', shortfall=shortfall, position_qty=position.qty,
                       balance=balance.total)
            return
        if time.monotonic() - self._mismatch[1] < self._settle_seconds:
            return
        removed, cost = self.store.record_transfer_out(shortfall, balance.total, self._now_ms(),
                                                       base=self.base, quote=self.quote)
        self._mismatch = None
        LOG.warning('%s: %s bot-bought tokens treated as withdrawn (cost %s)', self.base, removed, cost)

    def _position_event(self) -> None:
        now = self._now_ms()
        cash = self.balances.get(self.quote, Balance(ZERO, ZERO))
        held = self.balances.get(self.base, Balance(ZERO, ZERO))
        position = self.store.position()
        snap = self._index()
        lines = [f'После сверки исполнений ({utc_text(now)}): {self.quote} всего {usd(cash.total)}, '
                 f'свободно {usd(cash.free)}.']
        line = f'{self.base}: остаток {fmt(held.total)}, свободно {fmt(held.free)}'
        index = snap.index if snap.ok else None
        if index is not None:
            line += f'; оценка по индексу {usd(held.total * index)} {self.quote}'
        else:
            line += '; оценка по индексу недоступна'
        if position.avg_cost is not None:
            line += f'; куплено ботом {fmt(position.qty)} по средней {fmt(position.avg_cost, 8)}'
            if index is not None:
                line += f', нереализовано {signed(position.qty * index - position.cost)} {self.quote}'
        if position.realized or position.qty:
            line += f'; реализовано {signed(position.realized)} {self.quote}'
        if position.unmatched_qty:
            line += f'; продано из прежнего запаса {fmt(position.unmatched_qty)} (себестоимость не учтена)'
        lines.append(line + '.')
        self.event('position:' + str(time.time_ns()), '\n'.join(lines))
