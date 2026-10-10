"""Offline fakes for the engine tests.

* ``FakeVenue``: scripted in-memory Gate spot exchange implementing the Venue protocol
  (balances with locks, post-only crossing, partial fills, history lag, foreign orders
  and trades, read errors, lost replies before/after acting, BudgetNotSent, countdown).
* ``FakeHub``: DESIGN §16 FeedHub API backed by plain values; books are rebuilt fresh
  (received = now) on every call unless a source/spot error is set.
* ``FakeNotifier``: records ``event(key, text)``; ``healthy()`` / ``status()``.
* ``Rig`` / ``make_rig``: config from ``config.example.yaml`` with tweaks + store +
  fakes + Engine, plus small driving helpers.
"""
from __future__ import annotations

import copy
import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml

from gate_stonk.config import Config
from gate_stonk.dec import ZERO, D
from gate_stonk.engine import Engine
from gate_stonk.errors import BudgetWait, NotSent, ReadError, Rejected, UnknownResult
from gate_stonk.index import IndexBuilder
from gate_stonk.store import Store
from gate_stonk.types import (Balance, Book, CancelRequest, PairMeta, PlaceRequest, PlaceResult, SourceSnapshot,
                              VenueOrder, VenueTrade)
from gate_stonk.venue import BudgetNotSent

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / 'config.example.yaml'
PAIR, BASE, QUOTE = 'STONK_USDT', 'STONK', 'USDT'
READS = ('open_orders', 'get_order', 'finished_orders', 'my_trades', 'order_trades', 'balances', 'server_time',
         'account_check')


def now_ms() -> int:
    return int(time.time() * 1000)


# ============================================================================ exchange
@dataclass
class FakeOrder:
    order_id: str
    text: str
    side: str
    price: Decimal
    amount: Decimal
    left: Decimal
    iceberg: Decimal
    status: str = 'open'
    finish_as: str | None = None
    create_ms: int = 0
    update_ms: int = 0
    own: bool = True
    amend_text: str | None = None

    @property
    def lock(self) -> Decimal:
        if self.status != 'open' or not self.own:
            return ZERO
        return self.left * self.price if self.side == 'buy' else self.left

    def view(self) -> VenueOrder:
        return VenueOrder(order_id=self.order_id, text=self.text, currency_pair=PAIR, side=self.side, type='limit',
                          account='spot', time_in_force='poc', price=self.price, amount=self.amount, left=self.left,
                          filled_amount=self.amount - self.left, iceberg=self.iceberg, status=self.status,
                          finish_as=self.finish_as, create_time_ms=self.create_ms, update_time_ms=self.update_ms,
                          amend_text=self.amend_text)


@dataclass
class FakeTrade:
    trade: VenueTrade
    visible_ms: int
    in_window: bool = True


class FakeVenue:
    """Scripted exchange. Plans are deques consumed one entry per call/item:

    * ``place_plan`` (per created item, single or batch): None | 'poc' |
      'reject:<LABEL>' | 'lost_before' (unknown, nothing created) | 'lost_after'
      (created, reply lost) | 'not_sent' | 'budget' (single place only);
    * ``batch_plan`` (per place_batch call): None | 'not_sent' | 'budget' |
      'lost_before' | 'lost_after' | 'raise_unknown';
    * ``cancel_plan`` (per cancel_many call): None | 'lost_before' (nothing done,
      all unknown) | 'lost_after' (done, all unknown) | 'refuse' (all rejected);
    * ``amend_plan``: None | 'reject:<LABEL>' | 'not_sent' | 'lost_before' | 'lost_after';
    * ``countdown_plan``: None | 'unknown';
    * ``read_errors[name] = n``: the next n reads of that kind raise ReadError.
    """

    def __init__(self, meta: PairMeta, *, usdt: Any = '1000', base: Any = '0', live: bool = False,
                 fee_pct: Any = '0.1', crossing_book: Callable[[], Book | None] | None = None):
        self.live = live
        self.meta = meta
        self.before_read: Callable[[], None] = lambda: None
        self.funds = {QUOTE: D(usdt), BASE: D(base)}        # total minus own locks (= available)
        self.orders: dict[str, FakeOrder] = {}
        self.trades: list[FakeTrade] = []
        self.calls: list[str] = []
        self.place_plan: deque = deque()
        self.batch_plan: deque = deque()
        self.cancel_plan: deque = deque()
        self.amend_plan: deque = deque()
        self.countdown_plan: deque = deque()
        self.read_errors: dict[str, int] = {}
        self.read_hooks: dict[str, Callable[[], None]] = {}
        self.dms_deadline_ms = 0
        self.dms_fired = 0
        self.server_skew_ms = 0
        self.account_problems: list[str] = []
        self.fee_rate = D(fee_pct) / 100
        self.crossing_book = crossing_book
        self.on_place: Callable[[PlaceRequest], None] | None = None
        self.place_log: list[PlaceRequest] = []
        self.batch_log: list[list[PlaceRequest]] = []
        self.cancel_log: list[list[CancelRequest]] = []
        self.amend_log: list[tuple[str, Decimal]] = []
        self.countdown_log: list[int] = []
        self._next_id = 7_000_000
        self._next_trade = 9_000_000

    # ------------------------------------------------------------------ plumbing
    def _tick(self) -> None:
        if self.dms_deadline_ms and now_ms() >= self.dms_deadline_ms:
            self.dms_deadline_ms = 0
            self.dms_fired += 1
            for order in self.orders.values():
                if order.status == 'open':
                    self._cancel(order)

    def _read(self, name: str) -> None:
        self.before_read()
        self._tick()
        self.calls.append(name)
        hook = self.read_hooks.pop(name, None)
        if hook is not None:
            hook()
        count = self.read_errors.get(name, 0)
        if count:
            self.read_errors[name] = count - 1
            raise ReadError('FAKE_READ_ERROR:' + name)

    def _write(self, name: str) -> None:
        self._tick()
        self.calls.append(name)

    @staticmethod
    def _pop(plan: deque) -> Any:
        return plan.popleft() if plan else None

    def reads_since(self, index: int) -> list[str]:
        return [c for c in self.calls[index:] if c in READS]

    def own_open(self) -> list[FakeOrder]:
        return [o for o in self.orders.values() if o.own and o.status == 'open']

    def by_text(self, text: str) -> FakeOrder | None:
        return next((o for o in self.orders.values() if o.text == text), None)

    def total(self, currency: str) -> Decimal:
        locked = sum((o.lock for o in self.orders.values()
                      if (o.side == 'buy') == (currency == QUOTE)), ZERO)
        return self.funds[currency] + locked

    # ------------------------------------------------------------------ order mechanics
    def _crosses(self, req: PlaceRequest) -> bool:
        book = self.crossing_book() if self.crossing_book else None
        if book is None:
            return False
        return req.price >= book.ask if req.side == 'buy' else req.price <= book.bid

    def _create(self, req: PlaceRequest, *, force_poc: bool = False) -> FakeOrder:
        need = req.amount * req.price if req.side == 'buy' else req.amount
        token = QUOTE if req.side == 'buy' else BASE
        now = now_ms()
        self._next_id += 1
        order = FakeOrder(str(self._next_id), req.text, req.side, req.price, req.amount, req.amount,
                          req.iceberg or ZERO, create_ms=now, update_ms=now)
        if force_poc or self._crosses(req):
            order.status, order.finish_as = 'cancelled', 'poc'
        else:
            if self.funds[token] < need:
                raise Rejected('Not enough balance', label='BALANCE_NOT_ENOUGH', status=400)
            self.funds[token] -= need
        self.orders[order.order_id] = order
        return order

    def _cancel(self, order: FakeOrder, finish_as: str = 'cancelled') -> None:
        if order.status != 'open':
            return
        if order.own:
            if order.side == 'buy':
                self.funds[QUOTE] += order.left * order.price
            else:
                self.funds[BASE] += order.left
        order.status, order.finish_as, order.update_ms = 'cancelled', finish_as, now_ms()

    # ------------------------------------------------------------------ writes
    def pair_meta(self) -> PairMeta:
        return self.meta

    def place(self, req: PlaceRequest) -> VenueOrder:
        self._write('place')
        plan = self._pop(self.place_plan)
        if plan == 'not_sent':
            raise NotSent('FAKE_NOT_SENT')
        if plan == 'budget':
            raise BudgetNotSent(BudgetWait(1.5, 'order'))
        self.place_log.append(req)
        if self.on_place:
            self.on_place(req)
        if plan == 'lost_before':
            raise UnknownResult('FAKE_LOST_BEFORE')
        if isinstance(plan, str) and plan.startswith('reject:'):
            raise Rejected('fake reject', label=plan[7:], status=400)
        order = self._create(req, force_poc=plan == 'poc')
        if plan == 'lost_after':
            raise UnknownResult('FAKE_LOST_AFTER')
        return order.view()

    def place_batch(self, reqs) -> list[PlaceResult]:
        self._write('place_batch')
        reqs = list(reqs)
        plan = self._pop(self.batch_plan)
        if plan == 'not_sent':
            raise NotSent('FAKE_NOT_SENT')
        if plan == 'budget':
            raise BudgetNotSent(BudgetWait(1.5, 'order'))
        self.batch_log.append(reqs)
        for req in reqs:
            self.place_log.append(req)
            if self.on_place:
                self.on_place(req)
        if plan == 'raise_unknown':
            raise UnknownResult('FAKE_BATCH_LOST')
        if plan == 'lost_before':
            return [PlaceResult(r.text, 'unknown', None, None, 'FAKE_LOST_BEFORE') for r in reqs]
        results = []
        for req in reqs:
            item = self._pop(self.place_plan)
            if item == 'lost_before':
                results.append(PlaceResult(req.text, 'unknown', None, None, 'FAKE_LOST_BEFORE'))
                continue
            if isinstance(item, str) and item.startswith('reject:'):
                results.append(PlaceResult(req.text, 'rejected', None, item[7:], 'fake reject'))
                continue
            try:
                order = self._create(req, force_poc=item == 'poc')
            except Rejected as exc:
                results.append(PlaceResult(req.text, 'rejected', None, exc.label, 'fake reject'))
                continue
            if item == 'lost_after' or plan == 'lost_after':
                results.append(PlaceResult(req.text, 'unknown', None, None, 'FAKE_LOST_AFTER'))
            else:
                results.append(PlaceResult(req.text, 'accepted', order.view(), None, None))
        return results

    def cancel_many(self, reqs) -> dict[str, str]:
        self._write('cancel_many')
        reqs = list(reqs)
        self.cancel_log.append(reqs)
        plan = self._pop(self.cancel_plan)
        if plan == 'lost_before':
            return {r.text: 'unknown' for r in reqs}
        if plan == 'refuse':
            return {r.text: 'rejected' for r in reqs}
        out = {}
        for req in reqs:
            order = self.orders.get(req.order_id) if req.order_id else next(
                (o for o in self.orders.values() if o.text == req.text and o.status == 'open'), None)
            if order is None or order.status != 'open':
                out[req.text] = 'rejected'
                continue
            self._cancel(order)
            out[req.text] = 'sent'
        if plan == 'lost_after':
            return {r.text: 'unknown' for r in reqs}
        return out

    def cancel_all_pair(self) -> None:
        self._write('cancel_all_pair')
        for order in list(self.orders.values()):
            self._cancel(order)

    def amend(self, order_id: str, text: str, *, price=None, amount=None, amend_text: str) -> VenueOrder:
        self._write('amend')
        plan = self._pop(self.amend_plan)
        if plan == 'not_sent':
            raise NotSent('FAKE_NOT_SENT')
        self.amend_log.append((order_id, price))
        if plan == 'lost_before':
            raise UnknownResult('FAKE_AMEND_LOST')
        if isinstance(plan, str) and plan.startswith('reject:'):
            raise Rejected('fake amend reject', label=plan[7:], status=400)
        order = self.orders.get(order_id)
        if order is None or order.status != 'open' or order.text != text:
            raise Rejected('not found', label='ORDER_NOT_FOUND', status=400)
        if price is not None:
            if order.side == 'buy':
                delta = order.left * (D(price) - order.price)
                if delta > self.funds[QUOTE]:
                    raise Rejected('Not enough balance', label='BALANCE_NOT_ENOUGH', status=400)
                self.funds[QUOTE] -= delta
            order.price = D(price)
        order.amend_text = amend_text
        order.update_ms = now_ms()
        if plan == 'lost_after':
            raise UnknownResult('FAKE_AMEND_LOST_AFTER')
        return order.view()

    def countdown(self, timeout_seconds: int) -> int | None:
        self._write('countdown')
        self.countdown_log.append(timeout_seconds)
        if self._pop(self.countdown_plan) == 'unknown':
            raise UnknownResult('FAKE_DMS_UNKNOWN')
        if timeout_seconds == 0:
            self.dms_deadline_ms = 0
            return None
        self.dms_deadline_ms = now_ms() + timeout_seconds * 1000
        return self.dms_deadline_ms

    # ------------------------------------------------------------------ reads
    def open_orders(self) -> list[VenueOrder]:
        self._read('open_orders')
        return [o.view() for o in self.orders.values() if o.status == 'open']

    def get_order(self, *, order_id: str | None = None, text: str | None = None) -> VenueOrder | None:
        self._read('get_order')
        if order_id is not None:
            order = self.orders.get(order_id)
            return order.view() if order is not None else None
        order = next((o for o in self.orders.values() if o.text == text and o.status == 'open'), None)
        return order.view() if order is not None else None

    def finished_orders(self, from_ms: int, to_ms: int) -> list[VenueOrder]:
        self._read('finished_orders')
        return [o.view() for o in self.orders.values() if o.status != 'open' and from_ms <= o.update_ms <= to_ms]

    def _visible(self) -> list[FakeTrade]:
        now = now_ms()
        return [t for t in self.trades if t.visible_ms <= now]

    def my_trades(self, from_ms: int, to_ms: int) -> list[VenueTrade]:
        self._read('my_trades')
        rows = [t.trade for t in self._visible() if t.in_window and from_ms <= t.trade.create_time_ms <= to_ms]
        return sorted(rows, key=lambda t: (t.create_time_ms, int(t.trade_id)))

    def order_trades(self, order_id: str) -> list[VenueTrade]:
        self._read('order_trades')
        return [t.trade for t in self._visible() if t.trade.order_id == order_id]

    def balances(self) -> dict[str, Balance]:
        self._read('balances')
        result = {}
        for currency in (QUOTE, BASE):
            locked = sum((o.lock for o in self.orders.values() if (o.side == 'buy') == (currency == QUOTE)), ZERO)
            result[currency] = Balance(self.funds[currency] + locked, locked)
        return result

    def server_time_ms(self) -> int:
        self._read('server_time')
        return now_ms() + self.server_skew_ms

    def account_check(self) -> dict:
        self._read('account_check')
        return {'problems': list(self.account_problems)}

    # ------------------------------------------------------------------ scenario helpers
    def fill(self, order_id: str, qty: Any, *, price: Any = None, at_ms: int | None = None,
             visible_delay_ms: int = 0, in_window: bool = True) -> VenueTrade:
        order = self.orders[order_id]
        qty = D(qty)
        assert order.status == 'open' and ZERO < qty <= order.left, (order.status, qty, order.left)
        price = order.price if price is None else D(price)
        if order.side == 'buy':
            self.funds[QUOTE] += qty * (order.price - price)
            fee, currency = qty * self.fee_rate, BASE
            self.funds[BASE] += qty - fee
        else:
            proceeds = qty * price
            fee, currency = proceeds * self.fee_rate, QUOTE
            self.funds[QUOTE] += proceeds - fee
        order.left -= qty
        now = now_ms()
        if order.left == 0:
            order.status, order.finish_as = 'closed', 'filled'
        order.update_ms = now
        self._next_trade += 1
        trade = VenueTrade(trade_id=str(self._next_trade), order_id=order.order_id, text=order.text,
                           currency_pair=PAIR, side=order.side, role='maker', amount=qty, price=price, fee=fee,
                           fee_currency=currency, point_fee=ZERO, gt_fee=ZERO,
                           create_time_ms=now if at_ms is None else at_ms)
        self.trades.append(FakeTrade(trade, now + visible_delay_ms, in_window))
        return trade

    def add_foreign_order(self, side: str = 'buy', price: Any = '0.1200', amount: Any = '100',
                          text: str = 'web') -> FakeOrder:
        now = now_ms()
        self._next_id += 1
        order = FakeOrder(str(self._next_id), text, side, D(price), D(amount), D(amount), ZERO,
                          create_ms=now, update_ms=now, own=False)
        self.orders[order.order_id] = order
        return order

    def add_foreign_trade(self, side: str = 'buy', price: Any = '0.1360', amount: Any = '10') -> VenueTrade:
        self._next_trade += 1
        self._next_id += 1
        trade = VenueTrade(trade_id=str(self._next_trade), order_id=str(self._next_id), text='web',
                           currency_pair=PAIR, side=side, role='taker', amount=D(amount), price=D(price),
                           fee=ZERO, fee_currency=QUOTE, point_fee=ZERO, gt_fee=ZERO, create_time_ms=now_ms())
        self.trades.append(FakeTrade(trade, now_ms()))
        return trade


# ============================================================================ feeds
class FakeHub:
    """DESIGN §16 FeedHub API backed by values; books are fresh at every call."""

    def __init__(self, cfg: Config, *, center: Any = '0.1359', spot_bid: Any = '0.1350', spot_ask: Any = '0.1365',
                 spread_pct: Any = '0.02', depth: Any = '100000'):
        self.source_ids = [source.id for source in cfg.sources()]
        self.mid = {sid: D(center) for sid in self.source_ids}
        self.errors: dict[str, str] = {}
        self.spread_pct = D(spread_pct)
        self.depth = D(depth)
        self.spot_bid, self.spot_ask = D(spot_bid), D(spot_ask)
        self.spot_error: str | None = None
        self.wake = threading.Event()

    def set_index(self, price: Any) -> None:
        for sid in self.source_ids:
            self.mid[sid] = D(price)

    @property
    def center(self) -> Decimal:
        return self.mid[self.source_ids[0]]

    def set_spot(self, bid: Any, ask: Any) -> None:
        self.spot_bid, self.spot_ask = D(bid), D(ask)

    def _book(self, bid: Decimal, ask: Decimal) -> Book:
        return Book(((bid, self.depth),), ((ask, self.depth),), time.monotonic(), now_ms())

    def source_snapshot(self, source_id: str) -> SourceSnapshot:
        error = self.errors.get(source_id)
        if error:
            return SourceSnapshot(source_id, None, None, error, False)
        mid = self.mid[source_id]
        half = mid * self.spread_pct / 200
        return SourceSnapshot(source_id, self._book(mid - half, mid + half), None, None, True)

    def fx_snapshot(self):
        return None

    def spot_book(self) -> Book | None:
        if self.spot_error:
            return None
        return self._book(self.spot_bid, self.spot_ask)

    def spot_book_error(self) -> str | None:
        return self.spot_error

    def diagnostics(self) -> dict:
        return {sid: {'ok': sid not in self.errors, 'error': self.errors.get(sid)} for sid in self.source_ids}

    def trades_since(self, seq: int) -> list:
        return []

    def last_trade_seq(self) -> int:
        return 0


class FakeNotifier:
    def __init__(self, healthy: bool = True):
        self.events: list[tuple[str, str]] = []
        self.is_healthy = healthy

    def event(self, key: str, text: str) -> None:
        self.events.append((key, text))

    def healthy(self) -> bool:
        return self.is_healthy

    def status(self) -> dict:
        return {'enabled': True, 'healthy': self.is_healthy, 'pending': 0}

    def keys(self, prefix: str = '') -> list[str]:
        return [key for key, _text in self.events if key.startswith(prefix)]


# ============================================================================ config / rig
def config_data(overrides: dict | None = None, *, mode: str = 'paper') -> dict:
    """config.example.yaml without the USDC (Lighter) source, plus dotted-key overrides."""
    data = yaml.safe_load(EXAMPLE.read_text(encoding='utf-8'))
    data['index']['sources'] = [s for s in data['index']['sources'] if s['quote'] == 'USDT']
    data['app']['mode'] = mode
    data['telegram']['summary_minutes'] = None
    for dotted, value in (overrides or {}).items():
        node = data
        parts = dotted.split('.')
        for part in parts[:-1]:
            node = node[part]
        node[parts[-1]] = copy.deepcopy(value)
    return data


def pair_meta(*, precision: int = 4, amount_precision: int = 2, min_base: Any = '1', min_quote: Any = '3',
              trade_status: str = 'tradable') -> PairMeta:
    return PairMeta(PAIR, BASE, QUOTE, precision, amount_precision,
                    None if min_base is None else D(min_base), None if min_quote is None else D(min_quote),
                    None, None, trade_status)


@dataclass
class Rig:
    cfg: Config
    store: Store
    venue: FakeVenue
    hub: FakeHub
    notifier: FakeNotifier
    engine: Engine
    clock: list
    tmp_path: Path

    def advance(self, seconds: float) -> None:
        self.clock[0] += seconds

    def step(self, advance: float = 0.0) -> str:
        self.advance(advance)
        return self.engine.step()

    def rows(self, active: bool = True) -> list[dict]:
        return self.store.orders(active_only=active)

    def row(self, slot: str) -> dict | None:
        side, level = slot.split(':')
        return self.store.slot_order(side, level)

    def stop_file(self, name: str = 'STOP_TRADING', present: bool = True) -> None:
        path = self.cfg.base_dir / name
        if present:
            path.write_text('stop', encoding='utf-8')
        elif path.exists():
            path.unlink()

    def settle(self, max_steps: int = 10, dt: float = 2.5) -> str:
        """Run steps until no order is cancel-acked / terminal-observed (or max_steps)."""
        state = self.engine.last_reason
        for _ in range(max_steps):
            if not any(o['cancel_ack'] or o['terminal_status'] for o in self.rows()):
                break
            state = self.step(dt)
        return state

    def status_json(self) -> dict:
        status = self.engine.status()
        json.dumps(status, allow_nan=False)
        return status

    def reservations(self) -> Decimal:
        factor = 1 + D(self.cfg.portfolio['fee_reserve_pct']) / 100
        total = ZERO
        for o in self.rows():
            if o['side'] == 'buy':
                price = max([o['price']] + ([o['amend_price']] if o['amend_state'] and o['amend_price'] else []))
                total += (o['qty'] - o['filled_qty']) * price * factor
        return total


def make_rig(tmp_path: Path, clock: list, *, usdt: Any = '1000', base: Any = '0', overrides: dict | None = None,
             mode: str = 'paper', meta: PairMeta | None = None, healthy: bool = True, limiter: Any = None,
             store: Store | None = None, venue: FakeVenue | None = None, hub: FakeHub | None = None,
             center: Any = '0.1359') -> Rig:
    cfg = Config(config_data(overrides, mode=mode), tmp_path)
    meta = meta or pair_meta()
    store = store or Store(tmp_path / 'engine.sqlite')
    hub = hub or FakeHub(cfg, center=center)
    venue = venue or FakeVenue(meta, usdt=usdt, base=base, live=mode == 'live', crossing_book=hub.spot_book)
    notifier = FakeNotifier(healthy)
    engine = Engine(cfg, store, venue, hub, IndexBuilder(cfg.index_settings()), notifier, live=mode == 'live',
                    meta=meta, limiter=limiter)
    return Rig(cfg, store, venue, hub, notifier, engine, clock, tmp_path)


def texts(requests: Iterable) -> list[str]:
    return [r.text for r in requests]
