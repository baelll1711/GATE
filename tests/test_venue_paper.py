"""PaperVenue: persistent deterministic trade_through simulator (DESIGN §7 "PaperVenue").

Offline: a small fake hub provides the traded book and the public tape; the ``clock``
fixture controls ``time.time``. Restarts are simulated by building a new PaperVenue on the
same Store file.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from gate_stonk.config import Config
from gate_stonk.errors import NotSent, ReadInterrupted, Rejected, Unsafe
from gate_stonk.store import Store
from gate_stonk.types import (Balance, Book, CancelRequest, PairMeta, PlaceRequest, PublicTrade, VenueOrder,
                              VenueTrade)
from gate_stonk.venue import Venue, is_post_only_rejection
from gate_stonk.venue_paper import PaperVenue

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / 'config.example.yaml'
T0 = 1_800_000_000.0           # clock fixture start (seconds)
T0_MS = int(T0 * 1000)


def d(value) -> Decimal:
    return Decimal(str(value))


class FakeHub:
    """Traded book + public tape (``seq`` strictly increasing), like FeedHub's paper API."""

    def __init__(self, bid='0.98', ask='1.05', *, seq_start: int = 0):
        self.book: Book | None = None
        self.seq = seq_start
        self.tape: list[PublicTrade] = []
        self.on_spot_book = None
        self.set_book(bid, ask)

    def set_book(self, bid, ask) -> None:
        bids = ((d(bid), d(100)),) if bid is not None else ()
        asks = ((d(ask), d(100)),) if ask is not None else ()
        self.book = Book(bids=bids, asks=asks, received=0.0, event_ms=0)

    def spot_book(self) -> Book | None:
        if self.on_spot_book is not None:
            hook, self.on_spot_book = self.on_spot_book, None
            hook()
        return self.book

    def last_trade_seq(self) -> int:
        return self.seq

    def trades_since(self, seq: int) -> list[PublicTrade]:
        return [t for t in self.tape if t.seq > seq]

    def trade(self, price, amount, *, time_ms: int | None = None, trade_id: str | None = None,
              side: str = 'sell') -> PublicTrade:
        import time
        self.seq += 1
        trade = PublicTrade(trade_id=trade_id or f'pt{self.seq}', price=d(price), amount=d(amount), side=side,
                            create_time_ms=time_ms if time_ms is not None else int(time.time() * 1000),
                            received=0.0, seq=self.seq)
        self.tape.append(trade)
        return trade


def make_cfg(tmp_path: Path, *, usdt=1000, base=100, fee_pct=None, fee_estimate=Decimal('0.1')) -> Config:
    raw = yaml.safe_load(EXAMPLE.read_text(encoding='utf-8'))
    raw['paper']['initial_usdt'] = usdt
    raw['paper']['initial_base'] = base
    raw['paper']['fee_pct'] = fee_pct
    raw['portfolio']['fee_estimate_pct'] = float(fee_estimate)
    return Config(raw, tmp_path)


def make_meta(**over) -> PairMeta:
    fields = dict(currency_pair='STONK_USDT', base='STONK', quote='USDT', precision=4, amount_precision=2,
                  min_base_amount=d('0.01'), min_quote_amount=d('1'), max_base_amount=d('100000'),
                  max_quote_amount=None, trade_status='tradable', fee_pct=None)
    fields.update(over)
    return PairMeta(**fields)


@pytest.fixture
def env(tmp_path, clock):
    hub = FakeHub()
    cfg = make_cfg(tmp_path)
    store = Store(tmp_path / 'paper.sqlite')
    venue = PaperVenue(cfg, store, hub, make_meta())
    yield venue, hub, store, cfg
    store.close()


def req(text, side, price, amount, iceberg=None) -> PlaceRequest:
    return PlaceRequest(text=text, side=side, price=d(price), amount=d(amount),
                        iceberg=d(iceberg) if iceberg is not None else None)


def advance(clock, seconds: float) -> None:
    clock[0] += seconds


# ----------------------------------------------------------------------------- basics
def test_protocol_shape_and_initial_balances(env):
    venue, hub, store, cfg = env
    assert isinstance(venue, Venue) and venue.live is False
    bal = venue.balances()
    assert bal == {'STONK': Balance(d(100), d(0)), 'USDT': Balance(d(1000), d(0))}
    assert venue.pair_meta().currency_pair == 'STONK_USDT'
    assert venue.server_time_ms() == T0_MS
    check = venue.account_check()
    assert check['paper'] is True and check['problems'] == [] and check['account_ok'] is True
    assert venue.open_orders() == [] and venue.my_trades(0, T0_MS) == []


def test_place_locks_balances_and_cancel_releases(env):
    venue, hub, *_ = env
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9000', '100'))
    sell = venue.place(req('t-stk1-s1-a01', 'sell', '1.1000', '40'))
    assert isinstance(buy, VenueOrder) and buy.status == 'open' and buy.finish_as == 'open'
    assert buy.left == d(100) and buy.filled_amount == 0 and buy.time_in_force == 'poc' and buy.type == 'limit'
    assert buy.create_time_ms == T0_MS and int(sell.order_id) == int(buy.order_id) + 1
    bal = venue.balances()
    assert bal['USDT'] == Balance(d(1000), d(90)) and bal['USDT'].free == d(910)
    assert bal['STONK'] == Balance(d(100), d(40))
    assert [o.order_id for o in venue.open_orders()] == [buy.order_id, sell.order_id]
    result = venue.cancel_many([CancelRequest(buy.text, buy.order_id)])
    assert result == {buy.text: 'sent'}
    bal = venue.balances()
    assert bal['USDT'] == Balance(d(1000), d(0)) and bal['STONK'].hold == d(40)
    done = venue.get_order(order_id=buy.order_id)
    assert done.status == 'cancelled' and done.finish_as == 'cancelled' and done.left == d(100)


def test_insufficient_balance_rejected_counting_other_holds(env):
    venue, *_ = env
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9000', '1000'))       # holds 900 USDT
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-b2-a01', 'buy', '0.8000', '200'))    # needs 160 > 100 free
    assert exc.value.label == 'BALANCE_NOT_ENOUGH'
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-s1-a01', 'sell', '1.2000', '100.01'))
    assert exc.value.label == 'BALANCE_NOT_ENOUGH'
    assert len(venue.open_orders()) == 1
    assert venue.get_order(text='t-stk1-b2-a01') is None


def test_precision_minimum_and_structural_validation(env):
    venue, *_ = env
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-b1-a01', 'buy', '0.90001', '10'))
    assert exc.value.label == 'INVALID_PRECISION'
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-b1-a02', 'buy', '0.9', '10.001'))
    assert exc.value.label == 'INVALID_PRECISION'
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-b1-a03', 'buy', '0.9', '1'))          # 0.9 USDT < min_quote 1
    assert exc.value.label == 'AMOUNT_TOO_LITTLE'
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-b1-a04', 'buy', '0.0001', '100001'))  # above max_base_amount
    assert exc.value.label == 'AMOUNT_TOO_MUCH'
    with pytest.raises(NotSent):
        venue.place(req('bad-text', 'buy', '0.9', '10'))
    with pytest.raises(NotSent):
        venue.place(PlaceRequest('t-stk1-b1-a05', 'buy', d('0.9'), d('10'), None, time_in_force='gtc'))
    with pytest.raises(NotSent):
        venue.place(PlaceRequest('t-stk1-b1-a06', 'buy', 0.9, d('10')))  # float price
    with pytest.raises(NotSent):
        venue.place(req('t-stk1-b1-a07', 'buy', '0.9', '10', iceberg='10'))
    assert venue.open_orders() == [] and venue.balances()['USDT'].hold == 0


def test_pair_trade_status_blocks_side_before_sending(tmp_path, clock):
    store = Store(tmp_path / 'p.sqlite')
    venue = PaperVenue(make_cfg(tmp_path), store, FakeHub(), make_meta(trade_status='buyable'))
    with pytest.raises(NotSent, match='PAIR_NOT_TRADABLE'):
        venue.place(req('t-stk1-s1-a01', 'sell', '1.2', '10'))
    assert venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10')).status == 'open'
    store.close()


def test_post_only_crossing_creates_cancelled_poc_order(env):
    venue, hub, *_ = env
    hub.set_book('0.98', '1.05')
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '1.0500', '10'))       # at the ask -> would take
    sell = venue.place(req('t-stk1-s1-a01', 'sell', '0.9800', '10'))      # at the bid -> would take
    for order in (buy, sell):
        assert order.status == 'cancelled' and order.finish_as == 'poc' and order.left == d(10)
        assert is_post_only_rejection(order)
    assert venue.balances()['USDT'].hold == 0 and venue.balances()['STONK'].hold == 0
    assert venue.open_orders() == []
    assert venue.get_order(order_id=buy.order_id).finish_as == 'poc'
    assert venue.get_order(text=buy.text) is None                        # text lookup: open orders only
    assert {o.order_id for o in venue.finished_orders(T0_MS, T0_MS)} == {buy.order_id, sell.order_id}
    # just inside the spread is fine
    assert venue.place(req('t-stk1-b1-a02', 'buy', '1.0499', '10')).status == 'open'


def test_iceberg_is_stored_and_echoed(env):
    venue, *_ = env
    order = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '50', iceberg='5'))
    assert order.iceberg == d(5)
    assert venue.open_orders()[0].iceberg == d(5)
    assert venue.place(req('t-stk1-b2-a01', 'buy', '0.8', '50')).iceberg == 0


# ----------------------------------------------------------------------------- trade_through
def test_fill_only_strictly_through_never_at_price(env, clock):
    venue, hub, *_ = env
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9000', '10'))
    sell = venue.place(req('t-stk1-s1-a01', 'sell', '1.1000', '10'))
    advance(clock, 1)
    hub.trade('0.9000', 50)          # at the BUY price -> no fill
    hub.trade('1.1000', 50)          # at the SELL price -> no fill
    assert venue.my_trades(0, T0_MS + 10_000) == []
    assert venue.get_order(order_id=buy.order_id).left == d(10)
    hub.trade('0.8999', 4)           # strictly through the BUY
    hub.trade('1.1001', 3)           # strictly through the SELL
    trades = venue.my_trades(0, T0_MS + 10_000)
    assert [(t.side, t.amount, t.price) for t in trades] == [('buy', d(4), d('0.9000')),
                                                             ('sell', d(3), d('1.1000'))]
    assert all(t.role == 'maker' and t.currency_pair == 'STONK_USDT' for t in trades)
    assert trades[0].order_id == buy.order_id and trades[0].text == buy.text
    assert trades[1].order_id == sell.order_id and trades[1].text == sell.text
    assert trades[0].create_time_ms == T0_MS + 1000
    assert len({t.trade_id for t in trades}) == 2


def test_trade_with_exchange_time_before_placement_is_ignored(env, clock):
    venue, hub, *_ = env
    advance(clock, 5)
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    hub.trade('0.5', 100, time_ms=buy.create_time_ms - 1)      # received after, printed before placement
    assert venue.process_trades() == []
    hub.trade('0.5', 100, time_ms=buy.create_time_ms)          # same millisecond counts
    assert [t.amount for t in venue.process_trades()] == [d(10)]


def test_trade_received_before_placement_is_ignored_by_tape_sequence(env, clock):
    venue, hub, *_ = env
    advance(clock, 1)
    # A print arrives between the venue's tape processing and the placement boundary read:
    # it was received before the order existed, so it can never fill it (even same ms).
    hub.on_spot_book = lambda: hub.trade('0.5', 100)
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    assert venue.process_trades() == []
    assert venue.get_order(order_id=buy.order_id).left == d(10)
    hub.trade('0.5', 100)
    assert len(venue.process_trades()) == 1


def test_trade_amount_is_shared_across_buy_levels(env, clock):
    venue, hub, *_ = env
    b1 = venue.place(req('t-stk1-b1-a01', 'buy', '1.0000', '10'))
    b2 = venue.place(req('t-stk1-b2-a01', 'buy', '0.9900', '10'))
    advance(clock, 1)
    hub.trade('0.9950', 15)          # through b1 only: b2 gets nothing, the remainder 5 is unused
    fills = venue.process_trades()
    assert [(t.order_id, t.amount) for t in fills] == [(b1.order_id, d(5 + 5))]
    assert venue.get_order(order_id=b2.order_id).left == d(10)
    b1b = venue.place(req('t-stk1-b1-a02', 'buy', '1.0000', '10'))
    advance(clock, 1)
    hub.trade('0.9800', 15)          # through both: best price first, deeper level gets the rest
    fills = venue.process_trades()
    assert [(t.order_id, t.amount) for t in fills] == [(b1b.order_id, d(10)), (b2.order_id, d(5))]
    assert venue.get_order(order_id=b2.order_id).left == d(5)


def test_same_price_orders_fill_oldest_first(env, clock):
    venue, hub, *_ = env
    old = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    advance(clock, 1)
    new = venue.place(req('t-stk1-b2-a01', 'buy', '0.9', '10'))
    advance(clock, 1)
    hub.trade('0.8', 12)
    fills = venue.process_trades()
    assert [(t.order_id, t.amount) for t in fills] == [(old.order_id, d(10)), (new.order_id, d(2))]


def test_partial_fills_accumulate_and_close_the_order(env, clock):
    venue, hub, *_ = env
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9000', '10'))
    advance(clock, 1)
    hub.trade('0.8', '3.333')        # floored to the amount grid (0.01)
    hub.trade('0.8', 4)
    venue.process_trades()
    order = venue.get_order(order_id=buy.order_id)
    assert order.status == 'open' and order.left == d('2.67') and order.filled_amount == d('7.33')
    assert venue.balances()['USDT'].hold == d('2.67') * d('0.9')
    advance(clock, 1)
    hub.trade('0.8', 100)
    venue.process_trades()
    order = venue.get_order(order_id=buy.order_id)
    assert order.status == 'closed' and order.finish_as == 'filled' and order.left == 0
    assert order.update_time_ms == T0_MS + 2000
    assert venue.get_order(text=buy.text) is None
    assert venue.open_orders() == []
    assert [t.amount for t in venue.my_trades(0, T0_MS + 5000)] == [d('3.33'), d(4), d('2.67')]
    assert venue.finished_orders(T0_MS + 2000, T0_MS + 2000)[0].order_id == buy.order_id


def test_fees_per_side_and_balances(tmp_path, clock):
    store = Store(tmp_path / 'p.sqlite')
    hub = FakeHub()
    venue = PaperVenue(make_cfg(tmp_path, usdt=1000, base=100, fee_pct=0.2), store, hub, make_meta())
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9000', '10'))
    sell = venue.place(req('t-stk1-s1-a01', 'sell', '1.1000', '20'))
    advance(clock, 1)
    hub.trade('0.85', 10)
    hub.trade('1.15', 20)
    buy_fill, sell_fill = venue.my_trades(0, T0_MS + 5000)
    assert (buy_fill.fee, buy_fill.fee_currency) == (d('0.02'), 'STONK')      # 10 * 0.2 %
    assert (sell_fill.fee, sell_fill.fee_currency) == (d('0.044'), 'USDT')    # 20 * 1.1 * 0.2 %
    bal = venue.balances()
    assert bal['STONK'] == Balance(d(100) + d(10) - d('0.02') - d(20), d(0))
    assert bal['USDT'] == Balance(d(1000) - d(9) + d(22) - d('0.044'), d(0))
    assert venue.get_order(order_id=buy.order_id).fee == d('0.02')
    assert venue.get_order(order_id=sell.order_id).fee_currency == 'USDT'
    store.close()


def test_fee_defaults_to_portfolio_estimate(tmp_path, clock):
    store = Store(tmp_path / 'p.sqlite')
    hub = FakeHub()
    venue = PaperVenue(make_cfg(tmp_path, fee_pct=None, fee_estimate=Decimal('0.1')), store, hub, make_meta())
    venue.place(req('t-stk1-s1-a01', 'sell', '2.0000', '10'))
    advance(clock, 1)
    hub.trade('2.5', 10)
    (fill,) = venue.process_trades()
    assert fill.fee == d('0.02') and fill.fee_currency == 'USDT'
    store.close()


def test_each_public_trade_applies_once(env, clock):
    venue, hub, *_ = env
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    advance(clock, 1)
    trade = hub.trade('0.8', 3)
    venue.process_trades()
    venue.process_trades()
    venue.balances()
    # the hub re-delivers the same print with a new sequence number (e.g. resubscription)
    hub.seq += 1
    hub.tape.append(PublicTrade(trade.trade_id, trade.price, trade.amount, trade.side, trade.create_time_ms,
                                0.0, hub.seq))
    assert venue.process_trades() == []
    assert [t.amount for t in venue.my_trades(0, T0_MS + 5000)] == [d(3)]


# ----------------------------------------------------------------------------- cancel / DMS / amend
def test_cancel_results(env, clock):
    venue, hub, *_ = env
    a = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    b = venue.place(req('t-stk1-b2-a01', 'buy', '0.8', '10'))
    c = venue.place(req('t-stk1-b3-a01', 'buy', '0.7', '10'))
    advance(clock, 1)
    hub.trade('0.85', 10)                               # fills a completely
    result = venue.cancel_many([
        CancelRequest(a.text, a.order_id),               # already filled -> rejected
        CancelRequest(b.text, b.order_id),               # open -> sent
        CancelRequest(c.text, None),                     # by text while open -> sent
        CancelRequest('t-stk1-b4-a01', '999'),           # unknown id -> rejected
        CancelRequest('t-stk1-b5-a01', None),            # unknown text -> rejected
        CancelRequest('t-stk1-b6-a01', 'garbage'),       # invalid id -> unknown (never "sent")
    ])
    assert result == {a.text: 'rejected', b.text: 'sent', c.text: 'sent', 't-stk1-b4-a01': 'rejected',
                      't-stk1-b5-a01': 'rejected', 't-stk1-b6-a01': 'unknown'}
    assert venue.get_order(order_id=a.order_id).status == 'closed'      # the received print came first
    assert venue.cancel_many([CancelRequest(b.text, b.order_id)]) == {b.text: 'rejected'}
    assert venue.cancel_many([]) == {}
    with pytest.raises(ValueError):
        venue.cancel_many([CancelRequest(b.text, b.order_id), CancelRequest(b.text, None)])
    assert venue.balances()['USDT'].hold == 0


def test_cancel_all_pair(env):
    venue, *_ = env
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    venue.place(req('t-stk1-s1-a01', 'sell', '1.2', '10'))
    venue.cancel_all_pair()
    assert venue.open_orders() == []
    assert {o.finish_as for o in venue.finished_orders(0, T0_MS)} == {'cancelled'}


def test_dms_fires_lazily_after_applying_earlier_prints(env, clock):
    venue, hub, *_ = env
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    sell = venue.place(req('t-stk1-s1-a01', 'sell', '1.2', '10'))
    trigger = venue.countdown(10)
    assert trigger == T0_MS + 10_000
    advance(clock, 9)
    assert len(venue.open_orders()) == 2                # not yet
    hub.trade('0.8', 4, time_ms=T0_MS + 9_500)          # printed before the deadline, not yet applied
    hub.trade('1.3', 4, time_ms=T0_MS + 10_500)         # printed after the deadline
    advance(clock, 5)                                   # loop stalled past the deadline
    assert venue.balances()['USDT'].hold == 0           # first call after the deadline fires it
    b, s = venue.get_order(order_id=buy.order_id), venue.get_order(order_id=sell.order_id)
    assert (b.status, b.finish_as, b.left, b.update_time_ms) == ('cancelled', 'cancelled', d(6), T0_MS + 10_000)
    assert (s.status, s.left) == ('cancelled', d(10))
    assert [(t.side, t.amount) for t in venue.my_trades(0, T0_MS + 60_000)] == [('buy', d(4))]
    assert venue.countdown(0) is None


def test_dms_disarm_and_rearm(env, clock):
    venue, *_ = env
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    venue.countdown(10)
    advance(clock, 5)
    assert venue.countdown(10) == T0_MS + 15_000         # renewal moves the deadline
    advance(clock, 8)
    assert len(venue.open_orders()) == 1
    assert venue.countdown(0) is None                    # disarmed
    advance(clock, 100)
    assert len(venue.open_orders()) == 1
    with pytest.raises(NotSent):
        venue.countdown(3)
    with pytest.raises(NotSent):
        venue.countdown(True)


def test_amend_price_keeps_id_and_resets_priority(env, clock):
    venue, hub, *_ = env
    a = venue.place(req('t-stk1-b1-a01', 'buy', '0.9900', '10'))
    advance(clock, 1)
    b = venue.place(req('t-stk1-b2-a01', 'buy', '1.0000', '10'))
    advance(clock, 1)
    amended = venue.amend(a.order_id, a.text, price=d('1.0000'), amount=None, amend_text='t-rp1')
    assert amended.order_id == a.order_id and amended.text == a.text
    assert amended.price == d('1.0000') and amended.amount == d(10) and amended.amend_text == 't-rp1'
    assert venue.balances()['USDT'].hold == d(20)
    advance(clock, 1)
    hub.trade('0.98', 10)                     # same price: b keeps priority over the amended a
    fills = venue.process_trades()
    assert [(t.order_id, t.amount, t.price) for t in fills] == [(b.order_id, d(10), d('1.0000'))]
    hub.trade('0.98', 4)
    assert [(t.order_id, t.price) for t in venue.process_trades()] == [(a.order_id, d('1.0000'))]


def test_amend_resets_trade_boundary_and_checks_post_only(env, clock):
    venue, hub, *_ = env
    hub.set_book('0.98', '1.05')
    a = venue.place(req('t-stk1-b1-a01', 'buy', '0.9000', '10'))
    advance(clock, 2)
    hub.on_spot_book = lambda: hub.trade('0.95', 10)   # received before the amend took effect
    venue.amend(a.order_id, a.text, price=d('1.0000'), amount=None, amend_text='t-rp1')
    assert venue.process_trades() == []                # never fills the new price retroactively
    hub.trade('0.95', 10, time_ms=T0_MS + 1_000)       # printed before the amend (exchange time)
    assert venue.process_trades() == []
    with pytest.raises(Rejected) as exc:
        venue.amend(a.order_id, a.text, price=d('1.0500'), amount=None, amend_text='t-rp2')
    assert exc.value.label == 'POC_FILL_IMMEDIATELY'
    assert venue.get_order(order_id=a.order_id).price == d('1.0000')
    with pytest.raises(Rejected) as exc:
        venue.amend(a.order_id, a.text, price=d('1.00001'), amount=None, amend_text='t-rp3')
    assert exc.value.label == 'INVALID_PRECISION'
    with pytest.raises(NotSent):
        venue.amend(a.order_id, a.text, price=d('1.01'), amount=d(5), amend_text='t-rp4')
    with pytest.raises(Rejected) as exc:
        venue.amend('424242', a.text, price=d('1.01'), amount=None, amend_text='t-rp5')
    assert exc.value.label == 'ORDER_NOT_FOUND'
    hub.trade('0.95', 3)
    assert [t.price for t in venue.process_trades()] == [d('1.0000')]


def test_amend_amount_and_balance(env, clock):
    venue, hub, *_ = env
    hub.set_book('0.98', '3.00')
    a = venue.place(req('t-stk1-b1-a01', 'buy', '1.0000', '500'))
    with pytest.raises(Rejected) as exc:
        venue.amend(a.order_id, a.text, price=None, amount=d('1001'), amend_text='t-up')
    assert exc.value.label == 'BALANCE_NOT_ENOUGH'
    with pytest.raises(Rejected) as exc:
        venue.amend(a.order_id, a.text, price=d('2.5000'), amount=None, amend_text='t-up')
    assert exc.value.label == 'BALANCE_NOT_ENOUGH'           # 500 * 1.5 more > 500 free
    advance(clock, 1)
    hub.trade('0.9', 100)
    venue.process_trades()
    smaller = venue.amend(a.order_id, a.text, price=None, amount=d('300'), amend_text='t-dn')
    assert (smaller.amount, smaller.left, smaller.filled_amount) == (d(300), d(200), d(100))
    assert venue.balances()['USDT'].hold == d(200)
    gone = venue.amend(a.order_id, a.text, price=None, amount=d('100'), amend_text='t-dn2')
    assert gone.status == 'cancelled' and gone.order_id == a.order_id
    with pytest.raises(Rejected) as exc:
        venue.amend(a.order_id, a.text, price=d('0.9'), amount=None, amend_text='t-x')
    assert exc.value.label == 'ORDER_CANCELLED'


# ----------------------------------------------------------------------------- persistence
def test_restart_keeps_orders_balances_and_never_reapplies(tmp_path, clock):
    hub = FakeHub()
    path = tmp_path / 'paper.sqlite'
    store = Store(path)
    cfg = make_cfg(tmp_path)
    venue = PaperVenue(cfg, store, hub, make_meta())
    buy = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    sell = venue.place(req('t-stk1-s1-a01', 'sell', '1.2', '10'))
    advance(clock, 1)
    hub.trade('0.8', 4)
    venue.process_trades()
    snapshot = (venue.open_orders(), venue.balances(), venue.my_trades(0, T0_MS + 5000))
    store.close()

    # same hub (process kept the tape), new venue + new store connection
    store = Store(path)
    venue = PaperVenue(cfg, store, hub, make_meta())
    assert (venue.open_orders(), venue.balances(), venue.my_trades(0, T0_MS + 5000)) == snapshot
    assert venue.process_trades() == []
    store.close()

    # new process: a new hub numbers its tape from scratch and replays recent prints
    hub2 = FakeHub()
    for trade in hub.tape:
        hub2.trade(trade.price, trade.amount, time_ms=trade.create_time_ms, trade_id=trade.trade_id)
    store = Store(path)
    venue = PaperVenue(cfg, store, hub2, make_meta())
    assert venue.process_trades() == []                       # replayed print not re-applied
    advance(clock, 1)
    hub2.trade('0.8', 1)
    hub2.trade('1.3', 2)
    fills = venue.process_trades()
    assert [(t.order_id, t.amount) for t in fills] == [(buy.order_id, d(1)), (sell.order_id, d(2))]
    newer = venue.place(req('t-stk1-b2-a01', 'buy', '0.5', '10'))
    assert int(newer.order_id) > int(sell.order_id)
    assert len({t.trade_id for t in venue.my_trades(0, T0_MS + 10_000)}) == 3
    store.close()


def test_dms_deadline_persists_across_restart(tmp_path, clock):
    hub = FakeHub()
    path = tmp_path / 'paper.sqlite'
    cfg = make_cfg(tmp_path)
    store = Store(path)
    venue = PaperVenue(cfg, store, hub, make_meta())
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    venue.countdown(30)
    store.close()
    advance(clock, 31)
    store = Store(path)
    venue = PaperVenue(cfg, store, hub, make_meta())
    assert venue.open_orders() == []
    (order,) = venue.finished_orders(0, T0_MS + 60_000)
    assert order.finish_as == 'cancelled' and order.update_time_ms == T0_MS + 30_000
    store.close()


def test_initial_balances_set_once_per_campaign(tmp_path, clock):
    path = tmp_path / 'paper.sqlite'
    store = Store(path)
    PaperVenue(make_cfg(tmp_path, usdt=500, base=0), store, FakeHub(), make_meta())
    store.close()
    store = Store(path)
    richer = PaperVenue(make_cfg(tmp_path, usdt=999999, base=50), store, FakeHub(), make_meta())
    assert richer.balances() == {'STONK': Balance(d(0), d(0)), 'USDT': Balance(d(500), d(0))}
    store.close()


def test_identity_mismatch_refused(tmp_path, clock):
    path = tmp_path / 'paper.sqlite'
    store = Store(path)
    PaperVenue(make_cfg(tmp_path), store, FakeHub(), make_meta())
    with pytest.raises(Unsafe, match='SPOT_PAIR_MAPPING_MISMATCH'):
        PaperVenue(make_cfg(tmp_path), store, FakeHub(), make_meta(currency_pair='OTHER_USDT', base='OTHER'))
    store.close()


# ----------------------------------------------------------------------------- windows / batch / reads
def test_my_trades_window_filter(env, clock):
    venue, hub, *_ = env
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '100'))
    for second in (1, 2, 3):
        advance(clock, 1)
        hub.trade('0.8', 1)
    venue.process_trades()
    times = [t.create_time_ms for t in venue.my_trades(0, T0_MS + 10_000)]
    assert times == [T0_MS + 1000, T0_MS + 2000, T0_MS + 3000]
    assert [t.create_time_ms for t in venue.my_trades(T0_MS + 2000, T0_MS + 3000)] == times[1:]
    assert [t.create_time_ms for t in venue.my_trades(T0_MS + 1500, T0_MS + 2500)] == [T0_MS + 2000]
    assert venue.my_trades(T0_MS + 3001, T0_MS + 9000) == []
    with pytest.raises(NotSent):
        venue.my_trades(T0_MS + 10, T0_MS)


def test_finished_orders_window_by_finish_time(env, clock):
    venue, *_ = env
    a = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    b = venue.place(req('t-stk1-b2-a01', 'buy', '0.8', '10'))
    advance(clock, 10)
    venue.cancel_many([CancelRequest(a.text, a.order_id)])
    advance(clock, 10)
    venue.cancel_many([CancelRequest(b.text, b.order_id)])
    assert [o.order_id for o in venue.finished_orders(T0_MS, T0_MS + 30_000)] == [a.order_id, b.order_id]
    assert [o.order_id for o in venue.finished_orders(T0_MS + 15_000, T0_MS + 30_000)] == [b.order_id]
    assert venue.finished_orders(T0_MS, T0_MS + 5_000) == []        # created then, finished later
    with pytest.raises(NotSent):
        venue.finished_orders(-1, T0_MS)


def test_place_batch_mixed_results_share_balance(env):
    venue, hub, *_ = env
    hub.set_book('0.98', '1.05')
    results = venue.place_batch([
        req('t-stk1-b1-a01', 'buy', '0.9000', '600'),     # 540 USDT
        req('t-stk1-b2-a01', 'buy', '0.8000', '600'),     # 480 > 460 free -> rejected
        req('t-stk1-b3-a01', 'buy', '0.9000', '1.001'),   # precision -> rejected
        req('t-stk1-b4-a01', 'buy', '1.0600', '10'),      # crosses the ask -> accepted, poc-cancelled
        req('t-stk1-s1-a01', 'sell', '1.2000', '10'),
    ])
    assert [r.outcome for r in results] == ['accepted', 'rejected', 'rejected', 'accepted', 'accepted']
    assert [r.label for r in results] == [None, 'BALANCE_NOT_ENOUGH', 'INVALID_PRECISION', None, None]
    assert results[3].order.finish_as == 'poc' and results[0].order.status == 'open'
    assert [r.text for r in results] == ['t-stk1-b1-a01', 't-stk1-b2-a01', 't-stk1-b3-a01', 't-stk1-b4-a01',
                                         't-stk1-s1-a01']
    assert len(venue.open_orders()) == 2
    with pytest.raises(NotSent):
        venue.place_batch([req('t-stk1-b5-a01', 'buy', '0.5', '10'), req('t-stk1-b5-a01', 'buy', '0.4', '10')])
    with pytest.raises(NotSent):
        venue.place_batch([req('t-stk1-b6-a01', 'buy', '0.5', '10'), req('nope', 'buy', '0.4', '10')])
    assert len(venue.open_orders()) == 2 and venue.place_batch([]) == []


def test_duplicate_text_rejected(env):
    venue, *_ = env
    order = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    venue.cancel_many([CancelRequest(order.text, order.order_id)])
    with pytest.raises(Rejected) as exc:
        venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    assert exc.value.label == 'INVALID_PARAM_VALUE'


def test_get_order_by_text_open_only_and_mismatch(env):
    venue, *_ = env
    order = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    assert venue.get_order(text=order.text).order_id == order.order_id
    assert venue.get_order(order_id=order.order_id, text=order.text).order_id == order.order_id
    with pytest.raises(Unsafe):
        venue.get_order(order_id=order.order_id, text='t-stk1-b9-zzz')
    assert venue.get_order(order_id='123') is None
    with pytest.raises(NotSent):
        venue.get_order()


def test_reads_call_before_read_and_writes_do_not(env):
    venue, *_ = env
    calls = []
    venue.before_read = lambda: calls.append(1)
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    venue.countdown(10)
    venue.cancel_many([CancelRequest('t-stk1-b1-a01', None)])
    assert calls == []
    venue.open_orders()
    venue.balances()
    venue.my_trades(0, T0_MS)
    venue.finished_orders(0, T0_MS)
    venue.get_order(order_id='1')
    assert len(calls) == 5

    def interrupt():
        raise ReadInterrupted('PENDING_CANCEL')
    venue.before_read = interrupt
    with pytest.raises(ReadInterrupted):
        venue.open_orders()


def test_reentrant_cancel_from_before_read(env):
    """The engine checkpoint may cancel through the same venue before a read."""
    venue, *_ = env
    order = venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    venue.before_read = lambda: venue.cancel_many([CancelRequest(order.text, order.order_id)])
    assert venue.open_orders() == []


def test_paper_fills_are_accepted_by_the_store_ledger(env, clock):
    venue, hub, store, cfg = env
    store.initialize(cfg.identity(), T0_MS)
    text = 't-stk1-b1-a01'
    store.reserve_order(text=text, side='buy', level='b1', qty=d(10), price=d('0.9'), now_ms=T0_MS,
                        reason='TEST', index_price=d(1), center_price=d(1))
    order = venue.place(req(text, 'buy', '0.9', '10'))
    store.update_order(text, order_id=order.order_id, state='open')
    advance(clock, 1)
    hub.trade('0.8', 4)
    trades = venue.my_trades(0, T0_MS + 5000)
    added = store.record_trades(trades, base='STONK', quote='USDT', now_ms=T0_MS + 1000, amount_precision=2)
    assert len(added) == 1
    position = store.position()
    assert position.qty == d(4) - d('0.004') and position.fills == 1
    assert isinstance(trades[0], VenueTrade) and trades[0].role == 'maker'


def test_diagnostics_is_json_safe(env):
    import json
    venue, *_ = env
    venue.place(req('t-stk1-b1-a01', 'buy', '0.9', '10'))
    data = venue.diagnostics()
    json.dumps(data)
    assert data['paper'] is True and data['open_orders'] == 1
