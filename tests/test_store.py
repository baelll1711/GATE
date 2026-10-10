"""SQLite store invariants: slots, orders, execution ledger, outbox, actions, index samples.

All trades below are SYNTHETIC ``VenueTrade`` objects; their shape follows
notes/api_gate_rest.md §8 (``GET /spot/my_trades`` row). Each raw payload carries a
``_comment`` saying so.
"""
from __future__ import annotations

import dataclasses
import hashlib
import itertools
import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest

from gate_stonk.errors import Unsafe
from gate_stonk.ledger import Position, replay
from gate_stonk.store import Store, fill_key_for, utc_day_start_ms
from gate_stonk.types import IndexComponent, IndexSnapshot, VenueOrder, VenueTrade

D = Decimal
NOW = 1_800_000_000_000
PAIR = 'STONK_USDT'
IDENTITY = {'campaign_id': 'test', 'currency_pair': PAIR, 'mode': 'paper', 'client_prefix': 'stk1'}
SYNTHETIC = 'synthetic test trade; schema: notes/api_gate_rest.md §8 GET /spot/my_trades'
_texts = itertools.count(1)


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / 'state.sqlite3')
    s.initialize(IDENTITY, NOW)
    yield s
    s.close()


def reserve(store, side='buy', level='b1', qty='10', price='2', order_id='1', text=None, now=NOW):
    text = text or f't-stk1-{level}-{next(_texts)}'
    store.reserve_order(text=text, side=side, level=level, qty=D(qty), price=D(price), now_ms=now,
                        reason='FIXED_EDGE', index_price=D('2.02'), center_price=D('2.02'))
    if order_id is not None:
        store.update_order(text, order_id=order_id, state='open')
    return store.order(text)


def trade(order_id='1', tid='1', side='buy', qty='1', price='2', fee='0.002', cur='USDT', ms=NOW + 100,
          text=None, point='0', gt='0', pair=PAIR, role='maker'):
    raw = {'_comment': SYNTHETIC, 'id': tid, 'order_id': order_id, 'create_time_ms': f'{ms}.000000',
           'currency_pair': pair, 'side': side, 'role': role, 'amount': qty, 'price': price, 'fee': fee,
           'fee_currency': cur, 'point_fee': point, 'gt_fee': gt, 'text': text}
    return VenueTrade(trade_id=tid, order_id=order_id, text=text, currency_pair=pair, side=side, role=role,
                      amount=D(qty), price=D(price), fee=D(fee), fee_currency=cur, point_fee=D(point),
                      gt_fee=D(gt), create_time_ms=ms, sequence_id=None, raw=raw)


def rec(store, trades, now=NOW + 200, **kw):
    return store.record_trades(trades, base='STONK', quote='USDT', now_ms=now, **kw)


def texts(store):
    return [row['text'] for row in store.outbox_rows(1000)]


# ----------------------------------------------------------------------------- durability / identity

def test_wal_full_sync_and_reopen_after_restart(store, tmp_path):
    path = tmp_path / 'state.sqlite3'
    with store.read() as db:
        assert db.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        assert db.execute('PRAGMA synchronous').fetchone()[0] == 2   # FULL
    row = reserve(store)
    rec(store, [trade()])
    store.update_order(row['text'], state='cancel_pending', cancel_reason='STOP', cancel_attempts=3,
                       next_cancel_ms=NOW + 10_000, last_cancel_ms=NOW + 1000, cancel_ack=False,
                       terminal_status='cancelled', finish_as='cancelled', terminal_since_ms=NOW + 1000,
                       terminal_observations=1, last_terminal_ms=NOW + 1000)
    assert (tmp_path / 'state.sqlite3-wal').exists()
    snapshot, position = store.order(row['text']), store.position()
    other = Store(path)
    try:
        other.initialize(IDENTITY, NOW + 50_000)
        assert other.start_ms() == NOW
        assert other.order(row['text']) == snapshot
        assert other.totals('buy') == (D(1), D(2))
        assert other.outbox_pending_count() == 1
        assert other.position() == position and position.qty == 1
    finally:
        other.close()
    store.close()
    reopened = Store(path)
    try:
        assert reopened.order(row['text']) == snapshot and reopened.position() == position
    finally:
        reopened.close()


def test_identity_mismatch_does_not_reset_campaign(store):
    reserve(store)
    with pytest.raises(Unsafe, match='^DATABASE_IDENTITY_MISMATCH_DO_NOT_RESET$'):
        store.initialize({**IDENTITY, 'campaign_id': 'new'}, NOW + 1000)
    assert store.identity() == IDENTITY and len(store.orders()) == 1
    store.initialize(dict(IDENTITY), NOW + 5000)          # same identity: no-op
    assert store.start_ms() == NOW
    store.set_meta('schema', 2)
    with pytest.raises(Unsafe, match='IDENTITY_MISMATCH'):
        store.initialize(IDENTITY, NOW)


def test_identity_missing_with_history_and_uninitialized_database(tmp_path):
    s = Store(tmp_path / 'x.sqlite3')
    try:
        with pytest.raises(Unsafe, match='^DATABASE_NOT_INITIALIZED$'):
            s.reserve_order(text='t-a', side='buy', level='b1', qty=D(1), price=D(1), now_ms=NOW,
                            reason='R', index_price=D(1), center_price=D(1))
        with pytest.raises(Unsafe, match='^DATABASE_NOT_INITIALIZED$'):
            rec(s, [trade()])
        with s.write() as db:
            db.execute("INSERT INTO orders(text,side,level,qty,price,index_price,center_price,created_ms,reason,"
                       "state) VALUES('t-a','buy','b1','1','1','1','1',1,'R','open')")
        with pytest.raises(Unsafe, match='^DATABASE_IDENTITY_MISSING_DO_NOT_RESET$'):
            s.initialize(IDENTITY, NOW)
        with pytest.raises(ValueError):
            s.initialize({}, NOW)
    finally:
        s.close()


# ----------------------------------------------------------------------------- slots and orders

def test_reserve_is_durable_before_any_order_id(store):
    row = reserve(store, order_id=None)
    assert row['state'] == 'reserved' and row['order_id'] is None and row['slot'] == 'buy:b1'
    assert row['qty'] == D(10) and row['filled_qty'] == 0 and row['cancel_ack'] is False
    other = Store(store.path)
    try:
        assert other.orders() == [row]
        with pytest.raises(Unsafe, match='^UNRESOLVED_ORDER_RESERVATION$'):
            reserve(other, order_id=None)
    finally:
        other.close()


def test_one_active_order_per_slot_but_levels_are_independent(store):
    b1 = reserve(store, level='b1', order_id='1')
    b2 = reserve(store, level='b2', price='1.9', order_id='2')
    s1 = reserve(store, side='sell', level='s1', price='2.1', order_id='3')
    assert [o['text'] for o in store.orders()] == [b1['text'], b2['text'], s1['text']]
    assert store.slot_order('buy', 'b2')['text'] == b2['text'] and store.slot_order('sell', 's2') is None
    store.update_order(b1['text'], state='unknown')
    with pytest.raises(Unsafe, match='^UNRESOLVED_ORDER_RESERVATION$'):
        reserve(store, level='b1', order_id='4')
    store.update_order(b1['text'], state='cancel_pending', cancel_reason='REPRICE', cancel_ack=True)
    assert store.possible_open_order_count() == 2
    with pytest.raises(Unsafe, match='UNRESOLVED'):
        reserve(store, level='b1', order_id='4')
    store.close_order(b1['text'], 'cancelled', NOW + 5000)
    reserve(store, level='b1', order_id='4')
    assert len(store.orders()) == 3 and len(store.orders(active_only=False)) == 4
    # The partial unique index itself protects the slot (not only the Python check).
    with store.write() as db:
        with pytest.raises(Exception):
            db.execute("UPDATE orders SET state='open' WHERE text=?", (b1['text'],))


def test_concurrent_reservations_cannot_duplicate_a_slot(store):
    def attempt(i):
        try:
            return reserve(store, order_id=None, text=f't-race-{i}')['text']
        except Unsafe:
            return None
    with ThreadPoolExecutor(max_workers=6) as pool:
        outcomes = list(pool.map(attempt, range(12)))
    assert len([o for o in outcomes if o is not None]) == 1


@pytest.mark.parametrize('changes', [
    {'text': 'x-abc'}, {'text': 't-'}, {'text': 't-' + 'a' * 29}, {'text': 't-a b'}, {'side': 'both'},
    {'level': 'B1'}, {'level': 'b12345'}, {'qty': D(0)}, {'price': D(-1)}, {'index_price': D(0)},
    {'center_price': D('NaN')}, {'iceberg': D(10)}, {'iceberg': D(0)}, {'reason': ''}, {'now_ms': True},
])
def test_reserve_validation(store, changes):
    args = dict(text='t-stk1-b1-zz', side='buy', level='b1', qty=D(10), price=D(2), now_ms=NOW, reason='R',
                index_price=D(2), center_price=D(2))
    args.update(changes)
    with pytest.raises(ValueError):
        store.reserve_order(**args)
    assert store.orders(active_only=False) == []


def test_reserve_rejects_reused_text(store):
    row = reserve(store)
    store.close_order(row['text'], 'cancelled', NOW)
    with pytest.raises(Unsafe, match='^DUPLICATE_ORDER_TEXT$'):
        reserve(store, text=row['text'])


def test_order_id_cannot_move_or_be_shared(store):
    b1 = reserve(store, order_id='1')
    s1 = reserve(store, side='sell', level='s1', order_id=None)
    with pytest.raises(Unsafe, match='^ORDER_ID_ALREADY_BOUND$'):
        store.update_order(b1['text'], order_id='3')
    with pytest.raises(Unsafe, match='^DUPLICATE_ORDER_ID_OR_ACTIVE_SLOT$'):
        store.update_order(s1['text'], order_id='1')
    assert store.order(s1['text'])['order_id'] is None
    store.update_order(b1['text'], order_id='1')            # same id again: fine
    assert store.order_by_id('1')['text'] == b1['text']


@pytest.mark.parametrize('fields,error', [
    ({}, ValueError), ({'price': D(1)}, ValueError), ({'state': 'closed'}, ValueError),
    ({'state': 'rejected'}, ValueError), ({'state': 'bogus'}, ValueError), ({'cancel_ack': 1}, ValueError),
    ({'order_id': 12}, ValueError), ({'order_id': 'abc'}, ValueError), ({'cancel_attempts': -1}, ValueError),
    ({'terminal_since_ms': 1.5}, ValueError), ({'cancel_reason': ''}, ValueError),
])
def test_update_order_validation(store, fields, error):
    row = reserve(store)
    with pytest.raises(error):
        store.update_order(row['text'], **fields)
    with pytest.raises(Unsafe, match='^MISSING_LOCAL_ORDER$'):
        store.update_order('t-missing', state='open')


def test_terminal_order_does_not_reopen_and_filled_cannot_be_rejected(store):
    row = reserve(store)
    rec(store, [trade()])
    with pytest.raises(Unsafe, match='^FILLED_ORDER_CANNOT_BE_REJECTED$'):
        store.close_order(row['text'], 'rejected', NOW + 200)
    store.set_meta('terminal_fp:' + row['text'], ['1', 'cancelled'])
    store.close_order(row['text'], 'cancelled', NOW + 200, finish_as='cancelled')
    closed = store.order(row['text'])
    assert closed['state'] == 'closed' and closed['terminal_status'] == 'cancelled'
    assert closed['finish_as'] == 'cancelled' and closed['terminal_since_ms'] == NOW + 200
    assert store.meta('terminal_fp:' + row['text']) is None
    store.close_order(row['text'], 'rejected', NOW + 300)        # already final: no-op
    assert store.order(row['text'])['state'] == 'closed'
    with pytest.raises(Unsafe, match='^TERMINAL_ORDER_CANNOT_REOPEN$'):
        store.update_order(row['text'], state='open')
    with pytest.raises(Unsafe, match='^FILLED_QUANTITY_REQUIRES_EXECUTION_LEDGER$'):
        store.update_order(row['text'], filled_qty=D(5))
    store.update_order(row['text'], filled_qty=D(1))             # equal to the ledger: allowed
    unfilled = reserve(store, order_id='2')
    store.close_order(unfilled['text'], 'rejected', NOW + 400)
    assert store.order(unfilled['text'])['state'] == 'rejected'


def test_own_levels_use_original_qty_and_recently_closed_with_clock_skew(store):
    row = reserve(store)
    reserve(store, side='sell', level='s1', price='2.1', order_id='2')
    rec(store, [trade(qty='9')])
    assert store.own_levels('buy', NOW + 200) == {D(2): D(10)}
    store.update_order(row['text'], terminal_since_ms=NOW + 1000)
    store.close_order(row['text'], 'cancelled', NOW + 3000)
    assert store.own_levels('buy', NOW + 3000) == {D(2): D(10)}
    assert store.own_levels('buy', NOW + 3001) == {}
    rejected = reserve(store, order_id='4')
    store.close_order(rejected['text'], 'rejected', NOW + 4000)
    assert store.own_levels('buy', NOW + 4000) == {}
    assert store.own_levels('sell', NOW) == {D('2.1'): D(10)}


# ----------------------------------------------------------------------------- execution ledger

def test_fill_key_is_sha256_of_pair_order_trade_time(store):
    reserve(store)
    added = rec(store, [trade(order_id='1', tid='77', ms=NOW + 100)])
    expected = hashlib.sha256(json.dumps([PAIR, '1', '77', NOW + 100], separators=(',', ':')).encode()).hexdigest()
    assert added[0]['fill_key'] == expected == fill_key_for(PAIR, '1', '77', NOW + 100)
    assert added[0]['level'] == 'b1' and added[0]['late'] is False
    assert all(isinstance(added[0][k], Decimal) for k in ('qty', 'price', 'notional', 'fee', 'point_fee', 'gt_fee'))
    raw = store.fills()[0]['raw']
    assert raw['_comment'] == SYNTHETIC


def test_fill_deduplication_is_durable_and_does_not_count_again(store):
    row = reserve(store)
    assert len(rec(store, [trade(), trade()])) == 1
    assert store.fill_qty(row['text']) == 1 and store.order(row['text'])['filled_qty'] == 1
    other = Store(store.path)
    try:
        # WS/REST formatting differences ('2.000' vs '2') are the same execution.
        same = dataclasses.replace(trade(), price=D('2.000'), amount=D('1.0'), fee=D('0.0020'), role=None)
        assert rec(other, [same]) == []
        assert other.fill_qty(row['text']) == 1 and other.outbox_pending_count() == 1
    finally:
        other.close()


def test_same_execution_identity_with_changed_content_is_a_conflict(store):
    reserve(store)
    rec(store, [trade()])
    for changed in (trade(qty='2'), trade(fee='0.003'), trade(gt='0.1'), trade(ms=NOW + 101),
                    trade(order_id='2'), trade(text='t-other'), trade(role='taker')):
        with pytest.raises(Unsafe, match='^FILL_IDENTITY_CONFLICT$'):
            rec(store, [changed])
    assert store.totals('buy')[0] == 1 and store.outbox_pending_count() == 1


def test_ownership_by_text_binds_order_id_in_the_same_transaction(store):
    row = reserve(store, order_id=None)
    added = rec(store, [trade(order_id='555', text=row['text'])])
    assert len(added) == 1 and added[0]['text'] == row['text']
    assert store.order(row['text'])['order_id'] == '555' and store.order_by_id('555')['text'] == row['text']
    # A later failing check rolls the binding back together with the trade.
    other = reserve(store, side='sell', level='s1', price='2.5', order_id=None)
    with pytest.raises(Unsafe, match='^FILL_PRICE_VIOLATES_LIMIT$'):
        rec(store, [trade(order_id='556', tid='2', side='sell', price='2.4', text=other['text'])])
    assert store.order(other['text'])['order_id'] is None
    # Our text on a different order id than the one already bound: identity conflict.
    with pytest.raises(Unsafe, match='^FILL_ORDER_ID_TEXT_CONFLICT$'):
        rec(store, [trade(order_id='999', tid='3', text=row['text'])])


def test_fill_text_must_match_the_owner(store):
    row = reserve(store)
    with pytest.raises(Unsafe, match='^FILL_TEXT_MISMATCH$'):
        rec(store, [trade(text='t-stk1-zz-1')])
    rec(store, [trade(text=row['text'])])
    assert store.fill_qty(row['text']) == 1


def test_foreign_fill_policy_ignore_old_raise_or_quarantine(store):
    reserve(store, order_id=None)
    assert rec(store, [trade(order_id='999', ms=NOW - 2001)]) == []            # pre-campaign: ignored
    with pytest.raises(Unsafe, match='^FOREIGN_FILL$'):
        rec(store, [trade(order_id='999', ms=NOW - 2000)])
    with pytest.raises(Unsafe, match='^FOREIGN_FILL$'):
        rec(store, [trade(order_id='999', ms=NOW - 3000)], start_ms=NOW - 5000)  # explicit start_ms
    assert rec(store, [trade(order_id='999', ms=NOW - 3000)], start_ms=NOW - 500) == []
    assert store.external_activity() is None
    assert rec(store, [trade(order_id='999', ms=NOW + 50)], quarantine_foreign=True) == []
    activity = store.external_activity()
    assert activity['reason'] == 'FOREIGN_FILL' and activity['detected_ms'] == NOW + 200
    assert rec(store, [trade(order_id='999', ms=NOW + 50)], quarantine_foreign=True) == []   # idempotent
    assert len(store.external_fills()) == 1 and store.external_fills()[0]['raw']['raw']['_comment'] == SYNTHETIC
    with pytest.raises(Unsafe, match='^EXTERNAL_FILL_IDENTITY_CONFLICT$'):
        rec(store, [trade(order_id='999', qty='2', ms=NOW + 50)], quarantine_foreign=True)
    with pytest.raises(Unsafe, match='^INVALID_EXTERNAL_FILL$'):
        rec(store, [trade(order_id='998', tid='5', side='x')], quarantine_foreign=True)
    with pytest.raises(Unsafe, match='^INVALID_EXTERNAL_FILL$'):
        rec(store, [trade(order_id='998', tid='6', ms=NOW + 5000)], quarantine_foreign=True)
    # Quarantined trades never touch the bot ledger.
    assert store.fills() == [] and store.position() == Position() and store.totals('buy') == (0, 0)
    assert store.outbox_pending_count() == 0


def test_late_fill_on_closed_order_is_accepted_but_rejected_order_stops(store):
    row = reserve(store)
    store.close_order(row['text'], 'cancelled', NOW + 50)
    assert len(rec(store, [trade()])) == 1                      # late fill of a closed own order
    assert store.order(row['text'])['state'] == 'closed' and store.order(row['text'])['filled_qty'] == 1
    rejected = reserve(store, order_id='2')
    store.close_order(rejected['text'], 'rejected', NOW + 60)
    with pytest.raises(Unsafe, match='^FILL_FOR_REJECTED_ORDER$'):
        rec(store, [trade(order_id='2', tid='2')])


@pytest.mark.parametrize('changes,reason', [
    ({'currency_pair': 'OTHER_USDT'}, '^FILL_MARKET_MISMATCH$'),
    ({'side': 'sell'}, '^FILL_SIDE_MISMATCH$'),
    ({'side': 'unknown'}, '^FILL_SIDE_MISMATCH$'),
    ({'amount': D(0)}, '^INVALID_FILL_AMOUNT_OR_FEE_TOKEN$'),
    ({'amount': D(-1)}, '^INVALID_FILL_AMOUNT_OR_FEE_TOKEN$'),
    ({'amount': D('NaN')}, '^INVALID_FILL$'),
    ({'amount': D('0.001')}, '^INVALID_FILL_QUANTITY_PRECISION$'),
    ({'price': D(-2)}, '^INVALID_FILL_AMOUNT_OR_FEE_TOKEN$'),
    ({'price': D('2.01')}, '^FILL_PRICE_VIOLATES_LIMIT$'),
    ({'fee': D('Infinity')}, '^INVALID_FILL$'),
    ({'gt_fee': 'x'}, '^INVALID_FILL$'),
    ({'fee_currency': ''}, '^INVALID_FILL_AMOUNT_OR_FEE_TOKEN$'),
    ({'fee_currency': None}, '^INVALID_FILL_AMOUNT_OR_FEE_TOKEN$'),
    ({'create_time_ms': NOW - 10_001}, '^INVALID_FILL_TIMESTAMP$'),   # host may run <= 10 s ahead
    ({'create_time_ms': NOW + 2201}, '^INVALID_FILL_TIMESTAMP$'),     # exchange <= 2 s ahead of now
    ({'create_time_ms': True}, '^INVALID_FILL_ID_OR_TIMESTAMP$'),
    ({'create_time_ms': 1.5}, '^INVALID_FILL_ID_OR_TIMESTAMP$'),
    ({'order_id': 'abc'}, '^INVALID_FILL_ID_OR_TIMESTAMP$'),
    ({'trade_id': ''}, '^INVALID_FILL_ID_OR_TIMESTAMP$'),
    ({'trade_id': 'a b'}, '^INVALID_FILL_ID_OR_TIMESTAMP$'),
])
def test_invalid_fills_do_not_change_ledger_or_notifications(store, changes, reason):
    row = reserve(store)
    bad = dataclasses.replace(trade(), **changes)
    with pytest.raises(Unsafe, match=reason):
        rec(store, [bad], amount_precision=2)
    assert store.fill_qty(row['text']) == 0 and store.outbox_pending_count() == 0
    assert store.position() == Position() and not store.pending_position()


def test_not_a_venue_trade_is_invalid(store):
    reserve(store)
    with pytest.raises(Unsafe, match='^INVALID_FILL$'):
        rec(store, [{'id': '1'}])


def test_sell_price_below_limit_violates(store):
    reserve(store, side='sell', level='s1', price='2.1')
    with pytest.raises(Unsafe, match='^FILL_PRICE_VIOLATES_LIMIT$'):
        rec(store, [trade(side='sell', price='2.09')])
    assert len(rec(store, [trade(side='sell', price='2.15')])) == 1      # better than the limit


def test_exact_original_quantity_cap_no_epsilon_oversell(store):
    row = reserve(store, qty='1')
    rec(store, [trade(qty='0.999')])
    with pytest.raises(Unsafe, match='^FILLS_EXCEED_ORDER_QUANTITY$'):
        rec(store, [trade(qty='0.002', tid='2')])
    assert store.fill_qty(row['text']) == D('0.999')
    rec(store, [trade(qty='0.001', tid='3')])
    assert store.order(row['text'])['filled_qty'] == D(1)
    assert store.order(row['text'])['state'] == 'open'   # the engine alone releases the reservation


def test_valid_prefix_survives_later_foreign_fill_and_restart(store):
    row = reserve(store)
    with pytest.raises(Unsafe, match='^FOREIGN_FILL$'):
        rec(store, [trade(ms=NOW + 100), trade(order_id='99', tid='2', ms=NOW + 101),
                    trade(tid='3', ms=NOW + 102)])
    other = Store(store.path)
    try:
        assert other.fill_qty(row['text']) == 1 and other.outbox_pending_count() == 1
        assert rec(other, [trade(ms=NOW + 100)]) == []
    finally:
        other.close()


def test_outbox_failure_rolls_back_only_its_own_fill(store, monkeypatch):
    row = reserve(store)
    real_emit = store._emit_sql
    calls = []

    def fail_second(key, text, now_ms=None):
        calls.append(key)
        real_emit(key, text, now_ms)
        if len(calls) == 2:
            raise RuntimeError('simulated disk/write error')
    monkeypatch.setattr(store, '_emit_sql', fail_second)
    with pytest.raises(RuntimeError, match='simulated'):
        rec(store, [trade(), trade(tid='2', ms=NOW + 101)])
    assert store.fill_qty(row['text']) == 1 and store.outbox_pending_count() == 1
    assert store.position().qty == 1
    monkeypatch.setattr(store, '_emit_sql', real_emit)
    rec(store, [trade(), trade(tid='2', ms=NOW + 101)])
    assert store.fill_qty(row['text']) == 2 and store.outbox_pending_count() == 2


def test_failed_first_fill_leaves_no_pending_position_flag(store, monkeypatch):
    reserve(store)

    def boom(key, text, now_ms=None):
        raise RuntimeError('outbox down')
    monkeypatch.setattr(store, '_emit_sql', boom)
    with pytest.raises(RuntimeError):
        rec(store, [trade()])
    assert not store.pending_position() and store.fills() == []
    monkeypatch.undo()
    rec(store, [trade()])
    assert store.pending_position()
    store.clear_pending_position()
    assert not store.pending_position()


def test_fees_in_stonk_usdt_gt_point_and_rebate_through_the_store(store):
    reserve(store, level='b1', qty='10', price='2', order_id='1')
    reserve(store, level='b2', qty='10', price='2', order_id='2')
    reserve(store, side='sell', level='s1', qty='20', price='2.5', order_id='3')
    rec(store, [
        trade(order_id='1', tid='1', qty='10', price='2', fee='0.01', cur='STONK', ms=NOW + 100),   # BUY fee base
        trade(order_id='2', tid='2', qty='10', price='2', fee='0.02', cur='USDT', ms=NOW + 110,     # BUY fee quote
              point='1.1', gt='0.3'),
        trade(order_id='3', tid='3', side='sell', qty='5', price='2.5', fee='0.0125', ms=NOW + 120),  # SELL quote fee
        trade(order_id='3', tid='4', side='sell', qty='5', price='2.5', fee='-0.001', ms=NOW + 130),  # rebate
        trade(order_id='3', tid='5', side='sell', qty='1', price='2.5', fee='0.0005', cur='GT', ms=NOW + 140),
    ], amount_precision=2)
    pos = store.position()
    assert pos.qty == D('19.99') - 11 and pos.fees_base == D('0.01') and pos.fees_quote == D('0.0315')
    assert pos.fees_other == {'POINT': D('1.1'), 'GT': D('0.3005')}
    assert pos.bought_qty == 20 and pos.bought_cost == 40 and pos.sold_qty == 11 and pos.sold_proceeds == D('27.5')
    assert pos == replay(store.fills(), 'STONK', 'USDT')
    msgs = texts(store)
    assert msgs[0].startswith('[PAPER] STONK BUY [b1]: исполнено 10 STONK по 2 USDT = 20 USDT.')
    assert 'Комиссия: 0.01 STONK (≈0.02 USDT).' in msgs[0]
    assert 'Получено 9.99 STONK, оплачено 20 USDT.' in msgs[0]
    assert 'Комиссия: 0.02 USDT; также 1.1 POINT, 0.3 GT (в USDT не пересчитывается).' in msgs[1]
    assert 'Отдано 5 STONK, получено 12.4875 USDT.' in msgs[2] and 'Прибыль по сделке:' in msgs[2]
    assert 'Комиссия: -0.001 USDT.' in msgs[3] and 'получено 12.501 USDT' in msgs[3]
    assert 'Комиссия: 0.0005 GT.' in msgs[4] and 'получено 2.5 USDT' in msgs[4]
    assert 'Кампания: куплено 20 STONK на 40 USDT; продано 11 STONK на 27.5 USDT.' in msgs[4]
    assert 'order_id: 3; trade_id: 5; роль: maker; время: ' in msgs[4]
    assert 'PnL' not in ''.join(msgs)


def test_average_price_realized_and_unmatched_with_initial_price(store):
    reserve(store, level='b1', qty='4', price='1', order_id='1')
    reserve(store, side='sell', level='s1', qty='10', price='2', order_id='2')
    rec(store, [trade(order_id='1', tid='1', qty='4', price='1', fee='0'),
                trade(order_id='2', tid='2', side='sell', qty='10', price='2', fee='0.2', ms=NOW + 150)],
        initial_avg_price=D('1.5'))
    pos = store.position()
    assert pos.qty == 0 and pos.realized == D('3.92') and pos.unmatched_qty == 6
    assert pos.unmatched_proceeds == D('11.88') and pos.realized_with(D('1.5')) == D('6.8')
    msg = texts(store)[1]
    assert 'Прибыль по сделке: +3.92 USDT (+98%) к себестоимости 4 USDT; всего реализовано +3.92 USDT.' in msg
    assert 'Из прежнего запаса 6 STONK по initial_avg_price 1.5: +2.88 USDT.' in msg
    reserve(store, side='sell', level='s2', qty='1', price='2', order_id='3')
    rec(store, [trade(order_id='3', tid='3', side='sell', qty='1', price='2', fee='0', ms=NOW + 160)])
    assert 'Из запаса, купленного не ботом (себестоимость не учтена): 1 STONK.' in texts(store)[2]


def test_late_fill_triggers_replay_and_correction_message(store):
    b1 = reserve(store, level='b1', qty='10', price='1', order_id='1')
    reserve(store, level='b2', qty='10', price='1.5', order_id='2')
    reserve(store, side='sell', level='s1', qty='15', price='2', order_id='3')
    rec(store, [trade(order_id='1', tid='1', qty='10', price='1', fee='0', ms=NOW + 100),
                trade(order_id='3', tid='3', side='sell', qty='15', price='2', fee='0', ms=NOW + 300)])
    before = store.position()
    assert before.realized == 10 and before.unmatched_qty == 5 and before.qty == 0
    late = rec(store, [trade(order_id='2', tid='2', qty='10', price='1.5', fee='0', ms=NOW + 200)], now=NOW + 400)
    assert late[0]['late'] is True
    after = store.position()
    assert after.realized == D('11.25') and after.unmatched_qty == 0 and after.qty == 5 and after.cost == D('6.25')
    assert after == replay(store.fills(), 'STONK', 'USDT')
    msg = texts(store)[-1]
    assert ('Исполнение стало видно с опозданием: позиция и прибыль пересчитаны в порядке времени исполнения; '
            'всего реализовано было +10, стало +11.25 USDT.') in msg
    # In-order fills after the replay apply incrementally and agree with a replay.
    reserve(store, level='b3', qty='1', price='1', order_id='4')
    assert rec(store, [trade(order_id='4', tid='9', qty='1', price='1', fee='0', ms=NOW + 500)],
               now=NOW + 600)[0]['late'] is False
    assert store.position() == replay(store.fills(), 'STONK', 'USDT')
    assert store.order(b1['text'])['filled_qty'] == 10


def test_batch_is_processed_in_execution_order(store):
    reserve(store, level='b1', qty='10', price='1', order_id='1')
    reserve(store, side='sell', level='s1', qty='10', price='2', order_id='2')
    added = rec(store, [trade(order_id='2', tid='20', side='sell', qty='10', price='2', fee='0', ms=NOW + 300),
                        trade(order_id='1', tid='10', qty='10', price='1', fee='0', ms=NOW + 100)])
    assert [f['trade_id'] for f in added] == ['10', '20'] and not any(f['late'] for f in added)
    assert store.position().realized == 10 and store.position().unmatched_qty == 0


def test_position_cache_is_rebuilt_when_stale_or_corrupt(store):
    reserve(store, qty='10', price='2')
    rec(store, [trade(qty='3')])
    expected = store.position()
    with store.write() as db:
        db.execute("UPDATE positions SET json='{\"qty\": \"NaN\"}'")
    assert store.position() == expected
    with store.write() as db:
        db.execute("UPDATE meta SET value='0' WHERE key='positions_rowid'")
    assert store.position() == expected
    assert store.sync_position(base='STONK', quote='USDT') == expected
    with store.read() as db:
        assert Position.from_json(db.execute('SELECT json FROM positions').fetchone()[0]) == expected


def test_ledger_pair_must_match_the_campaign(store):
    reserve(store)
    with pytest.raises(Unsafe, match='^LEDGER_PAIR_MISMATCH$'):
        store.record_trades([trade()], base='ABC', quote='USDT', now_ms=NOW + 200)
    with pytest.raises(ValueError):
        store.record_trades([trade()], base='stonk', quote='USDT', now_ms=NOW + 200)


def test_daily_totals_by_utc_midnight(store):
    midnight = NOW + (86_400_000 - NOW % 86_400_000)
    assert utc_day_start_ms(midnight + 5) == midnight and utc_day_start_ms(midnight - 1) == midnight - 86_400_000
    reserve(store, level='b1', qty='10', order_id='1', now=midnight - 5000)
    reserve(store, side='sell', level='s1', qty='10', price='2.1', order_id='2', now=midnight - 5000)
    rec(store, [trade(order_id='1', tid='1', qty='1', fee='0.001', cur='STONK', ms=midnight - 1),
                trade(order_id='1', tid='2', qty='2', fee='0.004', ms=midnight),
                trade(order_id='2', tid='3', side='sell', qty='3', price='2.1', fee='-0.01', ms=midnight + 1)],
        now=midnight + 100)
    assert store.totals('buy') == (D(3), D(6))
    assert store.totals('buy', utc_day_start_ms(midnight + 100)) == (D(2), D(4))
    assert store.totals('sell', midnight) == (D(3), D('6.3'))
    assert store.totals('sell', midnight + 86_400_000) == (0, 0)
    assert store.count_fills_since(midnight) == 2
    with pytest.raises(ValueError):
        store.totals('both')


# ----------------------------------------------------------------------------- amend

def test_amend_lifecycle_and_price_limits(store):
    row = reserve(store, qty='10', price='2', order_id='1')
    with pytest.raises(Unsafe, match='^AMEND_REQUIRES_OPEN_BOUND_ORDER$'):
        unbound = reserve(store, level='b2', order_id=None)
        store.begin_amend(unbound['text'], price=D('1.9'), amend_text='t-rp-1', now_ms=NOW)
    pending = store.begin_amend(row['text'], price=D('2.05'), amend_text='t-rp-1', now_ms=NOW + 10)
    assert pending['amend_state'] == 'sent' and pending['amend_price'] == D('2.05') and pending['price'] == 2
    with pytest.raises(Unsafe, match='^AMEND_ALREADY_PENDING$'):
        store.begin_amend(row['text'], price=D('2.06'), amend_text='t-rp-2', now_ms=NOW + 11)
    store.amend_unknown(row['text'])
    assert store.order(row['text'])['amend_state'] == 'unknown'
    assert store.own_levels('buy', NOW) == {D(2): D(20), D('2.05'): D(10)}   # + unbound b2 at 2
    # While unresolved a fill at the amended price is within the limit.
    rec(store, [trade(tid='1', price='2.05', ms=NOW + 100)])
    with pytest.raises(Unsafe, match='^FILL_PRICE_VIOLATES_LIMIT$'):
        rec(store, [trade(tid='2', price='2.06', ms=NOW + 100)])
    confirmed = store.confirm_amend(row['text'], NOW + 1000)
    assert confirmed['price'] == D('2.05') and confirmed['amend_state'] is None
    assert confirmed['prior_prices'] == [(D(2), NOW + 1000)]
    # Old price (worse for nobody: lower BUY) is still accepted; a higher price is not.
    rec(store, [trade(tid='3', price='2', ms=NOW + 2500)], now=NOW + 3000)
    with pytest.raises(Unsafe, match='^FILL_PRICE_VIOLATES_LIMIT$'):
        rec(store, [trade(tid='4', price='2.06', ms=NOW + 2600)], now=NOW + 3000)
    with pytest.raises(Unsafe, match='^NO_PENDING_AMEND$'):
        store.confirm_amend(row['text'], NOW + 3000)


def test_amend_old_price_only_for_fills_before_the_amend(store):
    row = reserve(store, side='sell', level='s1', qty='10', price='2.2', order_id='1')
    store.begin_amend(row['text'], price=D('2.1'), amend_text='t-rp-1', now_ms=NOW)
    store.confirm_amend(row['text'], NOW + 1000)
    # SELL moved down to 2.1; a fill between 2.1 and 2.2 is fine at any time.
    rec(store, [trade(tid='1', side='sell', price='2.15', ms=NOW + 500)], now=NOW + 5000)
    # Fills below 2.1 are never allowed.
    with pytest.raises(Unsafe, match='^FILL_PRICE_VIOLATES_LIMIT$'):
        rec(store, [trade(tid='2', side='sell', price='2.09', ms=NOW + 500)], now=NOW + 5000)
    row2 = reserve(store, level='b1', qty='10', price='2', order_id='2')
    store.begin_amend(row2['text'], price=D('1.9'), amend_text='t-rp-2', now_ms=NOW)
    store.confirm_amend(row2['text'], NOW + 1000)
    rec(store, [trade(order_id='2', tid='3', price='2', ms=NOW + 3000)], now=NOW + 5000)   # within +2 s
    with pytest.raises(Unsafe, match='^FILL_PRICE_VIOLATES_LIMIT$'):
        rec(store, [trade(order_id='2', tid='4', price='2', ms=NOW + 3001)], now=NOW + 5000)


def test_own_levels_keep_the_price_an_amend_left_for_two_seconds(store):
    row = reserve(store, qty='10', price='2', order_id='1')
    store.begin_amend(row['text'], price=D('2.05'), amend_text='t-rp-1', now_ms=NOW)
    store.confirm_amend(row['text'], NOW + 1000)
    # A book snapshot up to 2 s older than the amend may still show the old price.
    assert store.own_levels('buy', NOW + 500) == {D('2.05'): D(10), D(2): D(10)}
    assert store.own_levels('buy', NOW + 3000) == {D('2.05'): D(10), D(2): D(10)}
    assert store.own_levels('buy', NOW + 3001) == {D('2.05'): D(10)}


def test_mark_sent_stamps_reserved_rows_once(store):
    first = reserve(store, order_id=None)
    second = reserve(store, level='b2', order_id=None)
    assert first['sent_ms'] is None
    store.mark_sent([first['text'], second['text']], NOW + 20_000)
    assert store.order(first['text'])['sent_ms'] == NOW + 20_000
    assert store.order(second['text'])['created_ms'] == NOW
    with pytest.raises(Unsafe, match='^ORDER_ALREADY_SENT$'):
        store.mark_sent([first['text']], NOW + 30_000)
    with pytest.raises(Unsafe, match='^MISSING_LOCAL_ORDER$'):
        store.mark_sent(['t-stk1-b9-none'], NOW)
    with pytest.raises(ValueError):
        store.mark_sent([], NOW)


def test_sent_ms_column_is_added_to_an_older_database(tmp_path):
    import sqlite3
    path = tmp_path / 'old.sqlite3'
    old = Store(path)
    old.initialize(IDENTITY, NOW)
    row = reserve(old, order_id=None)
    old.close()
    db = sqlite3.connect(str(path))
    db.execute('ALTER TABLE orders DROP COLUMN sent_ms')          # the schema before the column existed
    db.commit()
    db.close()
    reopened = Store(path)
    reopened.initialize(IDENTITY, NOW)
    assert reopened.order(row['text'])['sent_ms'] is None         # old rows: expiry falls back to created_ms
    reopened.mark_sent([row['text']], NOW + 5)
    assert reopened.order(row['text'])['sent_ms'] == NOW + 5
    reopened.close()


def test_quarantine_external_trades_of_other_pairs(store):
    foreign = trade(order_id='77', tid='901', pair='BTC_USDT', ms=NOW + 100)
    old = trade(order_id='78', tid='902', pair='BTC_USDT', ms=NOW - 10_000)       # before the campaign
    assert store.quarantine_external_trades([foreign, old], NOW + 200) == 1
    assert store.external_activity()['reason'] == 'FOREIGN_FILL'
    [evidence] = store.external_fills()
    assert evidence['trade_id'] == '901' and evidence['raw']['canonical']['currency_pair'] == 'BTC_USDT'
    assert store.fills() == [] and store.position().qty == 0
    assert store.quarantine_external_trades([foreign], NOW + 300) == 1          # idempotent
    assert len(store.external_fills()) == 1


def test_amend_qty_rules_and_clear(store):
    row = reserve(store, qty='10', price='2', order_id='1')
    rec(store, [trade(qty='4')])
    with pytest.raises(Unsafe, match='^AMEND_QTY_BELOW_FILLED$'):
        store.begin_amend(row['text'], price=D(2), qty=D(3), amend_text='a', now_ms=NOW)
    store.begin_amend(row['text'], price=D('1.99'), qty=D(6), amend_text='a', now_ms=NOW)
    store.clear_amend(row['text'])
    assert store.order(row['text'])['amend_state'] is None and store.order(row['text'])['price'] == 2
    store.clear_amend(row['text'])            # idempotent
    store.begin_amend(row['text'], price=D('1.99'), qty=D(6), amend_text='b', now_ms=NOW)
    done = store.confirm_amend(row['text'], NOW + 10, price=D('1.99'), qty=D(6))
    assert done['qty'] == 6 and done['price'] == D('1.99')
    rec(store, [trade(tid='2', qty='2', price='1.99')])
    with pytest.raises(Unsafe, match='^FILLS_EXCEED_ORDER_QUANTITY$'):
        rec(store, [trade(tid='3', qty='1', price='1.99', ms=NOW + 101)])
    with pytest.raises(ValueError):
        store.begin_amend(row['text'], price=D(2), amend_text='x' * 32, now_ms=NOW)


# ----------------------------------------------------------------------------- outbox and actions

def test_outbox_idempotent_fifo_retries_and_skips(store):
    assert store.outbox_add('health:1', 'first', NOW) is True
    assert store.outbox_add('health:1', 'duplicate', NOW) is False
    store.outbox_add('health:2', 'second', NOW)
    assert [r['key'] for r in store.outbox_due(NOW)] == ['health:1', 'health:2']
    delays = []
    for attempt in range(8):
        next_ms = store.outbox_mark_failed('health:1', NOW, 'ReadTimeout\nhttps://api.telegram.org/bot')
        delays.append((next_ms - NOW) // 1000)
    assert delays == [2, 4, 8, 16, 32, 60, 60, 60]
    head = store.outbox_head()
    assert head['key'] == 'health:1' and head['attempts'] == 8 and '\n' not in head['last_error']
    # A backing-off head blocks newer events (delivery order is preserved).
    assert store.outbox_due(NOW + 59_000) == []
    assert [r['key'] for r in store.outbox_due(NOW + 60_000)] == ['health:1', 'health:2']
    assert store.outbox_mark_failed('health:1', NOW, 'TelegramError', retry_after=120) == NOW + 120_000
    store.outbox_mark_sent('health:1', NOW + 130_000)
    assert store.outbox_add('health:1', 'after delivery', NOW) is False
    assert store.outbox_head()['key'] == 'health:2' and store.outbox_pending_count() == 1
    store.outbox_mark_skipped('health:2', NOW)
    assert store.outbox_pending_count() == 0 and store.outbox_head() is None
    rows = {r['key']: r for r in store.outbox_rows()}
    assert rows['health:1']['status'] == 'sent' and rows['health:1']['text'] == 'first'
    assert rows['health:2']['status'] == 'skipped'
    assert store.outbox_mark_failed('missing', NOW, 'x') is None
    other = Store(store.path)
    try:
        assert other.outbox_rows() == store.outbox_rows()
    finally:
        other.close()


def test_signed_actions_share_one_durable_window_and_emergency_counts(store):
    store.action('buy:b1:create', NOW, 2)
    store.action('sell:s1:create', NOW + 1, 2)
    with pytest.raises(Unsafe, match='^SIGNED_ACTION_HOURLY_LIMIT$'):
        store.action('DMS', NOW + 2, 2)
    store.action('STOP:cancel', NOW + 2, 2, emergency=True)
    other = Store(store.path)
    try:
        assert other.action_count(NOW + 3) == 3
        assert other.action_count(NOW + 3_600_000) == 2
        other.action('DMS', NOW + 3_600_003, 2)
        assert other.action_count(NOW + 3_600_003) == 1
    finally:
        other.close()
    with pytest.raises(ValueError):
        store.action('', NOW, 2)


def test_concurrent_action_checks_do_not_exceed_shared_cap(store):
    def attempt(i):
        try:
            store.action(str(i), NOW, 3)
            return True
        except Unsafe:
            return False
    with ThreadPoolExecutor(max_workers=6) as pool:
        accepted = list(pool.map(attempt, range(20)))
    assert sum(accepted) == store.action_count(NOW) == 3


# ----------------------------------------------------------------------------- index samples, marks, review

def _snapshot(at_ms, ok=True, index='100'):
    component = IndexComponent(source_id='bybit_perp', venue='bybit_linear', symbol='STONKUSDT', quote='USDT',
                               price_kind='mid', weight=D('0.4'), required=True, included=ok,
                               reason=None if ok else 'STALE_QUOTE', native_price=D(index), usdt_price=D(index),
                               bid=D('99.9'), ask=D('100.1'), spread_pct=D('0.2'), event_ms=at_ms - 5,
                               received_age_s=0.01, effective_ms=at_ms - 5)
    return IndexSnapshot(ok=ok, index=D(index) if ok else None, center=D(index) if ok else None,
                         reason=None if ok else 'INSUFFICIENT_SOURCES:0/1', components=(component,), fx_rate=D(1),
                         fx_reason=None, method='weighted_median', at_ms=at_ms, n_included=1 if ok else 0)


def test_index_observations_components_and_marks(store):
    store.record_index_snapshot(_snapshot(NOW), spot_bid=D('99'), spot_ask=D('101'))
    store.record_index_snapshot(_snapshot(NOW + 200))                     # components throttled (< 1 s)
    store.record_index_snapshot(_snapshot(NOW + 400, ok=False))
    store.record_index(NOW + 1200, D('101'), D('101'), True, components=[{'x': D(1)}])
    obs = store.index_obs()
    assert [o['ok'] for o in obs] == [True, True, False, True]
    assert obs[0]['index'] == 100 and obs[0]['spot_bid'] == 99 and obs[2]['reason'] == 'INSUFFICIENT_SOURCES:0/1'
    comps = store.index_components()
    assert [c['ts_ms'] for c in comps] == [NOW, NOW + 1200]
    assert comps[0]['components'][0]['usdt_price'] == '100' and comps[1]['components'] == [{'x': '1'}]
    assert store.index_samples(NOW, NOW + 2000) == [(NOW, D(100)), (NOW + 200, D(100)), (NOW + 1200, D(101))]
    assert store.index_mark(NOW + 100) == (D(100), NOW)
    assert store.index_mark(NOW + 800) == (D(101), NOW + 1200)              # 600 vs 400 ms
    assert store.index_mark(NOW + 2300) is None
    with pytest.raises(ValueError):
        store.record_index(NOW, None, None, True)
    assert store.prune_index(NOW + 300) == 3
    assert [o['ts_ms'] for o in store.index_obs()] == [NOW + 400, NOW + 1200]


def test_fill_index_marks_first_value_wins(store):
    store.record_marks([('k1', 0, D('100'), NOW), ('k1', 5, D('101'), NOW + 5000)])
    store.record_marks([('k1', 0, D('99'), NOW + 1)])
    marks = store.marks('k1')
    assert [(m['horizon_s'], m['index_price'], m['sample_ms']) for m in marks] == [(0, D(100), NOW), (5, D(101), NOW + 5000)]
    with pytest.raises(ValueError):
        store.record_marks([('k2', 0, D(0), NOW)])


def test_account_review_keeps_first_reason_and_foreign_orders(store, clock):
    store.require_account_review('FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT')
    store.require_account_review('FOREIGN_FILL', NOW)
    assert store.external_activity() == {'reason': 'FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT',
                                         'detected_ms': int(clock[0] * 1000)}
    order = VenueOrder(order_id='42', text='apiv4', currency_pair=PAIR, side='buy', type='limit', account='spot',
                       time_in_force='gtc', price=D(1), amount=D(1), left=D(1), filled_amount=D(0), iceberg=D(0),
                       status='open', finish_as='open', create_time_ms=NOW, update_time_ms=NOW,
                       raw={'_comment': 'synthetic; schema: notes/api_gate_rest.md §3.2'})
    store.record_external_orders([order], NOW)
    store.record_external_orders([order], NOW + 5)
    with store.read() as db:
        row = db.execute('SELECT * FROM external_orders').fetchone()
    assert (row['first_seen_ms'], row['last_seen_ms']) == (NOW, NOW + 5)
    assert store.meta('external_open_orders_last_seen') == ['42']


def test_order_rebates_recorded_separately(store):
    order = VenueOrder(order_id='7', text='t-stk1-b1-1', currency_pair=PAIR, side='buy', type='limit',
                       account='spot', time_in_force='poc', price=D(1), amount=D(1), left=D(0),
                       filled_amount=D(1), iceberg=D(0), status='closed', finish_as='filled', create_time_ms=NOW,
                       update_time_ms=NOW, rebated_fee=D('0.001'), rebated_fee_currency='USDT')
    store.record_order_rebate(order, NOW)
    store.record_order_rebate(dataclasses.replace(order, rebated_fee=D('0.002')), NOW + 1)
    store.record_order_rebate(dataclasses.replace(order, order_id='8', rebated_fee=D(0)), NOW)
    assert [(r['order_id'], r['rebated_fee']) for r in store.order_rebates()] == [('7', D('0.002'))]


def test_confirmed_withdrawal_is_booked_at_average_cost(store):
    reserve(store, qty='10', price='2')
    rec(store, [trade(qty='10', price='2', fee='0')])
    removed, cost = store.record_transfer_out(D(4), D(6), NOW + 1000, base='STONK', quote='USDT')
    assert (removed, cost) == (D(4), D(8))
    pos = store.position()
    assert pos.qty == 6 and pos.cost == 12 and pos.realized == 0 and pos.transferred_qty == 4
    assert 'Похоже на вывод токенов: позиция бота уменьшена на 4' in texts(store)[-1]
    assert store.record_transfer_out(D(1), D(0), NOW + 2000, base='STONK', quote='USDT') == (1, 2)
    # A fill with an exchange time before the transfer goes through a replay (fill first).
    reserve(store, level='b2', qty='5', price='2', order_id='2')
    late = rec(store, [trade(order_id='2', tid='2', qty='5', price='1', fee='0', ms=NOW + 900)], now=NOW + 3000)
    assert late[0]['late'] is True
    pos = store.position()
    assert pos.qty == 10 and pos.transferred_qty == 5
    assert pos.cost == D(25) - D(4) * D(25) / D(15) - D(1) * (D(25) - D(4) * D(25) / D(15)) / D(11)


def test_memory_store_and_tables_owned_by_other_modules():
    s = Store(':memory:')
    try:
        s.initialize(IDENTITY, NOW)
        s.ensure_tables('CREATE TABLE IF NOT EXISTS paper_orders(order_id TEXT PRIMARY KEY, data TEXT NOT NULL);')
        with s.write() as db:
            db.execute("INSERT INTO paper_orders VALUES('1', '{}')")
        with s.read() as db:
            assert db.execute('SELECT COUNT(*) FROM paper_orders').fetchone()[0] == 1
        with s.write():
            with pytest.raises(RuntimeError, match='Nested'):
                s.set_meta('x', 1)
        assert s.meta('x') is None
    finally:
        s.close()
