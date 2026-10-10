"""Read-only report per ladder slot on a store populated with SYNTHETIC orders, fills,
index samples and marks. Every expected value below is computed by hand from the data
(the arithmetic is written out explicitly, in the same operation order as the ledger).

Timeline (T0 = window start = NOW - 1 h; ``+x`` = T0 + x ms):

* before the window: O0 buy:b1 (BUY 1 @ 97, fill F0), O8 sell:s1 (SELL 0.5 @ 100.5, fill F6)
* buy:b1  O1 +1000  p 99   q 2  C 100   fills F1 (+10 000) and F2 (+20 000), closed +21 000
          O2 +30000 p 98.5 q 1  C 99.5  no fill, REPRICE cancel, terminal +40 000
          O3 +50000 rejected; O4 +60000 post-only rejection (finish_as poc)
* buy:b2  O5 +2000  p 98   q 3  C 100   fill F3 (+15 000), still open
* sell:s1 O6 +3000  p 101  q 2  C 100   fill F4 1.5 (+25 000), BAD_MARKET cancel, terminal +35 000
* sell:s2 O7 +4000  p 103  q 1  C 100.5 (index_price 100.4) fill F5 (+45 000), terminal +46 000
"""
from __future__ import annotations

import json
import sqlite3
from decimal import Decimal

import pytest

from gate_stonk.report import build_report, distribution, edge_pct, format_report_ru
from gate_stonk.status import dumps
from gate_stonk.store import Store, fill_key_for
from gate_stonk.types import VenueTrade

D = Decimal
H = 3_600_000
NOW = 1_800_000_000_000
T0 = NOW - H
START = T0 - H
PAIR = 'STONK_USDT'
IDENTITY = {'campaign_id': 'rep', 'currency_pair': PAIR, 'mode': 'paper', 'client_prefix': 'stk1'}
SYNTHETIC = 'synthetic test trade; schema: notes/api_gate_rest.md §8 GET /spot/my_trades'


def buy_edge(ref, price):
    return (D(ref) - D(price)) / D(ref) * 100


def sell_edge(ref, price):
    return (D(price) - D(ref)) / D(ref) * 100


def weighted(pairs):
    total = sum((q for _, q in pairs), D(0))
    return sum((e * q for e, q in pairs), D(0)) / total


def trade(tid, order_id, side, qty, price, fee, cur, ms, gt='0', point='0'):
    raw = {'_comment': SYNTHETIC, 'id': tid, 'order_id': order_id, 'create_time_ms': f'{ms}.000000',
           'currency_pair': PAIR, 'side': side, 'role': 'maker', 'amount': qty, 'price': price, 'fee': fee,
           'fee_currency': cur, 'point_fee': point, 'gt_fee': gt}
    return VenueTrade(trade_id=tid, order_id=order_id, text=None, currency_pair=PAIR, side=side, role='maker',
                      amount=D(qty), price=D(price), fee=D(fee), fee_currency=cur, point_fee=D(point),
                      gt_fee=D(gt), create_time_ms=ms, sequence_id=None, raw=raw)


def key(order_id, tid, ms):
    return fill_key_for(PAIR, order_id, tid, ms)


class Campaign:
    """Builds the synthetic campaign; keeps fill keys by name (F0..F6)."""

    def __init__(self, store: Store):
        self.store = store
        self.keys: dict[str, str] = {}
        self.texts: dict[str, str] = {}

    def order(self, name, side, level, qty, price, created, center, *, order_id, reason='FIXED_EDGE', index=None):
        text = f't-stk1-{level}-{name.lower()}'
        self.store.reserve_order(text=text, side=side, level=level, qty=D(qty), price=D(price), now_ms=created,
                                 reason=reason, index_price=D(index or center), center_price=D(center))
        if order_id is not None:
            self.store.update_order(text, order_id=order_id, state='open')
        self.texts[name] = text
        return text

    def fill(self, name, tid, order_id, side, qty, price, fee, cur, ms, **kw):
        self.store.record_trades([trade(tid, order_id, side, qty, price, fee, cur, ms, **kw)], base='STONK',
                                 quote='USDT', now_ms=NOW)
        self.keys[name] = key(order_id, tid, ms)

    def close(self, name, status, terminal_ms, **fields):
        text = self.texts[name]
        if fields or terminal_ms is not None:
            self.store.update_order(text, terminal_status=status, terminal_since_ms=terminal_ms, **fields)
        self.store.close_order(text, status, (terminal_ms or T0) + 2000)


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / 'state.sqlite3')
    s.initialize(IDENTITY, START)
    yield s
    s.close()


@pytest.fixture
def campaign(store):
    c = Campaign(store)
    # before the window
    c.order('O0', 'buy', 'b1', '1', '97', T0 - 600_000, '98', order_id='100')
    c.fill('F0', '1', '100', 'buy', '1', '97', '0.097', 'USDT', T0 - 500_000)
    c.close('O0', 'filled', T0 - 499_000)
    c.order('O8', 'sell', 's1', '0.5', '100.5', T0 - 400_000, '99.5', order_id='108')
    c.fill('F6', '6', '108', 'sell', '0.5', '100.5', '0.05025', 'USDT', T0 - 300_000)
    c.close('O8', 'filled', T0 - 299_000)
    # buy:b1
    c.order('O1', 'buy', 'b1', '2', '99', T0 + 1000, '100', order_id='101')
    c.fill('F1', '2', '101', 'buy', '1', '99', '0.001', 'STONK', T0 + 10_000)
    c.fill('F2', '4', '101', 'buy', '1', '99', '0.001', 'STONK', T0 + 20_000)
    c.close('O1', 'filled', T0 + 21_000)
    c.order('O2', 'buy', 'b1', '1', '98.5', T0 + 30_000, '99.5', order_id='102', reason='RANGE_NEAREST_INDEX')
    c.close('O2', 'cancelled', T0 + 40_000, cancel_reason='REPRICE', state='cancel_pending', cancel_ack=True)
    c.order('O3', 'buy', 'b1', '1', '98', T0 + 50_000, '99', order_id=None)
    c.store.close_order(c.texts['O3'], 'rejected', T0 + 50_200)
    c.order('O4', 'buy', 'b1', '1', '99.9', T0 + 60_000, '101', order_id='104')
    c.store.close_order(c.texts['O4'], 'cancelled', T0 + 60_100, finish_as='poc')
    # buy:b2 (left open)
    c.order('O5', 'buy', 'b2', '3', '98', T0 + 2000, '100', order_id='105')
    c.fill('F3', '3', '105', 'buy', '1', '98', '0.098', 'USDT', T0 + 15_000)
    # sell:s1
    c.order('O6', 'sell', 's1', '2', '101', T0 + 3000, '100', order_id='106')
    c.fill('F4', '5', '106', 'sell', '1.5', '101', '0.1515', 'USDT', T0 + 25_000, gt='0.01')
    c.close('O6', 'cancelled', T0 + 35_000, cancel_reason='BAD_MARKET:INSUFFICIENT_SOURCES:1/2',
            state='cancel_pending')
    # sell:s2 (index_price differs from center: the report must use the center)
    c.order('O7', 'sell', 's2', '1', '103', T0 + 4000, '100.5', order_id='107', index='100.4')
    c.fill('F5', '7', '107', 'sell', '1', '103', '0.103', 'USDT', T0 + 45_000, point='0.02')
    c.close('O7', 'filled', T0 + 46_000)
    # index observations: (offset, index, ok, reason, spot_bid, spot_ask)
    obs = [
        (-5000, None, False, 'STALE_BEFORE_WINDOW', None, None),
        (19_800, '100.2', True, None, '100.0', '100.4'),
        (20_300, '100.4', True, None, '100.4', '100.6'),
        (24_900, '100.3', True, None, None, None),
        (25_600, '100.6', True, None, None, None),
        (26_000, None, False, 'INSUFFICIENT_SOURCES:1/2', None, None),
        (26_500, None, False, 'INSUFFICIENT_SOURCES:1/2', None, None),
        (28_000, None, False, 'REQUIRED_SOURCE_INVALID:bybit_perp:STALE_QUOTE', None, None),
        (29_500, '100.0', True, None, '99.9', '100.1'),
        (30_500, '101.0', True, None, '101.2', '101.4'),
        (45_000, '102', True, None, '101.9', '102.3'),
        (H + 500, None, False, 'AFTER_NOW', None, None),
    ]
    for offset, index, ok, reason, bid, ask in obs:
        store.record_index(T0 + offset, None if index is None else D(index), None if index is None else D(index),
                           ok, reason, spot_bid=None if bid is None else D(bid),
                           spot_ask=None if ask is None else D(ask))
    # engine-recorded marks: F1 all horizons, F4 only at execution time
    f1, f4 = c.keys['F1'], c.keys['F4']
    store.record_marks([(f1, 0, D('100'), T0 + 10_000), (f1, 5, D('99.5'), T0 + 15_000),
                        (f1, 30, D('101'), T0 + 40_000), (f1, 60, D('98'), T0 + 70_000),
                        (f4, 0, D('100.5'), T0 + 25_000)])
    return c


# ----------------------------------------------------------------------------- expectations

def expected_profit():
    """Average-cost replay written out by hand (fills in execution order)."""
    # F0 BUY 1 @ 97 fee 0.097 USDT -> qty 1, cost 97.097
    # F6 SELL 0.5 @ 100.5 fee 0.05025 USDT: proceeds 50.19975, removed 97.097*0.5/1
    removed6 = D('97.097') * D('0.5') / D('1')
    r6 = D('50.19975') * (D('0.5') / D('0.5')) - removed6
    assert r6 == D('1.65125')
    cost, qty = D('97.097') - removed6, D('0.5')
    cost, qty = cost + D('99'), qty + D('0.999')        # F1 BUY 1 @ 99, fee 0.001 STONK
    cost, qty = cost + D('98.098'), qty + D('1')         # F3 BUY 1 @ 98, fee 0.098 USDT
    cost, qty = cost + D('99'), qty + D('0.999')         # F2 BUY 1 @ 99, fee 0.001 STONK
    assert (cost, qty) == (D('344.6465'), D('3.498'))
    removed4 = cost * D('1.5') / qty                      # F4 SELL 1.5 @ 101, fee 0.1515 USDT
    r4 = D('151.3485') * (D('1.5') / D('1.5')) - removed4
    cost, qty = cost - removed4, qty - D('1.5')
    removed5 = cost * D('1') / qty                        # F5 SELL 1 @ 103, fee 0.103 USDT
    r5 = D('102.897') * (D('1') / D('1')) - removed5
    cost, qty = cost - removed5, qty - D('1')
    return {'r6': r6, 'r4': r4, 'r5': r5, 'cost': cost, 'qty': qty}


# ----------------------------------------------------------------------------- tests

def test_per_slot_metrics_match_hand_computation(store, campaign):
    report = build_report(store, hours=1, now_ms=NOW, slots=['sell:s3'])
    assert report['since_ms'] == T0 and report['until_ms'] == NOW
    assert list(report['slots']) == ['buy:b1', 'buy:b2', 'sell:s1', 'sell:s2', 'sell:s3']
    p = expected_profit()

    b1 = report['slots']['buy:b1']
    assert (b1['side'], b1['level']) == ('buy', 'b1')
    assert b1['orders'] == 2 and b1['orders_rejected'] == 1 and b1['orders_post_only_rejected'] == 1
    assert b1['orders_with_fills'] == 1 and b1['fill_ratio_pct'] == D(50)
    assert b1['fills'] == 2 and b1['filled_qty'] == D(2) and b1['filled_notional_usdt'] == D(198)
    assert b1['avg_fill_price'] == D(99)
    assert b1['avg_edge_at_placement_pct'] == (buy_edge(100, 99) + buy_edge('99.5', '98.5')) / 2
    assert b1['avg_edge_of_fills_vs_placement_index_pct'] == D(1)
    # F1: engine mark 100; F2: computed from samples 19 800 (100.2, 200 ms before) / 20 300 (300 ms after)
    assert b1['fills_with_index_at_fill'] == 2
    assert b1['avg_edge_of_fills_vs_index_at_fill_pct'] == weighted([(buy_edge(100, 99), D(1)),
                                                                     (buy_edge('100.2', 99), D(1))])
    # +5 s: F1 engine 99.5; F2 at +25 000 -> sample 24 900 (100.3, 100 ms) beats 25 600
    assert b1['fills_with_markout_5s'] == 2
    assert b1['markout_5s_pct'] == weighted([(buy_edge('99.5', 99), D(1)), (buy_edge('100.3', 99), D(1))])
    assert b1['markout_30s_pct'] == buy_edge(101, 99) and b1['fills_with_markout_30s'] == 1
    assert b1['markout_60s_pct'] == buy_edge(98, 99) < 0 and b1['fills_with_markout_60s'] == 1
    assert b1['median_rest_seconds'] == D(15)          # (20 s + 10 s) / 2
    assert b1['placement_rules'] == {'FIXED_EDGE': 1, 'RANGE_NEAREST_INDEX': 1}
    assert b1['cancel_reasons'] == {'REPRICE': 1}
    assert b1['fees'] == {'STONK': D('0.002'), 'USDT': D(0), 'GT': D(0), 'POINT': D(0)}
    assert b1['fees_usdt_equiv'] == D('0.198')
    assert b1['realized_usdt'] is None and b1['realized_in_window_usdt'] is None

    b2 = report['slots']['buy:b2']
    assert (b2['orders'], b2['orders_with_fills'], b2['fill_ratio_pct'], b2['fills']) == (1, 1, D(100), 1)
    assert (b2['filled_qty'], b2['filled_notional_usdt'], b2['avg_fill_price']) == (D(1), D(98), D(98))
    assert b2['avg_edge_at_placement_pct'] == D(2) == b2['avg_edge_of_fills_vs_placement_index_pct']
    assert b2['avg_edge_of_fills_vs_index_at_fill_pct'] is None and b2['fills_with_index_at_fill'] == 0
    assert b2['markout_5s_pct'] == buy_edge('100.2', 98)     # +20 000 -> 100.2
    assert b2['markout_30s_pct'] == buy_edge(102, 98)        # +45 000 -> exact sample 102
    assert b2['markout_60s_pct'] is None and b2['fills_with_markout_60s'] == 0
    assert b2['median_rest_seconds'] is None                 # still open
    assert b2['fees'] == {'STONK': D(0), 'USDT': D('0.098'), 'GT': D(0), 'POINT': D(0)}
    assert b2['fees_usdt_equiv'] == D('0.098')

    s1 = report['slots']['sell:s1']
    assert (s1['orders'], s1['orders_with_fills'], s1['fill_ratio_pct'], s1['fills']) == (1, 1, D(100), 1)
    assert (s1['filled_qty'], s1['filled_notional_usdt'], s1['avg_fill_price']) == (D('1.5'), D('151.5'), D(101))
    assert s1['avg_edge_at_placement_pct'] == D(1) == s1['avg_edge_of_fills_vs_placement_index_pct']
    assert s1['avg_edge_of_fills_vs_index_at_fill_pct'] == sell_edge('100.5', 101)   # engine mark wins
    assert s1['markout_5s_pct'] == D(1)          # +30 000: tie 29 500 / 30 500 -> earlier sample 100.0
    assert s1['markout_30s_pct'] is None and s1['markout_60s_pct'] is None
    assert s1['median_rest_seconds'] == D(32)
    assert s1['cancel_reasons'] == {'BAD_MARKET:INSUFFICIENT_SOURCES:1/2': 1}
    assert s1['fees'] == {'STONK': D(0), 'USDT': D('0.1515'), 'GT': D('0.01'), 'POINT': D(0)}
    assert s1['fees_usdt_equiv'] == D('0.1515')
    assert s1['realized_in_window_usdt'] == p['r4']
    assert s1['realized_usdt'] == p['r6'] + p['r4']           # campaign: includes the pre-window F6
    assert s1['window_unmatched_sold_qty'] == D(0) and s1['window_has_unknown_cost_sales'] is False

    s2 = report['slots']['sell:s2']
    assert s2['avg_edge_at_placement_pct'] == sell_edge('100.5', 103)        # center, not index_price
    assert s2['avg_edge_of_fills_vs_placement_index_pct'] == sell_edge('100.5', 103)
    assert s2['avg_edge_of_fills_vs_index_at_fill_pct'] == sell_edge(102, 103)
    assert s2['fills_with_markout_5s'] == 0 and s2['markout_5s_pct'] is None
    assert s2['median_rest_seconds'] == D(42)
    assert s2['fees'] == {'STONK': D(0), 'USDT': D('0.103'), 'GT': D(0), 'POINT': D('0.02')}
    assert s2['realized_usdt'] == p['r5'] == s2['realized_in_window_usdt']

    s3 = report['slots']['sell:s3']                            # configured, no activity
    assert s3['orders'] == 0 and s3['fill_ratio_pct'] is None and s3['fills'] == 0
    assert s3['avg_fill_price'] is None and s3['realized_usdt'] == D(0)


def test_side_and_campaign_totals(store, campaign):
    report = build_report(store, hours=1, now_ms=NOW)
    p = expected_profit()
    buy, sell, totals = report['sides']['buy'], report['sides']['sell'], report['totals']
    assert (buy['orders'], buy['orders_with_fills'], buy['orders_rejected'], buy['orders_post_only_rejected']) == (
        3, 2, 1, 1)
    assert buy['fill_ratio_pct'] == D(2) / 3 * 100
    assert (buy['fills'], buy['filled_qty'], buy['filled_notional_usdt']) == (3, D(3), D(296))
    assert buy['avg_fill_price'] == D(296) / D(3)
    assert (sell['orders'], sell['orders_with_fills'], sell['fill_ratio_pct']) == (2, 2, D(100))
    assert (sell['filled_qty'], sell['filled_notional_usdt'], sell['avg_fill_price']) == (D('2.5'), D('254.5'),
                                                                                           D('101.8'))
    assert sell['realized_in_window_usdt'] == p['r4'] + p['r5']
    assert totals['orders'] == 5 and totals['orders_with_fills'] == 4 and totals['fill_ratio_pct'] == D(80)
    assert totals['fills'] == 5 and totals['filled_qty'] == D('5.5') and totals['filled_notional_usdt'] == D('550.5')
    assert totals['avg_fill_price'] is None                      # BUY and SELL are not averaged together
    assert totals['turnover_usdt'] == D('550.5')
    assert totals['avg_edge_of_fills_vs_index_at_fill_pct'] == weighted([
        (buy_edge(100, 99), D(1)), (buy_edge('100.2', 99), D(1)), (sell_edge('100.5', 101), D('1.5')),
        (sell_edge(102, 103), D(1))])
    assert totals['fills_with_index_at_fill'] == 4
    assert totals['avg_sell_minus_avg_buy_pct'] == (D('101.8') - D(296) / D(3)) / (D(296) / D(3)) * 100
    assert totals['realized_usdt'] == p['r6'] + p['r4'] + p['r5'] == store.position().realized
    assert totals['realized_in_window_usdt'] == p['r4'] + p['r5']
    assert totals['fees'] == {'STONK': D('0.002'), 'USDT': D('0.3525'), 'GT': D('0.01'), 'POINT': D('0.02')}
    assert totals['fees_usdt_equiv'] == D('0.5505')
    assert totals['placement_rules'] == {'FIXED_EDGE': 4, 'RANGE_NEAREST_INDEX': 1}
    pos = report['position']
    assert pos['qty'] == p['qty'] == D('0.998') and pos['cost_usdt'] == p['cost']
    assert pos['avg_cost'] == p['cost'] / p['qty']
    assert pos['realized_usdt'] == totals['realized_usdt']
    assert (pos['bought_qty'], pos['bought_cost_usdt'], pos['sold_qty'], pos['sold_proceeds_usdt']) == (
        D(4), D(393), D(3), D('304.75'))
    assert pos['fees_base'] == D('0.002') and pos['fees_quote'] == D('0.49975')
    assert pos['fees_other'] == {'GT': D('0.01'), 'POINT': D('0.02')} and pos['fills'] == 7
    assert report['marks'] == {'bracket_ms': 1000, 'horizons_s': [0, 5, 30, 60], 'stored': 5, 'computed': 6}
    assert report['currency_pair'] == PAIR and report['base'] == 'STONK' and report['quote'] == 'USDT'
    assert report['mode'] == 'paper' and report['campaign_id'] == 'rep' and report['start_ms'] == START
    assert report['account_review_required'] is False and report['external_fills_excluded'] == 0


def test_blocked_index_time_and_reasons(store, campaign):
    index = build_report(store, hours=1, now_ms=NOW)['index']
    # durations (gap to next sample capped at 1 s; last sample capped by now and 1 s):
    # ok 500, ok 1000, ok 700, ok 400, blk 500, blk 1000, blk 1000, ok 1000, ok 1000, ok 1000
    assert index['samples'] == 10 and index['blocked_samples'] == 3
    assert index['observed_seconds'] == D('8.1') and index['blocked_seconds'] == D('2.5')
    assert index['blocked_index_time_pct'] == D(2500) / 8100 * 100
    assert index['coverage_pct'] == D(8100) / H * 100
    assert index['blocked_reasons'] == {
        'INSUFFICIENT_SOURCES:1/2': {'seconds': D('1.5'), 'samples': 2, 'pct_of_observed': D(1500) / 8100 * 100},
        'REQUIRED_SOURCE_INVALID:bybit_perp:STALE_QUOTE': {'seconds': D(1), 'samples': 1,
                                                           'pct_of_observed': D(1000) / 8100 * 100}}
    assert list(index['blocked_reason_codes']) == ['INSUFFICIENT_SOURCES', 'REQUIRED_SOURCE_INVALID']
    assert index['blocked_reason_codes']['REQUIRED_SOURCE_INVALID']['seconds'] == D(1)


def test_basis_statistics(store, campaign):
    basis = build_report(store, hours=1, now_ms=NOW)['basis']

    def b(bid, ask, index):
        return ((D(bid) + D(ask)) / 2 / D(index) - 1) * 100

    values = sorted([b('100.0', '100.4', '100.2'), b('100.4', '100.6', '100.4'), b('99.9', '100.1', '100.0'),
                     b('101.2', '101.4', '101.0'), b('101.9', '102.3', '102')])
    assert values[0] == values[1] == 0
    assert basis['count'] == 5
    assert basis['mean_pct'] == sum(values, D(0)) / 5
    assert basis['median_pct'] == values[2] == b('101.9', '102.3', '102')
    assert basis['p5_pct'] == D(0) and basis['p95_pct'] == values[4] == b('101.2', '101.4', '101.0')
    assert basis['min_pct'] == 0 and basis['max_pct'] == values[4]
    assert basis['definition'] == '(spot_mid / index - 1) * 100'


def test_whole_campaign_includes_pre_window_activity(store, campaign):
    report = build_report(store, hours=None, now_ms=NOW)
    p = expected_profit()
    assert report['since_ms'] is None and report['window_hours'] is None
    b1, s1 = report['slots']['buy:b1'], report['slots']['sell:s1']
    assert (b1['orders'], b1['fills'], b1['filled_qty']) == (3, 3, D(3))
    assert (s1['orders'], s1['fills']) == (2, 2)
    assert s1['realized_in_window_usdt'] == s1['realized_usdt'] == p['r6'] + p['r4']
    index = report['index']
    # + the pre-window blocked sample (1 s, capped); the sample after "now" is excluded
    assert index['samples'] == 11 and index['blocked_seconds'] == D('3.5') and index['observed_seconds'] == D('9.1')
    assert index['coverage_pct'] == D(9100) / (NOW - START) * 100
    assert 'AFTER_NOW' not in index['blocked_reasons'] and 'STALE_BEFORE_WINDOW' in index['blocked_reasons']


def test_window_upper_bound_excludes_later_activity(store, campaign):
    now = T0 + 22_000
    report = build_report(store, hours=1, now_ms=now)
    assert report['since_ms'] == now - H
    b1 = report['slots']['buy:b1']
    assert (b1['orders'], b1['orders_rejected'], b1['orders_post_only_rejected'], b1['fills']) == (2, 0, 0, 3)
    assert report['totals']['fills'] == 5                       # F0, F6, F1, F3, F2
    assert report['sides']['sell']['fills'] == 1
    index = report['index']
    # -5000 blocked (1 s cap), 19 800 ok 500 ms, 20 300 ok until now (capped 1 s)
    assert index['samples'] == 3 and index['observed_seconds'] == D('2.5')
    assert index['blocked_index_time_pct'] == D(40)


def test_report_is_read_only_single_snapshot(store, campaign, tmp_path):
    before = store.db.total_changes
    first = build_report(store, hours=1, now_ms=NOW)
    assert store.db.total_changes == before
    assert store.db.execute('PRAGMA query_only').fetchone()[0] == 0 and not store.db.in_transaction
    assert store.outbox_add('after:report', 'still writable', NOW)
    # A writer commits from another connection while the report is reading: the report
    # keeps its first snapshot (no new slot), a later report sees the new row.
    other = sqlite3.connect(str(tmp_path / 'state.sqlite3'), isolation_level=None)
    done = []

    def trace(statement):
        if 'FROM orders' in statement and not done:
            done.append(1)
            other.execute("INSERT INTO orders(text,side,level,qty,price,index_price,center_price,created_ms,reason,"
                          "state) VALUES('t-stk1-s9-late','sell','s9','1','105','100','100',?,'FIXED_EDGE','closed')",
                          (T0 + 5000,))

    store.db.set_trace_callback(trace)
    try:
        during = build_report(store, hours=1, now_ms=NOW)
    finally:
        store.db.set_trace_callback(None)
        other.close()
    assert done and 'sell:s9' not in during['slots']
    assert during['slots'] == first['slots']
    assert 'sell:s9' in build_report(store, hours=1, now_ms=NOW)['slots']


def test_report_from_path_matches_store(store, campaign, tmp_path):
    by_store = build_report(store, hours=1, now_ms=NOW)
    by_path = build_report(tmp_path / 'state.sqlite3', hours=1, now_ms=NOW)
    assert by_path == by_store
    with pytest.raises(ValueError):
        build_report(tmp_path / 'missing.sqlite3', hours=1, now_ms=NOW)


def test_report_is_json_safe_and_russian_text(store, campaign):
    report = build_report(store, hours=1, now_ms=NOW, slots=['sell:s3'])
    data = json.loads(dumps(report))
    assert data['slots']['buy:b1']['fill_ratio_pct'] == '50.0'
    text = format_report_ru(report)
    lines = text.splitlines()
    assert lines[0] == ("Отчёт paper 'rep' (STONK_USDT) за 1 ч (2027-01-15 07:00:00 UTC — 2027-01-15 08:00:00 UTC).")
    header = next(line for line in lines if line.startswith('Слот'))
    assert 'Fill,%' in header and 'Исп./индекс,% (n)' in header and '60 с,%' in header
    row = next(line for line in lines if line.startswith('buy:b1'))
    cells = [c.strip() for c in row.split('|')]
    assert cells[:8] == ['buy:b1', '2', '1', '50', '2', '2', '198.00', '99']
    assert cells[-1] == 'н/д'
    s1 = [c.strip() for c in next(line for line in lines if line.startswith('sell:s1')).split('|')]
    assert s1[-1] == str(expected_profit()['r4'].quantize(D('0.0001'))).join(['+', ''])
    assert any(line.startswith('Итого BUY') for line in lines) and any(line.startswith('Всего') for line in lines)
    assert 'Отклонено биржей: 1; post-only отказов: 1.' in text
    assert 'заблокирован 30.86% наблюдаемого времени' in text
    assert 'INSUFFICIENT_SOURCES:1/2 — 1.5 с (18.5%)' in text
    assert 'Базис (спот mid / индекс − 1), %' in text and 'наблюдений 5' in text
    assert 'buy:b1: FIXED_EDGE×1, RANGE_NEAREST_INDEX×1' in text
    assert 'Причины отмен: buy:b1: REPRICE×1; sell:s1: BAD_MARKET:INSUFFICIENT_SOURCES:1/2×1.' in text
    assert 'Комиссии за окно: STONK 0.002; USDT 0.3525; GT 0.01; POINT 0.02; ≈ 0.55 USDT' in text
    assert 'Отметки индекса: из журнала 5, рассчитано по наблюдениям 6' in text


def test_empty_store_report(store):
    report = build_report(store, hours=24, now_ms=NOW)
    assert report['slots'] == {} and report['totals']['orders'] == 0 and report['totals']['fill_ratio_pct'] is None
    assert report['totals']['realized_usdt'] == D(0) and report['position']['avg_cost'] is None
    assert report['index']['blocked_index_time_pct'] is None and report['basis']['count'] == 0
    text = format_report_ru(report)
    assert 'Базис: нет наблюдений со спот-стаканом.' in text and 'Правила выставления: нет заявок.' in text
    json.loads(dumps(report))


def test_external_activity_flag(store):
    store.require_account_review('FOREIGN_FILL', NOW - 1000)
    report = build_report(store, hours=1, now_ms=NOW)
    assert report['account_review_required'] is True
    assert report['external_activity'] == {'reason': 'FOREIGN_FILL', 'detected_ms': NOW - 1000}
    assert 'посторонняя активность' in format_report_ru(report)


def test_initial_avg_price_values_unmatched_sales(store):
    c = Campaign(store)
    c.order('O1', 'sell', 's1', '2', '10', T0 + 1000, '9.5', order_id='201')
    c.fill('F1', '11', '201', 'sell', '2', '10', '0.02', 'USDT', T0 + 2000)
    unknown = build_report(store, hours=1, now_ms=NOW)
    s1 = unknown['slots']['sell:s1']
    assert s1['window_unmatched_sold_qty'] == D(2) and s1['window_has_unknown_cost_sales'] is True
    assert s1['realized_in_window_usdt'] == D(0)
    known = build_report(store, hours=1, now_ms=NOW, initial_avg_price=D('8'))
    s1 = known['slots']['sell:s1']
    assert s1['window_has_unknown_cost_sales'] is False
    assert s1['realized_in_window_incl_initial_usdt'] == D('19.98') - D(2) * D(8)
    assert known['position']['realized_incl_initial_usdt'] == D('19.98') - D(16)


def test_helpers():
    assert edge_pct('buy', D(99), D(100)) == D(1) and edge_pct('sell', D(101), D(100)) == D(1)
    assert edge_pct('buy', D(99), None) is None and edge_pct('sell', D(1), D(0)) is None
    stats = distribution([D(i) for i in range(1, 21)])
    assert stats == {'count': 20, 'mean': D('10.5'), 'median': D('10.5'), 'p5': D(1), 'p95': D(19),
                     'min': D(1), 'max': D(20)}
    assert distribution([])['count'] == 0 and distribution([D(3)])['p95'] == D(3)
    with pytest.raises(ValueError):
        build_report(':memory:', hours=0, now_ms=NOW)


def test_edge_at_placement_uses_the_creation_price_of_an_amended_order(store):
    """Regression (found by the synthetic simulation with reprice_via_amend): the centre
    is recorded at creation, so the placement edge must use the creation price, not the
    amended price (BUY created at 99 against centre 100 = +1 %, later amended to 99.6)."""
    store.reserve_order(text='t-stk1-b1-amd1', side='buy', level='b1', qty=D('1'), price=D('99'), now_ms=T0 + 1000,
                        reason='FIXED_EDGE', index_price=D('100'), center_price=D('100'))
    store.update_order('t-stk1-b1-amd1', order_id='501', state='open')
    store.begin_amend('t-stk1-b1-amd1', price=D('99.6'), amend_text='a1', now_ms=T0 + 5000)
    store.confirm_amend('t-stk1-b1-amd1', T0 + 5000)
    assert store.order('t-stk1-b1-amd1')['price'] == D('99.6')
    report = build_report(store, hours=1, now_ms=NOW)
    assert report['slots']['buy:b1']['avg_edge_at_placement_pct'] == buy_edge('100', '99') == D(1)
