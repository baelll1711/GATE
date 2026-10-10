"""Ladder targets (DESIGN §5): min-new-edge buffer after rounding, strict monotonicity
(+LADDER_SHIFT / LEVEL_COLLAPSED), +TOUCH inside range bands, exact levels that wait,
self-cross against every own opposite order, own visible (iceberg) volume.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from gate_stonk.config import LevelConfig
from gate_stonk.dec import D
from gate_stonk.pricing import (
    coupled_buy_ceiling, edge_pct, ladder_targets, maker_limit, post_only_ok, select_level_price,
)
from gate_stonk.types import Book, PairMeta


def meta(precision: int = 3) -> PairMeta:
    return PairMeta('STONK_USDT', 'STONK', 'USDT', precision, 2, None, None, None, None, 'tradable')


def book(bids, asks) -> Book:
    return Book(tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), 100.0, 100000)


def ex(level_id: str, side: str, edge, **kw) -> LevelConfig:
    return LevelConfig(level_id, side, 'exact', edge_pct=D(edge), **kw)


def rg(level_id: str, side: str, lo, hi, skip=0, **kw) -> LevelConfig:
    return LevelConfig(level_id, side, 'range', min_edge_pct=D(lo), max_edge_pct=D(hi),
                       skip_notional_usdt=D(skip), **kw)


def ladder(center, snap, levels, side, *, precision=3, own_levels=None, own_opposite=(), floor=None,
           ceiling=None, min_new_edge='1', profit_floor_mode='hold', resting=None):
    limit = maker_limit(side, snap, [D(p) for p in own_opposite])
    return ladder_targets(D(center), snap, meta(precision), levels, side, own_levels=own_levels, floor=floor,
                          ceiling=ceiling, maker_limit=limit, profit_floor_mode=profit_floor_mode,
                          min_new_edge_pct=D(min_new_edge), resting=resting)


def summary(targets):
    return [(t.level, t.decision.price if t.decision else None, t.reason, t.placeable) for t in targets]


WIDE = book(bids=(('0.1200', '1000'),), asks=(('0.1400', '1000'),))


# ============================================================================ prompt example
def test_prompt_example_two_levels_per_side():
    snap = book(bids=(('0.1350', '1000'),), asks=(('0.1365', '1000'),))
    buys = ladder('0.1359', snap, [ex('b1', 'buy', 1), ex('b2', 'buy', 2)], 'buy', precision=4)
    sells = ladder('0.1359', snap, [ex('s1', 'sell', 1), ex('s2', 'sell', 2)], 'sell', precision=4)
    assert summary(buys) == [('b1', D('0.1345'), 'FIXED_EDGE', True), ('b2', D('0.1331'), 'FIXED_EDGE', True)]
    assert summary(sells) == [('s1', D('0.1373'), 'FIXED_EDGE', True), ('s2', D('0.1387'), 'FIXED_EDGE', True)]
    for target in buys + sells:
        assert edge_pct(target.side, target.decision.price, D('0.1359')) >= 1
        assert target.decision.lower == target.decision.upper == target.decision.price


def test_rounding_direction_is_away_from_center_on_both_sides():
    # 0.13595 * 0.99 = 0.1345905 -> 0.1345 (floor), 0.13595 * 1.01 = 0.1373095 -> 0.1374 (ceil)
    [buy] = ladder('0.13595', WIDE, [ex('b1', 'buy', 1)], 'buy', precision=4)
    [sell] = ladder('0.13595', WIDE, [ex('s1', 'sell', 1)], 'sell', precision=4)
    assert (buy.decision.price, sell.decision.price) == (D('0.1345'), D('0.1374'))
    assert buy.placeable and sell.placeable


# ============================================================================ min-new-edge buffer after rounding
def test_exact_edge_equal_to_the_buffer_after_rounding_is_allowed():
    # 0.13 * 0.99 = 0.1287 exactly: edge is exactly 1.0 %, the buffer is inclusive.
    [buy] = ladder('0.13', WIDE, [ex('b1', 'buy', 1)], 'buy', precision=4)
    [sell] = ladder('0.13', WIDE, [ex('s1', 'sell', 1)], 'sell', precision=4)
    assert (buy.decision.price, buy.placeable) == (D('0.1287'), True)
    assert (sell.decision.price, sell.placeable) == (D('0.1313'), True)
    assert edge_pct('buy', buy.decision.price, D('0.13')) == 1


@pytest.mark.parametrize('side,center,price,placeable', [
    ('buy', '0.1359', '0.1345', True),     # floor(0.13455459): edge 1.030 % >= 1 % although min_edge is 0.99
    ('buy', '0.1301', '0.1288', False),    # floor(0.12881201): edge 0.999 % < 1 %
    ('sell', '0.1359', '0.1373', True),    # ceil(0.13724541): edge 1.030 %
    ('sell', '0.1301', '0.1314', False),   # ceil(0.13138799): edge 0.999 %
])
def test_buffer_is_measured_after_rounding_against_the_center(side, center, price, placeable):
    [target] = ladder(center, WIDE, [rg('x1', side, '0.99', 3)], side, precision=4)
    assert target.decision.price == D(price)
    assert target.placeable is placeable
    if placeable:
        assert target.reason == 'ZERO_SKIP'
    else:
        assert target.reason == 'MIN_NEW_EDGE_BUFFER'
        assert target.decision.reason == 'ZERO_SKIP'     # decision kept for diagnostics


def test_range_upper_bound_inside_the_buffer_blocks_only_when_the_price_lands_there():
    level = rg('b1', 'buy', '0.5', 3, skip=50000)
    thin = book(bids=(('99.6', '1'),), asks=(('101', '100'),))
    [blocked] = ladder(100, thin, [level], 'buy')
    assert blocked.decision.price == D('99.5') and blocked.decision.reason == 'INSUFFICIENT_VISIBLE_DEPTH'
    assert (blocked.reason, blocked.placeable) == ('MIN_NEW_EDGE_BUFFER', False)
    deep = book(bids=(('99.6', '100'), ('98.5', '600')), asks=(('101', '100'),))
    [ok] = ladder(100, deep, [level], 'buy')
    assert (ok.decision.price, ok.reason, ok.placeable) == (D('98.499'), 'BOOK_VOLUME', True)


# ============================================================================ strict monotonicity
def test_exact_levels_that_round_to_the_same_tick_shift_deeper():
    buys = ladder('0.1359', WIDE, [ex('b1', 'buy', 1), ex('b2', 'buy', '1.03'), ex('b3', 'buy', '1.05')], 'buy',
                  precision=4)
    assert summary(buys) == [
        ('b1', D('0.1345'), 'FIXED_EDGE', True),
        ('b2', D('0.1344'), 'FIXED_EDGE+LADDER_SHIFT', True),    # 0.13450023 -> 0.1345 collides
        ('b3', D('0.1343'), 'FIXED_EDGE+LADDER_SHIFT', True),    # 0.13447305 -> 0.1344 collides with b2
    ]
    # the reported band includes the shifted price so a resting order there is not "outside the band"
    assert (buys[1].decision.lower, buys[1].decision.upper) == (D('0.1344'), D('0.1345'))
    sells = ladder('0.1359', WIDE, [ex('s1', 'sell', 1), ex('s2', 'sell', '1.03')], 'sell', precision=4)
    assert summary(sells) == [('s1', D('0.1373'), 'FIXED_EDGE', True),
                              ('s2', D('0.1374'), 'FIXED_EDGE+LADDER_SHIFT', True)]
    assert (sells[1].decision.lower, sells[1].decision.upper) == (D('0.1373'), D('0.1374'))


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_range_level_shifts_inside_its_band_and_evidence_is_recomputed(side):
    snap = book(bids=(('99', '100'), ('98', '100')), asks=(('101', '100'), ('102', '100')))
    first, second = ladder(100, snap, [rg('l1', side, 1, 3), rg('l2', side, 1, 4)], side)
    if side == 'buy':
        assert (first.decision.price, second.decision.price) == (D(99), D('98.999'))
        assert (second.decision.lower, second.decision.upper) == (D(96), D(99))
        assert (first.decision.skipped_notional, second.decision.skipped_notional) == (0, D(9900))
    else:
        assert (first.decision.price, second.decision.price) == (D(101), D('101.001'))
        assert (second.decision.lower, second.decision.upper) == (D(101), D(104))
        assert (first.decision.skipped_notional, second.decision.skipped_notional) == (0, D(10100))
    assert second.reason == 'ZERO_SKIP+LADDER_SHIFT' and second.placeable
    assert second.decision.ahead_volume_complete


@pytest.mark.parametrize('side,prices', [('buy', ('0.99', '0.99', '0.98')), ('sell', ('1.01', '1.01', '1.02'))])
def test_range_levels_whose_bands_coincide_at_coarse_precision_collapse(side, prices):
    # center 1, precision 2: ranges [1.0, 1.5] and [1.0, 1.9] both round to the single tick 0.99 / 1.01.
    snap = book(bids=(('0.95', '1000'),), asks=(('1.05', '1000'),))
    levels = [rg('l1', side, '1.0', '1.5'), rg('l2', side, '1.0', '1.9'), ex('l3', side, 2)]
    targets = ladder(1, snap, levels, side, precision=2)
    assert summary(targets) == [
        ('l1', D(prices[0]), 'ZERO_SKIP', True),
        ('l2', D(prices[1]), 'LEVEL_COLLAPSED', False),
        ('l3', D(prices[2]), 'FIXED_EDGE', True),
    ]
    assert targets[1].decision.lower == targets[1].decision.upper == D(prices[1])


def test_collapsed_level_is_not_a_reference_so_the_ladder_cannot_invert():
    snap = book(bids=(('90', '1'),), asks=(('110', '1'),))
    levels = [ex('b1', 'buy', 2), rg('b2', 'buy', 1, '1.5'), rg('b3', 'buy', '1.5', 3)]
    targets = ladder(100, snap, levels, 'buy')
    assert summary(targets) == [
        ('b1', D(98), 'FIXED_EDGE', True),
        ('b2', D(99), 'LEVEL_COLLAPSED', False),           # 97.999 is outside [98.5, 99]
        ('b3', D('97.999'), 'ZERO_SKIP+LADDER_SHIFT', True),   # below b1, not merely below b2's 99
    ]


def test_level_collapsed_takes_precedence_over_the_buffer():
    snap = book(bids=(('90', '1'),), asks=(('110', '1'),))
    targets = ladder(100, snap, [ex('b1', 'buy', 2), rg('b2', 'buy', 1, '1.5')], 'buy', min_new_edge='1.2')
    assert targets[1].reason == 'LEVEL_COLLAPSED'


def test_exact_level_cannot_shift_below_the_smallest_tick():
    snap = book(bids=(('0.01', '1'),), asks=(('0.02', '1'),))
    targets = ladder('0.0102', snap, [ex('b1', 'buy', 1), ex('b2', 'buy', '1.5')], 'buy', precision=2)
    assert summary(targets) == [('b1', D('0.01'), 'FIXED_EDGE', True), ('b2', D('0.01'), 'LEVEL_COLLAPSED', False)]


def test_unsafe_level_has_no_decision_and_is_skipped_as_reference():
    snap = book(bids=(('99.5', '1'),), asks=(('101', '1'),))
    levels = [ex('b1', 'buy', 1), rg('b2', 'buy', 2, 3, skip=10**6, insufficient_depth='wait'), ex('b3', 'buy', 3)]
    targets = ladder(100, snap, levels, 'buy')
    assert summary(targets) == [
        ('b1', D(99), 'FIXED_EDGE', True),
        ('b2', None, 'INSUFFICIENT_VISIBLE_DEPTH', False),
        ('b3', D(97), 'FIXED_EDGE', True),
    ]


# ============================================================================ monotonicity against resting orders
def test_deeper_level_shifts_below_a_shallower_resting_order():
    # Index 0.1380: fresh b2 (2 %) = 0.1352, but b1 still rests deeper-tolerated at 0.1345.
    levels = [ex('b1', 'buy', 1), ex('b2', 'buy', 2)]
    targets = ladder('0.1380', WIDE, levels, 'buy', precision=4, resting={'b1': [D('0.1345')]})
    assert summary(targets) == [('b1', D('0.1366'), 'FIXED_EDGE', True),
                                ('b2', D('0.1344'), 'FIXED_EDGE+LADDER_SHIFT', True)]
    assert targets[1].decision.lower == D('0.1344')                 # band widened to the shifted price
    sells = ladder('0.1340', WIDE, [ex('s1', 'sell', 1), ex('s2', 'sell', 2)], 'sell', precision=4,
                   resting={'s1': [D('0.1370')]})                 # fresh s2 = 0.1367 <= resting s1
    assert summary(sells)[1] == ('s2', D('0.1371'), 'FIXED_EDGE+LADDER_SHIFT', True)


def test_deeper_range_level_collapses_when_it_cannot_pass_a_shallower_resting_order():
    levels = [ex('b1', 'buy', 1), rg('b2', 'buy', '1.05', '1.2')]
    targets = ladder('0.1380', WIDE, levels, 'buy', precision=4, resting={'b1': [D('0.1360'), D('0.1345')]})
    assert summary(targets)[1] == ('b2', D('0.1365'), 'LEVEL_COLLAPSED', False)   # needs < 0.1345: out of band


def test_shallower_level_waits_for_a_deeper_resting_order():
    # Index falls to 0.1320: fresh b1 = 0.1306, but b2 still rests at 0.1331 (not repriced yet).
    levels = [ex('b1', 'buy', 1), ex('b2', 'buy', 2)]
    targets = ladder('0.1320', WIDE, levels, 'buy', precision=4, resting={'b2': [D('0.1331')]})
    assert summary(targets) == [('b1', D('0.1306'), 'LEVEL_ORDER_WAIT', False),
                                ('b2', D('0.1293'), 'FIXED_EDGE', True)]
    equal = ladder('0.1320', WIDE, levels, 'buy', precision=4, resting={'b2': [D('0.1306')]})
    assert summary(equal)[0] == ('b1', D('0.1306'), 'LEVEL_ORDER_WAIT', False)    # equal is not strictly above
    sells = ladder('0.1400', WIDE, [ex('s1', 'sell', 1), ex('s2', 'sell', 2)], 'sell', precision=4,
                   resting={'s2': [D('0.1387')]})
    assert summary(sells)[0] == ('s1', D('0.1414'), 'LEVEL_ORDER_WAIT', False)


def test_own_resting_order_and_ordered_resting_orders_change_nothing():
    levels = [ex('b1', 'buy', 1), ex('b2', 'buy', 2)]
    plain = summary(ladder('0.1359', WIDE, levels, 'buy', precision=4))
    resting = {'b1': [D('0.1345'), D('0.1350')], 'b2': [D('0.1331')]}
    assert summary(ladder('0.1359', WIDE, levels, 'buy', precision=4, resting=resting)) == plain


def test_invalid_resting_price_blocks_the_ladder():
    from gate_stonk.errors import Unsafe
    with pytest.raises(Unsafe, match='^INVALID_RESTING_PRICE$'):
        ladder('0.1359', WIDE, [ex('b1', 'buy', 1)], 'buy', precision=4, resting={'b2': [D('-1')]})


# ============================================================================ TOUCH / waiting / self-cross
def test_range_levels_touch_inside_their_band_and_keep_the_ladder_strict():
    snap = book(bids=(('97.2', '1'),), asks=(('97.6', '1'),))
    levels = [rg('b1', 'buy', 1, 3), rg('b2', 'buy', 2, 4), ex('b3', 'buy', '2.5')]
    targets = ladder(100, snap, levels, 'buy')
    assert summary(targets) == [
        ('b1', D('97.599'), 'ZERO_SKIP+TOUCH', True),
        ('b2', D('97.598'), 'ZERO_SKIP+TOUCH+LADDER_SHIFT', True),
        ('b3', D('97.5'), 'FIXED_EDGE', True),
    ]
    for target in targets:
        assert post_only_ok('buy', target.decision.price, snap, [])


@pytest.mark.parametrize('side,snap,waiting,next_price', [
    ('buy', book(bids=(('98', '1'),), asks=(('98.5', '1'),)), '99', '98'),
    ('sell', book(bids=(('101.5', '1'),), asks=(('102', '1'),)), '101', '102'),
])
def test_exact_level_never_moves_through_the_book_it_waits(side, snap, waiting, next_price):
    first, second = ladder(100, snap, [ex('l1', side, 1), ex('l2', side, 2)], side)
    assert (first.decision.price, first.reason, first.placeable) == (D(waiting), 'POST_ONLY_OR_SELF_CROSS_WAIT', False)
    assert first.decision.reason == 'FIXED_EDGE'          # no +TOUCH: an exact price is never moved
    assert (second.decision.price, second.placeable) == (D(next_price), True)


def test_range_band_entirely_through_the_book_waits():
    snap = book(bids=(('96.5', '1'),), asks=(('96.8', '1'),))
    for mode, price in (('nearest_index', '98'), ('clamp', '97')):
        [target] = ladder(100, snap, [rg('b1', 'buy', 2, 3, skip=50000, out_of_range=mode)], 'buy')
        assert (target.decision.price, target.reason, target.placeable) == (D(price), 'POST_ONLY_OR_SELF_CROSS_WAIT', False)


def test_buffer_reason_precedes_post_only_wait():
    snap = book(bids=(('99', '1'),), asks=(('99.4', '1'),))
    [target] = ladder(100, snap, [ex('b1', 'buy', '0.5')], 'buy')
    assert (target.decision.price, target.reason) == (D('99.5'), 'MIN_NEW_EDGE_BUFFER')


def test_buy_levels_never_cross_any_own_sell_level():
    # Paper: own orders are absent from the public book, so maker_limit must include every own SELL.
    snap = book(bids=(('99', '10'),), asks=(('101', '10'),))
    own_sells = ['102', '100.5']
    levels = [ex('b1', 'buy', 1), rg('b2', 'buy', 2, 6)]
    first, second = ladder(105, snap, levels, 'buy', own_opposite=own_sells)    # center jumped up
    assert (first.decision.price, first.reason) == (D('103.95'), 'POST_ONLY_OR_SELF_CROSS_WAIT')
    assert (second.decision.price, second.reason, second.placeable) == (D('100.499'), 'ZERO_SKIP+TOUCH', True)
    assert post_only_ok('buy', second.decision.price, snap, [D(p) for p in own_sells])
    # Using only the book would rest at 100.999 and cross the own SELL at 100.5.
    book_only = ladder(105, snap, levels, 'buy')
    assert book_only[1].decision.price == D('100.999')
    assert not post_only_ok('buy', book_only[1].decision.price, snap, [D(p) for p in own_sells])


def test_sell_levels_never_cross_any_own_buy_level():
    snap = book(bids=(('99', '10'),), asks=(('101', '10'),))
    own_buys = ['98', '99.5']
    first, second = ladder(95, snap, [ex('s1', 'sell', 1), rg('s2', 'sell', 2, 6)], 'sell', own_opposite=own_buys)
    assert (first.decision.price, first.reason) == (D('95.95'), 'POST_ONLY_OR_SELF_CROSS_WAIT')
    assert (second.decision.price, second.reason, second.placeable) == (D('99.501'), 'ZERO_SKIP+TOUCH', True)
    assert post_only_ok('sell', second.decision.price, snap, [D(p) for p in own_buys])


# ============================================================================ own visible volume
def test_own_visible_volume_of_all_same_side_levels_is_subtracted():
    # own b1 iceberg at 99: amount 500, visible 100 -> the caller passes 100;
    # own b2 normal order at 97.5 with 50 left.
    snap = book(bids=(('99', '300'), ('97.5', '600')), asks=(('101', '100'),))
    levels = [rg('b1', 'buy', 1, 3, skip=15000), rg('b2', 'buy', 2, 3, skip=60000)]
    own = {D(99): D(100), D('97.5'): D(50)}
    first, second = ladder(100, snap, levels, 'buy', own_levels=own)
    assert (first.decision.price, first.reason) == (D('98.999'), 'BOOK_VOLUME')    # 19800 foreign at 99
    assert first.decision.own_excluded_notional == D(9900) + D(4875)
    assert first.decision.visible_notional == D(19800) + D('53625')
    # b2 needs 60000: 19800 + 53625 reached at 97.5 -> 97.499
    assert (second.decision.price, second.reason) == (D('97.499'), 'BOOK_VOLUME')
    # Passing the iceberg's whole amount would hide 200 lots of foreign volume at 99.
    hidden = ladder(100, snap, levels, 'buy', own_levels={D(99): D(500), D('97.5'): D(50)})
    assert hidden[0].decision.price == D('97.499')


# ============================================================================ profit floor / ceiling on every level
def test_profit_floor_applies_to_every_sell_level():
    snap = book(bids=(('99', '10'),), asks=(('101', '10'),))
    levels = [ex('s1', 'sell', 1), rg('s2', 'sell', '1.5', 3)]
    held = ladder(100, snap, levels, 'sell', floor=D('102.0001'))
    assert summary(held) == [('s1', None, 'PROFIT_FLOOR_ABOVE_RANGE', False),
                             ('s2', D('102.001'), 'ZERO_SKIP+PROFIT_FLOOR', True)]
    quoted = ladder(100, snap, levels, 'sell', floor=D('102.0001'), profit_floor_mode='quote')
    assert summary(quoted) == [('s1', D('102.001'), 'PROFIT_FLOOR_QUOTE', True),
                               ('s2', D('102.002'), 'ZERO_SKIP+PROFIT_FLOOR+LADDER_SHIFT', True)]


def test_profit_ceiling_from_the_lowest_own_sell_applies_to_every_buy_level():
    snap = book(bids=(('98', '10'),), asks=(('102', '10'),))
    ceiling = coupled_buy_ceiling(D(100), D('0.3'), D('0.04'))     # 99.6211...
    levels = [ex('b1', 'buy', '1.2'), rg('b2', 'buy', 1, 3), ex('b3', 'buy', 2)]
    targets = ladder(101, snap, levels, 'buy', ceiling=ceiling, own_opposite=['100'])
    assert summary(targets) == [
        ('b1', None, 'PROFIT_CEILING_BELOW_RANGE', False),    # 99.788 is above the ceiling
        ('b2', D('99.621'), 'ZERO_SKIP+PROFIT_CEILING', True),
        ('b3', D('98.98'), 'FIXED_EDGE', True),
    ]


# ============================================================================ argument checks
def test_ladder_keeps_select_level_price_decisions_when_nothing_conflicts():
    snap = book(bids=(('99', '100'), ('97.5', '600')), asks=(('101', '100'), ('102.5', '500')))
    level = rg('b1', 'buy', 2, 3, skip=50000)
    [target] = ladder(100, snap, [level], 'buy')
    direct = select_level_price(D(100), snap, meta(), level, maker_limit=D(101))
    assert target.decision == direct and target.reason == direct.reason == 'BOOK_VOLUME'


def test_level_of_the_other_side_is_a_programming_error():
    with pytest.raises(ValueError):
        ladder(100, WIDE, [ex('s1', 'sell', 1)], 'buy')


@pytest.mark.parametrize('center', [D(0), Decimal('NaN'), None])
def test_invalid_center_blocks_every_level(center):
    snap = book(bids=(('99', '1'),), asks=(('101', '1'),))
    targets = ladder_targets(center, snap, meta(), [ex('b1', 'buy', 1), ex('b2', 'buy', 2)], 'buy',
                             maker_limit=D(101), min_new_edge_pct=D(1))
    assert [(t.decision, t.reason, t.placeable) for t in targets] == [(None, 'INVALID_PRICE_RANGE', False)] * 2


def test_malformed_book_blocks_every_level():
    snap = book(bids=(('101', '1'),), asks=(('101', '1'),))
    targets = ladder_targets(D(100), snap, meta(), [ex('b1', 'buy', 1), rg('b2', 'buy', 2, 3)], 'buy',
                             maker_limit=None, min_new_edge_pct=D(1))
    assert [t.reason for t in targets] == ['CROSSED_BOOK', 'CROSSED_BOOK']
