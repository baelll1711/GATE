"""Pricing on Gate's fixed grid: grid helpers, level bands, select_level_price, guards.

Pinned examples are ported from the 12-chagpt reference (notes/ref_pricing.md §1, §5-§8).
The reference used Hyperliquid's significant-figure tick (0.01 at >= 100); here the
grid is fixed (``precision 3`` at index 100 -> tick 0.001 everywhere), so values
above 100 are adapted: e.g. reference SELL 102.51 becomes 102.501.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from gate_stonk.config import LevelConfig
from gate_stonk.dec import D, on_grid
from gate_stonk.errors import Unsafe
from gate_stonk.pricing import (
    UNSAFE_CODES, adverse_boundary, ceil_price, coupled_buy_ceiling, coupled_sell_floor, edge_pct,
    floor_price, hard_guard_violation, level_bounds, maker_limit, outside_tolerance, post_only_ok,
    price_after, price_before, profit_floor_price, select_level_price,
)
from gate_stonk.types import Book, PairMeta


def meta(precision: int = 3, amount_precision: int = 2, **kw) -> PairMeta:
    fields = dict(min_base_amount=None, min_quote_amount=None, max_base_amount=None, max_quote_amount=None)
    fields.update(kw)
    return PairMeta('STONK_USDT', 'STONK', 'USDT', precision, amount_precision,
                    trade_status='tradable', **fields)


def book(bids=(('99', '100'), ('97.5', '600')), asks=(('101', '100'), ('102.5', '500'))) -> Book:
    return Book(tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), 100.0, 100000)


def rng(side: str, lo='2', hi='3', skip='50000', **kw) -> LevelConfig:
    return LevelConfig('x1', side, 'range', min_edge_pct=D(lo), max_edge_pct=D(hi),
                       skip_notional_usdt=D(skip), **kw)


def exact(side: str, edge, **kw) -> LevelConfig:
    return LevelConfig('x1', side, 'exact', edge_pct=D(edge), **kw)


_DEFAULT_BOOK = object()


def sel(level, snap=_DEFAULT_BOOK, center='100', m=None, **kw):
    return select_level_price(D(center), book() if snap is _DEFAULT_BOOK else snap, m or meta(), level, **kw)


def one_sided(side: str, levels) -> Book:
    """A book whose priced side is ``levels`` and whose other side is far away."""
    if side == 'buy':
        return book(bids=levels, asks=(('200', '1'),))
    return book(bids=(('1', '1'),), asks=levels)


# ============================================================================ grid helpers
@pytest.mark.parametrize('raw,precision,expected', [
    ('0.134541', 4, '0.1345'), ('0.1345', 4, '0.1345'), ('103.0009', 3, '103'),
    ('98.00049', 3, '98'), ('12.999', 0, '12'), ('0.0009', 3, '0'),
])
def test_floor_price_never_rounds_up(raw, precision, expected):
    result = floor_price(D(raw), meta(precision))
    assert result == D(expected) and result <= D(raw) and on_grid(result, precision)


@pytest.mark.parametrize('raw,precision,expected', [
    ('0.137259', 4, '0.1373'), ('0.1373', 4, '0.1373'), ('102.00051', 3, '102.001'),
    ('101.9991', 3, '102'), ('12.001', 0, '13'), ('0.0000001', 3, '0.001'),
])
def test_ceil_price_never_rounds_down(raw, precision, expected):
    result = ceil_price(D(raw), meta(precision))
    assert result == D(expected) and result >= D(raw) and on_grid(result, precision)


@pytest.mark.parametrize('raw,precision,expected', [
    ('100', 3, '99.999'), ('10', 3, '9.999'), ('1000', 3, '999.999'),   # constant tick: no finer tick below 10**k
    ('100.001', 3, '100'), ('97.5', 3, '97.499'), ('100.0005', 3, '100'),
    ('0.1345', 4, '0.1344'), ('10', 0, '9'), ('0.001', 3, '0'), ('0.0005', 3, '0'), ('1', 0, '0'),
])
def test_price_before_is_strict_and_zero_means_no_positive_tick(raw, precision, expected):
    result = price_before(D(raw), meta(precision))
    assert result == D(expected)
    assert result < D(raw) and on_grid(result, precision)


@pytest.mark.parametrize('raw,precision,expected', [
    ('99.999', 3, '100'), ('100', 3, '100.001'), ('102.5', 3, '102.501'), ('102.5005', 3, '102.501'),
    ('9.999', 3, '10'), ('0.0001', 3, '0.001'), ('12', 0, '13'), ('0.1373', 4, '0.1374'),
])
def test_price_after_is_strict(raw, precision, expected):
    result = price_after(D(raw), meta(precision))
    assert result == D(expected)
    assert result > D(raw) and on_grid(result, precision)


# ============================================================================ level bounds
@pytest.mark.parametrize('side,edge,expected', [
    ('buy', '1', '0.1345'), ('buy', '2', '0.1331'), ('sell', '1', '0.1373'), ('sell', '2', '0.1387'),
])
def test_prompt_example_exact_levels_round_away_from_center(side, edge, expected):
    # center 0.1359, precision 4: BUY 0.134541 -> 0.1345, 0.133182 -> 0.1331;
    # SELL 0.137259 -> 0.1373, 0.138618 -> 0.1387.
    assert level_bounds(D('0.1359'), meta(4), exact(side, edge)) == (D(expected), D(expected))


@pytest.mark.parametrize('side,center,expected', [
    ('sell', '100', '102'), ('buy', '100', '98'),
    ('sell', '100.0005', '102.001'),   # 102.00051 -> up: never below center x 1.02
    ('buy', '100.0005', '98'),         # 98.00049 -> down: never above center x 0.98
])
def test_fixed_edge_single_price_rounded_away_from_center(side, center, expected):
    assert level_bounds(D(center), meta(), exact(side, 2)) == (D(expected), D(expected))
    result = sel(exact(side, 2, skip_notional_usdt=D(50000)), center=center)
    assert (result.price, result.reason) == (D(expected), 'FIXED_EDGE')
    assert not result.volume_pricing_applied and result.volume_price is None


@pytest.mark.parametrize('side,center,expected', [('buy', '100.0005', '98'), ('sell', '100.0005', '102.001')])
def test_range_with_equal_edges_is_fixed_edge_like_reference(side, center, expected):
    level = rng(side, 2, 2)
    assert level_bounds(D(center), meta(), level) == (D(expected), D(expected))
    assert sel(level, center=center).reason == 'FIXED_EDGE'


def test_range_lower_bound_ceils_and_upper_bound_floors():
    center = D('100.0004')
    lower, upper = level_bounds(center, meta(), rng('sell'))
    assert (lower, upper) == (D('102.001'), D(103))      # raw 102.000408 / 103.000412
    assert lower >= center * D('1.02') and upper <= center * D('1.03')
    lower, upper = level_bounds(center, meta(), rng('buy'))
    assert (lower, upper) == (D('97.001'), D(98))        # raw 97.000388 / 98.000392
    assert lower >= center * D('0.97') and upper <= center * D('0.98')


def test_no_tick_inside_a_narrow_band_blocks_instead_of_rounding_outside():
    with pytest.raises(Unsafe, match='^EMPTY_PRICE_RANGE$'):
        level_bounds(D(100), meta(2), rng('sell', '2.001', '2.009'))
    assert level_bounds(D(100), meta(3), rng('sell', '2.001', '2.009')) == (D('102.001'), D('102.009'))


@pytest.mark.parametrize('side,center,precision,level', [
    ('buy', '0.00001', 5, exact('buy', 2)),          # 0.0000098 floors to 0
    ('buy', '100', 3, exact('buy', 100)),           # raw price 0
    ('buy', '100', 3, rng('buy', 50, 150)),          # raw lower negative
    ('buy', '0.01', 2, rng('buy', 1, 50)),           # upper 0.0099 floors to 0
])
def test_nonpositive_prices_are_refused(side, center, precision, level):
    with pytest.raises(Unsafe, match='^NONPOSITIVE_PRICE_RANGE$'):
        level_bounds(D(center), meta(precision), level)
    with pytest.raises(Unsafe, match='^NONPOSITIVE_PRICE_RANGE$'):
        select_level_price(D(center), book(), meta(precision), level)


@pytest.mark.parametrize('side,center,expected', [('buy', '0.0000103', '0.00001'), ('sell', '0.000001', '0.00001')])
def test_smallest_positive_tick_is_still_valid(side, center, expected):
    assert level_bounds(D(center), meta(5), exact(side, 2)) == (D(expected), D(expected))


@pytest.mark.parametrize('center', [D(0), D(-1), Decimal('NaN'), Decimal('Infinity'), None, 'x', True])
def test_invalid_center_refused(center):
    with pytest.raises(Unsafe, match='^INVALID_PRICE_RANGE$'):
        level_bounds(center, meta(), rng('buy'))


@pytest.mark.parametrize('level', [
    rng('buy', 3, 2),
    LevelConfig('x1', 'buy', 'exact', edge_pct=None),
    LevelConfig('x1', 'sell', 'range', min_edge_pct=D(1), max_edge_pct=None),
    LevelConfig('x1', 'sell', 'range', min_edge_pct=Decimal('NaN'), max_edge_pct=D(2)),
])
def test_invalid_edges_refused(level):
    with pytest.raises(Unsafe, match='^INVALID_PRICE_RANGE$'):
        level_bounds(D(100), meta(), level)


def test_unknown_level_mode_and_side():
    with pytest.raises(Unsafe, match='^INVALID_LEVEL_MODE$'):
        level_bounds(D(100), meta(), LevelConfig('x1', 'buy', 'coefficient', edge_pct=D(1)))
    with pytest.raises(ValueError):
        level_bounds(D(100), meta(), LevelConfig('x1', 'long', 'exact', edge_pct=D(1)))


# ============================================================================ select: book volume
def test_buy_skips_foreign_bids_from_high_to_low():
    result = sel(rng('buy'))
    assert (result.price, result.reason) == (D('97.499'), 'BOOK_VOLUME')
    assert (result.lower, result.upper) == (D(97), D(98))
    assert result.skipped_notional == D(68400) and result.visible_notional == D(68400)
    assert result.volume_price == D('97.499')
    assert result.depth_sufficient and result.skip_satisfied and result.volume_pricing_applied


def test_sell_skips_foreign_asks_from_low_to_high():
    result = sel(rng('sell'))
    assert (result.price, result.reason) == (D('102.501'), 'BOOK_VOLUME')   # reference (HL tick .01): 102.51
    assert (result.lower, result.upper) == (D(102), D(103))
    assert result.skipped_notional == D(61350)


@pytest.mark.parametrize('side,expected', [('buy', '98'), ('sell', '102')])
def test_insufficient_depth_falls_back_to_minimum_advantage(side, expected):
    result = sel(rng(side), book(bids=(('99', '1'),), asks=(('101', '1'),)))
    assert (result.price, result.reason) == (D(expected), 'INSUFFICIENT_VISIBLE_DEPTH')
    assert result.volume_price is None and not result.depth_sufficient


@pytest.mark.parametrize('side,level,expected,reason', [
    ('buy', '98.001', '98', 'BOOK_VOLUME'),
    ('buy', '97.001', '97', 'BOOK_VOLUME'),
    ('buy', '97', '98', 'VOLUME_BELOW_RANGE'),
    ('buy', '99', '98', 'VOLUME_ABOVE_RANGE'),
    ('sell', '101.999', '102', 'BOOK_VOLUME'),
    ('sell', '102.999', '103', 'BOOK_VOLUME'),
    ('sell', '101.99', '102', 'VOLUME_BELOW_RANGE'),   # strict tick 101.991 is still below the band
    ('sell', '103', '102', 'VOLUME_ABOVE_RANGE'),
    ('sell', '101', '102', 'VOLUME_BELOW_RANGE'),
])
def test_range_boundaries_apply_after_the_strict_tick(side, level, expected, reason):
    result = sel(rng(side), one_sided(side, ((level, '1000'),)))
    assert (result.price, result.reason) == (D(expected), reason)
    beyond = price_before if side == 'buy' else price_after
    assert result.volume_price == beyond(D(level), meta())


@pytest.mark.parametrize('side,price', [('buy', '97.5'), ('sell', '102.5')])
def test_exact_notional_threshold_must_be_behind_the_whole_level(side, price):
    level = rng(side, skip=D(price) * 100)
    result = sel(level, one_sided(side, ((price, '100'),)))
    assert result.price < D(price) if side == 'buy' else result.price > D(price)
    assert result.skipped_notional == level.skip_notional_usdt and result.skip_satisfied


def test_same_price_volume_is_not_counted_as_ahead():
    result = sel(rng('sell'), one_sided('sell', (('101', '10'), ('102', '10'), ('102.5', '10'))))
    assert (result.price, result.reason) == (D(102), 'INSUFFICIENT_VISIBLE_DEPTH')
    assert result.visible_notional == D(3055)
    assert result.skipped_notional == D(1010)


def test_zero_skip_selects_minimum_advantage_and_reports_visible_volume():
    result = sel(rng('sell', skip=0), one_sided('sell', (('101', '500'),)))
    assert (result.price, result.reason) == (D(102), 'ZERO_SKIP')
    assert result.volume_price is None and result.skipped_notional == D(50500)
    assert not result.volume_pricing_applied


@pytest.mark.parametrize('side,expected', [('buy', '100.01'), ('sell', '99.99')])
def test_signed_negative_minimum_edge_allows_buy_above_and_sell_below_center(side, expected):
    result = sel(rng(side, '-0.01', '0.01', skip=0))
    assert (result.price, result.reason) == (D(expected), 'ZERO_SKIP')


def test_negative_buy_range_is_not_clamped_at_center():
    result = sel(rng('buy', -3, -2, skip=0))
    assert (result.lower, result.upper, result.price) == (D(102), D(103), D(103))


def test_buy_below_the_smallest_positive_tick_falls_back():
    # One-tick band [0.00001, 0.00001] from unequal edges: price_before(0.00001) is ZERO.
    level = rng('buy', 0, 50, skip='0.00001')
    snap = book(bids=(('0.00001', '1'),), asks=(('0.00002', '1'),))
    result = select_level_price(D('0.00001'), snap, meta(5), level)
    assert result.price == D('0.00001') and result.volume_price == 0
    assert result.reason == 'VOLUME_BELOW_RANGE'


# ============================================================================ select: own volume
def test_own_bid_is_capped_at_the_level_and_never_counted_as_foreign():
    result = sel(rng('buy'), book(bids=(('99', '100'), ('97.5', '400'))), own_levels={D(99): D(500)})
    assert result.own_excluded_notional == D(9900)
    assert result.visible_notional == D(39000)
    assert (result.price, result.reason) == (D(98), 'INSUFFICIENT_VISIBLE_DEPTH')
    assert result.skipped_notional == 0    # final 98 bid sits ahead of the 97.5 level


def test_own_ask_does_not_shift_candidate_to_a_self_only_level():
    result = sel(rng('sell'), own_levels={D(101): D(100)})
    assert result.price == D('102.501')
    assert result.own_excluded_notional == D(10100)
    assert result.visible_notional == D(51250)


def test_own_volume_is_capped_per_level_and_not_carried_to_the_next():
    result = sel(rng('sell'), own_levels={D(101): D(1000), D(150): D(999)})
    assert result.price == D('102.501')
    assert result.own_excluded_notional == D(10100)
    assert result.visible_notional == D(51250)


def test_partly_own_level_can_turn_a_matching_price_into_fallback():
    result = sel(rng('sell'), one_sided('sell', (('102.5', '500'),)), own_levels={D('102.5'): D(20)})
    assert (result.price, result.reason) == (D(102), 'INSUFFICIENT_VISIBLE_DEPTH')
    assert result.visible_notional == D(49200) and result.own_excluded_notional == D(2050)


def test_own_iceberg_subtracts_only_the_visible_part_supplied_by_the_caller():
    # Own iceberg BUY at 99: amount 500, visible 100. The book shows 300 at 99 (200 foreign).
    snap = book(bids=(('99', '300'), ('97.5', '600')))
    level = rng('buy', skip=15000)
    visible_only = sel(level, snap, own_levels={D(99): D(100)})
    assert visible_only.own_excluded_notional == D(9900)
    assert visible_only.visible_notional == D(19800) + D(58500)
    assert (visible_only.price, visible_only.reason) == (D(98), 'VOLUME_ABOVE_RANGE')   # 19800 foreign at 99
    whole_amount = sel(level, snap, own_levels={D(99): D(500)})   # wrong: hides foreign volume
    assert whole_amount.own_excluded_notional == D(29700)
    assert (whole_amount.price, whole_amount.reason) == (D('97.499'), 'BOOK_VOLUME')


def test_own_levels_match_numerically_equal_prices():
    result = sel(rng('sell'), own_levels={D('101.000'): D(100)})
    assert result.own_excluded_notional == D(10100)


@pytest.mark.parametrize('own', [
    {D(99): D(-1)}, {D(99): Decimal('NaN')}, {Decimal('Infinity'): D(1)}, {D(0): D(1)},
    {99: D(1)}, {D(99): 1},
])
def test_invalid_own_levels_are_not_silently_ignored(own):
    with pytest.raises(Unsafe, match='^INVALID_OWN_LEVEL$'):
        sel(rng('buy'), own_levels=own)


# ============================================================================ select: book validation
@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_corrupt_opposite_depth_also_blocks(side):
    good = book()
    snap = Book(good.bids, ((D(101), D(100)), (D(102), Decimal('NaN'))), 100.0, 100000)
    with pytest.raises(Unsafe, match='^INVALID_BOOK_LEVEL$'):
        sel(rng(side), snap)


@pytest.mark.parametrize('snap,code', [
    (None, 'MISSING_BOOK_DEPTH'),
    (Book((), (), 100.0, 100000), 'MISSING_BOOK_DEPTH'),
    (Book(((D(99), D(1)),), (), 100.0, 100000), 'MISSING_BOOK_DEPTH'),
    (book(bids=(('99', '1'), ('99', '2'))), 'UNSORTED_OR_DUPLICATE_BOOK_LEVEL'),
    (book(asks=(('102', '1'), ('101', '2'))), 'UNSORTED_OR_DUPLICATE_BOOK_LEVEL'),
    (book(bids=(('101', '1'),)), 'CROSSED_BOOK'),
    (book(bids=(('103', '1'),)), 'CROSSED_BOOK'),
    (book(asks=(('101', '1'), ('103', '0'))), 'INVALID_BOOK_LEVEL'),
    (book(asks=(('101', '1'), ('103', '-1'))), 'INVALID_BOOK_LEVEL'),
    (Book(((D(99), D(1)),), ((D(101), D(1)), (D(103),)), 100.0, 100000), 'INVALID_BOOK_LEVEL'),
    (Book(((D(99), D(1)),), ((D(101), D(1)), (103, D(1))), 100.0, 100000), 'INVALID_BOOK_LEVEL'),
])
def test_missing_malformed_unsorted_and_crossed_books_block(snap, code):
    for side in ('buy', 'sell'):
        with pytest.raises(Unsafe, match=f'^{code}$'):
            sel(rng(side), snap)


def test_bad_deeper_level_cannot_hide_behind_an_already_reached_threshold():
    snap = Book(((D(99), D(1000)), (D(97), Decimal('Infinity'))), ((D(101), D(1)),), 100.0, 100000)
    with pytest.raises(Unsafe, match='^INVALID_BOOK_LEVEL$'):
        sel(rng('buy', skip=100), snap)


# ============================================================================ select: modes and policies
DEEP = book(bids=(('99', '100'), ('96', '1000')), asks=(('101', '100'), ('104', '1000')))


@pytest.mark.parametrize('mode,side,expected,reason', [
    ('nearest_index', 'buy', '98', 'VOLUME_BELOW_RANGE'),
    ('clamp', 'buy', '97', 'BOOK_VOLUME_CLAMP_LOWER'),
    ('nearest_index', 'sell', '102', 'VOLUME_ABOVE_RANGE'),
    ('clamp', 'sell', '103', 'BOOK_VOLUME_CLAMP_UPPER'),
])
def test_more_volume_required_in_front_never_makes_clamp_more_aggressive(mode, side, expected, reason):
    result = sel(rng(side, out_of_range=mode), DEEP)
    assert (result.price, result.reason) == (D(expected), reason)


def test_clamp_with_short_depth_rests_behind_all_visible_volume():
    shallow = book(bids=(('99', '10'), ('97.5', '10')), asks=(('101', '10'), ('102.5', '10')))
    buy = sel(rng('buy', out_of_range='clamp'), shallow)
    sell = sel(rng('sell', out_of_range='clamp'), shallow)
    assert (buy.price, buy.reason) == (D('97.499'), 'DEPTH_EXHAUSTED')
    assert (sell.price, sell.reason) == (D('102.501'), 'DEPTH_EXHAUSTED')
    legacy = sel(rng('buy', out_of_range='nearest_index'), shallow)
    assert (legacy.price, legacy.reason) == (D(98), 'INSUFFICIENT_VISIBLE_DEPTH')


def test_clamp_anchor_ignores_own_volume_no_ratchet():
    # The deepest visible bid (97.5) is entirely ours: anchoring on it would place the
    # new order just behind our own order and walk the price down step after step.
    snap = book(bids=(('99', '10'), ('97.5', '10')))
    result = sel(rng('buy', out_of_range='clamp'), snap, own_levels={D('97.5'): D(10)})
    assert (result.price, result.reason) == (D(98), 'DEPTH_EXHAUSTED_CLAMP_UPPER')
    assert result.volume_price == D('98.999')


def test_clamp_without_any_foreign_depth_falls_back():
    snap = book(bids=(('99', '1'),))
    result = sel(rng('buy', skip=100, out_of_range='clamp'), snap, own_levels={D(99): D(1)})
    assert (result.price, result.reason) == (D(98), 'NO_FOREIGN_DEPTH')
    assert result.volume_price is None and result.visible_notional == 0


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_depth_shortfall_with_clamp_reports_actual_visible_volume(side):
    snap = book(bids=(('99', '1'), ('98.5', '1')), asks=(('101', '1'), ('101.5', '1')))
    result = sel(rng(side, skip=10000, out_of_range='clamp'), snap)
    assert result.requested_skip_notional == 10000
    assert not result.depth_sufficient and not result.skip_satisfied
    assert result.volume_pricing_applied and result.visible_notional < 10000
    assert result.skipped_notional == result.visible_notional
    assert not result.ahead_volume_complete


def test_clamp_can_undo_requested_skip_and_own_volume_never_counts():
    snap = book(bids=(('99', '1'), ('98', '2'), ('96', '100')), asks=(('101', '2'),))
    result = sel(rng('buy', skip=200, out_of_range='clamp'), snap, own_levels={D(98): D(2)})
    assert (result.price, result.reason) == (D(97), 'BOOK_VOLUME_CLAMP_LOWER')
    assert result.depth_sufficient and not result.skip_satisfied
    assert result.skipped_notional == 99 and result.own_excluded_notional == 196
    assert result.ahead_volume_complete


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_wait_rejects_short_depth_but_default_clamp_is_unchanged(side):
    shallow = book(bids=(('99', '1'),), asks=(('101', '1'),))
    default = sel(rng(side, skip=10000, out_of_range='clamp'), shallow)
    explicit = sel(rng(side, skip=10000, out_of_range='clamp', insufficient_depth='clamp'), shallow)
    assert default == explicit
    with pytest.raises(Unsafe, match='^INSUFFICIENT_VISIBLE_DEPTH$'):
        sel(rng(side, skip=10000, out_of_range='clamp', insufficient_depth='wait'), shallow)


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_wait_requires_volume_ahead_of_the_final_price_after_band_clamp(side):
    snap = book(bids=(('99', '1'), ('96', '100')), asks=(('101', '1'), ('104', '100')))
    with pytest.raises(Unsafe, match='^SKIP_NOTIONAL_NOT_REACHED$'):
        sel(rng(side, skip=200, out_of_range='clamp', insufficient_depth='wait'), snap)


@pytest.mark.parametrize('side', ['buy', 'sell'])
def test_wait_accepts_enough_volume_but_excludes_own(side):
    snap = book(bids=(('99', '3'),), asks=(('101', '3'),))
    level = rng(side, skip=200, out_of_range='clamp', insufficient_depth='wait')
    result = sel(level, snap)
    assert result.skip_satisfied and result.depth_sufficient
    own = {D(99 if side == 'buy' else 101): D(3)}
    with pytest.raises(Unsafe, match='^INSUFFICIENT_VISIBLE_DEPTH$'):
        sel(level, snap, own_levels=own)


@pytest.mark.parametrize('make', [
    lambda **kw: exact('buy', 2, skip_notional_usdt=D(50000), **kw),
    lambda **kw: rng('buy', 2, 2, **kw),
    lambda **kw: rng('buy', skip=0, **kw),
])
def test_wait_does_not_change_fixed_or_zero_skip_results(make):
    shallow = book(bids=(('99', '1'),), asks=(('101', '1'),))
    assert sel(make(insufficient_depth='wait'), shallow) == sel(make(), shallow)


def test_invalid_modes_and_skip_are_refused():
    with pytest.raises(Unsafe, match='^INVALID_OUT_OF_RANGE_MODE$'):
        sel(rng('buy', out_of_range='nearest'))
    for policy in (None, 'ignore', True, 0):
        with pytest.raises(Unsafe, match='^INVALID_INSUFFICIENT_DEPTH_POLICY$'):
            sel(rng('buy', insufficient_depth=policy))
    with pytest.raises(Unsafe, match='^INVALID_PROFIT_FLOOR_MODE$'):
        sel(rng('sell'), profit_floor_mode='qoute')
    with pytest.raises(Unsafe, match='^INVALID_SKIP_NOTIONAL$'):
        sel(rng('buy', skip=-1))
    with pytest.raises(Unsafe, match='^INVALID_SKIP_NOTIONAL$'):
        sel(LevelConfig('x1', 'buy', 'range', min_edge_pct=D(2), max_edge_pct=D(3), skip_notional_usdt=None))
    # an empty out_of_range means the reference default; an exact level needs no skip
    assert sel(rng('buy', out_of_range=None)) == sel(rng('buy'))
    assert sel(LevelConfig('x1', 'buy', 'exact', edge_pct=D(2), skip_notional_usdt=None)).price == D(98)


# ============================================================================ select: LOGIC table (BUY 2-3 %, C=100)
@pytest.mark.parametrize('bids,asks,nearest,clamp', [
    ((('97.5', '600'),), (('101', '1'),), '97.499', '97.499'),         # 50k at 97.5
    ((('96', '600'),), (('101', '1'),), '98', '97'),                   # 50k only at 96
    ((('99', '100'), ('97.5', '103.6')), (('101', '1'),), '98', '97.499'),   # only ~20k down to 97.5
    ((('99', '100'), ('98.6', '102'))  , (('101', '1'),), '98', '98'),       # ~20k down to 98.6
    ((('97.2', '1'),), (('97.6', '1'),), '97.599', '97.199'),          # ask inside the band: nearest +TOUCH,
                                                                       # clamp rests behind the last bid
])
def test_reference_logic_table(bids, asks, nearest, clamp):
    snap = book(bids=bids, asks=asks)
    limit = maker_limit('buy', snap, [])
    for mode, expected in (('nearest_index', nearest), ('clamp', clamp)):
        assert sel(rng('buy', out_of_range=mode), snap, maker_limit=limit).price == D(expected), mode


def test_spot_fully_below_the_buy_band_returns_the_crossing_price_without_touch():
    snap = book(bids=(('96.5', '1'),), asks=(('96.8', '1'),))
    for mode, expected in (('nearest_index', '98'), ('clamp', '97')):
        result = sel(rng('buy', out_of_range=mode), snap, maker_limit=D('96.8'))
        assert result.price == D(expected) and '+TOUCH' not in result.reason
        assert not post_only_ok('buy', result.price, snap, [])     # the ladder waits


# ============================================================================ select: TOUCH
def test_buy_band_above_the_ask_rests_one_tick_under_the_ask():
    snap = Book(((D('99.95'), D(1)),), ((D(100), D(1)),), 1.0, 1)
    level = rng('buy', '-0.01', '1', skip=50000)
    plain = sel(level, snap)
    assert plain.price == D('100.01')          # would cross the ask: never placeable
    touched = sel(level, snap, maker_limit=D(100))
    assert (touched.price, touched.reason) == (D('99.999'), 'INSUFFICIENT_VISIBLE_DEPTH+TOUCH')
    assert touched.lower <= touched.price <= touched.upper


def test_sell_band_below_the_bid_rests_one_tick_over_the_bid():
    snap = Book(((D('102.5'), D(1)),), ((D(103), D(1)),), 1.0, 1)
    result = sel(rng('sell', skip=0), snap, maker_limit=D('102.5'))
    assert (result.price, result.reason) == (D('102.501'), 'ZERO_SKIP+TOUCH')    # reference: 102.51


def test_fixed_edge_never_moves_through_the_book():
    crossed = book(bids=(('100', '5'),), asks=(('100.2', '5'),))
    result = sel(exact('buy', '-0.5'), crossed, maker_limit=D('100.2'))     # BUY target 100.5 >= ask
    assert result.price == D('100.5') and '+TOUCH' not in result.reason


@pytest.mark.parametrize('limit', [D(0), D(-1), Decimal('NaN'), 'x'])
def test_invalid_maker_limit_refused(limit):
    with pytest.raises(Unsafe, match='^INVALID_MAKER_LIMIT$'):
        sel(rng('buy'), maker_limit=limit)


# ============================================================================ select: profit floor / ceiling
def test_profit_floor_raises_the_sell_band_or_holds_or_quotes():
    level = rng('sell', skip=0)
    assert sel(level, DEEP).price == D(102)
    raised = sel(level, DEEP, floor=D('102.4001'))
    assert raised.lower == D('102.401') and raised.price == D('102.401')     # reference: 102.41
    assert raised.reason == 'ZERO_SKIP+PROFIT_FLOOR'
    with pytest.raises(Unsafe, match='^PROFIT_FLOOR_ABOVE_RANGE$'):
        sel(level, DEEP, floor=D(105))
    quoted = sel(level, DEEP, floor=D(105), profit_floor_mode='quote')
    assert (quoted.price, quoted.lower, quoted.upper, quoted.reason) == (D(105), D(105), D(105), 'PROFIT_FLOOR_QUOTE')
    below = sel(level, DEEP, floor=D(101))
    assert below == sel(level, DEEP)
    with pytest.raises(ValueError):
        sel(rng('buy'), DEEP, floor=D(1))


def test_profit_floor_suffix_only_when_the_floor_bound_is_the_fallback():
    floor = D('102.0001')         # lower bound becomes 102.001
    fallback = sel(rng('sell'), one_sided('sell', (('101.999', '1000'),)), floor=floor)
    assert (fallback.price, fallback.reason) == (D('102.001'), 'VOLUME_BELOW_RANGE+PROFIT_FLOOR')
    volume = sel(rng('sell'), one_sided('sell', (('102', '1000'),)), floor=floor)
    assert (volume.price, volume.reason) == (D('102.001'), 'BOOK_VOLUME')


def test_profit_floor_on_exact_level_holds_or_quotes_never_moves_inside():
    level = exact('sell', 1)     # 101
    assert sel(level, floor=D('100.5')).price == D(101)
    with pytest.raises(Unsafe, match='^PROFIT_FLOOR_ABOVE_RANGE$'):
        sel(level, floor=D('101.0001'))
    quoted = sel(level, floor=D('101.0001'), profit_floor_mode='quote')
    assert (quoted.price, quoted.reason) == (D('101.001'), 'PROFIT_FLOOR_QUOTE')


def test_profit_floor_quote_is_exempt_from_the_wait_policy():
    level = rng('sell', skip=10**9, insufficient_depth='wait')
    result = sel(level, floor=D(110), profit_floor_mode='quote')
    assert (result.price, result.reason) == (D(110), 'PROFIT_FLOOR_QUOTE')


@pytest.mark.parametrize('value', [D(0), D(-1), Decimal('NaN'), 'x'])
def test_invalid_floor_or_ceiling_refused(value):
    with pytest.raises(Unsafe, match='^INVALID_PROFIT_FLOOR_INPUT$'):
        sel(rng('sell'), floor=value)
    with pytest.raises(Unsafe, match='^INVALID_PROFIT_FLOOR_INPUT$'):
        sel(rng('buy'), ceiling=value)


def test_profit_ceiling_from_own_sell_lowers_the_buy_band():
    ceiling = coupled_buy_ceiling(D(100), D('0.3'), D('0.04'))
    assert floor_price(ceiling, meta()) == D('99.621')   # reference R02: BUY <= 99.621 vs own SELL 100
    result = sel(rng('buy', '-0.01', '1', skip=0), ceiling=ceiling)
    assert (result.price, result.upper, result.reason) == (D('99.621'), D('99.621'), 'ZERO_SKIP+PROFIT_CEILING')
    above = sel(rng('buy', skip=0), ceiling=D(150))
    assert (above.price, above.reason) == (D(98), 'ZERO_SKIP')
    with pytest.raises(Unsafe, match='^PROFIT_CEILING_BELOW_RANGE$'):
        sel(rng('buy'), ceiling=D('96.9999'))
    with pytest.raises(ValueError):
        sel(rng('sell'), ceiling=D(200))


def test_profit_formulas():
    assert profit_floor_price(D(100), D(1), D(0)) == D(101)
    assert profit_floor_price(D(100), D(0), D('0.04')) == D(100) / D('0.9996')
    floor = coupled_sell_floor(D(100), D('0.3'), D('0.04'))
    assert floor == D(100) * D('1.003') / D('0.9996') ** 2
    # A round trip BUY at B (base fee) then SELL at the floor (quote fee) earns exactly m.
    assert abs(floor * D('0.9996') ** 2 / D(100) - D('1.003')) < D('1e-20')
    assert abs(coupled_buy_ceiling(floor, D('0.3'), D('0.04')) - D(100)) < D('1e-20')
    for args in [(D(0), D(1), D(0)), (D(100), D(-100), D(0)), (D(100), D(1), D(100)),
                 (D(100), D(1), D(-1)), (Decimal('NaN'), D(1), D(0)), (D(100), None, D(0))]:
        for fn in (profit_floor_price, coupled_sell_floor, coupled_buy_ceiling):
            with pytest.raises(Unsafe, match='^INVALID_PROFIT_FLOOR_INPUT$'):
                fn(*args)


# ============================================================================ maker limit / self-cross
def test_maker_limit_and_post_only_include_every_own_opposite_order():
    snap = book(bids=(('99', '1'),), asks=(('101', '1'),))
    assert maker_limit('buy', snap, []) == D(101)
    assert maker_limit('buy', snap, [D('102'), D('100.5')]) == D('100.5')
    assert maker_limit('sell', snap, [D('98'), D('99.5')]) == D('99.5')
    assert post_only_ok('buy', D('100.499'), snap, [D('102'), D('100.5')])
    assert not post_only_ok('buy', D('100.5'), snap, [D('102'), D('100.5')])
    assert not post_only_ok('buy', D(101), snap, [])
    assert post_only_ok('sell', D('99.501'), snap, [D('98'), D('99.5')])
    assert not post_only_ok('sell', D('99.5'), snap, [D('98'), D('99.5')])
    assert not post_only_ok('sell', D(99), snap, [])
    assert not post_only_ok('buy', D(1), None, [])
    with pytest.raises(Unsafe, match='^MISSING_BOOK_DEPTH$'):
        maker_limit('buy', None, [])


# ============================================================================ tolerance / guards
@pytest.mark.parametrize('price,expected', [('97.902', False), ('97.90199', True), ('102.102', False), ('102.10201', True)])
def test_tolerance_is_percent_of_price_boundary_and_inclusive(price, expected):
    assert outside_tolerance(D(price), D(98), D(102), D('0.1')) is expected


@pytest.mark.parametrize('args', [
    (D(0), D(98), D(102), D('0.1')), (D(100), D(0), D(102), D('0.1')), (D(100), D(103), D(102), D('0.1')),
    (D(100), D(98), D(102), D(-1)), (D(100), D(98), D(102), D(100)),
])
def test_tolerance_rejects_invalid_input(args):
    with pytest.raises(ValueError):
        outside_tolerance(*args)


def test_edge_pct_is_signed_distance_from_center():
    assert edge_pct('buy', D(99), D(100)) == D(1)
    assert edge_pct('sell', D(102), D(100)) == D(2)
    assert edge_pct('buy', D('100.5'), D(100)) == D('-0.5')
    with pytest.raises(ValueError):
        edge_pct('hold', D(1), D(1))


@pytest.mark.parametrize('side,price,violation', [
    ('buy', '99.2', False), ('buy', '99.2001', True), ('buy', '98', False),
    ('sell', '100.8', False), ('sell', '100.7999', True), ('sell', '102', False),
])
def test_hard_guard_is_strict_edge_below_hard_minimum(side, price, violation):
    assert hard_guard_violation(side, D(price), D(100), D('0.8')) is violation


@pytest.mark.parametrize('side,level,tol,expected', [
    ('buy', exact('buy', 1, band_tolerance_pct=D('0.15')), '0.15', '99.1485'),   # 99 * 1.0015 < 99.2
    ('buy', exact('buy', 1, band_tolerance_pct=D('0.5')), '0.5', '99.2'),        # 99.495 capped by hard guard
    ('sell', exact('sell', 1, band_tolerance_pct=D('0.15')), '0.15', '100.8485'),
    ('sell', exact('sell', 1, band_tolerance_pct=D('0.5')), '0.5', '100.8'),     # 100.495 capped
    ('buy', rng('buy', band_tolerance_pct=D('0.1')), '0.1', '98.098'),
    ('sell', rng('sell', band_tolerance_pct=D('0.1')), '0.1', '101.898'),
])
def test_adverse_boundary_tolerance_never_relaxes_hard_min_edge(side, level, tol, expected):
    boundary = adverse_boundary(side, D(100), meta(), level, D('0.8'))
    assert boundary == D(expected)
    hard = D(100) * (D(1) - D('0.008')) if side == 'buy' else D(100) * D('1.008')
    assert (boundary <= hard) if side == 'buy' else (boundary >= hard)
    # equality is accepted, one step beyond is adverse; consistent with outside_tolerance
    lower, upper = level_bounds(D(100), meta(), level)
    step = D('0.0001')
    worse = boundary + step if side == 'buy' else boundary - step
    if boundary != hard:
        assert not outside_tolerance(boundary, lower, upper, D(tol))
        assert outside_tolerance(worse, lower, upper, D(tol))
    else:
        assert not hard_guard_violation(side, boundary, D(100), D('0.8'))
        assert hard_guard_violation(side, worse, D(100), D('0.8'))


def test_adverse_boundary_rejects_side_mismatch_and_bad_tolerance():
    with pytest.raises(ValueError):
        adverse_boundary('sell', D(100), meta(), exact('buy', 1), D('0.8'))
    with pytest.raises(ValueError):
        adverse_boundary('buy', D(100), meta(), exact('buy', 1, band_tolerance_pct=D(100)), D('0.8'))


def test_documented_codes_cover_everything_raised_here():
    assert {'NONPOSITIVE_PRICE_RANGE', 'EMPTY_PRICE_RANGE', 'INVALID_OWN_LEVEL', 'CROSSED_BOOK',
            'SKIP_NOTIONAL_NOT_REACHED', 'PROFIT_FLOOR_ABOVE_RANGE'} <= UNSAFE_CODES
