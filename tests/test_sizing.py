"""Sizing helpers (DESIGN §6): requested size with base-fee compensation, iceberg visible part,
pair minimums. Gross-up examples are ported from the reference ``test_buy_fee_sizing.py``.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from gate_stonk.config import LevelConfig
from gate_stonk.dec import D
from gate_stonk.errors import Unsafe
from gate_stonk.sizing import iceberg_visible, meets_minimums, requested_size
from gate_stonk.types import PairMeta


def meta(amount_precision: int = 2, precision: int = 4, min_base=None, min_quote=None) -> PairMeta:
    return PairMeta('STONK_USDT', 'STONK', 'USDT', precision, amount_precision,
                    None if min_base is None else D(min_base), None if min_quote is None else D(min_quote),
                    None, None, 'tradable')


def level(side: str = 'buy', qty=None, notional=None, display_qty=None, display_pct=None) -> LevelConfig:
    return LevelConfig('b1', side, 'exact', edge_pct=D(1),
                       order_qty=None if qty is None else D(qty),
                       order_notional_usdt=None if notional is None else D(notional),
                       iceberg_display_qty=None if display_qty is None else D(display_qty),
                       iceberg_display_pct=None if display_pct is None else D(display_pct))


# ============================================================================ requested_size
@pytest.mark.parametrize('target,decimals,expected', [
    (100, 1, '100.1'), (8, 2, '8.01'), (5000, 0, '5003'), ('0.0189', 4, '0.0190'),
])
def test_gross_buy_rounded_up_to_the_lot_covers_the_net_target(target, decimals, expected):
    result = requested_size(level(qty=target), 'buy', D(1), meta(decimals), D('0.04'), True)
    gross, step = result['requested_gross_qty'], result['size_step']
    assert gross == D(expected) and step == D(1).scaleb(-decimals)
    assert gross * (1 - D('0.0004')) >= D(target)
    assert (gross - step) * (1 - D('0.0004')) < D(target)
    assert result['enabled'] and result['target_net_qty'] == D(target)
    assert result['estimated_base_fee_pct'] == D('0.04')


def test_disabled_and_sell_sizes_are_unchanged_and_notional_target_is_lot_floored():
    assert requested_size(level(qty=100), 'buy', D(1), meta(1), D('0.04'), False)['requested_gross_qty'] == 100
    sell = requested_size(level('sell', qty=100), 'sell', D(1), meta(1), D('0.04'), True)
    assert sell['requested_gross_qty'] == 100 and not sell['enabled'] and sell['estimated_base_fee_pct'] == 0
    result = requested_size(level(notional=25), 'buy', D(1320), meta(4), D('0.04'), True)
    assert result['target_net_qty'] == D('0.0189') and result['requested_gross_qty'] == D('0.019')


def test_smaller_of_qty_and_notional_wins_for_a_stonk_level():
    # 20 USDT at 0.1345 = 148.698... STONK -> 148.69 (floor to amount_precision 2)
    by_notional = requested_size(level(qty=200, notional=20), 'buy', D('0.1345'), meta(), D('0.1'), False)
    assert by_notional['target_net_qty'] == D('148.69') == by_notional['requested_gross_qty']
    by_qty = requested_size(level(qty=100, notional=20), 'buy', D('0.1345'), meta(), D('0.1'), False)
    assert by_qty['target_net_qty'] == D(100)
    compensated = requested_size(level(notional=20), 'buy', D('0.1345'), meta(), D('0.1'), True)
    assert compensated['requested_gross_qty'] == D('148.84')       # 148.69 / 0.999 = 148.8388.. -> up


def test_zero_fee_estimate_needs_no_gross_up():
    result = requested_size(level(qty='10.5'), 'buy', D(1), meta(), D(0), True)
    assert result['enabled'] and result['requested_gross_qty'] == D('10.5')


@pytest.mark.parametrize('fee', ['NaN', '-0.1', 1, '1.0', None, 'x', True])
def test_invalid_buy_fee_estimate_refused(fee):
    with pytest.raises(Unsafe, match='^INVALID_BUY_SIZE_FEE_ESTIMATE$'):
        requested_size(level(qty=1), 'buy', D(100), meta(1), fee, True)
    with pytest.raises(Unsafe, match='^INVALID_BUY_SIZE_FEE_ESTIMATE$'):
        requested_size(level(qty=1), 'buy', D(100), meta(1), fee, False)


def test_fee_just_below_one_percent_is_accepted_and_sell_ignores_the_fee():
    assert requested_size(level(qty=1), 'buy', D(100), meta(1), D('0.99'), True)['requested_gross_qty'] == D('1.1')
    assert requested_size(level('sell', qty=1), 'sell', D(100), meta(1), None, True)['requested_gross_qty'] == 1


@pytest.mark.parametrize('lvl', [level(), level(qty=0), level(notional=-1), level(qty=1, notional=0),
                                 LevelConfig('b1', 'buy', 'exact', edge_pct=D(1), order_qty=Decimal('NaN'))])
def test_level_without_a_positive_finite_size_is_refused(lvl):
    with pytest.raises(Unsafe, match='^INVALID_ORDER_SIZE$'):
        requested_size(lvl, 'buy', D(1), meta(), D('0.1'), False)


@pytest.mark.parametrize('price', [D(0), D(-1), Decimal('NaN'), None])
def test_nonpositive_price_refused(price):
    with pytest.raises(Unsafe, match='^NONPOSITIVE_PRICE_RANGE$'):
        requested_size(level(notional=20), 'buy', price, meta(), D('0.1'), False)


def test_programming_errors():
    with pytest.raises(ValueError):
        requested_size(level(qty=1), 'long', D(1), meta(), D('0.1'), False)
    with pytest.raises(ValueError):
        requested_size(level(qty=1), 'buy', D(1), meta(), D('0.1'), 'yes')


def test_tiny_notional_floors_to_zero_without_error():
    result = requested_size(level(notional='0.001'), 'buy', D('0.1345'), meta(), D('0.1'), True)
    assert result['target_net_qty'] == 0 and result['requested_gross_qty'] == 0


# ============================================================================ iceberg_visible
def test_normal_order_has_no_visible_part():
    assert iceberg_visible(level(notional=20), D('148.69'), meta()) is None


@pytest.mark.parametrize('display_qty,display_pct,amount,expected', [
    ('10', None, '148.69', '10'),
    ('10.555', None, '148.69', '10.55'),      # floored to amount_precision
    (None, '25', '148.69', '37.17'),          # 37.1725 -> 37.17
    (None, '99.99', '148.69', '148.67'),
])
def test_visible_part_is_floored_to_the_lot(display_qty, display_pct, amount, expected):
    lvl = level(notional=20, display_qty=display_qty, display_pct=display_pct)
    assert iceberg_visible(lvl, D(amount), meta()) == D(expected)


@pytest.mark.parametrize('lvl,amount', [
    (level(display_qty='200'), '148.69'),          # visible >= amount: not an iceberg
    (level(display_qty='148.69'), '148.69'),
    (level(display_pct='0.001'), '10'),            # floors to 0: Gate cannot hide the whole amount
    (level(display_qty='0.001'), '10'),
    (level(display_qty='10', display_pct='25'), '100'),     # mutually exclusive
    (level(display_qty='0'), '100'), (level(display_qty='-1'), '100'),
    (level(display_pct='0'), '100'), (level(display_pct='100'), '100'), (level(display_pct='-5'), '100'),
    (level(display_qty='10'), '0'), (level(display_qty='10'), 'NaN'),
])
def test_invalid_iceberg_display_refused(lvl, amount):
    with pytest.raises(Unsafe, match='^ICEBERG_DISPLAY_INVALID$'):
        iceberg_visible(lvl, D(amount) if amount != 'NaN' else Decimal('NaN'), meta())


def test_visible_part_must_meet_the_pair_minimums():
    lvl = level(display_pct='25')
    with pytest.raises(Unsafe, match='^ICEBERG_DISPLAY_BELOW_MINIMUM$'):
        iceberg_visible(lvl, D('148.69'), meta(min_base='50'))
    assert iceberg_visible(lvl, D('148.69'), meta(min_base='37.17')) == D('37.17')
    small = level(display_qty='10')
    with pytest.raises(Unsafe, match='^ICEBERG_DISPLAY_BELOW_MINIMUM$'):
        iceberg_visible(small, D('148.69'), meta(min_quote='3'), price=D('0.1345'))     # 1.345 USDT visible
    assert iceberg_visible(small, D('148.69'), meta(min_quote='3')) == D(10)          # price unknown: not checked
    assert iceberg_visible(small, D('148.69'), meta(min_quote='1.345'), price=D('0.1345')) == D(10)


# ============================================================================ meets_minimums
@pytest.mark.parametrize('qty,price,min_base,min_quote,ok', [
    ('148.69', '0.1345', None, None, True),
    ('148.69', '0.1345', '148.69', None, True),
    ('148.68', '0.1345', '148.69', None, False),
    ('37.18', '0.1345', None, '5', True),        # 5.00071
    ('37.17', '0.1345', None, '5', False),       # 4.999365
    ('0', '0.1345', None, None, False),
    ('10', '0', None, None, False),
    ('-1', '0.1345', None, None, False),
])
def test_meets_minimums(qty, price, min_base, min_quote, ok):
    assert meets_minimums(D(qty), D(price), meta(min_base=min_base, min_quote=min_quote)) is ok


def test_meets_minimums_rejects_garbage():
    assert meets_minimums(Decimal('NaN'), D(1), meta()) is False
    assert meets_minimums(D(1), None, meta()) is False
