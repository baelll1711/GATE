"""Pure average-cost ledger: Gate fee components, rebates, unmatched sells, replay order."""
from __future__ import annotations

import random
from decimal import Decimal

import pytest

from gate_stonk.ledger import (FillEffect, Position, apply_fill, apply_fill_effect, apply_transfer_out, edge_text,
                               event_key, realized_including_initial, replay, replay_effects, tid_number)

D = Decimal
BASE, QUOTE = 'STONK', 'USDT'


def buy(pos, qty, price, fee='0', cur=QUOTE, *, t=1, tid='1', **kw):
    return apply_fill_effect(pos, 'buy', D(qty), D(price), D(fee), cur, BASE, QUOTE, time_ms=t, trade_id=tid,
                             fill_key=f'k{tid}', **kw)


def sell(pos, qty, price, fee='0', cur=QUOTE, *, t=1, tid='1', **kw):
    return apply_fill_effect(pos, 'sell', D(qty), D(price), D(fee), cur, BASE, QUOTE, time_ms=t, trade_id=tid,
                             fill_key=f'k{tid}', **kw)


def test_buy_fee_in_base_reduces_received_quantity_not_cost():
    pos, effect = buy(Position(), '100', '0.5', '0.1', BASE)
    assert pos.qty == D('99.9') and pos.cost == D('50')
    assert pos.avg_cost == D(50) / D('99.9')
    assert pos.fees_base == D('0.1') and pos.fees_quote == 0 and pos.fees_other == {}
    assert pos.bought_qty == D(100) and pos.bought_cost == D(50)      # gross, fee excluded
    assert effect == FillEffect(net_qty=D('99.9'), net_quote=D(50))


def test_buy_fee_in_quote_adds_to_cost():
    pos, effect = buy(Position(), '10', '2', '0.02')
    assert pos.qty == 10 and pos.cost == D('20.02') and pos.avg_cost == D('2.002')
    assert pos.fees_quote == D('0.02') and pos.fees_base == 0
    assert effect.net_quote == D('20.02') and pos.bought_cost == D(20)


def test_sell_fee_in_quote_reduces_proceeds_and_realizes_against_average_cost():
    pos, _ = buy(Position(), '100', '0.5', '0.1', BASE, tid='1')
    pos, effect = sell(pos, '50', '0.6', '0.03', tid='2', t=2)
    cost_removed = D(50) * D(50) / D('99.9')
    assert effect.matched_qty == D(50) and effect.unmatched_qty == 0
    assert effect.net_qty == D(50) and effect.net_quote == D('29.97')
    assert effect.cost_removed == cost_removed
    assert effect.realized == D('29.97') - cost_removed
    assert pos.realized == effect.realized
    assert pos.qty == D('49.9') and pos.cost == D(50) - cost_removed
    assert pos.sold_qty == D(50) and pos.sold_proceeds == D(30)
    assert pos.fees_base == D('0.1') and pos.fees_quote == D('0.03')


def test_sell_fee_in_base_increases_base_given():
    pos, _ = buy(Position(), '10', '1', tid='1')
    pos, effect = sell(pos, '4', '2', '0.01', BASE, tid='2', t=2)
    assert effect.net_qty == D('4.01') and effect.matched_qty == D('4.01')
    assert effect.net_quote == D(8)
    assert effect.realized == D(8) - D('4.01')
    assert pos.qty == D('5.99') and pos.cost == D('5.99')
    assert pos.sold_qty == D(4)          # gross executed quantity; the fee is in fees_base
    assert pos.fees_base == D('0.01')


def test_gt_fee_is_recorded_separately_and_never_converted():
    pos, effect = buy(Position(), '10', '2', '0', QUOTE, gt_fee=D('0.05'))
    assert pos.qty == 10 and pos.cost == 20 and effect.net_quote == 20
    assert pos.fees_other == {'GT': D('0.05')}
    # A fee whose currency is GT itself behaves the same way.
    pos, effect = sell(pos, '5', '3', '0.04', 'GT', tid='2', t=2)
    assert effect.net_quote == D(15) and effect.net_qty == D(5)
    assert pos.fees_other == {'GT': D('0.09')} and pos.fees_quote == 0 and pos.fees_base == 0


def test_point_fee_is_recorded_separately_and_never_converted():
    pos, effect = buy(Position(), '10', '2', '0.001', QUOTE, point_fee=D('1.1'), gt_fee=D('2.2'))
    assert pos.cost == D('20.001') and pos.qty == 10
    assert pos.fees_other == {'POINT': D('1.1'), 'GT': D('2.2')}
    pos2, _ = buy(pos, '1', '2', '0.3', 'BTC', tid='2', t=2)
    assert pos2.fees_other == {'POINT': D('1.1'), 'GT': D('2.2'), 'BTC': D('0.3')}
    assert pos2.cost == D('22.001') and pos2.qty == 11
    assert pos.fees_other == {'POINT': D('1.1'), 'GT': D('2.2')}   # input never mutated


def test_negative_fee_is_a_rebate_in_both_currencies():
    pos, effect = buy(Position(), '10', '2', '-0.01', BASE)
    assert pos.qty == D('10.01') and pos.cost == 20 and effect.net_qty == D('10.01')
    assert pos.fees_base == D('-0.01')
    pos, effect = sell(pos, '10.01', '3', '-0.02', QUOTE, tid='2', t=2)
    assert effect.net_quote == D('30.05')
    assert effect.realized == D('30.05') - D(20)
    assert pos.qty == 0 and pos.cost == 0 and pos.fees_quote == D('-0.02')


def test_explicit_quote_rebate_lowers_cost_and_raises_proceeds():
    pos, effect = buy(Position(), '10', '2', '0.02', rebate=D('0.005'))
    assert effect.net_quote == D('20.015') and pos.cost == D('20.015')
    assert pos.rebates_quote == D('0.005') and pos.fees_quote == D('0.02')
    pos, effect = sell(pos, '10', '2', '0.02', tid='2', t=2, rebate=D('0.005'))
    assert effect.net_quote == D('19.985')
    assert pos.rebates_quote == D('0.01')
    assert pos.realized == D('19.985') - D('20.015')


def test_average_price_and_realized_over_multi_fill_sequence():
    pos = Position()
    pos, _ = buy(pos, '10', '1', tid='1', t=1)
    pos, _ = buy(pos, '10', '2', tid='2', t=2)
    assert pos.avg_cost == D('1.5') and pos.cost == 30
    pos, e1 = sell(pos, '5', '3', tid='3', t=3)
    assert e1.realized == D('7.5') and e1.realized_pct == D(100)
    pos, _ = buy(pos, '5', '1', tid='4', t=4)
    assert pos.qty == 20 and pos.cost == D('27.5') and pos.avg_cost == D('1.375')
    pos, e2 = sell(pos, '20', '1', '0.2', tid='5', t=5)
    assert e2.realized == D('19.8') - D('27.5')
    assert pos.qty == 0 and pos.cost == 0
    assert pos.realized == D('7.5') + D('19.8') - D('27.5')
    assert pos.bought_qty == 25 and pos.bought_cost == 35
    assert pos.sold_qty == 25 and pos.sold_proceeds == 35
    assert pos.fills == 5 and pos.last_time_ms == 5 and pos.last_trade_id == '5'


def test_unmatched_sell_and_initial_avg_price():
    pos, effect = sell(Position(), '10', '2', '0.02')
    assert effect.matched_qty == 0 and effect.unmatched_qty == 10 and effect.realized is None
    assert pos.unmatched_qty == 10 and pos.unmatched_proceeds == D('19.98') and pos.realized == 0
    assert pos.realized_with(None) is None
    assert pos.realized_with(D('1.5')) == D('19.98') - D(15)
    assert realized_including_initial(effect, None) is None
    assert realized_including_initial(effect, D('1.5')) == D('4.98')

    # Partly from bot inventory, partly from pre-bot stock: proceeds split pro rata.
    pos, _ = buy(Position(), '4', '1')
    pos, effect = sell(pos, '10', '2', '0.2', tid='2', t=2)
    assert effect.matched_qty == 4 and effect.unmatched_qty == 6
    assert effect.realized == D('19.8') * D('0.4') - D(4)
    assert pos.unmatched_proceeds == D('19.8') * D('0.6')
    assert realized_including_initial(effect, D('1.5')) == D('3.92') + D('11.88') - D(9)
    assert pos.realized_with(D('1.5')) == D('3.92') + D('11.88') - D(9)
    assert realized_including_initial(FillEffect(net_qty=D(1), net_quote=D(1)), None) is None  # BUY effect
    # Bought later: a new cycle has a known cost again; unmatched stays unmatched.
    pos, _ = buy(pos, '2', '3', tid='3', t=3)
    assert pos.qty == 2 and pos.avg_cost == 3 and pos.unmatched_qty == 6


def test_pathological_base_fee_keeps_cost_conservative():
    pos, effect = buy(Position(), '1', '2', '1', BASE)
    assert pos.qty == 0 and pos.cost == 2 and pos.avg_cost is None and effect.net_qty == 0


def test_replay_uses_execution_order_not_arrival_order():
    rows = [
        {'time_ms': 1000, 'trade_id': '9', 'fill_key': 'a', 'side': 'buy', 'qty': '10', 'price': '1',
         'fee': '0.01', 'fee_currency': BASE},
        {'time_ms': 1000, 'trade_id': '10', 'fill_key': 'b', 'side': 'sell', 'qty': '5', 'price': '2',
         'fee': '0.01', 'fee_currency': QUOTE},
        {'time_ms': 2000, 'trade_id': '3', 'fill_key': 'c', 'side': 'buy', 'qty': '1', 'price': '3',
         'fee': '0', 'fee_currency': QUOTE, 'gt_fee': '0.5', 'point_fee': None},
        {'time_ms': 3000, 'trade_id': '4', 'fill_key': 'd', 'side': 'sell', 'qty': '6', 'price': '2.5',
         'fee': '0.015', 'fee_currency': QUOTE},
    ]
    expected, effects = replay_effects(rows, BASE, QUOTE)
    for seed in range(10):
        shuffled = rows[:]
        random.Random(seed).shuffle(shuffled)
        assert replay(shuffled, BASE, QUOTE) == expected
    # trade id '9' sorts before '10' numerically (the BUY happens first: sell is fully matched).
    assert effects['b'].matched_qty == 5 and effects['b'].unmatched_qty == 0
    assert expected.fills == 4 and expected.last_time_ms == 3000 and expected.last_fill_key == 'd'
    assert expected.fees_other == {'GT': D('0.5')}
    # Incremental application in execution order gives the identical position.
    pos = Position()
    for row in sorted(rows, key=lambda r: event_key(r['time_ms'], 0, r['trade_id'], r['fill_key'])):
        pos = apply_fill(pos, row['side'], row['qty'], row['price'], row['fee'], row['fee_currency'], BASE, QUOTE,
                         point_fee=row.get('point_fee') or 0, gt_fee=row.get('gt_fee', 0), time_ms=row['time_ms'],
                         trade_id=row['trade_id'], fill_key=row['fill_key'])
    assert pos == expected


def test_transfer_out_removes_at_average_cost_without_profit():
    pos, _ = buy(Position(), '10', '2', t=100)
    moved, removed, cost = apply_transfer_out(pos, D(4), 500)
    assert removed == 4 and cost == 8 and moved.qty == 6 and moved.cost == 12 and moved.realized == 0
    assert moved.transferred_qty == 4 and moved.transferred_cost == 8
    assert moved.watermark() == (501, 0, '')
    # Replay: a fill at the transfer time sorts before the transfer.
    fills = [{'time_ms': 100, 'trade_id': '1', 'fill_key': 'a', 'side': 'buy', 'qty': '10', 'price': '2',
              'fee': '0', 'fee_currency': QUOTE},
             {'time_ms': 500, 'trade_id': '2', 'fill_key': 'b', 'side': 'buy', 'qty': '10', 'price': '4',
              'fee': '0', 'fee_currency': QUOTE}]
    replayed = replay(fills, BASE, QUOTE, transfers=[{'id': 't', 'time_ms': 500, 'qty': '4'}])
    assert replayed.qty == 16 and replayed.transferred_cost == D(4) * D(60) / D(20)
    nothing, removed, cost = apply_transfer_out(Position(), D(1), 5)
    assert removed == cost == 0 and nothing.qty == 0


def test_position_json_round_trip_ignores_unknown_keys():
    pos, _ = buy(Position(), '10', '2', '0.02', point_fee=D('1'), gt_fee=D('0.1'))
    text = pos.to_json()
    assert Position.from_json(text) == pos
    import json
    data = json.loads(text)
    data['future_field'] = '1'
    del data['rebates_quote']
    restored = Position.from_json(json.dumps(data))
    assert restored.rebates_quote == 0 and restored.qty == 10
    with pytest.raises(ValueError):
        Position.from_json(json.dumps({**data, 'qty': 'NaN'}))


@pytest.mark.parametrize('kwargs', [
    {'side': 'hold'}, {'qty': '0'}, {'qty': '-1'}, {'price': '0'}, {'qty': 'NaN'}, {'fee': 'Infinity'},
    {'qty': True}, {'fee_currency': ''}, {'fee_currency': None}, {'time_ms': True}, {'time_ms': -1},
    {'trade_id': ''}, {'base': QUOTE},
])
def test_invalid_executions_are_refused(kwargs):
    args = {'side': 'buy', 'qty': '1', 'price': '1', 'fee': '0', 'fee_currency': QUOTE, 'base': BASE,
            'time_ms': 1, 'trade_id': '1'}
    args.update(kwargs)
    with pytest.raises(ValueError):
        apply_fill(Position(), args['side'], args['qty'], args['price'], args['fee'], args['fee_currency'],
                   args['base'], QUOTE, time_ms=args['time_ms'], trade_id=args['trade_id'])


def test_sell_base_rebate_larger_than_quantity_is_refused():
    with pytest.raises(ValueError):
        sell(Position(), '1', '1', '-1', BASE)


def test_tid_number_and_edge_text():
    assert tid_number('10') == 10 and tid_number('abc') == 0 and tid_number(None) == 0 and tid_number('-5') == 0
    assert edge_text('buy', D('99'), D('100')) == 'ниже индекса на 1%'
    assert edge_text('buy', D('101'), D('100')) == 'выше индекса на 1%'
    assert edge_text('sell', D('102.5'), D('100')) == 'выше индекса на 2.5%'
    assert edge_text('sell', D('99'), D('100'), 'центра') == 'ниже центра на 1%'
    assert edge_text('buy', D('1'), None) == 'индекс недоступен'
