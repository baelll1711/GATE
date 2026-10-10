"""Engine: first step, batch creation, shared cash, limits, per-slot gates (DESIGN §11, §17)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from engine_fakes import make_rig, texts
from gate_stonk.dec import D
from gate_stonk.pricing import edge_pct

B1 = {'id': 'b1', 'edge_pct': 1.0, 'order_notional_usdt': 20}
B2 = {'id': 'b2', 'edge_pct': 2.0, 'order_notional_usdt': 30}
S1 = {'id': 's1', 'edge_pct': 1.0, 'order_notional_usdt': 20}
S2 = {'id': 's2', 'edge_pct': 2.0, 'order_notional_usdt': 30}


def level_of(text: str) -> str:
    return text.split('-')[2]


def test_first_step_one_batch_sell_first_one_dms_prompt_prices(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    assert rig.step() == 'RUNNING'
    calls = rig.venue.calls
    assert calls.count('place_batch') == 1 and 'place' not in calls
    assert rig.venue.countdown_log == [45]
    assert calls.index('countdown') < calls.index('place_batch')        # DMS armed before creating
    [batch] = rig.venue.batch_log
    assert [(r.side, level_of(r.text), r.price) for r in batch] == [
        ('sell', 's1', D('0.1373')), ('sell', 's2', D('0.1387')),
        ('buy', 'b1', D('0.1345')), ('buy', 'b2', D('0.1331'))]
    assert [(r.amount, r.iceberg) for r in batch] == [
        (D('145.66'), None), (D('216.29'), D('54.07')), (D('148.69'), None), (D('225.39'), D('56.34'))]
    for req in batch:
        assert req.time_in_force == 'poc'
        assert edge_pct(req.side, req.price, D('0.1359')) >= 1
    rows = rig.rows()
    assert len(rows) == 4 and all(r['state'] == 'open' and r['order_id'] for r in rows)
    assert rig.reservations() <= D('1000')
    assert all(rig.engine.slot_states[slot] == 'ORDER_SENT' for slot in ('sell:s1', 'sell:s2', 'buy:b1', 'buy:b2'))
    status = rig.status_json()
    assert status['slots']['buy:b1']['order']['price'] == '0.1345'
    assert status['index']['ok'] is True and D(status['index']['center']) == D('0.1359')
    assert status['spot']['bid'] == '0.1350' and status['dms_deadline_ms'] == rig.engine.dms_deadline_ms


def test_first_step_buy_only_without_base(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='0')
    assert rig.step() == 'RUNNING'
    [batch] = rig.venue.batch_log
    assert [(r.side, level_of(r.text), r.price) for r in batch] == [
        ('buy', 'b1', D('0.1345')), ('buy', 'b2', D('0.1331'))]
    assert rig.engine.slot_states['sell:s1'] == 'BALANCE_OR_LIMIT'
    assert rig.engine.slot_states['sell:s2'] == 'BALANCE_OR_LIMIT'
    assert rig.venue.countdown_log == [45]
    rig.status_json()


def test_shared_cash_never_over_reserved(tmp_path, clock):
    overrides = {'pair.buy.levels': [B1, B2, {'id': 'b3', 'edge_pct': 3.0, 'order_notional_usdt': 20}],
                 'pair.mode': 'buy'}
    rig = make_rig(tmp_path, clock, usdt='40', overrides=overrides)
    assert rig.step() == 'RUNNING'
    [batch] = rig.venue.batch_log
    by_level = {level_of(r.text): r for r in batch}
    assert set(by_level) == {'b1', 'b2'}
    assert by_level['b1'].amount == D('148.69')
    # b2 gets exactly what is left of the 40 USDT after b1's reservation (fee reserve 0.3 %).
    factor = D('1.003')
    left = D('40') - D('148.69') * D('0.1345') * factor
    expected_b2 = (left / (D('0.1331') * factor)).quantize(D('0.01'), rounding='ROUND_DOWN')
    assert by_level['b2'].amount == expected_b2 < D('225.39')
    assert rig.engine.slot_states['buy:b3'] == 'BALANCE_OR_LIMIT'
    assert rig.reservations() <= D('40')
    rig.status_json()


def test_scarce_cash_later_level_waits_balance_or_limit(tmp_path, clock):
    overrides = {'pair.buy.levels': [B1, B2], 'pair.mode': 'buy'}
    rig = make_rig(tmp_path, clock, usdt='22', overrides=overrides)
    rig.step()
    # A single intent goes out with place (no batch).
    assert rig.venue.calls.count('place') == 1 and 'place_batch' not in rig.venue.calls
    assert [level_of(r.text) for r in rig.venue.place_log] == ['b1']
    assert rig.engine.slot_states['buy:b2'] == 'BALANCE_OR_LIMIT'
    assert rig.reservations() <= D('22')


def test_sell_limited_by_base_minus_reserve_and_other_sells(tmp_path, clock):
    overrides = {'pair.sell.levels': [S1, S2], 'pair.inventory_reserve_qty': 100, 'pair.mode': 'sell'}
    rig = make_rig(tmp_path, clock, usdt='0', base='300', overrides=overrides)
    rig.step()
    by_level = {level_of(r.text): r for r in rig.venue.place_log}
    assert by_level['s1'].amount == D('145.66')
    assert by_level['s2'].amount == D('200') - D('145.66')
    assert sum(r.amount for r in rig.venue.place_log) <= D('200')
    rig.step(5)   # the resting SELLs are not reduced (their own reservations are added back)
    assert all(r['cancel_reason'] is None for r in rig.rows())


def test_iceberg_display_below_minimum_waits_with_its_code(tmp_path, clock):
    overrides = {'pair.sell.levels': [S1, dict(S2, iceberg_display_pct=25)], 'pair.inventory_reserve_qty': 100,
                 'pair.mode': 'sell'}
    rig = make_rig(tmp_path, clock, usdt='0', base='300', overrides=overrides)
    rig.step()
    assert [level_of(r.text) for r in rig.venue.place_log] == ['s1']
    assert rig.engine.slot_states['sell:s2'] == 'ICEBERG_DISPLAY_BELOW_MINIMUM'


def test_daily_notional_cap_utc_day(tmp_path, clock):
    overrides = {'pair.buy.levels': [B1, B2], 'pair.mode': 'buy', 'pair.buy.max_daily_notional_usdt': 30}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.step()
    by_level = {level_of(r.text): r for r in rig.venue.place_log}
    b2_cap = D('30') - D('148.69') * D('0.1345')
    assert by_level['b2'].amount == (b2_cap / D('0.1331')).quantize(D('0.01'), rounding='ROUND_DOWN')
    for row in rig.rows():
        rig.venue.fill(row['order_id'], row['qty'])
    rig.step(5)
    rig.settle()
    assert not rig.rows()
    assert rig.engine.slot_states['buy:b1'] == 'BALANCE_OR_LIMIT'
    assert rig.engine.slot_states['buy:b2'] == 'BALANCE_OR_LIMIT'
    placed = len(rig.venue.place_log)
    # Next UTC day: the daily cap is fresh again.
    clock[0] = (int(clock[0]) // 86400 + 1) * 86400 + 60
    rig.step()
    assert len(rig.venue.place_log) == placed + 2
    rig.status_json()


def test_inventory_cap_tolerance_for_resting_buy(tmp_path, clock):
    overrides = {'pair.buy.levels': [B1], 'pair.mode': 'buy', 'pair.max_inventory_usdt': 200}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.step()
    [b1] = rig.rows()
    # 200 USDT at center 0.1359 = 1471.67 STONK; the resting BUY gets 1 % tolerance (1486.39).
    rig.venue.funds['STONK'] += D('1330')        # deposit: 1471.67 - 1330 < 148.69 <= 1486.39 - 1330
    rig.step(5)
    assert rig.row('buy:b1')['cancel_reason'] is None
    rig.venue.funds['STONK'] += D('10')          # 1486.39 - 1340 < 148.69
    rig.step(5)
    row = rig.store.order(b1['text'])
    assert row['cancel_reason'] == 'LIMIT_REDUCTION' and row['cancel_ack']
    rig.settle()
    assert rig.store.order(b1['text'])['state'] == 'closed'
    # A new order is sized to what still fits (no tolerance for new orders).
    last = rig.venue.place_log[-1]
    assert level_of(last.text) == 'b1' and last.amount < D('148.69')
    rig.status_json()


def test_order_size_below_exchange_minimum(tmp_path, clock):
    overrides = {'pair.buy.levels': [{'id': 'b1', 'edge_pct': 1.0, 'order_notional_usdt': 2}], 'pair.mode': 'buy'}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.step()
    assert rig.venue.place_log == []
    assert rig.engine.slot_states['buy:b1'] == 'ORDER_SIZE_BELOW_MINIMUM'


def test_profit_ceiling_couples_buy_levels_with_own_sells(tmp_path, clock):
    overrides = {'pair.sell.min_profit_pct': 2.5}
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000', overrides=overrides)
    rig.step()
    [batch] = rig.venue.batch_log
    assert [level_of(r.text) for r in batch] == ['s1', 's2', 'b2']
    # ceiling = 0.1373 * 0.999**2 / 1.025 = 0.13369.. -> 0.1336 < b1 0.1345
    assert rig.engine.slot_states['buy:b1'] == 'PROFIT_CEILING_BELOW_RANGE'
    rig.step(5)
    assert all(r['cancel_reason'] is None for r in rig.rows())   # coupling holds both ways
    rig.status_json()


def test_iceberg_levels_wait_in_live_until_verified(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000', mode='live')
    rig.step()
    [batch] = rig.venue.batch_log
    assert [level_of(r.text) for r in batch] == ['s1', 'b1']
    assert rig.engine.slot_states['sell:s2'] == 'ICEBERG_POC_NOT_VERIFIED'
    assert rig.engine.slot_states['buy:b2'] == 'ICEBERG_POC_NOT_VERIFIED'
    rig.status_json()


def test_iceberg_sent_in_live_when_verified(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000', mode='live',
                   overrides={'execution.iceberg_poc_verified': True})
    rig.step()
    [batch] = rig.venue.batch_log
    assert [r.iceberg for r in batch] == [None, D('54.07'), None, D('56.34')]


def test_not_sent_releases_and_backs_off_five_seconds(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.batch_plan.append('not_sent')
    assert rig.step() == 'RUNNING'
    assert rig.rows() == []
    rejected = rig.rows(active=False)
    assert len(rejected) == 2 and all(r['state'] == 'rejected' for r in rejected)
    assert rig.engine.slot_states['buy:b1'] == 'NOT_SENT:FAKE_NOT_SENT'
    rig.step(1)
    assert rig.engine.slot_states['buy:b1'] == 'REJECT_BACKOFF'
    rig.step(4.5)
    assert len(rig.rows()) == 2


def test_budget_not_sent_is_rate_wait_without_reject_count(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.batch_plan.append('budget')
    rig.step()
    assert rig.rows() == []
    assert rig.engine.slot_states['buy:b1'] == 'ORDER_RATE_WAIT'
    assert 'buy:b1' not in rig.engine._reject_backoff
    assert rig.engine.counters['rejects'] == 0
    rig.step(1)
    assert len(rig.rows()) == 2


def test_auth_rejection_stops_trading(tmp_path, clock):
    overrides = {'pair.buy.levels': [B1], 'pair.mode': 'buy'}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.venue.place_plan.append('reject:INVALID_KEY')
    assert rig.step() == 'GATE_AUTH:INVALID_KEY'
    assert rig.engine.stop_reason == 'GATE_AUTH:INVALID_KEY'
    assert rig.notifier.keys('reject:')
    placed = len(rig.venue.place_log)
    rig.step(400)
    assert len(rig.venue.place_log) == placed


def test_balance_reject_backs_off_with_notice(tmp_path, clock):
    overrides = {'pair.buy.levels': [B1], 'pair.mode': 'buy'}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.venue.place_plan.extend(['reject:BALANCE_NOT_ENOUGH', 'reject:BALANCE_NOT_ENOUGH'])
    rig.step()
    attempts, until, _streak = rig.engine._reject_backoff['buy:b1']
    assert attempts == 1 and until == pytest.approx(clock[0] + 15)
    assert len(rig.notifier.keys('reject:')) == 1
    rig.step(10)
    assert rig.engine.slot_states['buy:b1'] == 'REJECT_BACKOFF'
    rig.step(5.5)
    attempts, until, _streak = rig.engine._reject_backoff['buy:b1']
    assert attempts == 2 and until == pytest.approx(clock[0] + 30)


def test_telegram_unhealthy_in_live_cancels_and_blocks(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', mode='live', overrides={'execution.iceberg_poc_verified': True})
    rig.step()
    assert len(rig.rows()) == 2
    rig.notifier.is_healthy = False
    assert rig.step(5) == 'TELEGRAM_UNAVAILABLE'
    assert all(r['cancel_reason'] == 'TELEGRAM_UNAVAILABLE' for r in rig.rows())
    placed = len(rig.venue.place_log)
    rig.settle()
    assert rig.rows() == [] and len(rig.venue.place_log) == placed
    assert rig.venue.countdown_log[-1] == 0             # empty account: DMS cleared
    rig.status_json()


def test_pair_trade_status_forbids_side(tmp_path, clock):
    from engine_fakes import pair_meta
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000', meta=pair_meta(trade_status='buyable'))
    rig.step()
    [batch] = rig.venue.batch_log
    assert [level_of(r.text) for r in batch] == ['b1', 'b2']
    assert rig.engine.slot_states['sell:s1'] == 'PAIR_NOT_TRADABLE:buyable'


def test_texts_are_unique_gate_client_ids(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    rig.step()
    names = texts(rig.venue.place_log)
    assert len(set(names)) == len(names)
    assert all(name.startswith('t-stk1-') and len(name) - 2 <= 28 for name in names)


def test_max_open_orders_counts_slots(tmp_path, clock):
    overrides = {'safety.max_open_orders': 4}
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000', overrides=overrides)
    rig.step()
    assert len(rig.rows()) == 4
    assert isinstance(rig.engine.counters['orders_sent'], int)
    assert Decimal(rig.status_json()['reservations']['usdt']) == rig.reservations()
