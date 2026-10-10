"""Engine: REPRICE batching and scheduling, placeable-only reprices, amend path (DESIGN §17)."""
from __future__ import annotations

import pytest

from engine_fakes import make_rig, texts
from gate_stonk.dec import D

WIDE_BUY = {'pair.buy.band_tolerance_pct': 1.0}
AMEND = dict(WIDE_BUY, **{'execution.reprice_via_amend': True})


def levels(rig):
    return {row['level']: row for row in rig.rows()}


def test_reprice_several_levels_one_cancel_batch_one_place_batch(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    old = levels(rig)
    rig.hub.set_index('0.13644')                 # +0.4 %: targets b1 0.1350, b2 0.1337
    assert rig.step(1.0) == 'RUNNING'            # younger than min_reprice_seconds (3 s)
    assert rig.venue.cancel_log == []
    assert rig.step(2.5) == 'RUNNING'
    assert len(rig.venue.cancel_log) == 1
    assert set(texts(rig.venue.cancel_log[0])) == {old['b1']['text'], old['b2']['text']}
    assert {r['cancel_reason'] for r in rig.rows()} == {'REPRICE'}
    assert rig.notifier.keys('cancel:') == []    # routine cancel sent: no notice
    assert rig.engine.next_step_seconds() == 0   # the ack requests an immediate reconciliation
    rig.step()
    assert {r['terminal_status'] for r in rig.rows()} == {'cancelled'}
    assert rig.engine.next_step_seconds() == pytest.approx(2.0)
    batches = len(rig.venue.batch_log)
    rig.step(2.0)
    assert len(rig.venue.batch_log) == batches + 1       # replacements in ONE batch, same step
    assert [(r.text.split('-')[2], r.price) for r in rig.venue.batch_log[-1]] == [
        ('b1', D('0.1350')), ('b2', D('0.1337'))]
    assert {rig.store.order(t)['state'] for t in (old['b1']['text'], old['b2']['text'])} == {'closed'}
    assert len(rig.venue.cancel_log) == 1
    assert rig.notifier.keys('terminal:') == []          # routine close: no notice
    rig.status_json()


def test_reprice_threshold_respected(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    rig.hub.set_index('0.13600')                 # b1 -> 0.1346, b2 -> 0.1332: both 0.07 % < 0.15 %
    for _ in range(6):
        rig.step(5)
    assert rig.venue.cancel_log == []


def test_never_withdraw_for_a_target_that_cannot_be_sent(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    old = levels(rig)
    rig.hub.set_index('0.13644')
    rig.hub.set_spot('0.1340', '0.1349')         # new b1 target 0.1350 would cross the ask
    rig.step(3.5)
    assert len(rig.venue.cancel_log) == 1
    assert texts(rig.venue.cancel_log[0]) == [old['b2']['text']]
    assert rig.store.order(old['b1']['text'])['cancel_reason'] is None
    assert rig.engine.targets['buy:b1'].reason == 'POST_ONLY_OR_SELF_CROSS_WAIT'


def test_reprice_paused_by_action_cap(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    now = int(clock[0] * 1000)
    for _ in range(960):
        rig.store.action('filler', now, 1000, emergency=True)
    rig.hub.set_index('0.13644')
    rig.step(3.5)
    assert rig.venue.cancel_log == []
    assert rig.engine._slot_notes['buy:b1'] == 'REPRICE_PAUSED_ACTION_CAP'
    assert rig.status_json()['slots']['buy:b1']['note'] == 'REPRICE_PAUSED_ACTION_CAP'


def test_reprice_paused_by_order_rate(tmp_path, clock):
    from gate_stonk.ratelimit import RateLimiter
    limiter = RateLimiter.from_settings(2)
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY, limiter=limiter)
    rig.step()
    limiter.admit('order')
    limiter.admit('order')
    rig.hub.set_index('0.13644')
    rig.step(3.5)
    assert rig.venue.cancel_log == []
    assert rig.engine._slot_notes['buy:b1'] == 'REPRICE_PAUSED_RATE'
    rig.status_json()


def test_deeper_tolerance_paces_safe_moves(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides={'pair.buy.deeper_tolerance_pct': 0.5})
    rig.step()
    rig.hub.set_index('0.13644')                 # +0.4 % < deeper 0.5 %: kept
    rig.step(5)
    assert rig.venue.cancel_log == []
    rig.hub.set_index('0.13685')                 # +0.7 %: paced REPRICE (not PREMIUM_RANGE_CHANGED)
    rig.step(5)
    assert {r['cancel_reason'] for r in rig.rows()} == {'REPRICE'}


# ---------------------------------------------------------------- amend
def test_amend_keeps_order_id_and_adjusts_reservation(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=AMEND)
    rig.step()
    old = levels(rig)
    rig.hub.set_index('0.13644')
    assert rig.step(3.5) == 'RUNNING'
    assert rig.venue.cancel_log == []
    assert rig.venue.amend_log == [(old['b1']['order_id'], D('0.1350')), (old['b2']['order_id'], D('0.1337'))]
    new = levels(rig)
    assert new['b1']['order_id'] == old['b1']['order_id'] and new['b1']['price'] == D('0.1350')
    assert new['b2']['price'] == D('0.1337') and new['b1']['amend_state'] is None
    assert new['b1']['prior_prices'][0][0] == D('0.1345')
    factor = D('1.003')
    assert rig.reservations() == (new['b1']['qty'] * D('0.1350') + new['b2']['qty'] * D('0.1337')) * factor
    assert rig.engine.next_step_seconds() == 0
    assert rig.step() == 'RUNNING'                # reconciliation accepts the amended price
    assert rig.step(5) == 'RUNNING'
    assert rig.venue.batch_log and len(rig.venue.batch_log) == 1
    rig.status_json()


def test_amend_refused_falls_back_to_cancel(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=AMEND)
    rig.step()
    old = levels(rig)
    rig.venue.amend_plan.extend(['reject:ORDER_NOT_FOUND', None])
    rig.hub.set_index('0.13644')
    rig.step(3.5)
    assert texts(rig.venue.cancel_log[-1]) == [old['b1']['text']]
    assert rig.store.order(old['b1']['text'])['amend_state'] is None
    assert rig.store.order(old['b2']['text'])['price'] == D('0.1337')


def test_unknown_amend_cancels_and_raises_unknown_create(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=AMEND)
    rig.step()
    old = levels(rig)
    rig.venue.amend_plan.extend(['lost_after', None])
    rig.hub.set_index('0.13644')
    assert rig.step(3.5) == 'UNKNOWN_AMEND_RESULT'
    row = rig.store.order(old['b1']['text'])
    assert row['amend_state'] == 'unknown' and row['amend_price'] == D('0.1350')
    assert row['cancel_reason'] == 'REPRICE' and row['cancel_ack']
    assert texts(rig.venue.cancel_log[-1]) == [old['b1']['text']]
    # Reservation of an unresolved BUY amend uses the higher price.
    b2 = rig.store.order(old['b2']['text'])
    factor = D('1.003')
    assert rig.reservations() == (row['qty'] * D('0.1350') + b2['qty'] * b2['price']) * factor
    status = rig.status_json()
    assert status['slots']['buy:b1']['order']['amend_state'] == 'unknown'
    rig.step()
    row = rig.store.order(old['b1']['text'])
    assert row['amend_state'] is None and row['price'] == D('0.1350')     # resolved by reconciliation
    rig.settle()
    assert rig.store.order(old['b1']['text'])['state'] == 'closed'


def test_amended_order_keeps_its_amount_without_limit_reduction(tmp_path, clock):
    """Regression (found by the synthetic simulation): a price-only amend keeps the amount
    that was sized at the original price; the next manage pass must not compare it with
    the configured size at the NEW (higher BUY) price and cancel it as LIMIT_REDUCTION.
    A real reduction (money at the new price) still cancels."""
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=AMEND)
    rig.step()
    old = levels(rig)
    rig.hub.set_index('0.13644')
    assert rig.step(3.5) == 'RUNNING'
    assert len(rig.venue.amend_log) == 2 and rig.venue.cancel_log == []
    for _ in range(3):
        assert rig.step(5) == 'RUNNING'
    assert rig.venue.cancel_log == []
    rows = levels(rig)
    for level in ('b1', 'b2'):
        assert rows[level]['text'] == old[level]['text'] and rows[level]['cancel_reason'] is None
        assert rows[level]['qty'] == old[level]['qty'] and rows[level]['prior_prices']
    rig.venue.funds['USDT'] = D('0')                 # money really shrank: the amended orders no longer fit
    rig.step(5)
    assert {r['cancel_reason'] for r in rig.rows()} == {'LIMIT_REDUCTION'}


def test_live_own_levels_keep_the_price_an_amend_just_left(tmp_path, clock):
    """Regression: right after an amend the cached Gate book (older than the amend) still
    shows the bot's volume at the OLD price; it must stay subtracted from the depth for
    2 s, exactly like a just-closed order, or a range level counts it as foreign volume."""
    rig = make_rig(tmp_path, clock, usdt='1000', mode='live', overrides=AMEND)
    rig.step()
    b1 = rig.row('buy:b1')
    rig.hub.set_index('0.13644')
    assert rig.step(3.5) == 'RUNNING'
    amended = rig.store.order(b1['text'])
    assert amended['price'] == D('0.1350') and amended['prior_prices'][0][0] == D('0.1345')
    left_ms = amended['prior_prices'][0][1]
    assert rig.engine._own_levels('buy', left_ms - 100) == {D('0.1350'): b1['qty'], D('0.1345'): b1['qty']}
    assert rig.engine._own_levels('buy', left_ms + 2000) == {D('0.1350'): b1['qty'], D('0.1345'): b1['qty']}
    assert rig.engine._own_levels('buy', left_ms + 2001) == {D('0.1350'): b1['qty']}


def test_recreated_deeper_level_never_rests_above_a_shallower_resting_order(tmp_path, clock):
    """Regression: b1 and b2 are kept by deeper_tolerance_pct after the index rises; when
    b2 fills, its slot was re-created at its fresh 2 % target ABOVE the resting b1 (an
    inverted ladder). Monotonicity now also holds against resting orders of other levels."""
    overrides = {'pair.mode': 'buy', 'pair.buy.deeper_tolerance_pct': 2.0}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.step()
    old = levels(rig)
    assert (old['b1']['price'], old['b2']['price']) == (D('0.1345'), D('0.1331'))
    rig.hub.set_index('0.1380')                  # fresh targets b1 0.1366, b2 0.1352: both kept (deeper)
    rig.step(5)
    assert rig.venue.cancel_log == []
    rig.venue.fill(old['b2']['order_id'], old['b2']['qty'])
    rig.step(1)
    rig.settle()
    new = levels(rig)
    assert new['b1']['text'] == old['b1']['text'] and new['b1']['price'] == D('0.1345')
    assert new['b2']['text'] != old['b2']['text']
    assert new['b2']['price'] == D('0.1344') < new['b1']['price']   # shifted below the resting b1
    assert rig.engine.targets['buy:b2'].reason == 'FIXED_EDGE+LADDER_SHIFT'
    for _ in range(4):                           # stable: nothing is repriced back and forth
        rig.step(5)
    assert rig.venue.cancel_log == [] and levels(rig)['b2']['price'] == D('0.1344')


def test_shallower_level_waits_for_a_deeper_resting_order_before_re_creation(tmp_path, clock):
    """A shallower slot is not re-created at or below a resting deeper order (that order
    is within its reprice threshold); it waits with LEVEL_ORDER_WAIT until it can be
    placed strictly above it."""
    overrides = {'pair.mode': 'buy', 'pair.buy.levels': [{'id': 'b1', 'edge_pct': 1.0, 'order_notional_usdt': 20},
                                                         {'id': 'b2', 'edge_pct': 1.05, 'order_notional_usdt': 20}]}
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=overrides)
    rig.step()
    old = levels(rig)
    assert (old['b1']['price'], old['b2']['price']) == (D('0.1345'), D('0.1344'))
    rig.hub.set_index('0.13585')                 # fresh b1 0.1344, b2 0.1343: both within the 0.15 % threshold
    rig.step(5)
    assert rig.venue.cancel_log == []
    rig.venue.fill(old['b1']['order_id'], old['b1']['qty'])
    rig.step(1)
    rig.settle()
    assert rig.store.order(old['b1']['text'])['state'] == 'closed'
    assert rig.row('buy:b1') is None                              # not at 0.1344 == the resting b2
    assert rig.engine.slot_states['buy:b1'] == 'LEVEL_ORDER_WAIT'
    assert levels(rig)['b2']['text'] == old['b2']['text']
    rig.hub.set_index('0.1359')                  # fresh b1 0.1345 is strictly above b2 again
    rig.step(5)
    assert rig.row('buy:b1')['price'] == D('0.1345') > rig.row('buy:b2')['price']
