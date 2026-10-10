"""Engine reconciliation: unknown creates, post-only, fills and history lag, foreign activity, restart."""
from __future__ import annotations

import pytest

from engine_fakes import FakeNotifier, make_rig, texts
from gate_stonk.dec import D
from gate_stonk.engine import Engine
from gate_stonk.index import IndexBuilder

B1 = {'id': 'b1', 'edge_pct': 1.0, 'order_notional_usdt': 20}


def levels(rig):
    return {row['level']: row for row in rig.rows()}


# ---------------------------------------------------------------- unknown create
def test_unknown_create_lost_after_acceptance_binds_by_text(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    rig.venue.batch_plan.append('lost_after')
    reserved_before = rig.reservations()
    assert rig.step() == 'UNKNOWN_CREATE_RESULT'
    rows = rig.rows()
    assert len(rows) == 4 and all(r['state'] == 'unknown' and r['order_id'] is None for r in rows)
    assert rig.reservations() > reserved_before                 # reservations kept
    assert len(rig.notifier.keys('unknown:')) == 4
    assert rig.venue.cancel_log == []                            # no ladder-wide cancel
    rig.status_json()
    assert rig.step(1) == 'RUNNING'                              # next sync binds by text
    rows = rig.rows()
    assert all(r['state'] == 'open' and r['order_id'] for r in rows)
    assert len(rig.venue.batch_log) == 1                         # never re-sent
    assert {r.text for r in rig.venue.batch_log[0]} == {r['text'] for r in rows}
    rig.step(14)                                                 # renewal due again only after binding
    assert rig.venue.countdown_log == [45, 45]


def test_unknown_create_never_seen_closes_after_expiry_window(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    t0 = clock[0]
    rig.venue.batch_plan.append('lost_before')
    assert rig.step() == 'UNKNOWN_CREATE_RESULT'
    lost = {r['text'] for r in rig.rows()}
    state = rig.step(5)
    assert state.startswith('UNKNOWN_ORDER_KEEP_RESERVATION:')
    assert set(texts(rig.venue.cancel_log[-1])) == lost          # cancels everything (by text)
    assert len(rig.venue.batch_log) == 1
    while clock[0] < t0 + 49:
        assert rig.step(5).startswith('UNKNOWN_ORDER_KEEP_RESERVATION:')
    assert len(rig.venue.batch_log) == 1 and len(rig.rows()) == 2
    # expiry = request_expiry 10 + 2 * timeout 5 + margin 30 = 50 s after the reservation
    rig.advance(t0 + 51 - clock[0])
    assert rig.step() == 'CANCEL_RECONCILIATION'
    assert {r['terminal_status'] for r in rig.rows()} == {'rejected'}
    assert rig.notifier.keys('unknown_absent:')
    assert rig.step(2.5) == 'RUNNING'
    assert {rig.store.order(t)['state'] for t in lost} == {'rejected'}   # NOT accepted, released
    assert len(rig.venue.batch_log) == 2                                 # trading resumed
    rig.status_json()


def test_unknown_create_absent_cancels_healthy_slots_too(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.place_plan.extend(['lost_before', None])
    assert rig.step() == 'UNKNOWN_CREATE_RESULT'
    rows = levels(rig)
    assert rows['b1']['state'] == 'unknown' and rows['b2']['state'] == 'open'
    assert rig.step(5).startswith('UNKNOWN_ORDER_KEEP_RESERVATION:' + rows['b1']['text'])
    assert set(texts(rig.venue.cancel_log[-1])) == {rows['b1']['text'], rows['b2']['text']}
    assert rig.venue.orders[rows['b2']['order_id']].status == 'cancelled'


# ---------------------------------------------------------------- post-only
def test_post_only_rejection_label_backoff_schedule(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.place_plan.extend(['reject:POC_FILL_IMMEDIATELY', None] + ['reject:POC_FILL_IMMEDIATELY'] * 3)
    rig.step()
    assert rig.engine._reject_backoff['buy:b1'][0] == 1
    assert rig.row('buy:b1') is None and rig.row('buy:b2') is not None
    delays = []
    for dt in (0.1, 0.1):
        rig.step(dt)
        attempts, until, streak = rig.engine._reject_backoff['buy:b1']
        delays.append(round(until - clock[0], 3))
    assert delays == [0, 5]                                      # attempts 2 and 3: 0, 5 s
    rig.step(1)
    assert rig.engine.slot_states['buy:b1'] == 'REJECT_BACKOFF'
    rig.step(4.5)
    attempts, until, streak = rig.engine._reject_backoff['buy:b1']
    assert (attempts, round(until - clock[0], 3), streak) == (4, 10, 4)
    assert len(rig.notifier.keys('reject:')) == 2               # no notice for the 0-delay rejections
    rig.step(10.5)
    assert 'buy:b1' not in rig.engine._reject_backoff            # accepted: backoff cleared
    assert rig.row('buy:b1')['state'] == 'open'
    assert rig.engine.counters['post_only_rejects'] == 4
    rig.status_json()


def test_accepted_but_cancelled_poc_backs_off_and_releases_via_terminal_protocol(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.place_plan.extend(['poc', None, 'poc'])
    rig.step()
    first = rig.row('buy:b1')
    assert first['state'] == 'open' and first['order_id']        # bound; exchange shows cancelled/poc
    assert rig.engine._reject_backoff['buy:b1'][0] == 1
    reserved = rig.reservations()
    assert rig.step(0.5) == 'RUNNING'
    row = rig.store.order(first['text'])
    assert (row['state'], row['terminal_status'], row['finish_as']) == ('open', 'cancelled', 'poc')
    assert rig.reservations() == reserved                        # not released by the reply
    assert rig.engine.slot_states['buy:b1'] == 'SLOT_ACTIVE'
    rig.step(2.0)
    closed = rig.store.order(first['text'])
    assert (closed['state'], closed['finish_as']) == ('closed', 'poc')
    second = rig.row('buy:b1')                                   # re-created in the same step (delay 0)
    assert second is not None and second['text'] != first['text']
    assert rig.engine._reject_backoff['buy:b1'][0] == 2          # counted once per rejection
    assert rig.engine.counters['post_only_rejects'] == 2


# ---------------------------------------------------------------- fills
def test_partial_fills_on_several_levels_in_one_pass(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    rig.step()
    rows = levels(rig)
    rig.venue.fill(rows['s1']['order_id'], '50')
    rig.venue.fill(rows['b1']['order_id'], '40')
    rig.venue.fill(rows['b2']['order_id'], '25.5')
    assert rig.step(1) == 'RUNNING'
    assert rig.venue.calls[-5:].count('my_trades') == 1
    after = levels(rig)
    assert (after['s1']['filled_qty'], after['b1']['filled_qty'], after['b2']['filled_qty']) == (
        D('50'), D('40'), D('25.5'))
    assert all(r['state'] == 'open' and r['cancel_reason'] is None for r in after.values())
    assert len(rig.store.fills()) == 3
    keys = [row['key'] for row in rig.store.outbox_rows(100)]
    assert sum(key.startswith('fill:') for key in keys) == 3
    assert any(key.startswith('position:') for key in rig.notifier.keys())
    position = rig.store.position()
    assert position.bought_qty == D('65.5') and position.sold_qty == D('50')
    rig.status_json()


def test_late_visible_trade_blocks_release_and_watermark(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rows = levels(rig)
    rig.step(5)
    watermark = rig.store.meta('last_sync_ms')
    rig.venue.fill(rows['b2']['order_id'], rows['b2']['qty'], visible_delay_ms=4000)
    assert rig.step(1) == 'WAIT_ACCOUNT_RECONCILIATION'
    assert 'order_trades' in rig.venue.calls                      # §19 fallback tried (also lagging)
    row = rig.store.order(rows['b2']['text'])
    assert row['state'] == 'open' and row['terminal_status'] is None and row['filled_qty'] == 0
    assert rig.store.meta('last_sync_ms') == watermark
    assert rig.engine.next_reconciliation_deadline() is None
    rig.status_json()
    assert rig.step(5) == 'RUNNING'                               # history caught up: consistent
    row = rig.store.order(rows['b2']['text'])
    assert row['terminal_status'] == 'filled' and row['filled_qty'] == rows['b2']['qty']
    assert rig.store.meta('last_sync_ms') > watermark
    rig.step(2.5)
    assert rig.store.order(rows['b2']['text'])['state'] == 'closed'
    assert rig.row('buy:b2') is not None                         # slot re-created after release


def test_order_trades_fallback_when_window_misses_trades(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rows = levels(rig)
    rig.venue.fill(rows['b1']['order_id'], rows['b1']['qty'], in_window=False)
    mark = len(rig.venue.calls)
    assert rig.step(1) == 'RUNNING'
    assert rig.venue.calls[mark:].count('order_trades') == 1
    row = rig.store.order(rows['b1']['text'])
    assert row['filled_qty'] == rows['b1']['qty'] and row['terminal_status'] == 'filled'


def test_fill_while_cancel_pending_is_accounted_before_release(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides={'pair.buy.band_tolerance_pct': 1.0})
    rig.step()
    rows = levels(rig)
    rig.venue.fill(rows['b1']['order_id'], '100')
    rig.hub.set_index('0.13555')
    rig.advance(0.2)
    rig.engine.preflight()
    rig.step(0.1)
    rig.settle()
    closed = rig.store.order(rows['b1']['text'])
    assert closed['state'] == 'closed' and closed['filled_qty'] == D('100')
    assert closed['terminal_status'] == 'cancelled'


# ---------------------------------------------------------------- foreign activity
def test_foreign_open_order_account_review_settles_own_and_keeps_dms(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    own = {r['text'] for r in rig.rows()}
    foreign = rig.venue.add_foreign_order()
    assert rig.step(1) == 'FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT'
    assert rig.store.external_activity()['reason'] == 'FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT'
    assert set(texts(rig.venue.cancel_log[-1])) == own
    assert foreign.status == 'open'                              # never cancelled by the bot
    assert rig.step(1) == 'STOP_ACCOUNT_REVIEW'
    rig.settle()
    assert rig.rows() == []
    assert 0 not in rig.venue.countdown_log                      # foreign order keeps the DMS
    placed = len(rig.venue.place_log)
    rig.venue._cancel(foreign)                                   # the operator removes it
    assert rig.step(1) == 'STOP_ACCOUNT_REVIEW'
    assert rig.venue.countdown_log[-1] == 0
    assert rig.notifier.keys('account_review_settled')
    assert len(rig.venue.place_log) == placed
    status = rig.status_json()
    assert status['account_review_required'] is True
    # Permanent: a restarted engine on the same store stays stopped.
    engine2 = Engine(rig.cfg, rig.store, rig.venue, rig.hub, IndexBuilder(rig.cfg.index_settings()),
                     FakeNotifier(), live=False, meta=rig.venue.meta)
    assert engine2.step() == 'STOP_ACCOUNT_REVIEW'


def test_foreign_trade_requires_review_and_is_quarantined(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.venue.add_foreign_trade()
    assert rig.step(1) == 'FOREIGN_FILL'
    assert rig.store.external_activity()['reason'] == 'FOREIGN_FILL'
    assert rig.step(1) == 'STOP_ACCOUNT_REVIEW'
    assert len(rig.store.external_fills()) == 1
    assert rig.store.fills() == []
    rig.settle()
    assert rig.rows() == []


def test_closed_order_reappearing_is_unsafe(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rows = levels(rig)
    rig.stop_file('STOP_TRADING')
    rig.step(0.2)
    rig.settle()
    rig.stop_file('STOP_TRADING', present=False)
    rig.venue.place_plan.extend([None, None])
    revived = rig.venue.orders[rows['b1']['order_id']]
    revived.status, revived.finish_as = 'open', None
    revived.left = revived.amount
    rig.venue.funds['USDT'] -= revived.lock
    assert rig.step(1) == 'CLOSED_ORDER_REAPPEARED'


# ---------------------------------------------------------------- restart
def test_restart_recovery_cancels_first_and_blocks_creates(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    rig.step()
    old = {r['text'] for r in rig.rows()}
    engine2 = Engine(rig.cfg, rig.store, rig.venue, rig.hub, IndexBuilder(rig.cfg.index_settings()),
                     FakeNotifier(), live=False, meta=rig.venue.meta)
    assert engine2.recovering
    mark = len(rig.venue.calls)
    rig.advance(1)
    engine2.startup()
    assert rig.venue.calls[mark:] == ['cancel_many']             # before any read
    assert set(texts(rig.venue.cancel_log[-1])) == old
    assert {r['cancel_reason'] for r in rig.rows()} == {'RECOVERY'}
    placed = len(rig.venue.place_log)
    rig.advance(0.5)
    assert engine2.step() == 'RECOVERY'
    assert len(rig.venue.place_log) == placed
    json_ok = engine2.status()
    assert json_ok['recovering'] is True
    rig.advance(2.5)
    assert engine2.step() == 'RUNNING'                           # closed, then created in the same step
    assert not engine2.recovering
    assert len(rig.venue.place_log) == placed + 4
    assert not (old & {r['text'] for r in rig.rows()})


def test_restart_lost_cancel_is_resent_immediately(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.venue.cancel_plan.append('lost_before')
    engine2 = Engine(rig.cfg, rig.store, rig.venue, rig.hub, IndexBuilder(rig.cfg.index_settings()),
                     FakeNotifier(), live=False, meta=rig.venue.meta)
    rig.advance(1)
    engine2.startup()
    assert len(rig.venue.cancel_log) == 2
    assert all(r['cancel_ack'] for r in rig.rows())


@pytest.mark.parametrize('settle_seconds', [2])
def test_next_step_seconds_follow_terminal_settle(tmp_path, clock, settle_seconds):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides={'pair.buy.levels': [B1], 'pair.mode': 'buy'})
    rig.step()
    assert rig.engine.next_step_seconds() == 5
    rig.stop_file('STOP_SELL')          # unrelated side: nothing to cancel
    rig.engine.request_stop('STOP_MANUAL')
    rig.advance(0.1)
    rig.engine.preflight()
    assert rig.engine.next_step_seconds() == 0                   # cancel ack -> immediate sync
    rig.step()
    assert rig.engine.next_step_seconds() == pytest.approx(settle_seconds)
    rig.advance(1.5)
    assert rig.engine.next_step_seconds() == pytest.approx(0.5)
    rig.step(0.5)
    assert rig.rows() == []
    assert rig.engine.clear_stop('STOP_MANUAL') is True
