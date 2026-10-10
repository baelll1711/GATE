"""Engine: fast local guard, STOP files, index blocking, lost cancels, DMS rules, clock check."""
from __future__ import annotations

import pytest

from engine_fakes import READS, make_rig, texts
from gate_stonk.dec import D

WIDE_BUY = {'pair.buy.band_tolerance_pct': 1.0}      # tolerance never relaxes the hard guard


def by_level(rig):
    return {row['level']: row for row in rig.rows()}


def test_hard_min_edge_cancels_from_preflight_without_account_reads(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    rows = by_level(rig)
    reserved = rig.reservations()
    mark = len(rig.venue.calls)
    rig.hub.set_index('0.13555')        # b1 0.1345: edge 0.77 % < hard 0.8 %; b2 still beyond the band
    rig.advance(0.2)
    assert rig.engine.preflight() == 'HARD_MIN_EDGE'
    assert rig.venue.calls[mark:] == ['cancel_many']
    assert texts(rig.venue.cancel_log[-1]) == [rows['b1']['text']]
    b1 = rig.store.order(rows['b1']['text'])
    assert (b1['cancel_reason'], b1['cancel_ack'], b1['state']) == ('HARD_MIN_EDGE', True, 'cancel_pending')
    assert rig.store.order(rows['b2']['text'])['cancel_reason'] is None
    assert rig.reservations() == reserved            # nothing released by a cancel ack
    rig.status_json()
    # Release only through the terminal protocol, then the slot is re-created deeper.
    assert rig.step(0.1) == 'RUNNING'
    assert rig.store.order(b1['text'])['terminal_status'] == 'cancelled'
    rig.settle()
    assert rig.store.order(b1['text'])['state'] == 'closed'
    new_b1 = rig.row('buy:b1')
    assert new_b1['price'] == D('0.1341')


def test_band_tolerance_never_relaxes_the_hard_guard(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides={'pair.buy.band_tolerance_pct': 50})
    rig.step()
    rig.hub.set_index('0.13555')
    rig.advance(0.2)
    assert rig.engine.preflight() == 'HARD_MIN_EDGE'


def test_adverse_band_move_premium_range_changed(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')          # default tolerance 0.15 %
    rig.step()
    rig.hub.set_index('0.1356')                           # both BUYs beyond the 0.15 % band, b1 not yet hard
    rig.advance(0.2)
    assert rig.engine.preflight() == 'PREMIUM_RANGE_CHANGED'
    assert {r['cancel_reason'] for r in rig.rows()} == {'PREMIUM_RANGE_CHANGED'}
    assert len(rig.venue.cancel_log) == 1                  # one batch


def test_safe_direction_move_only_by_manage_after_min_reprice(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    rig.hub.set_index('0.13644')                          # BUY targets move up: resting BUYs are deeper
    rig.advance(0.5)
    assert rig.engine.preflight() is None
    rig.step(0.5)                                         # age 1 s < min_reprice_seconds 3
    assert rig.venue.cancel_log == []
    rig.step(2.1)
    assert len(rig.venue.cancel_log) == 1
    assert {r['cancel_reason'] for r in rig.rows()} == {'REPRICE'}


def test_index_blocked_cancels_and_blocks_creates(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    mark = len(rig.venue.calls)
    rig.hub.errors['bybit_perp'] = 'DISCONNECTED'
    rig.advance(0.2)
    reason = rig.engine.preflight()
    assert reason.startswith('BAD_MARKET:REQUIRED_SOURCE_INVALID:bybit_perp')
    assert rig.venue.calls[mark:] == ['cancel_many'] and len(rig.venue.cancel_log[-1]) == 2
    placed = len(rig.venue.place_log)
    rig.step(0.2)
    rig.settle()
    assert rig.rows() == [] and len(rig.venue.place_log) == placed
    assert rig.engine.slot_states['buy:b1'].startswith('INDEX_BLOCKED:REQUIRED_SOURCE_INVALID:bybit_perp')
    status = rig.status_json()
    assert status['index']['ok'] is False
    # Source back: rejoin hysteresis (3 distinct snapshots) before trading resumes.
    rig.hub.errors.clear()
    for _ in range(4):
        rig.step(1)
    assert len(rig.venue.place_log) == placed + 2


def test_blocked_index_is_sampled_without_orders(tmp_path, clock):
    """Regression: with no orders left, a blocked index must still leave index_obs rows
    (about once a second) so the report's blocked-time share covers the outage."""
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.hub.errors['bybit_perp'] = 'DISCONNECTED'
    start_ms = int(clock[0] * 1000)
    for _ in range(100):                                   # 20 s of 0.2 s loop ticks, no orders at all
        rig.advance(0.2)
        rig.engine.sample_index()
    assert rig.rows() == []
    rows = rig.store.index_obs(start_ms)
    assert 23 <= len(rows) <= 26                           # one row per ~0.8 s
    assert all(not r['ok'] and r['reason'].startswith('REQUIRED_SOURCE_INVALID') for r in rows)
    assert max(b['ts_ms'] - a['ts_ms'] for a, b in zip(rows, rows[1:])) < 1000   # under the report gap cap
    # With an own order the dense 150 ms sampling still applies.
    rig.hub.errors.clear()
    for _ in range(4):
        rig.step(1)
    assert rig.rows()
    mark_ms = int(clock[0] * 1000)
    for _ in range(10):
        rig.advance(0.2)
        rig.engine.sample_index()
    assert len(rig.store.index_obs(mark_ms + 1)) == 10


def test_spot_book_invalid_is_bad_market(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.hub.spot_error = 'STALE_QUOTE'
    rig.advance(0.2)
    assert rig.engine.preflight() == 'BAD_MARKET:SPOT_BOOK:STALE_QUOTE'
    rig.step(0.1)
    rig.settle()
    assert rig.engine.slot_states['buy:b1'] == 'SPOT_BOOK:STALE_QUOTE'


def test_stop_trading_one_batch_before_first_read(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    rig.step()
    all_texts = {r['text'] for r in rig.rows()}
    rig.stop_file('STOP_TRADING')
    rig.venue.read_errors['open_orders'] = 1
    mark = len(rig.venue.calls)
    assert rig.step(1) == 'WAIT_ACCOUNT_RECONCILIATION'
    assert rig.venue.calls[mark] == 'cancel_many'
    assert set(texts(rig.venue.cancel_log[-1])) == all_texts and len(rig.venue.cancel_log) == 1
    assert all(r['cancel_reason'] == 'STOP_TRADING' and r['cancel_ack'] for r in rig.rows())
    assert len(rig.rows()) == 4                            # reservations kept until the terminal protocol
    rig.status_json()
    assert rig.step(1) == 'STOP_TRADING'
    rig.settle()
    assert rig.rows() == []
    assert rig.venue.countdown_log[-1] == 0 and rig.engine.dms_deadline_ms == 0
    placed = len(rig.venue.place_log)
    rig.step(5)
    assert len(rig.venue.place_log) == placed and rig.venue.countdown_log.count(0) == 1
    rig.stop_file('STOP_TRADING', present=False)
    rig.step(1)
    assert len(rig.venue.place_log) == placed + 4


def test_stop_sell_cancels_only_sell_slots(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', base='1000')
    rig.step()
    sells = {r['text'] for r in rig.rows() if r['side'] == 'sell'}
    rig.stop_file('STOP_SELL')
    rig.advance(0.2)
    assert rig.engine.preflight() == 'STOP_SELL'
    assert set(texts(rig.venue.cancel_log[-1])) == sells
    assert all(r['cancel_reason'] is None for r in rig.rows() if r['side'] == 'buy')
    rig.step(0.1)
    rig.settle()
    assert {r['side'] for r in rig.rows()} == {'buy'}
    assert rig.engine.slot_states['sell:s1'] == 'STOP_SELL'


def test_lost_cancel_retry_schedule_blocks_reads_dms_and_creates(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    t0 = clock[0]
    rig.step()
    b1 = rig.row('buy:b1')
    rig.venue.cancel_plan.extend(['lost_before'] * 20)
    rig.hub.set_index('0.13555')
    rig.advance(0.2)
    rig.engine.preflight()
    row = rig.store.order(b1['text'])
    assert (row['cancel_reason'], row['cancel_ack'], row['cancel_attempts']) == ('HARD_MIN_EDGE', False, 1)
    mark = len(rig.venue.calls)
    assert rig.step(0.2) == 'HARD_MIN_EDGE'                # the read checkpoint interrupts the sync
    assert rig.venue.reads_since(mark) == []
    for _ in range(3):                                     # retry only per backoff (5 s)
        rig.advance(1)
        rig.engine.preflight()
    assert len(rig.venue.cancel_log) == 1
    rig.advance(2.0)                                       # t0 + 5.4 > first retry (t0 + 0.2 + 5)
    rig.engine.preflight()
    assert len(rig.venue.cancel_log) == 2 and rig.store.order(b1['text'])['cancel_attempts'] == 2
    placed = len(rig.venue.place_log)
    while clock[0] < t0 + 40:                              # DMS still armed: no renewal, no creates
        rig.step(2.5)
    assert rig.venue.countdown_log == [45]
    assert len(rig.venue.place_log) == placed
    assert rig.engine.last_reason == 'HARD_MIN_EDGE'
    # STOP escalates immediately (priority 2 bypasses the retry backoff).
    sent = len(rig.venue.cancel_log)
    rig.stop_file('STOP_TRADING')
    rig.advance(0.1)
    rig.engine.preflight()
    assert len(rig.venue.cancel_log) == sent + 1
    assert rig.store.order(b1['text'])['cancel_reason'] == 'STOP_TRADING'
    rig.status_json()


@pytest.mark.parametrize('cancel_result', ['lost_before', 'refuse'])
def test_hard_guard_escalates_unconfirmed_routine_cancel_at_once(tmp_path, clock, cancel_result):
    rig = make_rig(tmp_path, clock, usdt='1000')            # default tolerance 0.15 %
    rig.step()
    b1, b2 = rig.row('buy:b1'), rig.row('buy:b2')
    rig.venue.cancel_plan.extend([cancel_result] * 10)
    rig.hub.set_index('0.1356')                            # routine PREMIUM_RANGE_CHANGED, reply lost/refused
    rig.advance(0.2)
    assert rig.engine.preflight() == 'PREMIUM_RANGE_CHANGED'
    assert len(rig.venue.cancel_log) == 1
    rig.hub.set_index('0.1350')                            # b1 0.1345: edge 0.37 % < hard 0.8 %
    rig.advance(0.2)
    assert rig.engine.preflight() == 'HARD_MIN_EDGE'      # long before the 5 s retry
    assert len(rig.venue.cancel_log) == 2 and texts(rig.venue.cancel_log[-1]) == [b1['text']]
    row = rig.store.order(b1['text'])
    assert (row['cancel_reason'], row['cancel_attempts']) == ('HARD_MIN_EDGE', 2)
    assert rig.store.order(b2['text'])['cancel_reason'] == 'PREMIUM_RANGE_CHANGED'   # b2 not in the hard zone
    for _ in range(10):                                    # escalated once: back on the retry schedule
        rig.advance(0.2)
        rig.engine.preflight()
    assert len(rig.venue.cancel_log) == 2


def test_blocked_index_escalates_unconfirmed_routine_cancels(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.venue.cancel_plan.extend(['lost_before'] * 10)
    rig.hub.set_index('0.1356')
    rig.advance(0.2)
    rig.engine.preflight()
    assert {r['cancel_reason'] for r in rig.rows()} == {'PREMIUM_RANGE_CHANGED'}
    rig.hub.errors['bybit_perp'] = 'DISCONNECTED'
    rig.advance(0.2)
    assert rig.engine.preflight().startswith('BAD_MARKET:')
    assert len(rig.venue.cancel_log) == 2 and len(rig.venue.cancel_log[-1]) == 2
    assert all(r['cancel_reason'].startswith('BAD_MARKET:') for r in rig.rows())


def test_refused_cancel_reads_continue_but_creates_wait(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000', overrides=WIDE_BUY)
    rig.step()
    rig.venue.cancel_plan.extend(['refuse'] * 10)
    rig.hub.set_index('0.13555')
    rig.advance(0.2)
    rig.engine.preflight()
    assert rig.step(0.2) == 'CANCEL_RECONCILIATION'        # explicit refusal: reads go on
    assert rig.venue.countdown_log == [45]


def test_dms_renewal_cadence(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    for _ in range(8):
        rig.step(5)
    assert rig.venue.countdown_log == [45, 45, 45]          # t0, t0+15, t0+30
    assert rig.engine.dms_deadline_ms == int(clock[0] * 1000) - 10_000 + 45_000


def test_dms_not_renewed_while_new_orders_are_unreconciled(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.batch_plan.append('lost_before')
    assert rig.step() == 'UNKNOWN_CREATE_RESULT'
    for _ in range(8):
        state = rig.step(5)
        assert state.startswith('UNKNOWN_ORDER_KEEP_RESERVATION:')
    assert rig.venue.countdown_log == [45]


def test_rest_read_dms_deadline_from_preflight(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.advance(29)                                        # 16 s left > margin 15.5 s
    assert rig.engine.preflight() is None
    rig.advance(1)                                         # 15 s left
    assert rig.engine.preflight() == 'REST_READ_DMS_DEADLINE'
    assert {r['cancel_reason'] for r in rig.rows()} == {'REST_READ_DMS_DEADLINE'}


def test_dms_unknown_result_cancels_everything(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.venue.countdown_plan.append('unknown')
    assert rig.step() == 'DMS_OR_ACTION_UNKNOWN'
    assert rig.venue.place_log == [] and rig.engine.dms_deadline_ms == 0
    assert rig.notifier.keys('state:')


def test_clock_skew_stops_quoting(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.venue.server_skew_ms = 2500
    rig.engine._last_clock_check = None
    assert rig.step(1) == 'CLOCK_SKEW:2500'
    assert {r['cancel_reason'] for r in rig.rows()} == {'CLOCK_SKEW:2500'}
    assert rig.status_json()['clock_skew_ms'] == 2500


def test_shutdown_cancels_and_keeps_dms(tmp_path, clock):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.engine.shutdown()
    assert {r['cancel_reason'] for r in rig.rows()} == {'PROCESS_SHUTDOWN'}
    assert 0 not in rig.venue.countdown_log
    assert rig.engine.dms_deadline_ms > 0


@pytest.mark.parametrize('read', ['open_orders', 'my_trades', 'balances'])
def test_read_errors_wait_then_cancel_after_grace(tmp_path, clock, read):
    rig = make_rig(tmp_path, clock, usdt='1000')
    rig.step()
    rig.venue.read_errors[read] = 100
    assert rig.step(1) == 'WAIT_ACCOUNT_RECONCILIATION'
    assert all(r['cancel_reason'] is None for r in rig.rows())
    rig.step(6)
    assert rig.step(6) == 'ACCOUNT_READ_FAILED'
    assert {r['cancel_reason'] for r in rig.rows()} == {'ACCOUNT_READ_FAILED'}
    assert set(READS) >= {read}
