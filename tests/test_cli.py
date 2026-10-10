"""CLI: validate, run gating and loop, cancel-only, status, report, logging (offline, fake clock)."""
from __future__ import annotations

import json
import logging
import os
import signal
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import pytest

from gate_stonk import cli
from gate_stonk.cli import Deps, ProcessLocks, main
from gate_stonk.config import load_config
from gate_stonk.errors import BudgetWait, Unsafe
from gate_stonk.status import write_status
from gate_stonk.store import Store

from cli_fakes import (FakeEngine, FakeHub, FakeLiveVenue, FakeNotifier, FakeFill, Output, advance, forbid,
                       live_env, make_live, order, pair_meta, write_config)


def quiet_deps(out: Output, **kwargs) -> Deps:
    kwargs.setdefault('logging', False)
    kwargs.setdefault('signals', False)
    return Deps(out=out, err=out, **kwargs)


def forbidden_deps(out: Output) -> Deps:
    return quiet_deps(out, hub_factory=forbid('hub'), meta_factory=forbid('meta'),
                      live_venue_factory=forbid('live venue'), paper_venue_factory=forbid('paper venue'),
                      engine_factory=forbid('engine'), notifier_factory=forbid('notifier'),
                      fill_wakeup_factory=forbid('fill wakeup'))


# ============================================================================ validate

def test_validate_prints_russian_summary(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    assert main(['validate', '--config', str(path)], deps=quiet_deps(out)) == 0
    text = out.text
    assert 'Конфигурация верна' in text
    assert 'Пара STONK_USDT' in text and 'режим' in text.lower()
    assert 'bybit_perp' in text and 'вес 0.4' in text and 'обязательный' in text
    assert 'b1: край 1%' in text and 's2: край 2%' in text and 'iceberg видимо 25%' in text
    assert 'DMS: вкл. таймаут 45 с' in text
    assert 'в сутки (UTC) 200 USDT' in text
    assert not (tmp_path / 'state').exists()        # validate never creates state


def test_config_option_before_or_after_command(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    assert main(['--config', str(path), 'validate'], deps=quiet_deps(out)) == 0
    assert main(['validate', '--config', str(path)], deps=quiet_deps(out)) == 0


def test_validate_failure_exit_code_and_key_path(tmp_path):
    path = write_config(tmp_path, lambda d: d['safety'].update(hard_min_edge_pct=2))
    out = Output()
    assert main(['validate', '--config', str(path)], deps=quiet_deps(out)) == 1
    assert 'Ошибка конфигурации' in out.text and 'safety.hard_min_edge_pct' in out.text


def test_validate_missing_file(tmp_path):
    out = Output()
    assert main(['validate', '--config', str(tmp_path / 'nope.yaml')], deps=quiet_deps(out)) == 1
    assert 'not found' in out.text


def test_validate_center_preview(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    rc = main(['validate', '--config', str(path), '--center', '0.1356', '--precision', '5',
               '--amount-precision', '1'], deps=quiet_deps(out))
    assert rc == 0
    text = out.text
    assert 'Лестница при центре 0.1356' in text
    assert 'b1: цена 0.13424' in text and 's1: цена 0.13696' in text
    assert 'b2: цена 0.13288' in text and 'видимо' in text


def test_validate_center_needs_precision(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    assert main(['validate', '--config', str(path), '--center', '0.1356'], deps=quiet_deps(out)) == 1
    assert '--precision' in out.text


# ============================================================================ run: gates

def _no_iceberg(data):
    for side in ('buy', 'sell'):
        for level in data['pair'][side]['levels']:
            level.pop('iceberg_display_pct', None)
            level.pop('iceberg_display_qty', None)


GATE_CASES = {
    'live_without_flag': (lambda d: d['app'].update(mode='live'), [], {}, 'LIVE_CONFIG_REQUIRES_EXPLICIT_RUN_LIVE_FLAG'),
    'flag_with_paper': (None, ['--live'], {}, 'LIVE_FLAG_REQUIRES_APP_MODE_LIVE'),
    'seconds_live': (make_live, ['--live', '--seconds', '10'], {'env': True}, 'SECONDS_IS_PAPER_ONLY'),
    'missing_confirm': (make_live, ['--live'], {'env': True, 'confirm': False}, 'LIVE_CONFIRM_MISSING'),
    'confirm_in_env_file': (make_live, ['--live'], {'env': True, 'dotenv': True}, 'LIVE_CONFIRM_IN_ENV_FILE'),
    'no_ack': (lambda d: (make_live(d), d['safety'].update(dedicated_account_ack=False)), ['--live'], {'env': True},
               'DEDICATED_ACCOUNT_ACK_REQUIRED'),
    'no_dms': (lambda d: (make_live(d), d['safety'].update(dead_man_switch=False)), ['--live'], {'env': True},
               'DEAD_MAN_SWITCH_REQUIRED'),
    'missing_keys': (make_live, ['--live'], {'env': True, 'keys': False}, 'GATE_API_KEYS_MISSING'),
    'telegram_disabled': (lambda d: (make_live(d), d['telegram'].update(enabled=False)), ['--live'], {'env': True},
                          'TELEGRAM_REQUIRED_BUT_DISABLED'),
    'telegram_no_token': (make_live, ['--live'], {'env': True, 'telegram': False}, 'TELEGRAM_CREDENTIALS_MISSING'),
    'iceberg_unverified': (lambda d: (make_live(d), d['execution'].update(iceberg_poc_verified=False)), ['--live'],
                           {'env': True}, 'ICEBERG_POC_NOT_VERIFIED'),
}


@pytest.mark.parametrize('case', sorted(GATE_CASES))
def test_run_gates_refuse_without_network_or_orders(tmp_path, monkeypatch, case):
    mutate, extra, opts, code = GATE_CASES[case]
    if opts.get('env'):
        live_env(monkeypatch, confirm=opts.get('confirm', True))
        if opts.get('keys') is False:
            monkeypatch.delenv('GATE_API_KEY')
            monkeypatch.delenv('GATE_API_SECRET')
        if opts.get('telegram') is False:
            monkeypatch.delenv('TELEGRAM_BOT_TOKEN')
    path = write_config(tmp_path, mutate)
    if opts.get('dotenv'):
        (tmp_path / '.env').write_text('GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING\n', encoding='utf-8')
    out = Output()
    rc = main(['run', '--config', str(path), *extra], deps=forbidden_deps(out))
    assert rc != 0
    assert code in out.text
    assert 'Запуск отклонён' in out.text
    assert not (tmp_path / 'state').exists()       # no DB, no lock, no status, no orders


@pytest.mark.parametrize('key', ['gate_live_confirm', 'Gate_Live_Confirm'])
def test_run_refuses_live_confirm_in_env_file_any_case(tmp_path, monkeypatch, key):
    """Regression: on Windows os.environ is case-insensitive, so a lowercase key in .env
    used to reach GATE_LIVE_CONFIRM and pass the shell-only gate. Refused by name, any case."""
    live_env(monkeypatch, confirm=False)
    monkeypatch.setenv(key, 'placeholder')
    monkeypatch.delenv(key)                              # restored (deleted) even if imported
    path = write_config(tmp_path, make_live)
    (tmp_path / '.env').write_text(f'{key}=I_UNDERSTAND_SPOT_TRADING\n', encoding='utf-8')
    out = Output()
    rc = main(['run', '--live', '--config', str(path)], deps=forbidden_deps(out))
    assert rc == 2 and 'LIVE_CONFIRM_IN_ENV_FILE' in out.text
    assert key not in os.environ and 'GATE_LIVE_CONFIRM' not in os.environ
    assert not (tmp_path / 'state').exists()


def test_run_rejects_bad_seconds(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    assert main(['run', '--config', str(path), '--seconds', '-1'], deps=forbidden_deps(out)) == 2
    assert main(['run', '--config', str(path), '--seconds', 'nan'], deps=forbidden_deps(out)) == 2


def test_run_gate_order_mode_first(tmp_path, monkeypatch):
    """Even with everything else wrong, the mode/flag mismatch is reported first."""
    path = write_config(tmp_path, lambda d: d['app'].update(mode='live'))
    cfg = load_config(path, read_env=False)
    assert cli.run_gate_refusal(cfg, False, None)[0] == 'LIVE_CONFIG_REQUIRES_EXPLICIT_RUN_LIVE_FLAG'
    assert cli.run_gate_refusal(cfg, True, None)[0] == 'LIVE_CONFIRM_MISSING'


# ============================================================================ run: paper loop

def paper_deps(out: Output, clock, engine: FakeEngine, hub: FakeHub | None = None, notifier=None) -> Deps:
    hub = hub or FakeHub(clock, {'bybit_perp': Decimal('0.1356')})
    venue = type('V', (), {'closed': 0, 'close': lambda self: setattr(self, 'closed', self.closed + 1)})()
    deps = quiet_deps(out, hub_factory=lambda cfg: hub, meta_factory=lambda cfg, http: pair_meta(),
                      paper_venue_factory=lambda cfg, store, hub_, meta: venue, engine_factory=engine.factory,
                      live_venue_factory=forbid('live venue'), fill_wakeup_factory=forbid('fill wakeup'))
    if notifier is not None:
        deps.notifier_factory = lambda cfg, store: notifier
    deps._hub, deps._venue = hub, venue        # type: ignore[attr-defined]
    return deps


def test_paper_run_for_seconds_writes_status(tmp_path, clock):
    path = write_config(tmp_path)
    engine = FakeEngine()
    out = Output()
    deps = paper_deps(out, clock, engine)        # real Notifier (Telegram disabled: events skipped)
    rc = main(['run', '--config', str(path), '--seconds', '3'], deps=deps)
    assert rc == 0, out.text
    assert engine.calls[0] == 'created' and engine.calls[1] == 'startup' and engine.calls[-1] == 'shutdown'
    assert engine.kwargs['live'] is False and engine.kwargs['meta'].precision == 5
    assert 3 <= engine.steps <= 4                      # every next_step_seconds()=1 s within 3 s
    assert len(engine.preflights) >= 14 and engine.samples >= 14     # every 0.2 s tick
    assert deps._hub.started == 1 and deps._hub.stopped == 1
    assert deps._venue.closed == 1
    status = json.loads((tmp_path / 'state' / 'status.json').read_text(encoding='utf-8'))
    assert status['state'] == 'RUNNING' and status['center'] == '0.13561'
    assert status['process']['mode'] == 'paper' and status['process']['exit_reason'].startswith('истекло --seconds')
    store = Store(tmp_path / 'state' / 'paper.sqlite3')
    try:
        assert store.identity()['mode'] == 'paper'
        keys = [row['key'] for row in store.outbox_rows(10)]
        assert any(k.startswith('started:') for k in keys) and any(k.startswith('stopped:') for k in keys)
        assert store.meta(cli.META_SNAPSHOT_KEY)['precision'] == 5
    finally:
        store.close()


def test_paper_run_budget_wait_and_unsafe(tmp_path, clock):
    path = write_config(tmp_path)
    engine = FakeEngine(script={1: BudgetWait(2.5, 'order'), 3: Unsafe('FOREIGN_ORDER')})
    out = Output()
    rc = main(['run', '--config', str(path), '--seconds', '12'], deps=paper_deps(out, clock, engine,
                                                                                  notifier=FakeNotifier()))
    assert rc == 0
    t = engine.step_times
    assert t[1] - t[0] >= 2.5 - 1e-6                  # BudgetWait: retried after the wait, no stop
    assert engine.stops == ['FOREIGN_ORDER']            # Unsafe escaping a step -> request_stop + preflight
    assert 'unsafe' in engine.preflights
    assert t[3] - t[2] >= 5.0 - 1e-6                  # next step after poll_seconds


def test_paper_run_meta_unavailable_refuses_without_saved_orders(tmp_path, clock):
    path = write_config(tmp_path)
    engine = FakeEngine()
    out = Output()
    deps = paper_deps(out, clock, engine, notifier=FakeNotifier())

    def no_meta(cfg, http):
        raise ConnectionError('https://secret.example/should-not-print')
    deps.meta_factory = no_meta
    assert main(['run', '--config', str(path), '--seconds', '2'], deps=deps) == 2
    assert 'PAIR_METADATA_UNAVAILABLE' in out.text and 'ConnectionError' in out.text
    assert 'secret.example' not in out.text
    assert engine.calls == []


def test_run_lock_held_refuses(tmp_path, clock):
    path = write_config(tmp_path)
    cfg = load_config(path, read_env=False)
    out = Output()
    with ProcessLocks(cfg, live=False):
        rc = main(['run', '--config', str(path), '--seconds', '1'], deps=forbidden_deps(out))
    assert rc == 2 and 'Уже запущен' in out.text


def test_run_identity_mismatch_refuses(tmp_path, clock):
    path = write_config(tmp_path)
    store = Store(tmp_path / 'state' / 'paper.sqlite3')
    store.initialize({'campaign_id': 'other', 'currency_pair': 'STONK_USDT', 'mode': 'paper',
                      'client_prefix': 'stk1', 'account': 'spot'}, 1)
    store.close()
    out = Output()
    engine = FakeEngine()
    assert main(['run', '--config', str(path), '--seconds', '1'],
                deps=paper_deps(out, clock, engine, notifier=FakeNotifier())) == 2
    assert 'DATABASE_IDENTITY_MISMATCH_DO_NOT_RESET' in out.text and engine.calls == []


# ============================================================================ run: live flow (fakes)

def live_deps(out, clock, engine, venue, fill, notifier, hub=None):
    hub = hub or FakeHub(clock, {'bybit_perp': Decimal('0.1356')})
    return quiet_deps(out, hub_factory=lambda cfg: hub, live_venue_factory=lambda cfg, limiter, http: venue,
                      paper_venue_factory=forbid('paper venue'), engine_factory=engine.factory,
                      notifier_factory=lambda cfg, store: notifier, fill_wakeup_factory=lambda cfg, cb: fill)


def test_live_run_startup_order_and_ctrl_c(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch)
    path = write_config(tmp_path, make_live)
    engine = FakeEngine(script={4: KeyboardInterrupt()})
    venue, fill, notifier = FakeLiveVenue(), FakeFill(['fill']), FakeNotifier()
    out = Output()
    rc = main(['run', '--live', '--config', str(path)], deps=live_deps(out, clock, engine, venue, fill, notifier))
    assert rc == 0, out.text
    assert engine.kwargs['live'] is True and engine.kwargs['limiter'] is not None
    assert engine.calls == ['created', 'startup', 'shutdown']
    assert venue.calls[:3] == ['pair_meta', 'account_check', 'server_time']    # checks AFTER startup
    assert fill.started == 1 and fill.stopped == 1 and venue.closed == 1
    assert engine.stops == [] and engine.steps == 4
    assert notifier.events[0][0].startswith('started:') and notifier.events[-1][0].startswith('stopped:')
    assert 'Ctrl+C' in notifier.events[-1][1] and notifier.flushed == [15.0]
    status = json.loads((tmp_path / 'state' / 'status.json').read_text(encoding='utf-8'))
    assert status['process']['mode'] == 'live' and status['process']['exit_reason'] == 'Ctrl+C'
    assert (tmp_path / 'state' / 'live.sqlite3').exists()
    key_lock = cli.key_lock_path(load_config(path, read_env=False))
    assert key_lock.parent == tmp_path / 'tmp' and key_lock.name.startswith('gate_stonk_')
    assert 'key-ABCDEFGH' not in str(key_lock)          # named by a hash, never the key


def test_live_key_lock_blocks_second_instance(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch)
    (tmp_path / 'a').mkdir()
    (tmp_path / 'b').mkdir()
    first = write_config(tmp_path / 'a', make_live)
    second = write_config(tmp_path / 'b', make_live)          # another state_dir, same key and pair
    out = Output()
    with ProcessLocks(load_config(first, read_env=False), live=True):
        rc = main(['run', '--live', '--config', str(second)], deps=forbidden_deps(out))
    assert rc == 2 and 'Уже запущен' in out.text


def test_live_run_account_mismatch_stops_and_exits_after_settle(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch)
    path = write_config(tmp_path, make_live)
    engine = FakeEngine(script={20: AssertionError('should have exited')})
    venue = FakeLiveVenue(problems=['ACCOUNT_MODE_MISMATCH:unified'])
    out = Output()
    rc = main(['run', '--live', '--config', str(path)],
              deps=live_deps(out, clock, engine, venue, FakeFill(), FakeNotifier()))
    assert rc == 2
    assert engine.stops == ['ACCOUNT_MODE_MISMATCH:unified']
    assert engine.calls[:2] == ['created', 'startup'] and engine.calls[-1] == 'shutdown'
    assert engine.steps == 1                     # one reconciliation step, store empty -> exit
    assert 'ACCOUNT_MODE_MISMATCH:unified' in out.text


def test_live_run_clock_skew_is_retried_then_cleared(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch)
    path = write_config(tmp_path, make_live)
    engine = FakeEngine(script={45: KeyboardInterrupt()})
    venue = FakeLiveVenue(skews_ms=[5000, 0])
    out = Output()
    rc = main(['run', '--live', '--config', str(path)],
              deps=live_deps(out, clock, engine, venue, FakeFill(), FakeNotifier()))
    assert rc == 0
    assert engine.stops == ['CLOCK_SKEW'] and engine.cleared == ['CLOCK_SKEW']
    assert venue.calls.count('account_check') == 2


class FlakyMetaVenue(FakeLiveVenue):
    """pair_meta() answers from a script: an exception (outage) or fresh metadata."""

    def __init__(self, metas, **kwargs):
        super().__init__(**kwargs)
        self.metas = list(metas)

    def pair_meta(self):
        self.calls.append('pair_meta')
        item = self.metas.pop(0)
        if isinstance(item, BaseException):
            raise item
        self.meta = item
        return item


def test_live_restart_after_meta_outage_uses_fresh_trade_status(tmp_path, clock, monkeypatch):
    """Regression: after a metadata outage the startup retry fetched fresh metadata but kept
    checking (and trading with) the saved snapshot. The pair became untradable meanwhile:
    the run must be refused with PAIR_NOT_TRADABLE and engine/venue get the fresh meta."""
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch)
    path = write_config(tmp_path, make_live)
    _store_with_known_order(tmp_path, 'live')                 # saved order -> the snapshot may be used
    store = Store(tmp_path / 'state' / 'live.sqlite3')
    store.set_meta(cli.META_SNAPSHOT_KEY, pair_meta().raw)    # snapshot: tradable
    store.close()
    fresh = pair_meta(trade_status='untradable', min_quote_amount='5')
    venue = FlakyMetaVenue([ConnectionError('outage'), fresh])
    engine = FakeEngine(script={6: KeyboardInterrupt()})      # the saved order never settles with the fake
    out = Output()
    main(['run', '--live', '--config', str(path)], deps=live_deps(out, clock, engine, venue, FakeFill(),
                                                                  FakeNotifier()))
    assert engine.kwargs['meta'].trade_status == 'tradable'   # built from the snapshot
    assert engine.stops == ['PAIR_METADATA_UNAVAILABLE', 'PAIR_NOT_TRADABLE:untradable']
    assert engine.cleared == []                                # the stop is never lifted
    assert engine.meta is fresh and venue.meta is fresh       # fresh metadata is authoritative
    assert 'Запуск отклонён: PAIR_NOT_TRADABLE:untradable' in out.text
    assert 'торговля возобновляется' not in out.text
    store = Store(tmp_path / 'state' / 'live.sqlite3')
    try:
        assert store.meta(cli.META_SNAPSHOT_KEY)['trade_status'] == 'untradable'
    finally:
        store.close()


# ============================================================================ run: signals

def _capture_runner(monkeypatch) -> list:
    runners: list = []
    monkeypatch.setattr(cli.Runner, 'install_signals', lambda self: runners.append(self))
    return runners


class SignalEngine(FakeEngine):
    """Delivers SIGTERM through the runner's real handler while a step is inside a
    'venue call' wrapped in a generic handler (as in GateRestClient / venue / engine)."""

    def __init__(self, runners: list, *, at_step: int, guard: type, **kwargs):
        super().__init__(**kwargs)
        self.runners, self.at_step, self.guard = runners, at_step, guard
        self.swallowed: list[str] = []

    def step(self) -> str:
        result = super().step()
        if self.steps == self.at_step:
            try:
                self.runners[0]._on_signal(signal.SIGTERM, None)
            except self.guard as exc:            # noqa: PERF203 - the generic handler under test
                self.swallowed.append(type(exc).__name__)
        return result


@pytest.mark.parametrize('guard,swallowed', [(Exception, []), (BaseException, ['StopRequested'])])
def test_sigterm_inside_venue_call_with_generic_except_still_stops(tmp_path, clock, monkeypatch, guard, swallowed):
    """Regression: StopRequested was an Exception, so `except Exception` in the REST/venue/
    engine code swallowed SIGTERM/SIGBREAK and the bot kept trading. It is a BaseException
    now, and the recorded stop is re-checked every tick even if something swallowed it."""
    assert not issubclass(cli.StopRequested, Exception)
    runners = _capture_runner(monkeypatch)
    path = write_config(tmp_path)
    engine = SignalEngine(runners, at_step=3, guard=guard)
    out = Output()
    rc = main(['run', '--config', str(path), '--seconds', '30'], deps=paper_deps(out, clock, engine,
                                                                                  notifier=FakeNotifier()))
    assert rc == 0, out.text
    assert engine.swallowed == swallowed
    assert engine.steps == 3                                   # no step after the signal
    assert engine.calls[-1] == 'shutdown'
    status = json.loads((tmp_path / 'state' / 'status.json').read_text(encoding='utf-8'))
    assert status['process']['exit_reason'] == 'SIGTERM'


def test_signal_during_shutdown_does_not_abort_the_shutdown_cancels(tmp_path, clock, monkeypatch):
    runners = _capture_runner(monkeypatch)

    class ShutdownSignalEngine(FakeEngine):
        def shutdown(self) -> None:
            runners[0]._on_signal(signal.SIGTERM, None)        # e.g. the console is closed while stopping
            super().shutdown()

    path = write_config(tmp_path)
    engine = ShutdownSignalEngine()
    out = Output()
    rc = main(['run', '--config', str(path), '--seconds', '2'], deps=paper_deps(out, clock, engine,
                                                                                 notifier=FakeNotifier()))
    assert rc == 0, out.text
    assert engine.calls[-1] == 'shutdown'                       # the cancels ran to the end
    status = json.loads((tmp_path / 'state' / 'status.json').read_text(encoding='utf-8'))
    assert status['process']['exit_reason'].startswith('истекло --seconds')


# ============================================================================ cancel-only

def _store_with_known_order(tmp_path: Path, mode: str) -> None:
    store = Store(tmp_path / 'state' / f'{mode}.sqlite3')
    store.initialize({'campaign_id': 'stonk_gate_test', 'currency_pair': 'STONK_USDT', 'mode': mode,
                      'client_prefix': 'stk1', 'account': 'spot'}, 1)
    store.reserve_order(text='t-stk1-b1-abc01', side='buy', level='b1', qty='100', price='0.13', now_ms=2,
                        reason='FIXED_EDGE', index_price='0.135', center_price='0.135')
    store.update_order('t-stk1-b1-abc01', order_id='111', state='open')
    store.close()


def cancel_deps(out, venue, clock):
    return quiet_deps(out, live_venue_factory=lambda cfg, limiter, http: venue, sleep=advance(clock))


def test_cancel_only_live_cancels_own_orders_keeps_foreign(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch, confirm=False)
    path = write_config(tmp_path, lambda d: d['app'].update(mode='live'))
    _store_with_known_order(tmp_path, 'live')
    venue = FakeLiveVenue(opens=[order('111', 't-stk1-b1-abc01'), order('222', 't-stk1-s1-xyz02'),
                                 order('333', 'apiv4')])
    out = Output()
    rc = main(['cancel-only', '--config', str(path)], deps=cancel_deps(out, venue, clock))
    assert rc == 0, out.text
    assert venue.cancel_calls[0] == [('t-stk1-b1-abc01', '111')]      # store rows first
    assert venue.cancel_calls[1] == [('t-stk1-s1-xyz02', '222')]      # then own by text prefix
    assert [o.order_id for o in venue.opens] == ['333']               # foreign order untouched
    assert venue.cancel_all_calls == 0
    assert 'Проверено чтением' in out.text and 'Посторонних открытых заявок по паре: 1' in out.text
    assert 'DMS не затрагивался' in out.text


def test_cancel_only_unverified_times_out(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch, confirm=False)
    path = write_config(tmp_path, lambda d: d['app'].update(mode='live'))
    venue = FakeLiveVenue(opens=[order('222', 't-stk1-s1-xyz02')], cancel_effect=False)
    out = Output()
    start = clock[0]
    rc = main(['cancel-only', '--config', str(path)], deps=cancel_deps(out, venue, clock))
    assert rc == 2
    assert clock[0] - start >= cli.CANCEL_VERIFY_SECONDS
    assert 'НЕ подтверждено' in out.text and '222' in out.text


def test_cancel_only_all_pair_requires_yes(tmp_path, clock, monkeypatch):
    live_env(monkeypatch, confirm=False)
    path = write_config(tmp_path, lambda d: d['app'].update(mode='live'))
    out = Output()
    assert main(['cancel-only', '--all-pair-orders', '--config', str(path)],
                deps=quiet_deps(out, live_venue_factory=forbid('venue'))) == 2
    assert '--yes' in out.text


def test_cancel_only_all_pair_with_yes(tmp_path, clock, monkeypatch):
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))
    live_env(monkeypatch, confirm=False)
    path = write_config(tmp_path, lambda d: d['app'].update(mode='live'))
    venue = FakeLiveVenue(opens=[order('222', 't-stk1-s1-xyz02'), order('333', 'apiv4')])
    out = Output()
    rc = main(['cancel-only', '--all-pair-orders', '--yes', '--config', str(path)],
              deps=cancel_deps(out, venue, clock))
    assert rc == 0 and venue.cancel_all_calls == 1 and venue.opens == []


def test_cancel_only_live_needs_keys(tmp_path):
    path = write_config(tmp_path, lambda d: d['app'].update(mode='live'))
    out = Output()
    assert main(['cancel-only', '--config', str(path)], deps=quiet_deps(out, live_venue_factory=forbid('v'))) == 2
    assert 'GATE_API_KEY' in out.text


def test_cancel_only_paper_without_database(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    assert main(['cancel-only', '--config', str(path)], deps=quiet_deps(out, paper_venue_factory=forbid('v'))) == 0
    assert 'бумажных заявок нет' in out.text


# ============================================================================ console encoding (Windows)

ROOT = Path(__file__).resolve().parents[1]


def test_configure_console_makes_ansi_codepage_streams_safe():
    import io

    from gate_stonk.__main__ import configure_console

    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding='cp1252', errors='strict')
    with pytest.raises(UnicodeEncodeError):
        stream.write('запущен ≤ →')
    configure_console([stream, object(), None])               # objects without reconfigure are skipped
    stream.write('gate_stonk запущен ≤ 1%\n')
    stream.flush()
    assert stream.encoding == 'cp1252' and stream.errors == 'backslashreplace'
    assert raw.getvalue().startswith(b'gate_stonk \\u0437')


def test_python_m_gate_stonk_survives_redirected_ansi_stdout(tmp_path):
    """Regression: with stdout redirected on Windows (ANSI code page, errors='strict') the
    first Russian line raised UnicodeEncodeError and the command exited with code 2."""
    import subprocess
    import sys

    path = write_config(tmp_path)
    env = {k: v for k, v in os.environ.items() if k not in ('PYTHONUTF8', 'PYTHONIOENCODING')}
    env['PYTHONIOENCODING'] = 'cp1252'                        # what a redirected Windows stdout gets
    done = subprocess.run([sys.executable, '-m', 'gate_stonk', 'validate', '--config', str(path)], cwd=str(ROOT),
                          env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    assert done.returncode == 0, done.stderr.decode('ascii', 'replace')
    assert b'\\u041a\\u043e\\u043d\\u0444' in done.stdout          # 'Конф' escaped, not a crash
    assert b'UnicodeEncodeError' not in done.stderr


@pytest.mark.parametrize('name', ['run_stonk.ps1', 'setup_windows.ps1'])
def test_windows_scripts_force_utf8_before_python_and_keep_bom(name):
    raw = (ROOT / name).read_bytes()
    assert raw.startswith(b'\xef\xbb\xbf')                      # UTF-8 BOM for Windows PowerShell 5.1
    text = raw[3:].decode('utf-8')
    utf8 = text.index('$env:PYTHONUTF8 = "1"')
    first_python = min(i for i in (text.find('& $Python'), text.find('py -3')) if i >= 0)
    assert utf8 < first_python


# ============================================================================ status / report

def test_status_missing_and_present(tmp_path, clock):
    path = write_config(tmp_path)
    out = Output()
    assert main(['status', '--config', str(path)], deps=quiet_deps(out)) == 1
    assert 'Статуса ещё нет' in out.text
    write_status(tmp_path / 'state' / 'status.json', {'state': 'RUNNING', 'center': Decimal('0.1')})
    clock[0] += 7.5
    out = Output()
    assert main(['status', '--config', str(path)], deps=quiet_deps(out)) == 0
    assert 'возраст файла' in out.text and 'не свежий запрос' in out.text
    assert '"center": "0.1"' in out.text


def test_report_missing_db_and_json_out(tmp_path):
    path = write_config(tmp_path)
    out = Output()
    assert main(['report', '--config', str(path), '--hours', '24'], deps=quiet_deps(out)) == 1
    assert 'База не найдена' in out.text
    store = Store(tmp_path / 'state' / 'paper.sqlite3')
    store.initialize({'campaign_id': 'stonk_gate_test', 'currency_pair': 'STONK_USDT', 'mode': 'paper',
                      'client_prefix': 'stk1', 'account': 'spot'}, int(time.time() * 1000))
    store.close()
    target = tmp_path / 'report.json'
    out = Output()
    assert main(['report', '--config', str(path), '--hours', '24', '--out', str(target)], deps=quiet_deps(out)) == 0
    assert 'Отчёт' in out.text
    data = json.loads(target.read_text(encoding='utf-8'))
    assert data['currency_pair'] == 'STONK_USDT'
    assert main(['report', '--config', str(path), '--hours', '0'], deps=quiet_deps(Output())) == 2


# ============================================================================ logging

def test_logging_file_and_secret_masking(tmp_path, monkeypatch):
    live_env(monkeypatch, confirm=False)
    path = write_config(tmp_path)
    cfg = load_config(path, read_env=False)
    handlers = cli.configure_logging(cfg, console_level=logging.CRITICAL)
    try:
        logging.getLogger('gate_stonk.test').warning('leak %s and %s', 'key-ABCDEFGH-123456', 'secret-ZYXWVUT-987654')
        try:
            raise RuntimeError('token 12345:token-QWERTY inside')
        except RuntimeError:
            logging.getLogger('gate_stonk.test').exception('boom')
    finally:
        cli.remove_logging(handlers)
    logs = list((tmp_path / 'state' / 'logs').glob('gate_stonk_*.log'))
    assert len(logs) == 1
    text = logs[0].read_text(encoding='utf-8')
    assert 'leak *** and ***' in text and 'boom' in text
    for secret in ('key-ABCDEFGH-123456', 'secret-ZYXWVUT-987654', '12345:token-QWERTY'):
        assert secret not in text
    assert not any(getattr(h, '_gate_stonk', False) for h in logging.getLogger().handlers)


def test_safe_error_hides_foreign_exception_text():
    assert cli.safe_error(ConnectionError('https://x/botSECRET/sendMessage')) == 'ConnectionError'
    assert cli.safe_error(Unsafe('CLOCK_SKEW')) == 'CLOCK_SKEW'


# ============================================================================ integration (real engine)

def test_paper_run_with_real_engine_then_paper_cancel_only(tmp_path, clock):
    """Real Engine + PaperVenue + Notifier wired by the CLI; only the market data hub is fake."""
    path = write_config(tmp_path, lambda d: d['index'].update(min_sources=1), two_sources=True)
    hub = FakeHub(clock, {'bybit_perp': Decimal('0.1356'), 'bitget_perp': Decimal('0.1357')},
                  spot=(Decimal('0.1350'), Decimal('0.1360')))
    out = Output()
    deps = quiet_deps(out, hub_factory=lambda cfg: hub, meta_factory=lambda cfg, http: pair_meta(),
                      live_venue_factory=forbid('live venue'))
    assert main(['run', '--seconds', '8', '--config', str(path)], deps=deps) == 0, out.text
    status = json.loads((tmp_path / 'state' / 'status.json').read_text(encoding='utf-8'))
    assert status['mode'] == 'paper' and status['process']['steps'] >= 2
    store = Store(tmp_path / 'state' / 'paper.sqlite3')
    try:
        rows = store.orders(active_only=False)
        assert {row['level'] for row in rows} >= {'b1', 'b2'}          # BUY ladder placed (no base for SELL)
        assert all(row['cancel_reason'] == 'PROCESS_SHUTDOWN' for row in rows)
    finally:
        store.close()
    out = Output()
    rc = main(['cancel-only', '--config', str(path)],
              deps=quiet_deps(out, sleep=advance(clock), live_venue_factory=forbid('live venue')))
    assert rc == 0, out.text
    assert 'Проверено чтением: открытых заявок бота по паре нет' in out.text


def test_cancel_bot_orders_survives_cancel_errors_and_all_pair_fallback(clock):
    venue = FakeLiveVenue(opens=[order('222', 't-stk1-s1-xyz02'), order('333', 'apiv4')], cancel_effect=False)
    calls = {'n': 0}
    original = venue.cancel_many

    def flaky(reqs):
        calls['n'] += 1
        if calls['n'] == 1:
            raise ConnectionError('https://api.gateio.ws/secret')
        return original(reqs)
    venue.cancel_many = flaky
    summary = cli.cancel_bot_orders(venue, [], 'stk1', all_pair=True, timeout=5, sleep=advance(clock))
    assert summary['results'].get('error') == 1 and summary['last_error'] == 'ConnectionError'
    assert venue.cancel_all_calls == 1 and summary['verified'] is True and venue.opens == []
