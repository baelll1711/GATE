"""Offline simulation on a SYNTHETIC market (``python -m gate_stonk simulate``).

The real Engine, PaperVenue, FeedHub (no threads, data via ``publish``), IndexBuilder,
Store, Notifier (Telegram off) and the real ``cli.Runner`` loop run on a virtual clock
against generated data — synthetic market, not real data. One 3-minute run with forced
sweeps (calm market, no random jumps/sweeps/dropouts so the forced events always meet
resting orders) is shared by most tests.
"""
from __future__ import annotations

import json
import socket
import sqlite3
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from cli_fakes import Output, write_config
from gate_stonk import simulate as S
from gate_stonk.cli import EXIT_ERROR, EXIT_OK, Deps, main
from gate_stonk.config import load_config
from gate_stonk.dec import D
from gate_stonk.pricing import edge_pct

MINUTES = 3
SEED = 7
SELL_SWEEP_AT, BUY_SWEEP_AT = 60.0, 150.0
CALM = S.MarketParams(volatility_pct_per_hour=0.3, jumps_per_hour=0, sweeps_per_hour=0, source_dropouts=False,
                      forced_sweeps=((SELL_SWEEP_AT, 'sell', 2.6), (BUY_SWEEP_AT, 'buy', 2.6)))
LEVEL_ORDER = {'buy': ['b1', 'b2'], 'sell': ['s1', 's2']}


def _offline_guard(monkeypatch) -> None:
    def blocked(*args, **kwargs):
        raise RuntimeError('network access is blocked in tests')

    monkeypatch.setattr(socket.socket, 'connect', blocked)
    monkeypatch.setattr(socket, 'create_connection', blocked)
    monkeypatch.setattr(socket, 'getaddrinfo', blocked)


@pytest.fixture(scope='module')
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp('simulation')
    cfg = load_config(write_config(tmp))
    out = Output()
    clocks = (time.time, time.monotonic, time.time_ns)
    with pytest.MonkeyPatch.context() as mp:
        _offline_guard(mp)
        result = S.run_simulation(cfg, minutes=MINUTES, seed=SEED, params=CALM,
                                  deps=Deps(out=out, err=out, logging=False, signals=False))
    return SimpleNamespace(result=result, out=out, cfg=cfg, clocks=clocks, tmp=tmp)


def _orders(path: Path) -> list[dict]:
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in db.execute('SELECT * FROM orders ORDER BY created_ms, rowid')]
    finally:
        db.close()


def test_runs_the_real_stack_without_escaping_exceptions(run):
    result = run.result
    assert result.run_exit_code == EXIT_OK and result.exit_code == EXIT_OK
    assert result.exit_reason == f'истекло --seconds {MINUTES * 60:g}'          # the loop ended by time, no error
    summary = result.summary
    assert summary['synthetic_market'] is True and summary['note'] == S.SYNTHETIC_RU
    assert summary['engine_errors'] == {}                       # no diagnostic helper failed either
    assert summary['engine_stop_reason'] == 'PROCESS_SHUTDOWN'  # normal shutdown cancel, not an Unsafe stop
    counters = summary['engine_counters']
    assert counters['steps'] >= MINUTES * 60 / 5 and counters['orders_sent'] >= 4
    assert summary['market'].get('publish_rejected', 0) == 0    # every synthetic update passed hub validation
    assert summary['market']['sweep'] == 2
    assert result.db_path == result.out_dir / 'paper.sqlite3'
    assert result.out_dir == (Path(run.cfg.state_dir) / f'simulation_{SEED}').resolve()
    assert not (Path(run.cfg.state_dir) / 'paper.sqlite3').exists()     # never the trading database


def test_virtual_clock_is_restored(run):
    assert (time.time, time.monotonic, time.time_ns) == run.clocks
    virtual = run.result.summary['virtual_end_utc']
    assert virtual == '2026-10-12 09:03:00 UTC'


def test_orders_are_created_at_least_min_new_edge_from_the_center(run):
    rows = _orders(run.result.db_path)
    assert len(rows) >= 4
    min_new = D(run.cfg.safety['min_new_edge_pct'])
    for row in rows:
        assert edge_pct(row['side'], D(row['price']), D(row['center_price'])) >= min_new, row['text']
    audit = run.result.summary['placement_audit']
    assert audit['below_min_new_edge_count'] == 0 and audit['min_edge_at_placement_pct'] >= min_new


def test_ladder_is_monotone(run):
    rows = _orders(run.result.db_path)
    passes: dict[tuple[int, str], dict[str, Decimal]] = {}
    for row in rows:
        passes.setdefault((row['created_ms'], row['side']), {})[row['level']] = D(row['price'])
    assert any(len(levels) == 2 for levels in passes.values())     # both levels created in one pass
    for (_created, side), levels in passes.items():
        prices = [levels[level] for level in LEVEL_ORDER[side] if level in levels]
        assert prices == sorted(prices, reverse=(side == 'buy')) and len(set(prices)) == len(prices)
    invariants = run.result.summary['invariants']
    assert 'LADDER_NOT_MONOTONE' not in invariants['violations']
    assert 'OWN_ORDERS_SELF_CROSS' not in invariants['violations']
    assert run.result.summary['placement_audit']['same_pass_non_monotone_count'] == 0


def test_forced_sweeps_fill_resting_orders(run):
    report = run.result.report
    slots = report['slots']
    assert slots['buy:b1']['fills'] >= 1 and slots['buy:b2']['fills'] >= 1      # the 2.6 % SELL sweep
    assert slots['sell:s1']['fills'] >= 1                                         # the BUY sweep after inventory
    db = sqlite3.connect(str(run.result.db_path))
    try:
        fills = db.execute('SELECT side, time_ms FROM fills ORDER BY time_ms').fetchall()
    finally:
        db.close()
    start = S.VIRTUAL_START_MS
    assert all(ms >= start + SELL_SWEEP_AT * 1000 for _side, ms in fills)
    assert any(side == 'sell' and ms >= start + BUY_SWEEP_AT * 1000 for side, ms in fills)
    totals = report['totals']
    assert totals['fills'] >= 3 and totals['realized_usdt'] > 0                  # bought -1 %, sold +1 %
    assert slots['buy:b1']['avg_edge_of_fills_vs_index_at_fill_pct'] > 0         # spot-only sweep: index did not move


def test_report_has_per_level_rows_and_says_synthetic(run):
    report = run.result.report
    assert list(report['slots']) == ['buy:b1', 'buy:b2', 'sell:s1', 'sell:s2']
    text = run.result.text
    for slot in report['slots']:
        assert any(line.startswith(slot) for line in text.splitlines()), slot
    assert S.SYNTHETIC_RU in text.splitlines()[0] and S.SYNTHETIC_RU in text.splitlines()[-1]
    saved = json.loads(run.result.report_path.read_text(encoding='utf-8'))
    assert saved['simulation']['synthetic_market'] is True and saved['simulation']['note'] == S.SYNTHETIC_RU
    assert saved['slots']['buy:b1']['fills'] == report['slots']['buy:b1']['fills']
    assert any(S.SYNTHETIC_RU in line for line in run.out.lines)                # banner printed before the run


def test_status_json_is_json_safe_and_marked_synthetic(run):
    text = run.result.status_path.read_text(encoding='utf-8')
    status = json.loads(text)
    json.dumps(status, allow_nan=False)
    assert status['synthetic_market'] is True and S.SYNTHETIC_RU in status['warning']
    assert status['mode'] == 'paper' and status['process']['simulation']['seed'] == SEED
    assert status['process']['exit_reason'] == run.result.exit_reason
    assert set(status['slots']) == {'buy:b1', 'buy:b2', 'sell:s1', 'sell:s2'}
    assert status['feeds']['gate_perp']['error'] == 'WIDE_SOURCE_SPREAD'            # 50 % spread drops out
    assert status['index']['ok'] in (True, False) and status['errors'] == {}


def test_invariants_hold(run):
    invariants = run.result.summary['invariants']
    assert invariants['violations'] == {}
    assert invariants['checks']['ticks'] > 500 and invariants['checks']['preflight_checks'] > 500
    assert invariants['checks']['reservation_checks'] == invariants['checks']['ticks']
    rows = _orders(run.result.db_path)
    active = [row for row in rows if row['state'] in S.ACTIVE_STATES]
    assert len({(row['side'], row['level']) for row in active}) == len(active)   # one active order per slot
    db = sqlite3.connect(str(run.result.db_path))
    try:
        opens = [text for (text,) in db.execute("SELECT text FROM paper_orders WHERE status='open'")]
    finally:
        db.close()
    assert opens == []                                                             # shutdown cancelled everything


def test_invariant_checker_detects_violations(tmp_path, monkeypatch):
    """The checker is not vacuous: two open paper orders of one slot and a BUY reservation
    above the USDT balance are reported."""
    _offline_guard(monkeypatch)
    cfg = load_config(write_config(tmp_path))
    out_dir = tmp_path / 'sim'
    out_dir.mkdir()
    sim_cfg = S.simulation_config(cfg, out_dir, 1)
    clock = S.VirtualClock()
    with clock.installed():
        sim = S.Simulation(sim_cfg, clock, minutes=1, seed=1, params=S.MarketParams(),
                           deps=Deps(out=Output(), logging=False, signals=False))
        sim.runner.build()
        store = sim.runner.store
        try:
            now = clock.wall_ms()
            with store.write() as db:
                for n in (1, 2):
                    db.execute("INSERT INTO paper_orders(order_id,text,side,time_in_force,price,amount,left_amount,"
                               "status,finish_as,create_time_ms,update_time_ms,placement_seq,priority_ms,"
                               "priority_rank) VALUES(?,?,'buy','poc','0.13','10','10','open','open',?,?,0,?,?)",
                               (900 + n, f't-stk1-b1-zz{n}', now, now, now, n))
            store.reserve_order(text='t-stk1-b2-zz3', side='buy', level='b2', qty=D('10000'), price=D('0.13'),
                                now_ms=now, reason='TEST', index_price=D('0.1359'), center_price=D('0.1359'))
            sim._check_invariants()
        finally:
            store.close()
    assert sim.violations['TWO_OPEN_ORDERS_PER_SLOT_VENUE'] == 1
    assert sim.violations['BUY_RESERVATIONS_EXCEED_USDT_TOTAL'] == 1


def test_same_seed_is_deterministic(tmp_path, monkeypatch):
    _offline_guard(monkeypatch)
    cfg = load_config(write_config(tmp_path))
    params = S.MarketParams(sweeps_per_hour=120, jumps_per_hour=60)
    results = [S.run_simulation(cfg, minutes=1, seed=11, out_dir=tmp_path / f'run{n}', params=params,
                                deps=Deps(out=Output(), logging=False, signals=False)) for n in (1, 2)]
    first, second = (json.loads(r.report_path.read_text(encoding='utf-8')) for r in results)
    for report in (first, second):
        report['simulation'].pop('wall_seconds')
        report['simulation'].pop('pair_meta')
    assert first == second
    assert [row['text'] for row in _orders(results[0].db_path)] == [row['text'] for row in _orders(results[1].db_path)]


def test_cli_simulate_command(tmp_path, monkeypatch):
    _offline_guard(monkeypatch)
    path = write_config(tmp_path)
    out = Output()
    target = tmp_path / 'sim_out'
    code = main(['simulate', '--config', str(path), '--minutes', '0.5', '--seed', '3', '--state-dir', str(target)],
                deps=Deps(out=out, err=out, logging=False, signals=False))
    assert code == EXIT_OK, out.text
    assert S.SYNTHETIC_RU in out.text and 'Слот' in out.text
    assert (target / 'status.json').is_file() and (target / 'report.json').is_file()
    assert (target / S.MARKER_NAME).is_file()
    # A second run into the same (marked) folder starts from a fresh simulation database.
    code = main(['simulate', '--config', str(path), '--minutes', '0.2', '--seed', '3', '--state-dir', str(target)],
                deps=Deps(out=out, err=out, logging=False, signals=False))
    assert code == EXIT_OK
    report = json.loads((target / 'report.json').read_text(encoding='utf-8'))
    assert report['simulation']['minutes'] == 0.2


def test_cli_refuses_trading_or_foreign_directories(tmp_path, monkeypatch):
    _offline_guard(monkeypatch)
    path = write_config(tmp_path)
    cfg = load_config(path)
    out = Output()
    deps = Deps(out=out, err=out, logging=False, signals=False)
    code = main(['simulate', '--config', str(path), '--minutes', '0.1', '--state-dir', str(cfg.state_dir)], deps=deps)
    assert code == EXIT_ERROR and 'совпадает с app.state_dir' in out.text
    foreign = tmp_path / 'foreign'
    foreign.mkdir()
    (foreign / 'paper.sqlite3').write_text('not a simulation', encoding='utf-8')
    code = main(['simulate', '--config', str(path), '--minutes', '0.1', '--state-dir', str(foreign)], deps=deps)
    assert code == EXIT_ERROR and S.MARKER_NAME in out.text
    assert (foreign / 'paper.sqlite3').read_text(encoding='utf-8') == 'not a simulation'
    code = main(['simulate', '--config', str(path), '--minutes', '0'], deps=deps)
    assert code == EXIT_ERROR and '--minutes' in out.text


def test_virtual_clock_patches_and_restores_on_error():
    clock = S.VirtualClock(start_wall_ms=1_000_000, start_monotonic=50.0)
    saved = (time.time, time.monotonic, time.time_ns)
    with pytest.raises(ZeroDivisionError):
        with clock.installed():
            assert time.time() == 1000.0 and time.monotonic() == 50.0
            clock.set_us(1_500_000)
            assert time.time() == 1001.5 and time.time_ns() == 1_001_500_000_000 and clock.wall_ms() == 1_001_500
            1 / 0
    assert (time.time, time.monotonic, time.time_ns) == saved
    with pytest.raises(ValueError):
        clock.set_us(0)
