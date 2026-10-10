"""index-watch: SQLite + CSV rows and summary statistics from synthetic snapshots (fake hub, fake clock)."""
from __future__ import annotations

import csv
import json
import sqlite3
from decimal import Decimal

import pytest

from gate_stonk.cli import Deps, main
from gate_stonk.config import load_config
from gate_stonk.watch import WATCH_DB_NAME, Histogram, IndexWatch, csv_header

from cli_fakes import FakeHub, Output, advance, write_config


def watch_config(tmp_path):
    def mutate(data):
        data['index']['min_sources'] = 1
    return write_config(tmp_path, mutate, two_sources=True)


def scripted_hub(clock):
    """bybit (required, weight 0.6) = 100 except stale for t in [1.0, 1.6); bitget = 100.2 except
    disconnected for t >= 2.0; spot 100.9/101.1 (mid 101) except missing for t < 0.4."""
    def bybit(t):
        return 'stale' if 1.0 <= t < 1.6 else Decimal('100')

    def bitget(t):
        return None if t >= 2.0 else Decimal('100.2')

    def spot(t):
        return None if t < 0.4 else (Decimal('100.9'), Decimal('101.1'))
    return FakeHub(clock, {'bybit_perp': bybit, 'bitget_perp': bitget}, spot=spot)


def test_index_watch_records_sqlite_csv_and_summary(tmp_path, clock):
    path = watch_config(tmp_path)
    csv_path = tmp_path / 'out' / 'watch.csv'
    out = Output()
    hub = scripted_hub(clock)
    rc = main(['index-watch', '--minutes', '0.05', '--csv', str(csv_path), '--config', str(path)],
              deps=Deps(out=out, err=out, hub_factory=lambda cfg: hub, sleep=advance(clock), logging=False))
    assert rc == 0, out.text
    assert hub.started == 1 and hub.stopped == 1

    db = sqlite3.connect(str(tmp_path / 'state' / WATCH_DB_NAME))
    db.row_factory = sqlite3.Row
    rows = db.execute('SELECT * FROM watch_obs ORDER BY ts_ms').fetchall()
    identity = json.loads(db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()[0])
    db.close()
    assert identity['mode'] == 'watch'
    assert not (tmp_path / 'state' / 'paper.sqlite3').exists()      # never the trading database
    assert len(rows) == 15                                           # 3 s every 0.2 s
    first = rows[0]
    assert first['ok'] == 1 and Decimal(first['index']) == 100 and first['spot_mid'] is None
    assert first['spot_reason'] == 'NO_DATA' and first['n_included'] == 2
    components = json.loads(first['components'])
    assert [c['source_id'] for c in components] == ['bybit_perp', 'bitget_perp']
    ok_with_spot = [r for r in rows if r['ok'] and r['spot_mid'] is not None]
    assert ok_with_spot and all(Decimal(r['basis_pct']) == Decimal('1') for r in ok_with_spot)
    blocked = [r for r in rows if not r['ok']]
    assert blocked and all(r['index'] is None and r['basis_pct'] is None for r in blocked)
    assert any(r['reason'] == 'REQUIRED_SOURCE_INVALID:bybit_perp:STALE_QUOTE' for r in blocked)
    assert any(r['reason'].startswith('REQUIRED_SOURCE_INVALID:bybit_perp:REJOIN_PENDING') for r in blocked)

    with open(csv_path, encoding='utf-8', newline='') as handle:
        table = list(csv.reader(handle))
    header = table[0]
    assert header == csv_header(['bybit_perp', 'bitget_perp'])
    assert len(table) == 1 + len(rows)
    data = [dict(zip(header, row)) for row in table[1:]]
    assert data[0]['bybit_perp_ok'] == '1' and Decimal(data[0]['bybit_perp_mid_usdt']) == 100
    assert Decimal(data[0]['bybit_perp_spread_pct']) == Decimal('0.1') and data[0]['bybit_perp_event_age_ms'] == '0'
    late = data[-1]
    assert late['bitget_perp_ok'] == '0' and late['bitget_perp_reason'].startswith('MISSING_SOURCE:bitget_perp')

    text = out.text
    assert 'Итог index-watch' in text and 'тиков 15' in text
    assert 'Причины блокировки: REQUIRED_SOURCE_INVALID:bybit_perp' in text
    assert 'bybit_perp (обязательный): валиден' in text
    assert 'Базис спота Gate к индексу, %: среднее 1, медиана 1' in text
    assert 'Спред спота Gate: медиана 0.198' in text


def test_index_watch_summary_numbers(tmp_path, clock):
    cfg = load_config(watch_config(tmp_path), read_env=False)
    out = Output()
    watch = IndexWatch(cfg, hub=scripted_hub(clock), csv_path=tmp_path / 'w.csv', out=out, sleep=advance(clock))
    summary = watch.run(0.05)
    assert summary['ticks'] == 15 and summary['rows_written'] == 15
    idx = summary['index']
    # blocked: bybit stale for 3 ticks (1.0, 1.2, 1.4) + 2 rejoin ticks (1.6, 1.8): 5 of 15
    assert idx['ok_ticks'] == 10 and idx['blocked_share_pct'] == '33.33'
    assert idx['blocked_episodes'] == 1
    assert idx['blocking_codes'] == {'REQUIRED_SOURCE_INVALID': 5}
    bybit = summary['sources']['bybit_perp']
    assert bybit['valid'] == 10 and bybit['exclusion_reasons']['STALE_QUOTE'] == 3
    assert bybit['spread_median_pct'] == '0.1' and bybit['event_age_median_ms'] == 0
    bitget = summary['sources']['bitget_perp']
    assert bitget['valid'] == 10 and bitget['excluded_share_pct'] == '33.33'
    assert set(bitget['exclusion_reasons']) == {'MISSING_SOURCE:bitget_perp:DISCONNECTED'}
    basis = summary['basis_pct']
    assert basis['count'] == 8 and basis['median'] == '1' and basis['p95'] == '1' and basis['mean'] == '1'
    assert summary['spot']['valid_ticks'] == 13 and summary['spot']['invalid_reasons'] == {'NO_DATA': 2}
    assert summary['interrupted'] is False


def test_index_watch_ctrl_c_still_summarizes(tmp_path, clock):
    cfg = load_config(watch_config(tmp_path), read_env=False)
    calls = {'n': 0}
    sleep = advance(clock)

    def interrupting_sleep(seconds):
        calls['n'] += 1
        if calls['n'] == 6:
            raise KeyboardInterrupt
        sleep(seconds)
    out = Output()
    hub = scripted_hub(clock)
    summary = IndexWatch(cfg, hub=hub, csv_path=tmp_path / 'w.csv', out=out, sleep=interrupting_sleep).run(None)
    assert summary['interrupted'] is True and summary['ticks'] >= 3
    assert summary['rows_written'] == summary['ticks'] and hub.stopped == 1
    assert 'остановлено Ctrl+C' in out.text


def test_histogram_nearest_rank():
    hist = Histogram()
    for value in ('0.1', '0.2', '0.3', '0.4', '10'):
        hist.add(Decimal(value))
    assert hist.median() == Decimal('0.3000')
    assert hist.percentile(95) == Decimal('10.0000')
    assert hist.percentile(5) == Decimal('0.1000')
    assert hist.mean() == Decimal('2.2')
    assert Histogram().summary()['count'] == 0


def test_index_watch_rejects_bad_minutes(tmp_path):
    out = Output()
    path = watch_config(tmp_path)
    assert main(['index-watch', '--minutes', '-1', '--config', str(path)], deps=Deps(out=out, err=out,
                                                                                     logging=False)) == 2
