"""index-watch: read-only recording of the own index against the Gate spot book (DESIGN §14).

Stage 1 of the test plan: run for ~30 minutes, then read the summary — per-source
spreads and exclusion share, basis of Gate spot to the index and how often the
index is blocked. No keys, no orders, no Telegram.

Every ``index.observation_interval_seconds`` one tick:

* copies the hub snapshots, then samples the clock and runs ``IndexBuilder.compute``
  (same validation as trading);
* reads the traded book (``hub.spot_book()``; used only when ``hub.spot_book_error()``
  is None, i.e. fresh and structurally valid);
* ``basis_pct = (spot_mid / index - 1) * 100`` when both are known;
* writes one row into SQLite table ``watch_obs`` of its OWN database
  ``state_dir/index_watch.sqlite3`` (never the trading database) and one row into a
  wide CSV (per source: ok, reason, mid_usdt, spread_pct, event_age_ms).

Rows are written in batches (about once per second, and at the end). A line is printed
every 10 s; the summary is printed and returned at the end (``--minutes`` elapsed or
Ctrl+C). Statistics use exact decimal histograms (values quantized to 1e-4 %, ages in
ms) so memory stays bounded on a multi-day run.
"""
from __future__ import annotations

import csv
import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Any, Callable, Iterable

from .dec import HUNDRED, ONE, fmt
from .index import IndexBuilder, index_snapshot_to_dict
from .types import IndexSnapshot

LOG = logging.getLogger(__name__)

WATCH_DB_NAME = 'index_watch.sqlite3'
PRINT_EVERY_SECONDS = 10.0
FLUSH_EVERY_SECONDS = 1.0
FLUSH_ROWS = 100
QUANTUM = Decimal('0.0001')

WATCH_DDL = """
CREATE TABLE IF NOT EXISTS watch_obs(
    ts_ms INTEGER NOT NULL, ok INTEGER NOT NULL, reason TEXT, "index" TEXT, center TEXT,
    spot_bid TEXT, spot_ask TEXT, spot_mid TEXT, basis_pct TEXT, n_included INTEGER NOT NULL,
    components TEXT NOT NULL, spot_reason TEXT, fx_rate TEXT);
CREATE INDEX IF NOT EXISTS watch_obs_ts ON watch_obs(ts_ms);
"""
INSERT_SQL = ('INSERT INTO watch_obs(ts_ms, ok, reason, "index", center, spot_bid, spot_ask, spot_mid, basis_pct, '
              'n_included, components, spot_reason, fx_rate) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)')


def _text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, 'f')


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def watch_identity(cfg: Any) -> dict:
    """Identity of the watch database (never equal to a trading identity: mode 'watch')."""
    return {'kind': 'index_watch', 'mode': 'watch', 'currency_pair': cfg.market['currency_pair']}


def default_csv_path(cfg: Any, now_ms: int | None = None) -> Path:
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    stamp = datetime.fromtimestamp(now_ms / 1000, timezone.utc).strftime('%Y%m%d_%H%M%S')
    return Path(cfg.state_dir) / f'index_watch_{stamp}.csv'


def csv_header(source_ids: Iterable[str]) -> list[str]:
    header = ['ts_ms', 'utc', 'ok', 'reason', 'index', 'center', 'n_included', 'fx_rate', 'spot_bid', 'spot_ask',
              'spot_mid', 'spot_spread_pct', 'basis_pct', 'spot_reason']
    for sid in source_ids:
        header += [f'{sid}_ok', f'{sid}_reason', f'{sid}_mid_usdt', f'{sid}_spread_pct', f'{sid}_event_age_ms']
    return header


# ----------------------------------------------------------------------------- statistics

class Histogram:
    """Exact counts of quantized decimal values: mean, nearest-rank percentiles."""

    def __init__(self, quantum: Decimal | None = QUANTUM) -> None:
        self.quantum = quantum
        self.counts: Counter = Counter()
        self.n = 0
        self.total = Decimal(0)

    def add(self, value: Decimal | int) -> None:
        value = Decimal(value)
        if self.quantum is not None:
            value = value.quantize(self.quantum, rounding=ROUND_HALF_EVEN)
        self.counts[value] += 1
        self.n += 1
        self.total += value

    def mean(self) -> Decimal | None:
        return None if not self.n else self.total / self.n

    def percentile(self, q: Decimal | int | str) -> Decimal | None:
        """Nearest rank: the smallest value with cumulative share >= q (0 < q <= 100)."""
        if not self.n:
            return None
        rank = max(1, int((Decimal(q) / HUNDRED * self.n).to_integral_value(rounding='ROUND_CEILING')))
        seen = 0
        for value in sorted(self.counts):
            seen += self.counts[value]
            if seen >= rank:
                return value
        return max(self.counts)

    def median(self) -> Decimal | None:
        return self.percentile(50)

    def summary(self, places: int = 4) -> dict:
        def r(value: Decimal | None) -> str | None:
            return None if value is None else fmt(value.quantize(Decimal(1).scaleb(-places)))
        if not self.n:
            return {'count': 0, 'mean': None, 'median': None, 'p5': None, 'p95': None, 'min': None, 'max': None}
        return {'count': self.n, 'mean': r(self.mean()), 'median': r(self.median()),
                'p5': r(self.percentile(5)), 'p95': r(self.percentile(95)),
                'min': r(min(self.counts)), 'max': r(max(self.counts))}


class _SourceStats:
    def __init__(self, source: Any) -> None:
        self.id = source.id
        self.required = bool(source.required)
        self.ticks = 0
        self.included = 0
        self.reasons: Counter = Counter()
        self.spread = Histogram()
        self.age = Histogram(None)

    def add(self, component: Any, wall_ms: int) -> None:
        self.ticks += 1
        if component.included:
            self.included += 1
        else:
            self.reasons[component.reason or 'UNKNOWN'] += 1
        if component.spread_pct is not None:
            self.spread.add(component.spread_pct)
        if component.event_ms is not None:
            self.age.add(max(0, wall_ms - int(component.event_ms)))

    def summary(self) -> dict:
        share = None if not self.ticks else Decimal(self.included) * HUNDRED / self.ticks
        return {
            'required': self.required, 'ticks': self.ticks, 'valid': self.included,
            'valid_share_pct': None if share is None else fmt(share.quantize(Decimal('0.01'))),
            'excluded_share_pct': None if share is None else fmt((HUNDRED - share).quantize(Decimal('0.01'))),
            'exclusion_reasons': dict(self.reasons.most_common()),
            'spread_median_pct': _q(self.spread.median()), 'spread_p95_pct': _q(self.spread.percentile(95)),
            'spread_samples': self.spread.n,
            'event_age_median_ms': None if not self.age.n else int(self.age.median()),
            'event_age_p95_ms': None if not self.age.n else int(self.age.percentile(95)),
        }


def _q(value: Decimal | None, places: int = 4) -> str | None:
    return None if value is None else fmt(value.quantize(Decimal(1).scaleb(-places)))


class WatchStats:
    """Aggregates ticks into the end-of-run summary."""

    def __init__(self, sources: Iterable[Any]) -> None:
        self.sources = {s.id: _SourceStats(s) for s in sources}
        self.ticks = 0
        self.ok = 0
        self.blocked_reasons: Counter = Counter()
        self.blocked_codes: Counter = Counter()
        self.episodes = 0
        self._last_ok: bool | None = None
        self.basis = Histogram()
        self.spot_spread = Histogram()
        self.spot_valid = 0
        self.spot_reasons: Counter = Counter()
        self.first_ms: int | None = None
        self.last_ms: int | None = None

    def add(self, snapshot: IndexSnapshot, wall_ms: int, spot: dict) -> None:
        self.ticks += 1
        self.first_ms = wall_ms if self.first_ms is None else self.first_ms
        self.last_ms = wall_ms
        if snapshot.ok:
            self.ok += 1
        else:
            reason = snapshot.reason or 'UNKNOWN'
            self.blocked_reasons[reason] += 1
            self.blocked_codes[reason.split(':', 1)[0]] += 1
            if self._last_ok is not False:
                self.episodes += 1
        self._last_ok = snapshot.ok
        for component in snapshot.components:
            stats = self.sources.get(component.source_id)
            if stats is not None:
                stats.add(component, wall_ms)
        if spot.get('mid') is not None:
            self.spot_valid += 1
            if spot.get('spread_pct') is not None:
                self.spot_spread.add(spot['spread_pct'])
        else:
            self.spot_reasons[spot.get('reason') or 'UNKNOWN'] += 1
        if spot.get('basis_pct') is not None:
            self.basis.add(spot['basis_pct'])

    def summary(self, duration_seconds: float) -> dict:
        ok_share = None if not self.ticks else Decimal(self.ok) * HUNDRED / self.ticks
        hours = Decimal(str(max(duration_seconds, 0.0))) / 3600
        per_hour = None if hours <= 0 else (Decimal(self.episodes) / hours).quantize(Decimal('0.01'))
        return {
            'duration_seconds': round(float(duration_seconds), 3),
            'ticks': self.ticks,
            'from_ms': self.first_ms, 'to_ms': self.last_ms,
            'index': {
                'ok_ticks': self.ok,
                'ok_share_pct': None if ok_share is None else fmt(ok_share.quantize(Decimal('0.01'))),
                'blocked_share_pct': None if ok_share is None else fmt((HUNDRED - ok_share).quantize(Decimal('0.01'))),
                'blocked_episodes': self.episodes,
                'blocked_episodes_per_hour': None if per_hour is None else fmt(per_hour),
                'blocking_reasons': dict(self.blocked_reasons.most_common()),
                'blocking_codes': dict(self.blocked_codes.most_common()),
            },
            'sources': {sid: stats.summary() for sid, stats in self.sources.items()},
            'basis_pct': self.basis.summary(),
            'spot': {
                'valid_ticks': self.spot_valid,
                'spread_median_pct': _q(self.spot_spread.median()),
                'spread_p95_pct': _q(self.spot_spread.percentile(95)),
                'invalid_reasons': dict(self.spot_reasons.most_common()),
            },
        }


# ----------------------------------------------------------------------------- recorder

class IndexWatch:
    """One index-watch session: hub -> IndexBuilder -> SQLite + CSV (+ summary)."""

    def __init__(self, cfg: Any, *, hub: Any = None, hub_factory: Callable[[Any], Any] | None = None,
                 store: Any = None, csv_path: str | Path | None = None,
                 out: Callable[[str], None] = print, sleep: Callable[[float], None] | None = None) -> None:
        self.cfg = cfg
        self.sources = cfg.sources()
        self.source_ids = [s.id for s in self.sources]
        self.builder = IndexBuilder(cfg.index_settings())
        self.interval = float(cfg.index['observation_interval_seconds'])
        self._hub = hub
        self._hub_factory = hub_factory
        self._store = store
        self._own_store = store is None
        self.csv_path = Path(csv_path) if csv_path is not None else default_csv_path(cfg)
        self.out = out
        self.sleep = sleep if sleep is not None else (lambda seconds: time.sleep(seconds))
        self.stats = WatchStats(self.sources)
        self._rows: list[tuple] = []
        self._csv_rows: list[list] = []
        self._csv_file: Any = None
        self._csv_writer: Any = None
        self.rows_written = 0
        self.write_errors: Counter = Counter()
        self.last: dict | None = None

    # ----- storage -----
    @property
    def store(self) -> Any:
        return self._store

    def _open(self) -> None:
        if self._store is None:
            from .store import Store
            self._store = Store(Path(self.cfg.state_dir) / WATCH_DB_NAME)
            self._store.initialize(watch_identity(self.cfg), int(time.time() * 1000))
        self._store.ensure_tables(WATCH_DDL)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not self.csv_path.exists() or self.csv_path.stat().st_size == 0
        self._csv_file = open(self.csv_path, 'a', encoding='utf-8', newline='')
        self._csv_writer = csv.writer(self._csv_file)
        if new_file:
            self._csv_writer.writerow(csv_header(self.source_ids))
            self._csv_file.flush()

    def _flush(self) -> None:
        rows, self._rows = self._rows, []
        csv_rows, self._csv_rows = self._csv_rows, []
        if rows:
            try:
                with self._store.write() as db:
                    db.executemany(INSERT_SQL, rows)
                self.rows_written += len(rows)
            except Exception as exc:
                self.write_errors['sqlite:' + type(exc).__name__] += 1
                LOG.warning('index-watch SQLite write failed: %s', type(exc).__name__)
        if csv_rows and self._csv_writer is not None:
            try:
                self._csv_writer.writerows(csv_rows)
                self._csv_file.flush()
            except Exception as exc:
                self.write_errors['csv:' + type(exc).__name__] += 1
                LOG.warning('index-watch CSV write failed: %s', type(exc).__name__)

    def _close(self) -> None:
        try:
            self._flush()
        finally:
            if self._csv_file is not None:
                try:
                    self._csv_file.close()
                except Exception:
                    pass
                self._csv_file = None
            if self._own_store and self._store is not None:
                try:
                    self._store.close()
                except Exception:
                    pass

    # ----- one tick -----
    def tick(self, hub: Any) -> dict:
        snapshots = {sid: hub.source_snapshot(sid) for sid in self.source_ids}
        fx = hub.fx_snapshot()
        book = hub.spot_book()
        spot_error = hub.spot_book_error()
        now, wall_ms = time.monotonic(), int(time.time() * 1000)   # sampled after copying snapshots
        snapshot = self.builder.compute(snapshots, fx, now, wall_ms)
        spot: dict[str, Any] = {'bid': None, 'ask': None, 'mid': None, 'spread_pct': None, 'basis_pct': None,
                                'reason': spot_error}
        if book is not None and spot_error is None:
            try:
                spot.update(bid=book.bid, ask=book.ask, mid=book.mid, spread_pct=book.spread_pct)
            except Exception as exc:
                spot['reason'] = f'SPOT_BOOK_INVALID:{type(exc).__name__}'
        elif book is None and spot_error is None:
            spot['reason'] = 'NO_DATA'
        if snapshot.ok and snapshot.index and spot['mid'] is not None:
            spot['basis_pct'] = (spot['mid'] / snapshot.index - ONE) * HUNDRED
        self.stats.add(snapshot, wall_ms, spot)
        components = index_snapshot_to_dict(snapshot)['components']
        self._rows.append((
            wall_ms, 1 if snapshot.ok else 0, snapshot.reason, _text(snapshot.index), _text(snapshot.center),
            _text(spot['bid']), _text(spot['ask']), _text(spot['mid']), _text(spot['basis_pct']),
            int(snapshot.n_included), json.dumps(components, ensure_ascii=False, separators=(',', ':')),
            spot['reason'], _text(snapshot.fx_rate)))
        row = [wall_ms, _utc(wall_ms), int(snapshot.ok), snapshot.reason or '', _text(snapshot.index) or '',
               _text(snapshot.center) or '', snapshot.n_included, _text(snapshot.fx_rate) or '',
               _text(spot['bid']) or '', _text(spot['ask']) or '', _text(spot['mid']) or '',
               _text(spot['spread_pct']) or '', _text(spot['basis_pct']) or '', spot['reason'] or '']
        by_id = {c.source_id: c for c in snapshot.components}
        for sid in self.source_ids:
            c = by_id.get(sid)
            if c is None:
                row += [0, 'MISSING_COMPONENT', '', '', '']
                continue
            age = '' if c.event_ms is None else max(0, wall_ms - int(c.event_ms))
            row += [int(c.included), c.reason or '', _text(c.usdt_price) or '', _text(c.spread_pct) or '', age]
        self._csv_rows.append(row)
        self.last = {'snapshot': snapshot, 'spot': spot, 'wall_ms': wall_ms}
        return self.last

    def progress_line(self) -> str:
        if self.last is None:
            return 'index-watch: ещё нет наблюдений'
        snapshot, spot, wall_ms = self.last['snapshot'], self.last['spot'], self.last['wall_ms']
        total = len(self.source_ids)
        if snapshot.ok:
            head = (f'индекс {fmt(snapshot.index, 8)} (центр {fmt(snapshot.center, 8)}), '
                    f'источников {snapshot.n_included}/{total}')
        else:
            head = f'ИНДЕКС ЗАБЛОКИРОВАН: {snapshot.reason}'
        if spot['mid'] is not None:
            spot_text = f"спот {fmt(spot['bid'])}/{fmt(spot['ask'])}"
            if spot['basis_pct'] is not None:
                spot_text += f", базис {fmt(spot['basis_pct'], 3)}%"
        else:
            spot_text = f"спот недоступен ({spot['reason']})"
        excluded = [f'{c.source_id}={c.reason}' for c in snapshot.components if not c.included]
        tail = f"; исключены: {', '.join(excluded)}" if excluded else ''
        ok_share = self.stats.ok * 100 / self.stats.ticks if self.stats.ticks else 0.0
        return (f"{_utc(wall_ms)[11:]} {head}; {spot_text}{tail} "
                f"[тиков {self.stats.ticks}, индекс валиден {ok_share:.1f}%]")

    # ----- session -----
    def run(self, minutes: float | None = None, stop: Any = None) -> dict:
        """Record until ``minutes`` elapse (None: until Ctrl+C / ``stop`` event). Returns the summary."""
        if minutes is not None and not minutes > 0:
            raise ValueError('minutes must be positive')
        hub = self._hub if self._hub is not None else (self._hub_factory or _default_hub)(self.cfg)
        self._open()
        start = time.monotonic()
        interrupted = False
        try:
            self.out(f'index-watch {self.cfg.market["currency_pair"]}: запись каждые {self.interval:g} с в '
                     f'{Path(self._store.path) if self._store is not None else "?"} и {self.csv_path}. '
                     f'Только чтение: заявки не отправляются.' + ('' if minutes else ' Остановка: Ctrl+C.'))
            hub.start()
            next_tick = start
            next_flush = start + FLUSH_EVERY_SECONDS
            next_print = start + PRINT_EVERY_SECONDS
            while True:
                now = time.monotonic()
                if minutes is not None and now - start >= minutes * 60:
                    break
                if stop is not None and stop.is_set():
                    break
                if now >= next_tick:
                    try:
                        self.tick(hub)
                    except Exception as exc:   # one bad tick never ends the recording
                        self.write_errors['tick:' + type(exc).__name__] += 1
                        LOG.warning('index-watch tick failed: %s', type(exc).__name__)
                    next_tick += self.interval
                    if next_tick <= now:
                        next_tick = now + self.interval
                if now >= next_flush or len(self._rows) >= FLUSH_ROWS:
                    self._flush()
                    next_flush = now + FLUSH_EVERY_SECONDS
                if now >= next_print:
                    self.out(self.progress_line())
                    next_print = now + PRINT_EVERY_SECONDS
                wake_at = min(next_tick, next_flush, next_print)
                if minutes is not None:
                    wake_at = min(wake_at, start + minutes * 60)
                self.sleep(max(0.0, wake_at - time.monotonic()))
        except KeyboardInterrupt:
            interrupted = True
        finally:
            try:
                hub.stop()
            except Exception as exc:
                LOG.warning('index-watch hub stop failed: %s', type(exc).__name__)
            self._close()
        summary = self.stats.summary(time.monotonic() - start)
        summary.update(csv_path=str(self.csv_path),
                       db_path=None if self._store is None else str(self._store.path),
                       rows_written=self.rows_written, write_errors=dict(self.write_errors),
                       interrupted=interrupted)
        for line in format_summary_ru(summary):
            self.out(line)
        return summary


def _default_hub(cfg: Any) -> Any:
    from .feeds import build_hub
    return build_hub(cfg)


def run_index_watch(cfg: Any, *, minutes: float | None = None, csv_path: str | Path | None = None,
                    hub: Any = None, hub_factory: Callable[[Any], Any] | None = None, store: Any = None,
                    out: Callable[[str], None] = print, sleep: Callable[[float], None] | None = None,
                    stop: Any = None) -> dict:
    watch = IndexWatch(cfg, hub=hub, hub_factory=hub_factory, store=store, csv_path=csv_path, out=out, sleep=sleep)
    return watch.run(minutes, stop=stop)


# ----------------------------------------------------------------------------- report text

def _reasons(mapping: dict, limit: int = 5) -> str:
    items = list(mapping.items())[:limit]
    text = ', '.join(f'{key} ×{count}' for key, count in items)
    if len(mapping) > limit:
        text += f' и ещё {len(mapping) - limit}'
    return text or 'нет'


def format_summary_ru(summary: dict) -> list[str]:
    idx = summary['index']
    lines = [
        '',
        f"Итог index-watch: {summary['duration_seconds']:.0f} с, тиков {summary['ticks']}"
        + (' (остановлено Ctrl+C)' if summary.get('interrupted') else '') + '.',
        f"Индекс валиден {idx['ok_share_pct'] or 'н/д'}% времени, заблокирован {idx['blocked_share_pct'] or 'н/д'}%; "
        f"эпизодов блокировки {idx['blocked_episodes']} (в час: {idx['blocked_episodes_per_hour'] or 'н/д'}).",
        f"Причины блокировки: {_reasons(idx['blocking_reasons'])}.",
        'Источники:',
    ]
    for sid, row in summary['sources'].items():
        flag = ' (обязательный)' if row['required'] else ''
        lines.append(
            f"  {sid}{flag}: валиден {row['valid_share_pct'] or 'н/д'}% (исключён {row['excluded_share_pct'] or 'н/д'}%), "
            f"спред медиана {row['spread_median_pct'] or 'н/д'}% / p95 {row['spread_p95_pct'] or 'н/д'}%, "
            f"возраст события медиана {row['event_age_median_ms'] if row['event_age_median_ms'] is not None else 'н/д'} мс; "
            f"причины исключения: {_reasons(row['exclusion_reasons'])}.")
    basis = summary['basis_pct']
    if basis['count']:
        lines.append(f"Базис спота Gate к индексу, %: среднее {basis['mean']}, медиана {basis['median']}, "
                     f"p5 {basis['p5']}, p95 {basis['p95']} (мин {basis['min']}, макс {basis['max']}, "
                     f"наблюдений {basis['count']}).")
    else:
        lines.append('Базис спота Gate к индексу: нет наблюдений (индекс или спот недоступны).')
    spot = summary['spot']
    lines.append(f"Спред спота Gate: медиана {spot['spread_median_pct'] or 'н/д'}%, p95 {spot['spread_p95_pct'] or 'н/д'}%; "
                 f"валидных тиков {spot['valid_ticks']}; причины недоступности: {_reasons(spot['invalid_reasons'])}.")
    if summary.get('write_errors'):
        lines.append(f"Ошибки записи: {_reasons(summary['write_errors'])}.")
    lines.append(f"Записано строк: {summary.get('rows_written', 0)}; CSV: {summary.get('csv_path')}; "
                 f"SQLite: {summary.get('db_path')} (таблица watch_obs).")
    return lines
