"""Command-line entry points: ``python -m gate_stonk <command> --config config.yaml`` (DESIGN §14, §18).

Commands: ``validate``, ``doctor``, ``index-watch``, ``run``, ``cancel-only``, ``status``,
``report``, ``simulate`` (offline run of the real engine on a SYNTHETIC market, see
:mod:`gate_stonk.simulate`). ``--config`` is accepted before or after the command. Human
output is Russian.

Exit codes: 0 success; 1 invalid configuration, failed doctor check, missing status/database;
2 refusal or error (live gates, lock held, unverified cancel-only, unexpected exception);
130 interrupted before a command could clean up.

``run`` ports the reference runner (notes/ref_config_cli_ops.md §2.6-2.8):

1. local gates BEFORE any network or file (binding order): ``app.mode: live`` <=> ``--live``;
   ``--seconds`` is paper only; ``GATE_LIVE_CONFIRM`` must not be defined in ``.env`` and must
   equal ``I_UNDERSTAND_SPOT_TRADING`` in the shell; ``safety.dedicated_account_ack``;
   ``safety.dead_man_switch``; API keys; ``require_telegram`` -> Telegram enabled with
   token/chat; iceberg levels need ``execution.iceberg_poc_verified``;
2. logging (``state_dir/logs/gate_stonk_<utc date>.log`` + console, secrets masked),
   single-instance file locks (``state_dir/process.lock``; live also a per-key/pair lock in the
   temp directory named by a hash, never the key), Store open + campaign identity;
3. venue (``GateVenue`` live / ``PaperVenue`` paper), pair metadata (fresh, else the saved
   snapshot when saved orders must be recovered), FeedHub, IndexBuilder, Notifier, Engine;
4. ``engine.startup()`` (recovery cancels saved orders first), feeds and the live fill
   wake-up stream start, THEN the slower live checks (account mode, pair tradable, clock
   skew); a transient failure stops trading and is retried after 30 s doubling to 300 s,
   a definitive one stops trading and the process exits once no saved order is left;
5. the loop: ``tick = min(0.2, loop_seconds)``; ``engine.preflight()`` and
   ``engine.sample_index()`` every tick; ``engine.step()`` when due (``next_step_seconds``,
   ``next_reconciliation_deadline``, own fill wake-ups); BudgetWait -> retry after the wait;
   ReadInterrupted -> next tick; Unsafe escaping a step -> ``request_stop`` + preflight;
   ``status.json`` every ``app.status_interval_seconds`` and after each step;
6. Ctrl+C / SIGTERM / ``--seconds`` -> ``engine.shutdown()`` (cancels saved orders; DMS and
   unconfirmed reservations stay), final status, feeds stop, Telegram stop message and up
   to 15 s of outbox delivery.

Every external dependency is injectable through ``Deps`` (tests run offline).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import signal
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from . import __version__
from .config import LIVE_CONFIRM_ENV, LIVE_CONFIRM_VALUE, Config, env_file_has_live_confirm, load_config
from .dec import D, ceil_to, floor_to, fmt, usd
from .doctor import safe_error
from .errors import BudgetWait, ConfigError, ReadError, ReadInterrupted, Rejected, TransientUnsafe, Unsafe
from .types import SIDES, Book, PairMeta

LOG = logging.getLogger(__name__)

EXIT_OK, EXIT_FAIL, EXIT_ERROR, EXIT_INTERRUPTED = 0, 1, 2, 130
DEFAULT_CONFIG = 'config.yaml'
STATUS_NAME = 'status.json'
LOCK_NAME = 'process.lock'
META_SNAPSHOT_KEY = 'pair_meta_snapshot'
CANCEL_VERIFY_SECONDS = 60.0
CANCEL_POLL_SECONDS = 1.0
STARTUP_RETRY_INITIAL = 30.0
STARTUP_RETRY_MAX = 300.0
TELEGRAM_FLUSH_SECONDS = 15.0
LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
MIN_SECRET_LEN = 6
STATUS_WARNING = 'Локальный сохранённый статус, не свежий запрос к бирже: смотрите на его возраст.'


# ============================================================================ paths

def db_path(cfg: Config, mode: str | None = None) -> Path:
    """Trading database: one per state_dir and mode (``live.sqlite3`` / ``paper.sqlite3``)."""
    return Path(cfg.state_dir) / f'{mode or cfg.mode}.sqlite3'


def status_path(cfg: Config) -> Path:
    return Path(cfg.state_dir) / STATUS_NAME


def lock_path(cfg: Config) -> Path:
    return Path(cfg.state_dir) / LOCK_NAME


def key_lock_path(cfg: Config) -> Path | None:
    """Host-wide lock per API key and pair, named by a non-reversible hash (never the key)."""
    key = cfg.secrets.gate_key
    if not key:
        return None
    digest = hashlib.sha256(f'gate_stonk|{key}|{cfg.currency_pair}'.encode('utf-8')).hexdigest()[:16]
    return Path(tempfile.gettempdir()) / f'gate_stonk_{digest}.lock'


def _utc(ms: int | float) -> str:
    return datetime.fromtimestamp(float(ms) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


# ============================================================================ logging

def _secret_values(cfg: Config) -> list[str]:
    secrets = cfg.secrets
    values = [secrets.gate_key, secrets.gate_secret, secrets.telegram_token]
    return sorted({v for v in values if isinstance(v, str) and len(v) >= MIN_SECRET_LEN}, key=len, reverse=True)


def mask_secrets(text: str, secrets: list[str]) -> str:
    for value in secrets:
        if value in text:
            text = text.replace(value, '***')
    return text


class SecretFilter(logging.Filter):
    """Replaces any configured secret value in a log record by ``***`` (defence in depth:
    the modules never log secrets; this catches a mistake or a third-party message)."""

    def __init__(self, secrets: list[str]) -> None:
        super().__init__()
        self.secrets = list(secrets)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self.secrets:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        masked = mask_secrets(message, self.secrets)
        if masked != message:
            record.msg, record.args = masked, ()
        return True


class MaskingFormatter(logging.Formatter):
    """Formatter that masks secrets in the final text (tracebacks included)."""

    def __init__(self, fmt_text: str, secrets: list[str]) -> None:
        super().__init__(fmt_text)
        self.secrets = list(secrets)

    def format(self, record: logging.LogRecord) -> str:
        return mask_secrets(super().format(record), self.secrets)


class UtcDailyFileHandler(logging.FileHandler):
    """``<dir>/gate_stonk_<YYYYMMDD>.log`` by UTC date; switches file after UTC midnight."""

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.day = self._today()
        super().__init__(self._path(self.day), encoding='utf-8')

    @staticmethod
    def _today() -> str:
        return datetime.fromtimestamp(time.time(), timezone.utc).strftime('%Y%m%d')

    def _path(self, day: str) -> str:
        return str(self.directory / f'gate_stonk_{day}.log')

    def emit(self, record: logging.LogRecord) -> None:
        day = self._today()
        if day != self.day:
            self.acquire()
            try:
                if self.stream is not None:
                    self.stream.close()
                    self.stream = None
                self.day = day
                self.baseFilename = os.path.abspath(self._path(day))
            finally:
                self.release()
        super().emit(record)


def configure_logging(cfg: Config, *, console_level: str | int | None = None) -> list[logging.Handler]:
    """File (``state_dir/logs``) + console handlers on the root logger, secrets masked."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, '_gate_stonk', False):
            root.removeHandler(handler)
            handler.close()
    secrets = _secret_values(cfg)
    level = logging.getLevelName(cfg.app['log_level'])
    file_handler = UtcDailyFileHandler(Path(cfg.state_dir) / 'logs')
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(console_level if console_level is not None else level)
    file_handler.setLevel(level)
    handlers: list[logging.Handler] = [file_handler, console]
    file_handler._gate_stonk_previous_level = root.level    # type: ignore[attr-defined]
    for handler in handlers:
        handler._gate_stonk = True    # type: ignore[attr-defined]
        handler.addFilter(SecretFilter(secrets))
        handler.setFormatter(MaskingFormatter(LOG_FORMAT, secrets))
        root.addHandler(handler)
    root.setLevel(min(level, console.level) if isinstance(level, int) else logging.INFO)
    for noisy in ('urllib3', 'websocket'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return handlers


def remove_logging(handlers: list[logging.Handler]) -> None:
    root = logging.getLogger()
    for handler in handlers:
        previous = getattr(handler, '_gate_stonk_previous_level', None)
        if previous is not None:
            root.setLevel(previous)
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass


# ============================================================================ dependencies

def _default_hub_factory(cfg: Config) -> Any:
    from .feeds import build_hub
    return build_hub(cfg)


def _offline_hub(cfg: Config) -> Any:
    from .feeds import build_hub
    return build_hub(cfg, start_threads=False)


def _default_live_venue_factory(cfg: Config, limiter: Any, http: Any) -> Any:
    from .gate_rest import GateRestClient
    from .venue_gate import GateVenue, GateVenueSettings
    client = GateRestClient.from_config(cfg, limiter=limiter, session=http)
    return GateVenue(GateVenueSettings.from_config(cfg), client)


def _default_meta_factory(cfg: Config, http: Any) -> PairMeta:
    """Public pair metadata (validated against ``market.expected_*`` pins)."""
    from .gate_rest import GateRestClient
    from .venue_gate import GateVenue, GateVenueSettings
    client = GateRestClient(cfg.market['rest_url'], None, None, timeout=float(cfg.app['request_timeout_seconds']),
                            request_expiry_seconds=float(cfg.app['request_expiry_seconds']), limiter=None,
                            session=http)
    try:
        return GateVenue(GateVenueSettings.from_config(cfg), client).pair_meta()
    finally:
        client.close()


def _default_paper_venue_factory(cfg: Config, store: Any, hub: Any, meta: PairMeta) -> Any:
    from .venue_paper import PaperVenue
    return PaperVenue(cfg, store, hub, meta)


def _default_engine_factory(cfg: Config, store: Any, venue: Any, hub: Any, builder: Any, notifier: Any, *,
                            live: bool, meta: PairMeta, limiter: Any) -> Any:
    from .engine import Engine
    return Engine(cfg, store, venue, hub, builder, notifier, live=live, meta=meta, limiter=limiter)


def _default_notifier_factory(cfg: Config, store: Any) -> Any:
    from .notify import Notifier
    return Notifier(cfg, store, start_thread=False)


def _default_fill_wakeup_factory(cfg: Config, on_wake: Callable[[], None]) -> Any:
    from .fill_wakeup import FillWakeup
    return FillWakeup(cfg, on_wake=on_wake)


@dataclass
class Deps:
    """Injectable dependencies (defaults talk to the real network / modules)."""

    http: Any = None                                         # requests-like session for public REST
    hub_factory: Callable[[Config], Any] | None = None       # run / index-watch / doctor feeds
    meta_factory: Callable[[Config, Any], PairMeta] | None = None
    live_venue_factory: Callable[[Config, Any, Any], Any] | None = None
    paper_venue_factory: Callable[[Config, Any, Any, PairMeta], Any] | None = None
    engine_factory: Callable[..., Any] | None = None
    notifier_factory: Callable[[Config, Any], Any] | None = None
    fill_wakeup_factory: Callable[[Config, Callable[[], None]], Any] | None = None
    doctor_venue_factory: Callable[[Config, Any], Any] | None = None
    offline_hub_factory: Callable[[Config], Any] | None = None
    sleep: Callable[[float], None] | None = None
    out: Callable[[str], None] | None = None
    err: Callable[[str], None] | None = None
    logging: bool = True
    signals: bool = True

    def say(self, text: str) -> None:
        (self.out or print)(text)

    def error(self, text: str) -> None:
        if self.err is not None:
            self.err(text)
        elif self.out is not None:
            self.out(text)
        else:
            print(text, file=sys.stderr)



# ============================================================================ locks

class LockBusy(Exception):
    def __init__(self, path: Path):
        super().__init__(str(path))
        self.path = path


class ProcessLocks:
    """``state_dir/process.lock`` (always) + the per-key/pair lock (live). Fail immediately."""

    def __init__(self, cfg: Config, live: bool) -> None:
        self.paths = [lock_path(cfg)]
        if live:
            extra = key_lock_path(cfg)
            if extra is not None:
                self.paths.append(extra)
        self._locks: list[Any] = []

    def __enter__(self) -> 'ProcessLocks':
        from filelock import FileLock, Timeout

        for path in self.paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            lock = FileLock(str(path), timeout=0)
            try:
                lock.acquire()
            except Timeout:
                self.release()
                raise LockBusy(path) from None
            self._locks.append(lock)
        return self

    def release(self) -> None:
        while self._locks:
            lock = self._locks.pop()
            try:
                lock.release()
            except Exception:
                pass

    def __exit__(self, *exc: Any) -> None:
        self.release()


# ============================================================================ validate

def _level_text(level: Any) -> str:
    if level.mode == 'exact':
        edge = f'край {fmt(level.edge_pct)}%'
    else:
        edge = (f'диапазон {fmt(level.min_edge_pct)}–{fmt(level.max_edge_pct)}%, пропуск '
                f'{fmt(level.skip_notional_usdt)} USDT, вне диапазона {level.out_of_range}, '
                f'мало глубины {level.insufficient_depth}')
    sizes = []
    if level.order_notional_usdt is not None:
        sizes.append(f'{fmt(level.order_notional_usdt)} USDT')
    if level.order_qty is not None:
        sizes.append(f'{fmt(level.order_qty)} шт.')
    size = ' или '.join(sizes) + (' (меньшее)' if len(sizes) > 1 else '')
    iceberg = ''
    if level.iceberg_display_qty is not None:
        iceberg = f', iceberg видимо {fmt(level.iceberg_display_qty)}'
    elif level.iceberg_display_pct is not None:
        iceberg = f', iceberg видимо {fmt(level.iceberg_display_pct)}%'
    deeper = ('' if level.deeper_tolerance_pct is None else f', допуск вглубь {fmt(level.deeper_tolerance_pct)}%')
    return (f'{level.id}: {edge}; размер {size}{iceberg}; допуск полосы {fmt(level.band_tolerance_pct)}%{deeper}, '
            f'перестановка от {fmt(level.reprice_threshold_pct)}% не чаще {fmt(level.min_reprice_seconds)} с')


def _limit(value: Any, unit: str = '') -> str:
    return 'выкл.' if value is None else f'{fmt(value)}{unit}'


def config_summary(cfg: Config) -> list[str]:
    app, market, index, safety = cfg.app, cfg.market, cfg.index, cfg.safety
    lines = [
        f'Конфигурация верна: {cfg.path}',
        f"Режим {app['mode']}, кампания {app['campaign_id']}, префикс заявок t-{app['client_prefix']}-, "
        f'папка состояния {cfg.state_dir}',
        f"Пара {market['currency_pair']} ({market['base']}/{market['quote']}), аккаунт {market['account']}, "
        f"pair.mode {cfg.pair['mode']}",
        f"Индекс: {index['method']}, минимум источников {index['min_sources']}, отклонение от медианы ≤ "
        f"{fmt(index['max_source_deviation_pct'])}%, возраст ≤ {index['max_age_seconds']:g} с, рассинхрон ≤ "
        f"{index['max_timestamp_skew_seconds']:g} с, сдвиг центра {fmt(index['basis_offset_pct'])}%",
    ]
    for source in cfg.all_sources():
        spread = 'нет' if source.max_spread_pct is None else f'{fmt(source.max_spread_pct)}%'
        state = '' if source.enabled else ' [выключен]'
        lines.append(f"  источник {source.id}{state}: {source.venue} {source.symbol} ({source.quote}), цена "
                     f"{source.price}, вес {fmt(source.weight)}{', обязательный' if source.required else ''}, "
                     f"макс. спред {spread}, возраст ≤ {source.max_age_seconds:g} с"
                     + (f', heartbeat {source.heartbeat_seconds:g} с' if source.heartbeat_seconds else ''))
    if any(s.quote == 'USDC' for s in cfg.sources()):
        fx = cfg.fx_settings()
        lines.append(f'  курс USDC→USDT: {fx.mode}' + (f' {fmt(fx.usdc_to_usdt)}' if fx.mode == 'fixed' else '')
                     + f', депег ≤ {fmt(fx.max_depeg_pct)}%')
    lines.append(f"Защита: новая заявка не ближе {fmt(safety['min_new_edge_pct'])}% к центру, стоящая снимается "
                 f"ближе {fmt(safety['hard_min_edge_pct'])}%; максимум заявок {safety['max_open_orders']}")
    for side in SIDES:
        levels = cfg.levels(side)
        settings = cfg.side(side)
        state = 'включена' if cfg.side_enabled(side) else 'ВЫКЛЮЧЕНА (pair.mode)'
        lines.append(f'{side.upper()} ({state}), уровней {len(levels)}:')
        lines += [f'  {_level_text(level)}' for level in levels]
        lines.append(f"  лимиты: всего {_limit(settings['max_total_qty'], ' шт.')}, на сумму "
                     f"{_limit(settings['max_total_notional_usdt'], ' USDT')}, в сутки (UTC) "
                     f"{_limit(settings['max_daily_notional_usdt'], ' USDT')}"
                     + (f", мин. прибыль {_limit(settings['min_profit_pct'], '%')} ({settings['profit_floor_mode']})"
                        if side == 'sell' else
                        f", компенсация комиссии в базе {'да' if settings['compensate_base_fee'] else 'нет'}"))
    pair = cfg.pair
    lines.append(f"Запас: не более {_limit(pair['max_inventory_usdt'], ' USDT')} / "
                 f"{_limit(pair['max_inventory_qty'], ' шт.')}, неприкосновенно {fmt(pair['inventory_reserve_qty'])} шт.")
    portfolio = cfg.portfolio
    lines.append(f"Портфель: резерв {fmt(portfolio['cash_reserve_usdt'])} USDT, оценка комиссии "
                 f"{fmt(portfolio['fee_estimate_pct'])}%, резерв на комиссию {fmt(portfolio['fee_reserve_pct'])}%")
    lines.append(f"DMS: {'вкл.' if safety['dead_man_switch'] else 'ВЫКЛ.'} таймаут {safety['dms_timeout_seconds']} с, "
                 f"продление каждые {safety['dms_refresh_seconds']:g} с; сверка каждые {app['poll_seconds']:g} с, "
                 f"локальный цикл {app['loop_seconds']:g} с, таймаут запроса {app['request_timeout_seconds']:g} с, "
                 f"x-gate-exptime {app['request_expiry_seconds']:g} с")
    execution = cfg.execution
    lines.append(f"Исполнение: post-only ({execution['time_in_force']}), пакеты {'да' if execution['use_batch'] else 'нет'}, "
                 f"amend {'да' if execution['reprice_via_amend'] else 'нет'}, iceberg+poc проверен "
                 f"{'да' if execution['iceberg_poc_verified'] else 'нет'}")
    telegram = cfg.telegram
    lines.append(f"Telegram: {'вкл.' if telegram['enabled'] else 'выкл.'}, обязателен в live: "
                 f"{'да' if safety['require_telegram'] else 'нет'}; dedicated_account_ack: "
                 f"{'да' if safety['dedicated_account_ack'] else 'нет'}")
    if cfg.mode == 'paper':
        paper = cfg.paper
        lines.append(f"Paper: начальный баланс {fmt(paper['initial_usdt'])} USDT и {fmt(paper['initial_base'])} "
                     f"{market['base']} (один раз на кампанию), модель {paper['fill_model']}")
    lines.append('Проверка локальная: биржа, ключи и торговля не проверялись.')
    return lines


def ladder_preview(cfg: Config, center: Decimal, precision: int, amount_precision: int) -> list[str]:
    """Ladder prices at ``center`` on a synthetic wide book (pricing.ladder_targets)."""
    from .pricing import edge_pct, ladder_targets, maker_limit
    from .sizing import iceberg_visible, requested_size

    market = cfg.market
    meta = PairMeta(currency_pair=market['currency_pair'], base=market['base'], quote=market['quote'],
                    precision=precision, amount_precision=amount_precision, min_base_amount=None,
                    min_quote_amount=None, max_base_amount=None, max_quote_amount=None, trade_status='tradable')
    depth = Decimal(10) ** 12
    bid = floor_to(center / 2, precision)
    ask = ceil_to(center * Decimal('1.5'), precision)
    if bid <= 0:
        raise ValueError('--center слишком мал для этой точности')
    book = Book(bids=((bid, depth),), asks=((ask, depth),), received=time.monotonic(),
                event_ms=int(time.time() * 1000))
    lines = [f'Лестница при центре {fmt(center)} (точность цены {precision}, количества {amount_precision}; '
             f'синтетический широкий стакан {fmt(bid)}/{fmt(ask)}):']
    fee = cfg.portfolio['fee_estimate_pct']
    for side in SIDES:
        levels = cfg.levels(side)
        if not levels:
            continue
        settings = cfg.side(side)
        state = '' if cfg.side_enabled(side) else ' (сторона выключена pair.mode)'
        lines.append(f'{side.upper()}{state}:')
        targets = ladder_targets(center, book, meta, levels, side, own_levels=None, floor=None, ceiling=None,
                                 maker_limit=maker_limit(side, book, []),
                                 profit_floor_mode=settings['profit_floor_mode'],
                                 min_new_edge_pct=cfg.safety['min_new_edge_pct'])
        by_id = {level.id: level for level in levels}
        for target in targets:
            level = by_id[target.level]
            if target.decision is None:
                lines.append(f'  {target.level}: нет цены ({target.reason})')
                continue
            price = target.decision.price
            text = (f'  {target.level}: цена {fmt(price)} (край {fmt(edge_pct(side, price, center), 4)}%, '
                    f'полоса {fmt(target.decision.lower)}–{fmt(target.decision.upper)}), {target.reason}'
                    + ('' if target.placeable else ' — не выставляется'))
            try:
                size = requested_size(level, side, price, meta, fee, bool(settings.get('compensate_base_fee')))
                qty = size['requested_gross_qty']
                text += f'; {fmt(qty)} {meta.base} ≈ {usd(qty * price)} {meta.quote}'
                visible = iceberg_visible(level, qty, meta, price=price) if level.has_iceberg and qty > 0 else None
                if visible is not None:
                    text += f', видимо {fmt(visible)}'
            except Unsafe as exc:
                text += f'; размер: {exc}'
            lines.append(text)
    lines.append('Минимумы пары не учтены: проверьте doctor (он читает их с биржи).')
    return lines


def cmd_validate(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    for line in config_summary(cfg):
        deps.say(line)
    if args.center is not None:
        try:
            center = D(args.center)
        except ValueError:
            deps.error('Ошибка: --center должен быть числом')
            return EXIT_FAIL
        if center <= 0:
            deps.error('Ошибка: --center должен быть больше 0')
            return EXIT_FAIL
        precision = args.precision if args.precision is not None else cfg.market.get('expected_precision')
        amount_precision = (args.amount_precision if args.amount_precision is not None
                            else cfg.market.get('expected_amount_precision'))
        if precision is None:
            deps.error('Ошибка: для --center укажите --precision (или market.expected_precision)')
            return EXIT_FAIL
        amount_precision = 4 if amount_precision is None else amount_precision
        if not (0 <= precision <= 18 and 0 <= amount_precision <= 18):
            deps.error('Ошибка: точность должна быть 0..18')
            return EXIT_FAIL
        try:
            lines = ladder_preview(cfg, center, precision, amount_precision)
        except (Unsafe, ValueError) as exc:
            deps.error(f'Ошибка расчёта лестницы: {safe_error(exc) if isinstance(exc, Unsafe) else exc}')
            return EXIT_FAIL
        for line in lines:
            deps.say(line)
    return EXIT_OK


# ============================================================================ doctor / index-watch

def cmd_doctor(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    from .doctor import Doctor

    if not (math.isfinite(args.seconds) and 1 <= args.seconds <= 600):
        deps.error('Ошибка: doctor --seconds должно быть 1..600')
        return EXIT_ERROR
    handlers = configure_logging(cfg, console_level=logging.ERROR) if deps.logging else []
    try:
        doctor = Doctor(cfg, seconds=args.seconds, probe_write=args.probe_write, http=deps.http,
                        hub_factory=deps.hub_factory, venue_factory=deps.doctor_venue_factory,
                        sleep=deps.sleep, out=deps.say)
        report = doctor.run()
    finally:
        remove_logging(handlers)
    return EXIT_OK if report['ok'] else EXIT_FAIL


def cmd_index_watch(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    from .watch import IndexWatch

    minutes = args.minutes
    if minutes is not None and not (math.isfinite(minutes) and minutes > 0):
        deps.error('Ошибка: --minutes должно быть положительным числом')
        return EXIT_ERROR
    handlers = configure_logging(cfg, console_level=logging.ERROR) if deps.logging else []
    try:
        watch = IndexWatch(cfg, hub_factory=deps.hub_factory or _default_hub_factory, csv_path=args.csv,
                           out=deps.say, sleep=deps.sleep)
        watch.run(minutes)
    except Unsafe as exc:
        deps.error(f'index-watch не запущен: {safe_error(exc)} (база {Path(cfg.state_dir) / "index_watch.sqlite3"})')
        return EXIT_ERROR
    finally:
        remove_logging(handlers)
    return EXIT_OK


# ============================================================================ run: gates

ICEBERG_CODE = 'ICEBERG_POC_NOT_VERIFIED'


def iceberg_levels(cfg: Config) -> list[str]:
    return [f'{side}:{level.id}' for side in SIDES if cfg.side_enabled(side)
            for level in cfg.levels(side) if level.has_iceberg]


def run_gate_refusal(cfg: Config, live_flag: bool, seconds: float | None) -> tuple[str, str] | None:
    """Local refusal (code, Russian text) in the binding order, or None. No network, no files."""
    if cfg.live and not live_flag:
        return ('LIVE_CONFIG_REQUIRES_EXPLICIT_RUN_LIVE_FLAG',
                'app.mode: live требует явного флага: run --live.')
    if live_flag and not cfg.live:
        return ('LIVE_FLAG_REQUIRES_APP_MODE_LIVE',
                'Флаг --live задан, но в конфиге app.mode: paper. Для live установите app.mode: live.')
    if not cfg.live:
        return None
    if seconds is not None:
        return ('SECONDS_IS_PAPER_ONLY', '--seconds разрешён только в paper.')
    if env_file_has_live_confirm(cfg):
        return ('LIVE_CONFIRM_IN_ENV_FILE',
                f'{LIVE_CONFIRM_ENV} записан в .env рядом с конфигом. Удалите его оттуда: подтверждение live '
                f'задаётся только в оболочке для текущего запуска.')
    if os.environ.get(LIVE_CONFIRM_ENV) != LIVE_CONFIRM_VALUE:
        return ('LIVE_CONFIRM_MISSING',
                f'Нет подтверждения live. В PowerShell: $env:{LIVE_CONFIRM_ENV} = "{LIVE_CONFIRM_VALUE}"')
    if not cfg.safety['dedicated_account_ack']:
        return ('DEDICATED_ACCOUNT_ACK_REQUIRED',
                'Нужно safety.dedicated_account_ack: true — отдельный (суб)аккаунт только для бота.')
    if not cfg.safety['dead_man_switch']:
        return ('DEAD_MAN_SWITCH_REQUIRED', 'Live требует safety.dead_man_switch: true.')
    if not cfg.secrets.has_gate_keys:
        return ('GATE_API_KEYS_MISSING', 'Не заданы GATE_API_KEY / GATE_API_SECRET (в .env рядом с конфигом).')
    if cfg.safety['require_telegram']:
        if not cfg.telegram['enabled']:
            return ('TELEGRAM_REQUIRED_BUT_DISABLED',
                    'safety.require_telegram: true, но telegram.enabled: false.')
        if not cfg.secrets.has_telegram:
            return ('TELEGRAM_CREDENTIALS_MISSING', 'Не заданы TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID.')
    icebergs = iceberg_levels(cfg)
    if icebergs and not cfg.execution['iceberg_poc_verified']:
        return (ICEBERG_CODE,
                f"Уровни {', '.join(icebergs)} заданы как iceberg, а execution.iceberg_poc_verified: false. "
                'Сначала проверьте iceberg+poc (этап 5) или уберите iceberg из уровней.')
    return None


class RunRefused(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(code)
        self.code = code
        self.message = message


class StopRequested(Exception):
    """Raised in the main thread by the SIGTERM handler."""


# ============================================================================ run: startup checks

class StartupChecks:
    """Slow startup checks run AFTER ``engine.startup()``; retried while transient."""

    def __init__(self, runner: 'Runner') -> None:
        self.runner = runner
        self.pending = True
        self.stop_reason: str | None = None
        self.next_retry = 0.0
        self.delay = STARTUP_RETRY_INITIAL
        self.attempts = 0
        self.last: str | None = None
        self.definitive: str | None = None

    def status(self) -> dict:
        return {'pending': self.pending, 'attempts': self.attempts, 'last_result': self.last,
                'stop_reason': self.stop_reason, 'definitive_refusal': self.definitive,
                'retry_in_seconds': (round(max(0.0, self.next_retry - time.monotonic()), 1)
                                     if self.pending and self.attempts else None)}

    def due(self, now: float) -> bool:
        return self.pending and now >= self.next_retry

    def check(self) -> tuple[str | None, bool]:
        """(refusal code or None, transient)."""
        r = self.runner
        cfg = r.cfg
        if r.meta_from_snapshot is not None:
            try:
                fresh = r.fetch_meta()
            except TransientUnsafe as exc:
                return f'PAIR_METADATA_UNAVAILABLE:{safe_error(exc)}', True
            except Unsafe as exc:          # e.g. PAIR_PRECISION_MISMATCH against market.expected_*: definitive
                return safe_error(exc), False
            except Exception as exc:
                return f'PAIR_METADATA_UNAVAILABLE:{safe_error(exc)}', True
            if (fresh.precision, fresh.amount_precision) != (r.meta.precision, r.meta.amount_precision):
                return 'PAIR_PRECISION_CHANGED', False
            r.meta_from_snapshot = None
            r.save_meta_snapshot(fresh)
        if not r.live:
            return None, False
        from .gate_rest import AUTH_LABELS
        from .venue import side_allowed

        venue = r.venue
        try:
            info = venue.account_check()
        except Rejected as exc:
            if exc.label in AUTH_LABELS:
                return f'GATE_AUTH:{exc.label}', False
            return f'STARTUP_CHECK_FAILED:{exc.label or "REJECTED"}', True
        except Exception as exc:
            return f'STARTUP_CHECK_FAILED:{safe_error(exc)}', True
        problems = list(info.get('problems') or []) if isinstance(info, dict) else ['INVALID_ACCOUNT_CHECK']
        if problems:
            return problems[0], False
        enabled = [side for side in SIDES if cfg.side_enabled(side)]
        status = r.meta.trade_status
        if not any(side_allowed(status, side) for side in enabled):
            return f'PAIR_NOT_TRADABLE:{status}', False
        try:
            t0 = time.time()
            server_ms = venue.server_time_ms()
            t1 = time.time()
        except Exception as exc:
            return f'STARTUP_CHECK_FAILED:{safe_error(exc)}', True
        skew = server_ms - (t0 + t1) / 2 * 1000
        if abs(skew) > int(cfg.safety['max_clock_skew_ms']):
            LOG.warning('Расхождение часов с Gate %+.0f мс (допуск %s мс)', skew, cfg.safety['max_clock_skew_ms'])
            return 'CLOCK_SKEW', True
        return None, False

    def run(self, now: float) -> None:
        r = self.runner
        self.attempts += 1
        code, transient = self.check()
        engine = r.engine
        if code is None:
            self.pending = False
            self.last = 'ok'
            if self.stop_reason is not None:
                engine.clear_stop(self.stop_reason)
                LOG.info('Проверки старта пройдены при повторе; причина остановки %s снята', self.stop_reason)
                r.deps.say(f'Проверки старта пройдены; торговля возобновляется (было: {self.stop_reason}).')
                self.stop_reason = None
            return
        self.last = code
        if self.stop_reason is None:
            self.stop_reason = code
            engine.request_stop(code)
        if transient:
            self.next_retry = now + self.delay
            LOG.warning('Проверка старта не пройдена (%s); торговля остановлена, повтор через %.0f с', code, self.delay)
            r.deps.say(f'Проверка старта не пройдена: {code}. Торговля остановлена, повтор через {self.delay:.0f} с.')
            self.delay = min(STARTUP_RETRY_MAX, self.delay * 2)
            return
        self.pending = False
        self.definitive = code
        if code != self.stop_reason:
            engine.request_stop(code)
        r.refuse_after_settle(code)
        LOG.error('Запуск отклонён: %s; сохранённые заявки снимаются, затем процесс завершится', code)
        r.deps.error(f'Запуск отклонён: {code}. Сохранённые заявки снимаются и сверяются, затем процесс завершится.')


# ============================================================================ run: runner

class Runner:
    """One ``run`` process: build, start, loop, shut down (paper or live)."""

    def __init__(self, cfg: Config, deps: Deps, *, live: bool, seconds: float | None) -> None:
        self.cfg = cfg
        self.deps = deps
        self.live = live
        self.seconds = seconds
        self.store: Any = None
        self.venue: Any = None
        self.meta: PairMeta | None = None
        self.meta_from_snapshot: str | None = None
        self.hub: Any = None
        self.builder: Any = None
        self.notifier: Any = None
        self.engine: Any = None
        self.limiter: Any = None
        self.fill_wakeup: Any = None
        self.checks: StartupChecks | None = None
        self.exit_reason = 'штатное завершение'
        self.exit_code = EXIT_OK
        self.refusal: str | None = None
        self.steps = 0
        self._steps_at_refusal = 0
        self.started_ms = int(time.time() * 1000)
        self._postponed: tuple[str, float] | None = None
        self._old_signals: dict[int, Any] = {}

    # ----- signals -----
    def _on_signal(self, signum: int, frame: Any) -> None:
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = str(signum)
        raise StopRequested(name)

    def install_signals(self) -> None:
        if not self.deps.signals or threading.current_thread() is not threading.main_thread():
            return
        for name in ('SIGTERM', 'SIGBREAK'):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                self._old_signals[signum] = signal.signal(signum, self._on_signal)
            except (ValueError, OSError):
                pass

    def restore_signals(self) -> None:
        for signum, handler in self._old_signals.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError, TypeError):
                pass
        self._old_signals.clear()

    # ----- building blocks -----
    @property
    def http(self) -> Any:
        return self.deps.http

    def fetch_meta(self) -> PairMeta:
        if self.live:
            return self.venue.pair_meta()
        return (self.deps.meta_factory or _default_meta_factory)(self.cfg, self.http)

    def save_meta_snapshot(self, meta: PairMeta) -> None:
        try:
            self.store.set_meta(META_SNAPSHOT_KEY, meta.raw)
        except Exception as exc:
            LOG.warning('pair metadata snapshot not saved: %s', type(exc).__name__)

    def _open_store(self) -> None:
        from .store import Store

        self.store = Store(db_path(self.cfg, 'live' if self.live else 'paper'))
        try:
            self.store.initialize(self.cfg.identity(), int(time.time() * 1000))
        except Unsafe as exc:
            raise RunRefused(str(exc), f'База {db_path(self.cfg)} принадлежит другой кампании/паре/режиму '
                                       f'({exc}). Не удаляйте её: используйте отдельную папку app.state_dir.') from None

    def _prepare_meta(self) -> PairMeta:
        from .venue import parse_pair_meta

        try:
            meta = self.fetch_meta()
        except Exception as exc:
            code = safe_error(exc)
            raw = self.store.meta(META_SNAPSHOT_KEY)
            if raw is None or not self.store.orders(active_only=True):
                raise RunRefused('PAIR_METADATA_UNAVAILABLE',
                                 f'Не удалось получить метаданные пары {self.cfg.currency_pair}: {code}') from None
            meta = parse_pair_meta(raw)
            self.meta_from_snapshot = code
            if self.live:
                self.venue.meta = meta
            LOG.warning('Метаданные пары недоступны (%s); для снятия сохранённых заявок используется снимок', code)
            return meta
        self.save_meta_snapshot(meta)
        return meta

    def build(self) -> None:
        from .index import IndexBuilder
        from .notify import started_text
        from .ratelimit import RateLimiter

        cfg, deps = self.cfg, self.deps
        self._open_store()
        if self.live:
            self.limiter = RateLimiter.from_config(cfg)
            self.venue = (deps.live_venue_factory or _default_live_venue_factory)(cfg, self.limiter, self.http)
        self.meta = self._prepare_meta()
        self.hub = (deps.hub_factory or _default_hub_factory)(cfg)
        self.builder = IndexBuilder(cfg.index_settings())
        if not self.live:
            self.venue = (deps.paper_venue_factory or _default_paper_venue_factory)(cfg, self.store, self.hub, self.meta)
        self.notifier = (deps.notifier_factory or _default_notifier_factory)(cfg, self.store)
        if self.notifier is not None:
            levels = {side: [level.id for level in cfg.levels(side)] for side in SIDES if cfg.side_enabled(side)}
            self.notifier.event(f'started:{time.time_ns()}', started_text(live=self.live, pair=cfg.currency_pair,
                                                                         levels=levels))
            start = getattr(self.notifier, 'start', None)
            if callable(start):
                try:
                    start()
                except RuntimeError:      # already started by its factory
                    pass
        self.engine = (deps.engine_factory or _default_engine_factory)(
            cfg, self.store, self.venue, self.hub, self.builder, self.notifier, live=self.live, meta=self.meta,
            limiter=self.limiter)
        self.checks = StartupChecks(self)
        if self.meta_from_snapshot is not None:
            self.checks.stop_reason = 'PAIR_METADATA_UNAVAILABLE'
            self.engine.request_stop('PAIR_METADATA_UNAVAILABLE')

    def refuse_after_settle(self, code: str) -> None:
        self.refusal = code
        self._steps_at_refusal = self.steps

    # ----- status -----
    def process_status(self) -> dict:
        fill = None
        if self.fill_wakeup is not None:
            try:
                fill = self.fill_wakeup.status()
            except Exception as exc:
                fill = {'diagnostic_error': type(exc).__name__}
        return {'pid': os.getpid(), 'version': __version__, 'mode': 'live' if self.live else 'paper',
                'started_ms': self.started_ms, 'steps': self.steps,
                'paper_seconds': self.seconds,
                'startup_checks': None if self.checks is None else self.checks.status(),
                'pair_meta_from_snapshot': self.meta_from_snapshot, 'refusal': self.refusal,
                'fill_wakeup': fill if self.live else {'enabled': False}}

    def write_status(self, final: bool = False) -> None:
        from .status import write_status

        value = self.engine.status()      # outside any try: a status failure must reach the runner
        if isinstance(value, dict):
            value = dict(value)
            value.setdefault('updated_ms', int(time.time() * 1000))
            value['process'] = self.process_status()
            if final:
                value['process']['exit_reason'] = self.exit_reason
                value['process']['stopped_ms'] = int(time.time() * 1000)
        write_status(status_path(self.cfg), value)

    # ----- loop -----
    def _postpone(self, wait: float, bucket: str, poll: float) -> None:
        key = (bucket, round(wait, 1))
        if self._postponed == key:
            return
        self._postponed = key
        level = logging.WARNING if wait >= poll else logging.INFO
        LOG.log(level, 'Сверка отложена лимитом запросов (%s) на %.2f с', bucket or '?', wait)

    def loop(self) -> int:
        cfg, engine, hub = self.cfg, self.engine, self.hub
        poll = float(cfg.app['poll_seconds'])
        tick = min(0.2, float(cfg.app['loop_seconds']))
        status_every = float(cfg.app['status_interval_seconds'])
        start = time.monotonic()
        next_step = start
        next_status = start
        retry_not_before = start
        last_step_started = start - poll
        while True:
            now = time.monotonic()
            if self.seconds is not None and now - start >= self.seconds:
                self.exit_reason = f'истекло --seconds {self.seconds:g}'
                return EXIT_OK
            if self.checks is not None and self.checks.due(now):
                self.checks.run(now)
            wake = self.fill_wakeup.consume() if self.fill_wakeup is not None else None
            if wake == 'fill':
                next_step = min(next_step, max(now, retry_not_before))
            elif wake:
                next_step = min(next_step, max(now, last_step_started + poll, retry_not_before))
            try:
                engine.preflight('loop')
                engine.sample_index()
                deadline = engine.next_reconciliation_deadline()
                if deadline is not None:
                    next_step = min(next_step, max(retry_not_before, deadline))
                if now >= next_step:
                    last_step_started = now
                    try:
                        engine.step()
                        next_step = now + float(engine.next_step_seconds())
                        retry_not_before = now
                        self._postponed = None
                    except BudgetWait as exc:
                        next_step = now + min(poll, max(tick, exc.wait_seconds))
                        retry_not_before = next_step
                        self._postpone(exc.wait_seconds, exc.bucket, poll)
                    except ReadInterrupted:
                        next_step = now + tick
                    except ReadError as exc:
                        LOG.warning('Чтение аккаунта отложено: %s', safe_error(exc))
                        next_step = now + poll
                    except Unsafe as exc:
                        reason = safe_error(exc)
                        engine.request_stop(reason)
                        LOG.error('Торговля остановлена: %s', reason)
                        try:
                            engine.preflight('unsafe')
                        except (BudgetWait, ReadInterrupted):
                            pass
                        next_step = now + poll
                    self.steps += 1
                    next_status = 0.0
            except (BudgetWait, ReadInterrupted):
                next_step = min(next_step, now + tick)
            if time.monotonic() >= next_status:
                self.write_status()
                next_status = time.monotonic() + status_every
            if (self.refusal is not None and self.steps > self._steps_at_refusal
                    and not self.store.orders(active_only=True)):
                self.exit_reason = f'запуск отклонён: {self.refusal}'
                return EXIT_ERROR
            hub.wake.wait(tick)
            hub.wake.clear()

    # ----- lifecycle -----
    def run(self) -> int:
        self.install_signals()
        try:
            self.build()
            self.engine.startup()
            self.hub.start()
            if self.live:
                self.fill_wakeup = (self.deps.fill_wakeup_factory or _default_fill_wakeup_factory)(
                    self.cfg, self.hub.wake.set)
                if self.fill_wakeup is not None:
                    self.fill_wakeup.start()
            self.checks.run(time.monotonic())
            self.deps.say(f"gate_stonk {'LIVE' if self.live else 'paper'} {self.cfg.currency_pair} запущен; "
                          f'статус: {status_path(self.cfg)}. Остановка: Ctrl+C или файл STOP_TRADING.')
            self.exit_code = self.loop()
        except RunRefused as exc:
            self.exit_reason = f'отказ запуска: {exc.code}'
            self.deps.error(f'Запуск отклонён ({exc.code}): {exc.message}')
            self.exit_code = EXIT_ERROR
        except KeyboardInterrupt:
            self.exit_reason = 'Ctrl+C'
            LOG.warning('Ctrl+C: снимаются сохранённые заявки; неподтверждённые резервы и DMS остаются')
            self.exit_code = EXIT_OK
        except StopRequested as exc:
            self.exit_reason = str(exc) or 'SIGTERM'
            LOG.warning('%s: снимаются сохранённые заявки; неподтверждённые резервы и DMS остаются', self.exit_reason)
            self.exit_code = EXIT_OK
        except Exception as exc:
            self.exit_reason = 'ошибка ' + safe_error(exc)
            LOG.error('Аварийное завершение: %s', safe_error(exc), exc_info=not isinstance(exc, (Unsafe, ReadError)))
            self.deps.error(f'Ошибка: {safe_error(exc)}')
            self.exit_code = EXIT_ERROR
        finally:
            self.shutdown()
            self.restore_signals()
        return self.exit_code

    def shutdown(self) -> None:
        from .notify import stopped_text

        if self.engine is not None:
            try:
                self.engine.shutdown()
            except BaseException as exc:
                LOG.error('Снятие заявок при остановке не подтверждено: %s', safe_error(exc))
            try:
                self.write_status(final=True)
            except BaseException as exc:
                LOG.warning('Финальный status.json не записан: %s', safe_error(exc))
        if self.fill_wakeup is not None:
            try:
                self.fill_wakeup.stop()
            except BaseException as exc:
                LOG.warning('fill wakeup stop failed: %s', type(exc).__name__)
        if self.hub is not None:
            try:
                self.hub.stop()
            except BaseException as exc:
                LOG.warning('feed hub stop failed: %s', type(exc).__name__)
        if self.notifier is not None:
            try:
                self.notifier.event(f'stopped:{time.time_ns()}', stopped_text(self.exit_reason, live=self.live))
                pending = self.notifier.flush(TELEGRAM_FLUSH_SECONDS)
                if pending:
                    LOG.warning('%s событий Telegram остаются в очереди до следующего запуска', pending)
            except BaseException as exc:
                LOG.warning('Telegram flush interrupted: %s', type(exc).__name__)
            try:
                self.notifier.close()
            except BaseException:
                pass
        if self.venue is not None:
            close = getattr(self.venue, 'close', None)
            if callable(close):
                try:
                    close()
                except BaseException:
                    pass
        if self.store is not None:
            try:
                self.store.close()
            except BaseException:
                pass


def cmd_run(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    seconds = args.seconds
    if seconds is not None and not (math.isfinite(seconds) and seconds > 0):
        deps.error('Ошибка: --seconds должно быть положительным конечным числом')
        return EXIT_ERROR
    refusal = run_gate_refusal(cfg, bool(args.live), seconds)
    if refusal is not None:
        code, message = refusal
        deps.error(f'Запуск отклонён ({code}): {message}')
        return EXIT_ERROR
    live = cfg.live
    handlers = configure_logging(cfg) if deps.logging else []
    try:
        with ProcessLocks(cfg, live):
            if live:
                LOG.warning('LIVE: реальные заявки на %s, аккаунт %s', cfg.currency_pair, cfg.market['account'])
            return Runner(cfg, deps, live=live, seconds=seconds).run()
    except LockBusy as exc:
        deps.error(f'Уже запущен другой процесс gate_stonk (занята блокировка {exc.path}). '
                   'Второй экземпляр не запускается.')
        return EXIT_ERROR
    finally:
        remove_logging(handlers)


# ============================================================================ cancel-only

def _cancel_key(order: Any, used: set[str]) -> str:
    text = getattr(order, 'text', None)
    key = text if isinstance(text, str) and text and text not in used else f'id-{order.order_id}'
    used.add(key)
    return key


def cancel_bot_orders(venue: Any, known: list[dict], prefix: str, *, all_pair: bool = False,
                      timeout: float = CANCEL_VERIFY_SECONDS, poll: float = CANCEL_POLL_SECONDS,
                      sleep: Callable[[float], None] | None = None) -> dict:
    """Cancel own orders (store rows + open orders with ``t-<prefix>-`` or a known id) and verify by reading.

    Never touches the DMS. Foreign open orders are cancelled only with ``all_pair`` (cancel_all_pair).
    """
    from .types import CancelRequest

    sleep = sleep or (lambda s: time.sleep(s))
    text_prefix = f't-{prefix}-'
    known_ids = {str(row['order_id']) for row in known if row.get('order_id')}
    results: Counter = Counter()
    summary: dict[str, Any] = {'known': len(known), 'requests': 0, 'results': results, 'rounds': 0,
                               'verified': False, 'remaining_own': [], 'foreign_open': 0, 'last_error': None,
                               'cancel_all_pair': None}

    def is_own(order: Any) -> bool:
        text = getattr(order, 'text', None)
        return (isinstance(text, str) and text.startswith(text_prefix)) or str(order.order_id) in known_ids

    def cancel(reqs: list) -> None:
        summary['requests'] += len(reqs)
        try:
            results.update(venue.cancel_many(reqs).values())
        except Exception as exc:          # outcome unknown: the verification reads decide
            results['error'] += len(reqs)
            summary['last_error'] = safe_error(exc)
            LOG.warning('cancel-only: cancel request failed: %s', summary['last_error'])

    if known:
        cancel([CancelRequest(text=row['text'], order_id=row.get('order_id')) for row in known])
    deadline = time.monotonic() + timeout
    opens: list = []
    while True:
        summary['rounds'] += 1
        try:
            opens = list(venue.open_orders())
        except Exception as exc:
            summary['last_error'] = safe_error(exc)
            LOG.warning('cancel-only: open orders read failed: %s', summary['last_error'])
            if time.monotonic() >= deadline:
                break
            sleep(poll)
            continue
        own = [o for o in opens if is_own(o)]
        summary['foreign_open'] = len(opens) - len(own)
        summary['remaining_own'] = [str(o.order_id) for o in own]
        if not own:
            summary['verified'] = True
            break
        if time.monotonic() >= deadline:
            break
        used: set[str] = set()
        cancel([CancelRequest(text=_cancel_key(o, used), order_id=str(o.order_id)) for o in own])
        sleep(poll)
    if all_pair and (summary['foreign_open'] or not summary['verified']):
        try:
            venue.cancel_all_pair()
            summary['cancel_all_pair'] = 'sent'
        except Exception as exc:
            summary['cancel_all_pair'] = 'error:' + safe_error(exc)
        deadline = time.monotonic() + timeout
        summary['verified'] = False
        while True:
            try:
                opens = list(venue.open_orders())
                summary['foreign_open'] = sum(1 for o in opens if not is_own(o))
                summary['remaining_own'] = [str(o.order_id) for o in opens if is_own(o)]
                if not opens:
                    summary['verified'] = True
                    break
            except Exception as exc:
                summary['last_error'] = safe_error(exc)
            if time.monotonic() >= deadline:
                break
            sleep(poll)
    summary['results'] = dict(results)
    return summary


def cmd_cancel_only(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    from .store import Store

    live = cfg.live
    if args.all_pair_orders and not args.yes:
        deps.error('--all-pair-orders снимает ВСЕ заявки пары, включая чужие. Повторите с --yes, если это нужно.')
        return EXIT_ERROR
    if live and not cfg.secrets.has_gate_keys:
        deps.error('Нет GATE_API_KEY / GATE_API_SECRET: снять live-заявки нельзя.')
        return EXIT_ERROR
    handlers = configure_logging(cfg) if deps.logging else []
    store = venue = None
    try:
        with ProcessLocks(cfg, live):
            path = db_path(cfg)
            known: list[dict] = []
            if path.exists():
                store = Store(path)
                identity = store.identity()
                if identity is not None and identity.get('currency_pair') != cfg.currency_pair:
                    deps.error(f'База {path} относится к другой паре ({identity.get("currency_pair")}); '
                               'её заявки не используются.')
                else:
                    if identity is not None and identity != json.loads(json.dumps(cfg.identity())):
                        deps.say('Внимание: идентичность базы отличается от конфига; используются её сохранённые заявки.')
                    known = store.orders(active_only=True)
            if live:
                from .ratelimit import RateLimiter
                venue = (deps.live_venue_factory or _default_live_venue_factory)(
                    cfg, RateLimiter.from_config(cfg), deps.http)
            else:
                if store is None:
                    deps.say(f'paper: базы {path} нет — бумажных заявок нет. DMS не затрагивался.')
                    return EXIT_OK
                from .venue import parse_pair_meta
                raw = store.meta(META_SNAPSHOT_KEY)
                meta = parse_pair_meta(raw) if raw is not None else (deps.meta_factory or _default_meta_factory)(
                    cfg, deps.http)
                hub = (deps.offline_hub_factory or _offline_hub)(cfg)
                venue = (deps.paper_venue_factory or _default_paper_venue_factory)(cfg, store, hub, meta)
            deps.say(f"cancel-only {'LIVE' if live else 'paper'} {cfg.currency_pair}: сохранённых активных заявок "
                     f'{len(known)}; снимаю заявки бота (t-{cfg.app["client_prefix"]}-...) и проверяю чтением.')
            summary = cancel_bot_orders(venue, known, cfg.app['client_prefix'], all_pair=bool(args.all_pair_orders),
                                        sleep=deps.sleep)
    except LockBusy as exc:
        deps.error(f'Бот запущен (занята блокировка {exc.path}). Остановите его (Ctrl+C или файл STOP_TRADING) '
                   'и повторите cancel-only.')
        return EXIT_ERROR
    finally:
        if venue is not None and callable(getattr(venue, 'close', None)):
            try:
                venue.close()
            except Exception:
                pass
        if store is not None:
            store.close()
        remove_logging(handlers)
    results = summary['results']
    deps.say(f"Запросов отмены: {summary['requests']} (отправлено {results.get('sent', 0)}, уже завершены "
             f"{results.get('rejected', 0)}, неизвестно {results.get('unknown', 0) + results.get('error', 0)}); "
             f"проверок чтением {summary['rounds']}.")
    if summary['verified']:
        deps.say('Проверено чтением: открытых заявок бота по паре нет.')
    else:
        deps.error(f"НЕ подтверждено: открытые заявки бота остаются ({', '.join(summary['remaining_own']) or 'н/д'})"
                   + (f"; последняя ошибка чтения {summary['last_error']}" if summary['last_error'] else '')
                   + '. Проверьте биржу вручную.')
    if summary['foreign_open'] and not args.all_pair_orders:
        deps.say(f"Посторонних открытых заявок по паре: {summary['foreign_open']} (не трогались; "
                 '--all-pair-orders --yes снимет все заявки пары).')
    if summary['cancel_all_pair'] is not None:
        deps.say(f"Снятие всех заявок пары: {summary['cancel_all_pair']}.")
    deps.say('DMS не затрагивался. Записи в базе будут сверены при следующем запуске run.')
    return EXIT_OK if summary['verified'] else EXIT_ERROR


# ============================================================================ status / report

def cmd_status(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    from .status import read_status, status_age_seconds

    path = status_path(cfg)
    try:
        data = read_status(path)
    except (ValueError, OSError) as exc:
        deps.error(f'status.json не читается: {type(exc).__name__}')
        return EXIT_FAIL
    if data is None:
        deps.error(f'Статуса ещё нет: {path}. Проверьте app.state_dir и запущен ли run.')
        return EXIT_FAIL
    age = status_age_seconds(path)
    deps.say(f"{path} — возраст файла {age:.1f} с. {STATUS_WARNING}" if age is not None else str(path))
    deps.say(json.dumps(data, ensure_ascii=False, indent=2))
    return EXIT_OK


def cmd_report(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    hours = args.hours
    if not (math.isfinite(hours) and hours > 0):
        deps.error('Ошибка: --hours должно быть положительным конечным числом')
        return EXIT_ERROR
    path = db_path(cfg)
    if not path.is_file():
        deps.error(f'База не найдена: {path}. Отчёт строится по базе режима app.mode={cfg.mode}.')
        return EXIT_FAIL
    from .report import build_report, format_report_ru
    from .status import write_status

    slots = [f'{side}:{level.id}' for side in SIDES for level in cfg.levels(side)]
    report = build_report(str(path), hours=Decimal(str(hours)), initial_avg_price=cfg.pair.get('initial_avg_price'),
                          slots=slots)
    deps.say(format_report_ru(report))
    if args.out:
        out = Path(args.out)
        if write_status(out, report):
            deps.say(f'JSON отчёта: {out}')
        else:
            deps.error(f'JSON отчёта не записан: {out}')
            return EXIT_ERROR
    return EXIT_OK


def cmd_simulate(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    from .simulate import cmd_simulate as simulate
    return simulate(cfg, args, deps)


# ============================================================================ parser / main

COMMANDS: dict[str, Callable[[Config, argparse.Namespace, Deps], int]] = {
    'validate': cmd_validate,
    'doctor': cmd_doctor,
    'index-watch': cmd_index_watch,
    'run': cmd_run,
    'cancel-only': cmd_cancel_only,
    'status': cmd_status,
    'report': cmd_report,
    'simulate': cmd_simulate,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='gate_stonk', description='Gate spot STONK_USDT: post-only лестница от индекса.')
    parser.add_argument('--config', default=DEFAULT_CONFIG, help=f'файл конфигурации (по умолчанию {DEFAULT_CONFIG})')
    parser.add_argument('--version', action='version', version=f'gate_stonk {__version__}')
    sub = parser.add_subparsers(dest='command', required=True, metavar='command')

    def command(name: str, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_text, description=help_text)
        p.add_argument('--config', default=argparse.SUPPRESS, help='файл конфигурации')
        return p

    p = command('validate', 'проверить конфигурацию и показать сводку')
    p.add_argument('--center', default=None, help='показать цены уровней при этом центре индекса')
    p.add_argument('--precision', type=int, default=None, help='точность цены пары (знаков) для --center')
    p.add_argument('--amount-precision', type=int, default=None, help='точность количества для --center')

    p = command('doctor', 'диагностика: пара, часы, источники, котировки, аккаунт (только чтение)')
    p.add_argument('--seconds', type=float, default=20.0, help='длительность выборки котировок (1..600 с)')
    p.add_argument('--probe-write', action='store_true',
                   help='проверить право записи ключа (countdown_cancel_all timeout=0: выключает DMS пары)')

    p = command('index-watch', 'запись индекса, спота Gate и источников в SQLite и CSV (только чтение)')
    p.add_argument('--minutes', type=float, default=None, help='длительность (по умолчанию до Ctrl+C)')
    p.add_argument('--csv', default=None, help='путь CSV (по умолчанию state_dir/index_watch_<utc>.csv)')

    p = command('run', 'запустить бота (paper или live по app.mode)')
    p.add_argument('--live', action='store_true', help='явное подтверждение live (нужно при app.mode: live)')
    p.add_argument('--seconds', type=float, default=None, help='только paper: остановиться через N секунд')

    p = command('cancel-only', 'снять все заявки бота по паре и проверить чтением (DMS не трогает)')
    p.add_argument('--all-pair-orders', action='store_true', help='снять ВСЕ заявки пары, включая чужие')
    p.add_argument('--yes', action='store_true', help='подтверждение для --all-pair-orders')

    command('status', 'показать сохранённый status.json')

    p = command('report', 'отчёт по уровням из базы')
    p.add_argument('--hours', type=float, default=24.0, help='окно отчёта в часах (по умолчанию 24)')
    p.add_argument('--out', default=None, help='сохранить JSON отчёта в файл')

    p = command('simulate', 'офлайн-симуляция: настоящий engine/paper на СИНТЕТИЧЕСКОМ рынке (не реальные данные)')
    p.add_argument('--minutes', type=float, default=60.0, help='виртуальная длительность в минутах (по умолчанию 60)')
    p.add_argument('--seed', type=int, default=1, help='зерно синтетического рынка (по умолчанию 1)')
    p.add_argument('--state-dir', default=None,
                   help='папка результатов (по умолчанию <app.state_dir>/simulation_<seed>; не папка торговли)')
    p.add_argument('--volatility', type=float, default=None,
                   help='волатильность справедливой цены, %% за √часа (по умолчанию 1.5)')
    return parser


def main(argv: list[str] | None = None, *, deps: Deps | None = None) -> int:
    deps = deps or Deps()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:      # argparse already printed usage / help
        return int(exc.code) if isinstance(exc.code, int) else EXIT_ERROR
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        deps.error(f'Ошибка конфигурации: {exc}')
        return EXIT_FAIL
    try:
        return COMMANDS[args.command](cfg, args, deps)
    except KeyboardInterrupt:
        deps.error('Прервано (Ctrl+C).')
        return EXIT_INTERRUPTED
    except ConfigError as exc:
        deps.error(f'Ошибка конфигурации: {exc}')
        return EXIT_FAIL
    except Exception as exc:
        LOG.debug('command failed', exc_info=True)
        deps.error(f'Ошибка: {safe_error(exc)}')
        return EXIT_ERROR


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
