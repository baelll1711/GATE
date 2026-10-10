"""doctor: read-only diagnostics before index-watch / paper / live (DESIGN §14, prompt §8).

Checks (each reports Russian lines; a failing REQUIRED check makes the exit code 1):

* ``config``        — the configuration loaded and validated (required);
* ``pair``          — Gate pair metadata over public REST: precision, amount precision,
                      minimums, maximums, trade_status vs ``pair.mode``, ``market.expected_*``
                      pins (required);
* ``clock``         — skew of the local clock against ``GET /spot/time`` (required, limit
                      ``safety.max_clock_skew_ms``); Bybit ``/v5/market/time`` when a Bybit
                      source is configured (informational);
* ``instrument:<id>`` — exact-symbol instrument check of every enabled index source
                      (active, exact base, no scaled contract, Lighter market index).
                      Required for required sources; for optional sources only a
                      definitive failure (wrong/inactive/scaled instrument) fails, a
                      transient network failure is a warning;
* ``fx``            — the USDC->USDT source when an enabled source quotes USDC (required
                      for streaming modes, informational for ``fixed``);
* ``gate_futures_index:<id>`` — constituents of the Gate futures index for Gate futures
                      sources; a Gate spot constituent is a self-reference warning
                      (strong for ``price: mark``);
* ``quotes``        — FeedHub for ``--seconds``; the index computed every 0.2 s; per source
                      ok share, median spread, last error; index ok share, current
                      index/center, Gate spot bid/ask and basis (required: the index was
                      valid, at least half of the samples after the first valid one, and
                      the Gate spot book was valid at least once);
* ``minimums``      — every enabled level at the current center: price, size, notional vs
                      ``min_base_amount``/``min_quote_amount``/maximums, iceberg visible part
                      (required when a center is known);
* ``iceberg_live``  — warning: iceberg levels while ``execution.iceberg_poc_verified`` is
                      false (live refuses to start);
* with GATE_API_KEY/GATE_API_SECRET: ``account`` (key mode classic/unified vs
  ``market.account``, pair whitelist, IP whitelist; required), ``open_orders``,
  ``key_permissions`` (Gate has no permission endpoint; ``--probe-write`` sends the harmless
  signed ``countdown_cancel_all {timeout: 0}`` — it DISARMS the pair countdown — and only
  when the pair has no open orders; ``cmd_doctor`` runs it under the bot's process locks
  (``state_dir`` + per-key/pair), so it is refused while a bot runs on the same state_dir
  or key/pair and no bot can start during it), ``balances`` (required), ``fees`` (GT
  deduction warning). Without keys these checks are skipped with a message.

Nothing here places or cancels an order or sends Telegram. Every network dependency is
injectable (``http`` session, ``hub_factory``, ``venue_factory``, ``sleep``).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .dec import HUNDRED, ONE, ZERO, ceil_to, floor_to, fmt, usd
from .errors import (BudgetWait, ConfigError, NotSent, ReadError, ReadInterrupted, Rejected, Unsafe,
                     UnknownResult)
from .types import SIDES, PairMeta

LOG = logging.getLogger(__name__)

OK, FAIL, WARN, SKIP = 'ok', 'fail', 'warn', 'skip'
LABELS = {OK: '[ OK ]', FAIL: '[СБОЙ]', WARN: '[ВНИМ]', SKIP: '[ПРОП]'}
SAMPLE_SECONDS = 0.2
MIN_SECONDS, MAX_SECONDS = 1.0, 600.0
QUOTES_MIN_OK_SHARE = Decimal(50)
REPORT_NAME = 'doctor.json'
_OWN_TYPES = (Unsafe, ConfigError, ReadError, Rejected, UnknownResult, NotSent, BudgetWait, ReadInterrupted)


def safe_error(exc: BaseException) -> str:
    """Our own exception types carry reason codes; anything else is reported by type name only
    (transport exceptions can contain URLs and request details)."""
    if isinstance(exc, _OWN_TYPES):
        text = ' '.join(str(exc).split())[:300]
        return text or type(exc).__name__
    return type(exc).__name__


def _utc(ms: int | float) -> str:
    return datetime.fromtimestamp(float(ms) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def _amount(value: Decimal | None, unit: str) -> str:
    return f'{fmt(value)} {unit}' if value is not None else f'{unit}: нет'


def _pct(value: Decimal | None, places: int = 4) -> str:
    return 'н/д' if value is None else fmt(value, places)


@dataclass
class Check:
    key: str
    title: str
    status: str
    required: bool
    lines: list[str] = field(default_factory=list)
    data: dict = field(default_factory=dict)

    @property
    def failed(self) -> bool:
        return self.status == FAIL and self.required

    def as_dict(self) -> dict:
        return {'title': self.title, 'status': self.status, 'required': self.required, 'ok': self.status != FAIL,
                'lines': list(self.lines), 'data': self.data}


class _Hist:
    def __init__(self) -> None:
        self.values: list[Decimal] = []

    def add(self, value: Decimal) -> None:
        self.values.append(value)

    def median(self) -> Decimal | None:
        if not self.values:
            return None
        ordered = sorted(self.values)
        mid = len(ordered) // 2
        return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def default_venue_factory(cfg: Any, http: Any) -> Any:
    """Live GateVenue with the configured keys (signed reads only in doctor)."""
    from .gate_rest import GateRestClient
    from .ratelimit import RateLimiter
    from .venue_gate import GateVenue, GateVenueSettings

    client = GateRestClient.from_config(cfg, limiter=RateLimiter.from_config(cfg), session=http)
    return GateVenue(GateVenueSettings.from_config(cfg), client)


def default_hub_factory(cfg: Any) -> Any:
    from .feeds import build_hub
    return build_hub(cfg)


class Doctor:
    """Runs every check once; ``run()`` returns the JSON-safe report (``report['ok']``)."""

    def __init__(self, cfg: Any, *, seconds: float = 20.0, probe_write: bool = False, http: Any = None,
                 hub_factory: Callable[[Any], Any] | None = None,
                 venue_factory: Callable[[Any, Any], Any] | None = None,
                 sleep: Callable[[float], None] | None = None, out: Callable[[str], None] = print,
                 write_report: bool = True) -> None:
        seconds = float(seconds)
        if not MIN_SECONDS <= seconds <= MAX_SECONDS:
            raise ValueError(f'doctor --seconds must be {MIN_SECONDS:g}..{MAX_SECONDS:g}')
        self.cfg = cfg
        self.seconds = seconds
        self.probe_write = bool(probe_write)
        self._http = http
        self._own_http: Any = None
        self.hub_factory = hub_factory or default_hub_factory
        self.venue_factory = venue_factory or default_venue_factory
        self.sleep = sleep if sleep is not None else (lambda s: time.sleep(s))
        self.out = out
        self.write_report = write_report
        self.timeout = float(cfg.app['request_timeout_seconds'])
        self.pair = cfg.market['currency_pair']
        self.base = cfg.market['base']
        self.quote = cfg.market['quote']
        self.checks: list[Check] = []
        self.warnings: list[str] = []
        self.meta: PairMeta | None = None
        self.center: Decimal | None = None
        self.index: Decimal | None = None
        self._client: Any = None

    # ----------------------------------------------------------------- plumbing
    @property
    def http(self) -> Any:
        if self._http is not None:
            return self._http
        if self._own_http is None:
            import requests
            self._own_http = requests.Session()
        return self._own_http

    @property
    def client(self) -> Any:
        """Unsigned Gate REST client (pair metadata, server time)."""
        if self._client is None:
            from .gate_rest import GateRestClient
            self._client = GateRestClient(self.cfg.market['rest_url'], None, None, timeout=self.timeout,
                                          request_expiry_seconds=float(self.cfg.app['request_expiry_seconds']),
                                          limiter=None, session=self.http)
        return self._client

    def add(self, key: str, title: str, status: str, required: bool, lines: list[str] | None = None,
            data: dict | None = None) -> Check:
        if status == FAIL and not required:
            status = WARN
        check = Check(key, title, status, required, list(lines or []), dict(data or {}))
        self.checks.append(check)
        return check

    def _guard(self, key: str, title: str, required: bool, fn: Callable[[], None]) -> None:
        """Run one check; any exception becomes a failed check (never a crash)."""
        try:
            fn()
        except Exception as exc:
            LOG.warning('doctor check %s failed: %s', key, safe_error(exc))
            self.add(key, title, FAIL, required, [f'проверка не выполнена: {safe_error(exc)}'])

    # ----------------------------------------------------------------- checks
    def check_config(self) -> None:
        cfg = self.cfg
        lines = [f"режим {cfg.mode}, пара {self.pair}, аккаунт {cfg.market['account']}, "
                 f"pair.mode {cfg.pair['mode']}"]
        for side in SIDES:
            levels = cfg.levels(side)
            state = 'включена' if cfg.side_enabled(side) else 'выключена pair.mode'
            lines.append(f"{side.upper()}: {len(levels)} уровн. ({', '.join(l.id for l in levels) or '—'}), {state}")
        sources = cfg.sources()
        lines.append('источники индекса: ' + ', '.join(
            f"{s.id} ({s.venue} {s.symbol}, {s.price}, вес {fmt(s.weight)}{', обязательный' if s.required else ''})"
            for s in sources))
        self.add('config', 'Конфигурация', OK, True, lines)

    def check_pair(self) -> None:
        from .venue import parse_pair_meta, side_allowed

        raw = self.client.public('GET', f'/spot/currency_pairs/{self.pair}')
        try:
            meta = parse_pair_meta(raw)
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise ReadError(f'INVALID_PAIR_METADATA:{exc}') from None
        market = self.cfg.market
        problems: list[str] = []
        if (meta.currency_pair, meta.base, meta.quote) != (self.pair, self.base, self.quote):
            problems.append('SPOT_PAIR_MAPPING_MISMATCH')
        for key, actual, name in (('expected_precision', meta.precision, 'PAIR_PRECISION_MISMATCH'),
                                  ('expected_amount_precision', meta.amount_precision,
                                   'PAIR_AMOUNT_PRECISION_MISMATCH')):
            pinned = market.get(key)
            if pinned is not None and pinned != actual:
                problems.append(f'{name}:{actual}!={pinned}')
        enabled = [side for side in SIDES if self.cfg.side_enabled(side)]
        blocked = [side for side in enabled if not side_allowed(meta.trade_status, side)]
        if blocked:
            problems.append(f"PAIR_NOT_TRADABLE:{meta.trade_status}:{'/'.join(s.upper() for s in blocked)}")
        lines = [
            f'точность цены {meta.precision} (шаг {fmt(meta.tick)}), количества {meta.amount_precision} '
            f'(шаг {fmt(meta.lot)})',
            f'минимум {_amount(meta.min_base_amount, meta.base)} и {_amount(meta.min_quote_amount, meta.quote)}; '
            f'максимум {_amount(meta.max_base_amount, meta.base)} и {_amount(meta.max_quote_amount, meta.quote)}',
            f"статус торговли {meta.trade_status}" + (f", комиссия пары {fmt(meta.fee_pct)}%"
                                                      if meta.fee_pct is not None else ''),
        ]
        for key, actual in (('expected_precision', meta.precision), ('expected_amount_precision', meta.amount_precision)):
            if market.get(key) is None:
                lines.append(f'market.{key} не зафиксирован (на бирже {actual}); можно закрепить в конфиге')
        lines += [f'проблема: {p}' for p in problems]
        self.meta = meta
        data = {'precision': meta.precision, 'amount_precision': meta.amount_precision,
                'min_base_amount': meta.min_base_amount, 'min_quote_amount': meta.min_quote_amount,
                'max_base_amount': meta.max_base_amount, 'max_quote_amount': meta.max_quote_amount,
                'trade_status': meta.trade_status, 'fee_pct': meta.fee_pct, 'problems': problems}
        self.add('pair', f'Пара {self.pair} (метаданные Gate)', FAIL if problems else OK, True, lines, data)

    def check_clock(self) -> None:
        limit = int(self.cfg.safety['max_clock_skew_ms'])
        t0 = time.time()
        raw = self.client.public('GET', '/spot/time')
        t1 = time.time()
        server = raw.get('server_time') if isinstance(raw, dict) else None
        if isinstance(server, bool) or not isinstance(server, int) or server <= 0:
            raise ReadError('INVALID_SERVER_TIME')
        skew = int(round(server - (t0 + t1) / 2 * 1000))
        rtt = int(round((t1 - t0) * 1000))
        ok = abs(skew) <= limit
        lines = [f'Gate: время сервера {_utc(server)}, расхождение {skew:+d} мс (допуск ±{limit} мс), '
                 f'задержка запроса {rtt} мс']
        if not ok:
            lines.append('часы компьютера расходятся с биржей: включите синхронизацию времени (NTP); '
                         'live остановится с CLOCK_SKEW')
        elif abs(skew) + rtt / 2 > limit:
            lines.append('расхождение близко к допуску с учётом задержки сети')
        data = {'gate_skew_ms': skew, 'gate_rtt_ms': rtt, 'limit_ms': limit}
        self.add('clock', 'Часы', OK if ok else FAIL, True, lines, data)
        self._guard('clock_bybit', 'Часы Bybit', False, self.check_bybit_clock)

    def check_bybit_clock(self) -> None:
        from .feeds import adapter_class
        from .feeds._venue_common import http_get

        bybit = [s for s in self.cfg.sources() if s.venue == 'bybit_linear']
        if not bybit:
            return
        adapter = adapter_class('bybit_linear')(bybit[0], timeout=self.timeout)
        t0 = time.time()
        status, payload = http_get(self.http, adapter.rest_url + '/v5/market/time', timeout=self.timeout)
        t1 = time.time()
        if status != 200 or not isinstance(payload, dict) or payload.get('retCode') != 0:
            raise ReadError(f'BYBIT_TIME_HTTP_{status}')
        result = payload.get('result') or {}
        nano = result.get('timeNano') if isinstance(result, dict) else None
        if isinstance(nano, str) and nano.isdigit():
            server = int(nano) // 1_000_000
        elif isinstance(payload.get('time'), int):
            server = int(payload['time'])
        else:
            raise ReadError('BYBIT_TIME_INVALID')
        skew = int(round(server - (t0 + t1) / 2 * 1000))
        self.add('clock_bybit', 'Часы Bybit', OK, False,
                 [f'Bybit: расхождение {skew:+d} мс, задержка {int(round((t1 - t0) * 1000))} мс'],
                 {'bybit_skew_ms': skew})

    def check_instruments(self) -> None:
        from .feeds import adapter_class

        for source in self.cfg.sources():
            key, title = f'instrument:{source.id}', f'Инструмент {source.id} ({source.venue} {source.symbol})'
            try:
                adapter = adapter_class(source.venue)(source, timeout=self.timeout, expected_base=self.base)
                info = adapter.instrument_info(self.http, self.timeout)
            except Exception as exc:
                self.add(key, title, FAIL, True, [f'проверка не выполнена: {safe_error(exc)}'])
                continue
            reason = info.get('reason')
            ok = bool(info.get('ok'))
            if ok and source.venue == 'lighter':
                index = info.get('market_index')
                if source.market_index is not None and index != source.market_index:
                    ok, reason = False, f'LIGHTER_MARKET_INDEX_MISMATCH:{index}!={source.market_index}'
            transient = bool(info.get('transient')) and not ok
            required = source.required or not transient
            parts = [f"статус {info.get('status')}", f"база {info.get('base')}", f"котировка {info.get('quote')}"]
            if info.get('tick') is not None:
                parts.append(f"шаг цены {info['tick']}")
            if info.get('contract_size') is not None:
                parts.append(f"размер контракта {info['contract_size']}")
            if info.get('multiplier') is not None:
                parts.append(f"множитель {info['multiplier']}")
            if info.get('market_index') is not None:
                parts.append(f"market_index {info['market_index']}")
            lines = [', '.join(parts)]
            if reason:
                lines.append(f'причина: {reason}' + (' (временная ошибка сети/лимита)' if transient else ''))
            for problem in info.get('problems') or []:
                lines.append(f'несоответствие: {problem}')
            for warning in info.get('warnings') or []:
                lines.append(f'предупреждение: {warning}')
            if info.get('warnings'):
                self.warnings.append(f"{source.id}: {', '.join(info['warnings'])}")
            data = {k: info.get(k) for k in ('ok', 'symbol', 'status', 'base', 'quote', 'tick', 'contract_size',
                                             'multiplier', 'market_index', 'reason', 'problems', 'warnings',
                                             'transient', 'raw_keys')}
            data['reason'] = reason
            self.add(key, title, OK if ok else FAIL, required, lines, data)

    def check_fx(self) -> None:
        from .feeds.fx import build_fx_adapter, fx_needed

        sources = self.cfg.sources()
        if not fx_needed(sources):
            return
        fx = self.cfg.fx_settings()
        usdc = ', '.join(s.id for s in sources if s.quote == 'USDC')
        if fx.mode == 'fixed':
            self.add('fx', 'Курс USDC→USDT', OK, False, [
                f'режим fixed: {fmt(fx.usdc_to_usdt)} USDT за 1 USDC для {usdc}; реальный депег не отслеживается '
                f'(допуск {fmt(fx.max_depeg_pct)}%)'])
            return
        adapter = build_fx_adapter(fx, timeout=self.timeout, ws_url=self.cfg.market.get('spot_ws_url'),
                                   rest_url=self.cfg.market.get('rest_url'))
        info = adapter.instrument_info(self.http, self.timeout)
        lines = [f"режим {fx.mode}: {info.get('symbol')} статус {info.get('status')}, "
                 f"база {info.get('base')}, котировка {info.get('quote')} (для {usdc})"]
        if info.get('reason'):
            lines.append(f"причина: {info['reason']}")
        self.add('fx', 'Курс USDC→USDT', OK if info.get('ok') else FAIL, True, lines,
                 {'mode': fx.mode, 'reason': info.get('reason')})

    def check_gate_futures_index(self) -> None:
        from .feeds._venue_common import http_get
        from .feeds.gate_futures import SIZE_HEADERS

        for source in self.cfg.sources():
            if source.venue != 'gate_futures_usdt':
                continue
            key, title = f'gate_futures_index:{source.id}', f'Состав индекса Gate futures {source.symbol}'
            url = f"{self.cfg.market['rest_url'].rstrip('/')}/futures/usdt/index_constituents/{source.symbol}"
            try:
                status, payload = http_get(self.http, url, headers=SIZE_HEADERS, timeout=self.timeout)
            except Exception as exc:
                self.add(key, title, WARN, False, [f'не удалось прочитать: {safe_error(exc)}'])
                continue
            rows = payload.get('constituents') if isinstance(payload, dict) else None
            if status != 200 or not isinstance(rows, list):
                self.add(key, title, WARN, False, [f'состав индекса недоступен (HTTP {status})'])
                continue
            names: list[str] = []
            gate_self = False
            for row in rows:
                if not isinstance(row, dict):
                    continue
                exchange = str(row.get('exchange') or '?')
                symbols = [str(s) for s in row.get('symbols') or [] if isinstance(s, str)]
                names.append(f"{exchange} ({', '.join(symbols) or '—'})")
                if 'gate' in exchange.lower():
                    gate_self = True
            lines = ['составляющие: ' + ('; '.join(names) or 'нет данных')]
            if gate_self and source.price == 'mark':
                lines.append('САМОССЫЛКА: mark Gate futures строится из индекса, в который входит спот Gate; '
                             'наши заявки на споте могут влиять на собственный индекс. Используйте mid/last '
                             'или уменьшите вес источника.')
                self.warnings.append(f'{source.id}: mark Gate futures зависит от спота Gate')
                status_code = WARN
            elif gate_self:
                lines.append(f'спот Gate входит в индекс фьючерса; источник берёт {source.price} '
                             '(стакан/сделки фьючерса), прямой петли через mark нет')
                status_code = OK
            else:
                status_code = OK
            self.add(key, title, status_code, False, lines, {'constituents': names, 'gate_spot_included': gate_self})

    def check_quotes(self) -> None:
        from .index import IndexBuilder

        cfg = self.cfg
        sources = cfg.sources()
        builder = IndexBuilder(cfg.index_settings())
        hub = self.hub_factory(cfg)
        stats = {s.id: {'ok': 0, 'n': 0, 'spread': _Hist(), 'last_error': None, 'price': None} for s in sources}
        samples = 0
        index_ok = 0
        after_warmup = 0
        after_warmup_ok = 0
        spot_valid = 0
        spot_last: dict[str, Any] = {}
        last_ok = None
        last_spot_error = None
        last_reason = None
        diagnostics: dict = {}
        hub.start()
        try:
            deadline = time.monotonic() + self.seconds
            while True:
                snapshots = {s.id: hub.source_snapshot(s.id) for s in sources}
                fx = hub.fx_snapshot()
                book = hub.spot_book()
                spot_error = hub.spot_book_error()
                now, wall_ms = time.monotonic(), int(time.time() * 1000)
                snapshot = builder.compute(snapshots, fx, now, wall_ms)
                samples += 1
                for component in snapshot.components:
                    row = stats.get(component.source_id)
                    if row is None:
                        continue
                    row['n'] += 1
                    if component.included:
                        row['ok'] += 1
                        row['price'] = component.usdt_price
                    elif component.reason:
                        row['last_error'] = component.reason
                    if component.spread_pct is not None:
                        row['spread'].add(component.spread_pct)
                if snapshot.ok:
                    index_ok += 1
                    last_ok = snapshot
                else:
                    last_reason = snapshot.reason
                if last_ok is not None:
                    after_warmup += 1
                    after_warmup_ok += 1 if snapshot.ok else 0
                if book is not None and spot_error is None:
                    spot_valid += 1
                    spot_last = {'bid': book.bid, 'ask': book.ask, 'mid': book.mid, 'spread_pct': book.spread_pct}
                    if snapshot.ok and snapshot.index:
                        spot_last['basis_pct'] = (book.mid / snapshot.index - ONE) * HUNDRED
                else:
                    last_spot_error = spot_error or 'NO_DATA'
                if time.monotonic() >= deadline:
                    break
                self.sleep(SAMPLE_SECONDS)
            try:
                diagnostics = hub.diagnostics() or {}
            except Exception as exc:
                diagnostics = {'diagnostic_error': type(exc).__name__}
        finally:
            try:
                hub.stop()
            except Exception as exc:
                LOG.warning('doctor: hub stop failed: %s', type(exc).__name__)
        lines = [f'выборка {self.seconds:g} с, {samples} расчётов индекса']
        data: dict[str, Any] = {'samples': samples, 'sources': {}}
        for source in sources:
            row = stats[source.id]
            share = Decimal(row['ok']) * HUNDRED / row['n'] if row['n'] else ZERO
            median = row['spread'].median()
            diag = diagnostics.get(source.id) if isinstance(diagnostics, dict) else None
            connected = diag.get('connected') if isinstance(diag, dict) else None
            text = (f"{source.id}: валиден {fmt(share, 1)}%, медианный спред {_pct(median)}%"
                    + (f", цена {fmt(row['price'], 8)} USDT" if row['price'] is not None else '')
                    + (f", последняя ошибка {row['last_error']}" if row['last_error'] else '')
                    + ('' if connected is None else (', соединение есть' if connected else ', нет соединения')))
            lines.append(text)
            data['sources'][source.id] = {'ok_share_pct': share, 'median_spread_pct': median,
                                          'last_error': row['last_error'], 'price_usdt': row['price']}
        ok_share = Decimal(index_ok) * HUNDRED / samples if samples else ZERO
        warm_share = Decimal(after_warmup_ok) * HUNDRED / after_warmup if after_warmup else ZERO
        if last_ok is not None:
            self.index, self.center = last_ok.index, last_ok.center
            lines.append(f'индекс валиден {fmt(ok_share, 1)}% выборок ({fmt(warm_share, 1)}% после первого '
                         f'валидного); текущий индекс {fmt(last_ok.index, 8)}, центр {fmt(last_ok.center, 8)} USDT, '
                         f'источников в индексе {last_ok.n_included}')
        else:
            lines.append(f'индекс ни разу не был валиден: {last_reason}')
        if last_ok is not None and last_reason and index_ok < samples:
            lines.append(f'последняя причина блокировки: {last_reason}')
        if spot_last:
            text = (f"спот Gate {self.pair}: bid {fmt(spot_last['bid'])}, ask {fmt(spot_last['ask'])}, "
                    f"спред {_pct(spot_last['spread_pct'])}%")
            if spot_last.get('basis_pct') is not None:
                text += f", базис к индексу {_pct(spot_last['basis_pct'], 3)}%"
            lines.append(text)
        else:
            lines.append(f'стакан спота Gate недоступен: {last_spot_error}')
        problems = []
        if last_ok is None:
            problems.append('INDEX_NEVER_VALID')
        elif warm_share < QUOTES_MIN_OK_SHARE:
            problems.append(f'INDEX_OK_SHARE_LOW:{fmt(warm_share, 1)}%')
        if not spot_valid:
            problems.append(f'SPOT_BOOK_UNAVAILABLE:{last_spot_error}')
        lines += [f'проблема: {p}' for p in problems]
        data.update(index_ok_share_pct=ok_share, index_ok_share_after_warmup_pct=warm_share,
                    index=None if last_ok is None else last_ok.index,
                    center=None if last_ok is None else last_ok.center, last_block_reason=last_reason,
                    spot=spot_last, spot_valid_samples=spot_valid, problems=problems)
        self.add('quotes', 'Котировки и индекс', FAIL if problems else OK, True, lines, data)

    def level_preview(self, center: Decimal, meta: PairMeta) -> tuple[list[str], list[str], list[dict]]:
        """Price/size of every enabled level at ``center`` vs the pair minimums."""
        from .sizing import iceberg_visible, meets_minimums, requested_size

        cfg = self.cfg
        lines: list[str] = []
        problems: list[str] = []
        rows: list[dict] = []
        fee = cfg.portfolio['fee_estimate_pct']
        for side in SIDES:
            if not cfg.side_enabled(side):
                continue
            compensate = bool(cfg.side(side).get('compensate_base_fee', False))
            for level in cfg.levels(side):
                edge = level.edge_pct if level.edge_pct is not None else level.min_edge_pct
                if side == 'buy':
                    price = floor_to(center * (ONE - edge / HUNDRED), meta.precision)
                else:
                    price = ceil_to(center * (ONE + edge / HUNDRED), meta.precision)
                issues: list[str] = []
                qty = notional = visible = None
                if price <= 0:
                    issues.append('NONPOSITIVE_PRICE')
                else:
                    try:
                        size = requested_size(level, side, price, meta, fee, compensate)
                        qty = size['requested_gross_qty']
                        notional = qty * price
                    except Unsafe as exc:
                        issues.append(str(exc))
                if qty is not None:
                    if qty <= 0:
                        issues.append('ORDER_SIZE_ZERO_AFTER_ROUNDING')
                    elif not meets_minimums(qty, price, meta):
                        issues.append('ORDER_SIZE_BELOW_MINIMUM')
                    if meta.max_base_amount is not None and qty > meta.max_base_amount:
                        issues.append('AMOUNT_ABOVE_MAX_BASE')
                    if meta.max_quote_amount is not None and notional is not None and notional > meta.max_quote_amount:
                        issues.append('AMOUNT_ABOVE_MAX_QUOTE')
                    if level.has_iceberg and qty > 0:
                        try:
                            visible = iceberg_visible(level, qty, meta, price=price)
                        except Unsafe as exc:
                            issues.append(str(exc))
                text = (f"{side.upper()} {level.id}: край {fmt(edge)}% → цена {fmt(price)}, "
                        + (f"{fmt(qty)} {meta.base} ≈ {usd(notional)} {meta.quote}" if qty is not None else 'размер н/д'))
                if visible is not None:
                    text += f', iceberg видимая часть {fmt(visible)} {meta.base}'
                if issues:
                    text += ' — ' + ', '.join(issues)
                    problems += [f'{side}:{level.id}:{issue}' for issue in issues]
                lines.append(text)
                rows.append({'side': side, 'level': level.id, 'edge_pct': edge, 'price': price, 'qty': qty,
                             'notional': notional, 'iceberg_visible': visible, 'issues': issues})
        return lines, problems, rows

    def check_minimums(self) -> None:
        if self.meta is None or self.center is None:
            self.add('minimums', 'Размеры уровней и минимумы пары', SKIP, True,
                     ['нет метаданных пары или валидного центра индекса — проверка пропущена'])
            return
        lines, problems, rows = self.level_preview(self.center, self.meta)
        lines.insert(0, f'при центре {fmt(self.center, 8)} {self.quote}; минимум '
                        f'{_amount(self.meta.min_base_amount, self.base)} и '
                        f'{_amount(self.meta.min_quote_amount, self.quote)} (до учёта баланса и лимитов)')
        self.add('minimums', 'Размеры уровней и минимумы пары', FAIL if problems else OK, True, lines,
                 {'levels': rows, 'problems': problems})

    def check_iceberg_gate(self) -> None:
        if self.cfg.execution['iceberg_poc_verified']:
            return
        ids = [f'{side}:{level.id}' for side in SIDES if self.cfg.side_enabled(side)
               for level in self.cfg.levels(side) if level.has_iceberg]
        if not ids:
            return
        self.warnings.append('iceberg без проверки iceberg+poc: live откажется запускаться')
        self.add('iceberg_live', 'Iceberg в live', WARN, False, [
            f"уровни {', '.join(ids)} заданы как iceberg, а execution.iceberg_poc_verified: false — "
            'run --live откажется (ICEBERG_POC_NOT_VERIFIED); в paper разрешено'])

    def check_account(self) -> None:
        cfg = self.cfg
        if not cfg.secrets.has_gate_keys:
            lines = ['ключи GATE_API_KEY/GATE_API_SECRET не заданы — проверки режима аккаунта, прав ключа, '
                     'балансов и комиссий пропущены']
            if cfg.live:
                lines.append('app.mode: live, но ключей нет — run --live откажется')
            self.add('account', 'Аккаунт Gate (ключи)', SKIP, False, lines)
            return
        venue = self.venue_factory(cfg, self.http)
        try:
            self._guard('account', 'Режим аккаунта и ключ', True, lambda: self._account(venue))
            holder: dict[str, list | None] = {'opens': None}     # None: open orders could not be read

            def read_opens() -> None:
                holder['opens'] = self._open_orders(venue)
            self._guard('open_orders', 'Открытые заявки пары', False, read_opens)
            self._guard('key_permissions', 'Права ключа', self.probe_write,
                        lambda: self._permissions(venue, holder['opens']))
            self._guard('balances', 'Балансы', True, lambda: self._balances(venue))
            self._guard('fees', 'Комиссии', False, lambda: self._fees(venue))
        finally:
            close = getattr(venue, 'close', None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass

    def _account(self, venue: Any) -> None:
        info = venue.account_check()
        problems = list(info.get('problems') or [])
        mode_names = {1: 'классический (classic)', 2: 'единый (legacy unified)'}
        lines = [f"режим ключа {info.get('key_mode')} ({mode_names.get(info.get('key_mode'), 'неизвестен')}), "
                 f"unified_mode {info.get('unified_mode') or info.get('unified_mode_error') or 'н/д'}; "
                 f"определён аккаунт {info.get('detected_account')}, в конфиге market.account "
                 f"{info.get('configured_account')}"]
        pairs = info.get('pair_whitelist') or []
        lines.append('белый список пар ключа: ' + (', '.join(pairs) if pairs else 'не ограничен')
                     + ('' if info.get('pair_whitelist_ok', True) else f' — {self.pair} НЕ разрешена'))
        ips = info.get('ip_whitelist') or []
        if ips:
            lines.append(f'ключ привязан к IP: {len(ips)} адрес(ов)')
        else:
            lines.append('ключ не привязан к IP — рекомендуется привязать')
            self.warnings.append('ключ Gate без привязки к IP')
        lines += [f'проблема: {p}' for p in problems]
        self.add('account', 'Режим аккаунта и ключ', FAIL if problems else OK, True, lines,
                 {k: info.get(k) for k in ('key_mode', 'unified_mode', 'detected_account', 'configured_account',
                                           'account_ok', 'pair_whitelist_ok', 'tier')} | {'problems': problems,
                                                                                         'ip_whitelist_count': len(ips)})

    def _open_orders(self, venue: Any) -> list:
        opens = list(venue.open_orders())
        prefix = f"t-{self.cfg.app['client_prefix']}-"
        own = [o for o in opens if isinstance(getattr(o, 'text', None), str) and o.text.startswith(prefix)]
        foreign = len(opens) - len(own)
        lines = [f'открыто {len(opens)}: бота {len(own)}, посторонних {foreign}']
        status = OK
        if foreign:
            lines.append('на аккаунте есть посторонние заявки по паре — нужен отдельный (суб)аккаунт только '
                         'для бота; live остановится с FOREIGN_OPEN_ORDER_USE_EXCLUSIVE_ACCOUNT')
            self.warnings.append('посторонние открытые заявки по паре')
            status = WARN
        self.add('open_orders', 'Открытые заявки пары', status, False, lines,
                 {'open': len(opens), 'own': len(own), 'foreign': foreign})
        return opens

    def _permissions(self, venue: Any, opens: list | None) -> None:
        lines = ['API Gate не сообщает права ключа. Проверьте в веб-интерфейсе: только чтение и спот-торговля, '
                 'БЕЗ вывода и переводов (отсутствие права на вывод через API не проверяется)']
        if not self.probe_write:
            lines.append('проба права записи не выполнялась (doctor --probe-write: отправляет countdown_cancel_all '
                         'timeout=0, что выключает DMS пары; только когда нет открытых заявок)')
            self.add('key_permissions', 'Права ключа', SKIP, False, lines)
            return
        if opens is None:
            lines.append('открытые заявки пары не прочитаны — проба записи НЕ выполнена (она выключила бы DMS)')
            self.add('key_permissions', 'Права ключа', FAIL, True, lines, {'probe': 'skipped_open_orders_unknown'})
            return
        if opens:
            lines.append(f'есть открытые заявки ({len(opens)}) — проба записи НЕ выполнена (она выключила бы DMS)')
            self.add('key_permissions', 'Права ключа', FAIL, True, lines, {'probe': 'skipped_open_orders'})
            return
        result = venue.probe_write_permission()
        label = result.get('label')
        if result.get('ok'):
            lines.append('проба записи: подписанный запрос принят (спот-торговля разрешена); DMS пары выключен')
            status = OK
        else:
            hints = {'READ_ONLY': 'ключ только для чтения', 'FORBIDDEN': 'нет права спот-торговли',
                     'INVALID_KEY': 'неверный ключ', 'IP_FORBIDDEN': 'IP не в белом списке ключа',
                     'INVALID_SIGNATURE': 'неверный секрет или подпись'}
            lines.append(f"проба записи отклонена: {label or result.get('outcome')}"
                         + (f' ({hints[label]})' if label in hints else '')
                         + (f"; {result['error']}" if result.get('error') else ''))
            status = FAIL
        self.add('key_permissions', 'Права ключа', status, True, lines,
                 {'probe': result.get('outcome'), 'label': label})

    def _balances(self, venue: Any) -> None:
        balances = venue.balances()
        lines = []
        for currency in (self.quote, self.base):
            balance = balances.get(currency)
            if balance is None:
                lines.append(f'{currency}: нет данных')
                continue
            lines.append(f'{currency}: всего {fmt(balance.total)}, свободно {fmt(balance.free)}, '
                         f'в заявках {fmt(balance.hold)}')
        self.add('balances', 'Балансы', OK, True, lines,
                 {c: {'total': b.total, 'hold': b.hold} for c, b in balances.items()
                  if c in (self.base, self.quote)})

    def _fees(self, venue: Any) -> None:
        info = venue.fee_info()
        maker, taker = info['maker_fee_pct'], info['taker_fee_pct']
        estimate = self.cfg.portfolio['fee_estimate_pct']
        lines = [f'maker {fmt(maker)}%, taker {fmt(taker)}%; оценка в конфиге portfolio.fee_estimate_pct '
                 f'{fmt(estimate)}%']
        status = OK
        if maker > estimate:
            lines.append('фактическая комиссия maker выше оценки в конфиге — увеличьте portfolio.fee_estimate_pct')
            status = WARN
        if info.get('gt_deduction_enabled'):
            lines.append('включено списание комиссии в GT: комиссии в GT учитываются отдельно и НЕ пересчитываются '
                         'в USDT; рекомендуется выключить')
            self.warnings.append('списание комиссии в GT включено')
            status = WARN
        elif info.get('gt_deduction_enabled') is None:
            lines.append(f"состояние списания в GT неизвестно ({info.get('debit_fee_error') or 'н/д'})")
        self.add('fees', 'Комиссии', status, False, lines,
                 {'maker_fee_pct': maker, 'taker_fee_pct': taker, 'gt_deduction_enabled': info.get('gt_deduction_enabled')})

    def check_live_gates(self) -> None:
        """Local live prerequisites (informational; ``run --live`` enforces them)."""
        cfg = self.cfg
        if not cfg.live:
            return
        missing = []
        if not cfg.safety['dedicated_account_ack']:
            missing.append('safety.dedicated_account_ack: true')
        if not cfg.safety['dead_man_switch']:
            missing.append('safety.dead_man_switch: true')
        if cfg.safety['require_telegram'] and not (cfg.telegram['enabled'] and cfg.secrets.has_telegram):
            missing.append('Telegram (telegram.enabled и TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)')
        if not cfg.secrets.has_gate_keys:
            missing.append('GATE_API_KEY/GATE_API_SECRET')
        lines = ['для run --live также нужна переменная оболочки GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING '
                 '(не в .env)']
        if missing:
            lines.insert(0, 'не выполнено: ' + '; '.join(missing))
        self.add('live_gates', 'Готовность к live (локально)', WARN if missing else OK, False, lines)

    # ----------------------------------------------------------------- run
    def run(self) -> dict:
        started_ms = int(time.time() * 1000)
        try:
            self._guard('config', 'Конфигурация', True, self.check_config)
            self._guard('pair', f'Пара {self.pair} (метаданные Gate)', True, self.check_pair)
            self._guard('clock', 'Часы', True, self.check_clock)
            self._guard('instruments', 'Инструменты источников', True, self.check_instruments)
            self._guard('fx', 'Курс USDC→USDT', True, self.check_fx)
            self._guard('gate_futures_index', 'Состав индекса Gate futures', False, self.check_gate_futures_index)
            self._guard('quotes', 'Котировки и индекс', True, self.check_quotes)
            self._guard('minimums', 'Размеры уровней и минимумы пары', True, self.check_minimums)
            self._guard('iceberg_live', 'Iceberg в live', False, self.check_iceberg_gate)
            self._guard('account', 'Аккаунт Gate (ключи)', True, self.check_account)
            self._guard('live_gates', 'Готовность к live (локально)', False, self.check_live_gates)
        finally:
            own, self._own_http = self._own_http, None
            if own is not None:
                try:
                    own.close()
                except Exception:
                    pass
        failed = [c.key for c in self.checks if c.failed]
        report = {
            'created_at_ms': started_ms, 'currency_pair': self.pair, 'mode': self.cfg.mode, 'ok': not failed,
            'failed_required': failed, 'trading_tested': False, 'live_ready': False,
            'probe_write': self.probe_write, 'seconds': self.seconds,
            'checks': {c.key: c.as_dict() for c in self.checks}, 'warnings': list(self.warnings),
            'note': ('Только чтение: заявки, отмены и Telegram не отправлялись'
                     + ('; проба записи выключила DMS пары.' if self.probe_write else '; DMS не трогался.')),
        }
        for line in self.format(report):
            self.out(line)
        if self.write_report:
            try:
                from .status import write_status
                path = Path(self.cfg.state_dir) / REPORT_NAME
                if write_status(path, report):
                    self.out(f'Отчёт сохранён: {path}')
            except Exception as exc:
                LOG.warning('doctor report not written: %s', type(exc).__name__)
        return report

    def format(self, report: dict) -> list[str]:
        lines = [f"gate_stonk doctor: {self.pair}, режим {self.cfg.mode}, {_utc(report['created_at_ms'])}"]
        for check in self.checks:
            head = f'{LABELS[check.status]} {check.title}'
            if check.status == FAIL and check.required:
                head += ' (обязательная)'
            lines.append(head)
            lines += [f'       {line}' for line in check.lines]
        if report['ok']:
            lines.append(f"Итог: обязательные проверки пройдены (предупреждений: "
                         f"{sum(1 for c in self.checks if c.status == WARN)}).")
        else:
            lines.append('Итог: НЕ пройдены обязательные проверки: ' + ', '.join(report['failed_required']) + '.')
        lines.append(report['note'])
        return lines


def run_doctor(cfg: Any, **kwargs: Any) -> dict:
    return Doctor(cfg, **kwargs).run()
