"""Configuration loading and validation (see DESIGN §3 and config.example.yaml).

``config.example.yaml`` in the repository root is the canonical schema: every key
there is accepted and validated, unknown keys anywhere are rejected (typo guard)
and optional keys that are missing get the defaults shown in that file.

Conventions ported from the reference validator:

* numbers are ``Decimal`` (``gate_stonk.dec.D``); booleans, NaN and Infinity are
  rejected where a number is expected; numeric strings (``'1.5'``) are accepted;
* every error is a ``ConfigError`` whose message starts with the dotted key path
  (``pair.buy.levels[1].edge_pct: ...``);
* ``null`` disables a limit, ``0`` forbids further volume under that limit; the
  order size can never be disabled (at least one positive size per level);
* the file is read as ``utf-8-sig`` (Windows Notepad BOM) and duplicate YAML keys
  are rejected at any depth;
* ``.env`` next to the config is loaded with python-dotenv WITHOUT overriding the
  real environment. ``GATE_LIVE_CONFIRM`` is never imported from that file: the
  live confirmation must come from the shell (see ``env_file_has_live_confirm``).

Types in ``Config.data`` (the validated, deep-copied dict with defaults):
integers are ``int`` (counts, ``dms_timeout_seconds``, ``depth_levels`` ...),
flags ``bool``, enumerations/identifiers ``str``, money/percent/quantity
``Decimal``; timing keys ending in ``_seconds`` are ``float`` in the sections
app/index/index.fx/index.sources/safety/telegram. Everything under ``pair`` is
``Decimal`` (including ``min_reprice_seconds``, as in ``LevelConfig``).

Required keys (no default) are the identity (app.mode/campaign_id/client_prefix,
market.currency_pair/base/quote/account), index.sources, the edge guards
(safety.min_new_edge_pct/hard_min_edge_pct), pair.mode, every limit
(pair.max_inventory_*, pair.<side>.max_*: write null to disable) and the levels.
Besides the canonical keys, ``telegram`` also accepts the reference notifier keys
``healthcheck_seconds`` (30), ``send_interval_seconds`` (1.1) and
``order_notify_interval_seconds`` (300).

The dataclasses below are the contract used by index/pricing/engine; their
public names/fields are fixed by the architect (fields may be added).
"""
from __future__ import annotations

import copy
import logging
import os
import re
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from typing import Any, NoReturn

import yaml
from dotenv import dotenv_values

from .dec import D, HUNDRED, ONE, ZERO
from .errors import ConfigError
from .types import SIDES

LOG = logging.getLogger(__name__)

SUPPORTED_VENUES = ('bybit_linear', 'bitget_usdt_futures', 'bingx_swap', 'lighter', 'gate_futures_usdt')
PRICE_KINDS = ('mid', 'microprice', 'mark', 'last')
INDEX_METHODS = ('weighted_median', 'weighted_mean')
FX_MODES = ('fixed', 'binance_spot', 'gate_spot')
LIVE_CONFIRM_VALUE = 'I_UNDERSTAND_SPOT_TRADING'
LIVE_CONFIRM_ENV = 'GATE_LIVE_CONFIRM'

APP_MODES = ('paper', 'live')
ACCOUNTS = ('spot', 'unified')
DEPTH_LEVELS = (5, 10, 20, 50, 100)
DEPTH_INTERVALS = ('100ms', '1000ms')
LOG_LEVELS = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
PAIR_MODES = ('buy', 'sell', 'both')
OUT_OF_RANGE = ('nearest_index', 'clamp')
INSUFFICIENT_DEPTH = ('clamp', 'wait')
PROFIT_FLOOR_MODES = ('hold', 'quote')
TIME_IN_FORCE = ('poc',)
FILL_MODELS = ('trade_through',)
SOURCE_QUOTES = ('USDT', 'USDC')
MARK_LAST = ('mark', 'last')
RESERVED_SOURCE_IDS = ('gate_spot', 'fx')   # feed-hub keys of the traded book and the FX feed
GATE_MAX_BATCH_CANCEL = 20                  # POST /spot/cancel_batch_orders: max 20 orders per request
GATE_MIN_COUNTDOWN_SECONDS = 5              # countdown_cancel_all: "At least 5 seconds, 0 means cancel countdown"
DMS_ACTION_RESERVE = 140                    # reference: hourly signed actions beyond DMS renewals

CAMPAIGN_RE = re.compile(r'[A-Za-z0-9_]{1,32}')
CLIENT_PREFIX_RE = re.compile(r'[a-z0-9]{1,6}')
LEVEL_ID_RE = re.compile(r'[a-z0-9]{1,4}')
SOURCE_ID_RE = re.compile(r'[a-z][a-z0-9_]{0,31}')
ASSET_RE = re.compile(r'[A-Z0-9]{1,20}')
PAIR_RE = re.compile(r'[A-Z0-9]{1,20}_[A-Z0-9]{1,20}')
SYMBOL_RE = re.compile(r'[A-Za-z0-9_./:-]{1,40}')
URL_RE = re.compile(r'\S+')

ENV_GATE_KEY = 'GATE_API_KEY'
ENV_GATE_SECRET = 'GATE_API_SECRET'
ENV_TELEGRAM_TOKEN = 'TELEGRAM_BOT_TOKEN'
ENV_TELEGRAM_CHAT = 'TELEGRAM_CHAT_ID'

# --------------------------------------------------------------------------------------
# Schema: required keys and optional keys with the defaults of config.example.yaml.
# --------------------------------------------------------------------------------------
SECTIONS_REQUIRED = ('app', 'market', 'index', 'safety', 'pair')
SECTIONS_OPTIONAL = ('execution', 'portfolio', 'paper', 'telegram')
SECTION_ORDER = ('app', 'market', 'index', 'safety', 'execution', 'portfolio', 'pair', 'paper', 'telegram')

REQUIRED: dict[str, tuple[str, ...]] = {
    'app': ('mode', 'campaign_id', 'client_prefix'),
    'market': ('currency_pair', 'base', 'quote', 'account'),
    'index': ('sources',),
    'index.fx': ('mode',),
    'source': ('id', 'venue', 'symbol', 'quote', 'price', 'weight'),
    'safety': ('min_new_edge_pct', 'hard_min_edge_pct'),
    'execution': (),
    'portfolio': (),
    # Limits have no default: write null explicitly to disable one (0 forbids).
    'pair': ('mode', 'max_inventory_usdt', 'max_inventory_qty', 'buy', 'sell'),
    'side': ('max_total_qty', 'max_total_notional_usdt', 'max_daily_notional_usdt', 'levels'),
    'level': ('id',),
    'paper': (),
    'telegram': (),
}

DEFAULTS: dict[str, dict[str, Any]] = {
    'app': {
        'state_dir': 'runtime_stonk',
        'loop_seconds': Decimal('0.2'),
        'poll_seconds': 5,
        'request_timeout_seconds': 5,
        'request_expiry_seconds': 10,
        'read_error_grace_seconds': 12,
        'status_interval_seconds': 1,
        'log_level': 'INFO',
    },
    'market': {
        'rest_url': 'https://api.gateio.ws/api/v4',
        'spot_ws_url': 'wss://api.gateio.ws/ws/v4/',
        'depth_levels': 20,
        'depth_interval': '100ms',
        'expected_precision': None,
        'expected_amount_precision': None,
    },
    'index': {
        'method': 'weighted_median',
        'min_sources': 2,
        'max_age_seconds': 3,
        'max_timestamp_skew_seconds': 2,
        'max_source_deviation_pct': Decimal('1.0'),
        'basis_offset_pct': 0,
        'rejoin_snapshots': 3,
        'observation_interval_seconds': Decimal('0.2'),
        # 'fx' is handled separately: required when an enabled source quotes USDC.
    },
    'index.fx': {
        'usdc_to_usdt': 1,
        'max_depeg_pct': Decimal('0.5'),
        'max_age_seconds': 3,
        'max_spread_pct': Decimal('0.2'),
    },
    'source': {
        'required': False,
        # max_spread_pct: required for mid/microprice, defaults to null for mark/last.
        # max_age_seconds: defaults to index.max_age_seconds.
        # heartbeat_seconds: defaults to 3 for bybit_linear, else 0.
        'rest_refresh_after_seconds': None,
        'enabled': True,
        'url': None,
        'market_index': None,
    },
    'safety': {
        'dedicated_account_ack': False,
        'dead_man_switch': True,
        'dms_timeout_seconds': 45,
        'dms_refresh_seconds': 15,
        'require_telegram': True,
        'terminal_settle_seconds': 2,
        'unknown_create_margin_seconds': 30,
        'max_open_orders': 8,
        'max_signed_actions_per_hour': 1000,
        'max_order_requests_per_10s': 8,
        'rate_limit_utilization': Decimal('0.5'),
        'max_clock_skew_ms': 1000,
        'clock_check_seconds': 300,
    },
    'execution': {
        'time_in_force': 'poc',
        'use_batch': True,
        'reprice_via_amend': False,
        'iceberg_poc_verified': False,
    },
    'portfolio': {
        'cash_reserve_usdt': 0,
        'fee_reserve_pct': Decimal('0.3'),
        'fee_estimate_pct': Decimal('0.1'),
        'inventory_cap_tolerance_pct': 1,
    },
    'pair': {
        'inventory_reserve_qty': 0,
        'initial_avg_price': None,
    },
    'side': {
        'band_tolerance_pct': Decimal('0.5'),
        'reprice_threshold_pct': Decimal('0.5'),
        'min_reprice_seconds': 3,
        'deeper_tolerance_pct': None,
    },
    'side.buy': {'compensate_base_fee': False},
    'side.sell': {'min_profit_pct': None, 'profit_floor_mode': 'hold'},
    'paper': {
        'initial_usdt': 500,
        'initial_base': 0,
        'fill_model': 'trade_through',
        'fill_on_touch': False,
        'fee_pct': None,
    },
    'telegram': {
        'enabled': False,
        'max_unhealthy_seconds': 120,
        'summary_minutes': 60,
        'notify_orders': False,
        # Not in config.example.yaml; reference defaults kept for the notifier.
        'healthcheck_seconds': 30,
        'send_interval_seconds': Decimal('1.1'),
        'order_notify_interval_seconds': 300,
    },
}

SOURCE_OPTIONAL_SPECIAL = ('max_spread_pct', 'max_age_seconds', 'heartbeat_seconds')
LEVEL_EDGE_KEYS = ('edge_pct', 'min_edge_pct', 'max_edge_pct')
LEVEL_RANGE_ONLY = ('skip_notional_usdt', 'out_of_range', 'insufficient_depth')
LEVEL_SIZE_KEYS = ('order_notional_usdt', 'order_qty', 'iceberg_display_qty', 'iceberg_display_pct')
LEVEL_INHERITED = ('band_tolerance_pct', 'reprice_threshold_pct', 'min_reprice_seconds', 'deeper_tolerance_pct')
LEVEL_OPTIONAL = LEVEL_EDGE_KEYS + LEVEL_RANGE_ONLY + LEVEL_SIZE_KEYS + LEVEL_INHERITED


# --------------------------------------------------------------------------------------
# Architect contract dataclasses (names/fields fixed; fields may be added).
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class SourceConfig:
    id: str
    venue: str                      # one of SUPPORTED_VENUES
    symbol: str
    quote: str                      # 'USDT' | 'USDC'
    price: str                      # one of PRICE_KINDS
    weight: Decimal
    required: bool
    max_spread_pct: Decimal | None  # None allowed only for price mark/last
    max_age_seconds: float
    heartbeat_seconds: float = 0.0
    rest_refresh_after_seconds: float | None = None
    enabled: bool = True
    url: str | None = None
    market_index: int | None = None  # Lighter: optional pinned market index


@dataclass(frozen=True)
class FxSettings:
    mode: str                       # one of FX_MODES
    usdc_to_usdt: Decimal
    max_depeg_pct: Decimal
    max_age_seconds: float
    max_spread_pct: Decimal


@dataclass(frozen=True)
class IndexSettings:
    method: str
    min_sources: int
    max_age_seconds: float
    max_timestamp_skew_seconds: float
    max_source_deviation_pct: Decimal
    basis_offset_pct: Decimal
    rejoin_snapshots: int
    observation_interval_seconds: float
    fx: FxSettings
    sources: tuple[SourceConfig, ...]   # enabled sources only, config order


@dataclass(frozen=True)
class LevelConfig:
    id: str                         # [a-z0-9]{1,4}, unique across both sides
    side: str                       # 'buy' | 'sell'
    mode: str                       # 'exact' | 'range'
    edge_pct: Decimal | None = None
    min_edge_pct: Decimal | None = None
    max_edge_pct: Decimal | None = None
    skip_notional_usdt: Decimal = Decimal(0)
    out_of_range: str = 'nearest_index'   # 'nearest_index' | 'clamp'
    insufficient_depth: str = 'clamp'     # 'clamp' | 'wait'
    order_notional_usdt: Decimal | None = None
    order_qty: Decimal | None = None
    iceberg_display_qty: Decimal | None = None
    iceberg_display_pct: Decimal | None = None
    band_tolerance_pct: Decimal = Decimal('0.15')
    reprice_threshold_pct: Decimal = Decimal('0.15')
    min_reprice_seconds: Decimal = Decimal(3)
    deeper_tolerance_pct: Decimal | None = None

    @property
    def slot(self) -> str:
        return f'{self.side}:{self.id}'

    @property
    def has_iceberg(self) -> bool:
        return self.iceberg_display_qty is not None or self.iceberg_display_pct is not None


@dataclass(frozen=True)
class Secrets:
    gate_key: str | None = field(default=None, repr=False)
    gate_secret: str | None = field(default=None, repr=False)
    telegram_token: str | None = field(default=None, repr=False)
    telegram_chat: str | None = field(default=None, repr=False)

    def __repr__(self) -> str:  # never leak secrets through repr/logging
        return (f'Secrets(gate_key={"set" if self.gate_key else None}, '
                f'gate_secret={"set" if self.gate_secret else None}, '
                f'telegram_token={"set" if self.telegram_token else None}, '
                f'telegram_chat={"set" if self.telegram_chat else None})')

    __str__ = __repr__

    @property
    def has_gate_keys(self) -> bool:
        return bool(self.gate_key) and bool(self.gate_secret)

    @property
    def has_telegram(self) -> bool:
        return bool(self.telegram_token) and bool(self.telegram_chat)


def secrets_from_env() -> Secrets:
    """Read credentials from the process environment (stripped; empty -> None)."""
    def get(name: str) -> str | None:
        value = os.environ.get(name, '').strip()
        return value or None
    return Secrets(gate_key=get(ENV_GATE_KEY), gate_secret=get(ENV_GATE_SECRET),
                   telegram_token=get(ENV_TELEGRAM_TOKEN), telegram_chat=get(ENV_TELEGRAM_CHAT))


# --------------------------------------------------------------------------------------
# Validator primitives. Every error names the full dotted path.
# --------------------------------------------------------------------------------------
_MISSING = object()


def _fail(path: str, message: str) -> NoReturn:
    raise ConfigError(f'{path}: {message}')


def _join(path: str, key: str) -> str:
    return f'{path}.{key}' if path else key


def _mapping(value: Any, path: str) -> dict:
    if not isinstance(value, dict):
        _fail(path, 'must be a mapping (key: value)')
    for key in value:
        if not isinstance(key, str):
            _fail(path, f'keys must be strings, found {key!r}')
    return value


def _keys(row: dict, path: str, required: tuple[str, ...], optional: tuple[str, ...] | dict) -> None:
    """Exact key set: required <= keys <= required | optional. Unknown keys first (typo guard)."""
    allowed = set(required) | set(optional)
    for key in row:
        if key not in allowed:
            _fail(_join(path, key), f'unknown key (allowed: {", ".join(sorted(allowed))})')
    for key in required:
        if key not in row:
            _fail(_join(path, key), 'required key is missing')


def _with_defaults(row: dict, defaults: dict) -> dict:
    out = dict(row)
    for key, value in defaults.items():
        if key not in out:
            out[key] = copy.deepcopy(value)
    return out


def _bound_text(gt, ge, lt, le) -> str:
    parts = []
    if gt is not None:
        parts.append(f'> {gt}')
    if ge is not None:
        parts.append(f'>= {ge}')
    if lt is not None:
        parts.append(f'< {lt}')
    if le is not None:
        parts.append(f'<= {le}')
    return ' and '.join(parts)


def _number(value: Any, path: str, *, nullable: bool = False, gt: Any = None, ge: Any = None,
            lt: Any = None, le: Any = None) -> Decimal | None:
    """Strict Decimal: rejects bool, NaN, Infinity and garbage; optional bounds."""
    if value is None:
        if nullable:
            return None
        _fail(path, 'must be a number (null is not allowed here)')
    if isinstance(value, bool):
        _fail(path, 'must be a number, not true/false')
    try:
        result = D(value)
    except (ValueError, TypeError):
        _fail(path, f'invalid number {value!r} (NaN and Infinity are not allowed)')
    if ((gt is not None and not result > D(gt)) or (ge is not None and not result >= D(ge))
            or (lt is not None and not result < D(lt)) or (le is not None and not result <= D(le))):
        _fail(path, f'must be {_bound_text(gt, ge, lt, le)} (got {result})')
    return result


def _integer(value: Any, path: str, *, nullable: bool = False, ge: int | None = None,
             le: int | None = None, choices: tuple[int, ...] | None = None) -> int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(path, f'must be an integer (got {value!r})')
    if choices is not None and value not in choices:
        _fail(path, f'must be one of {", ".join(str(c) for c in choices)} (got {value})')
    if (ge is not None and value < ge) or (le is not None and value > le):
        _fail(path, f'must be an integer {_bound_text(None, ge, None, le)} (got {value})')
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        _fail(path, f'must be true/false (got {value!r})')
    return value


def _choice(value: Any, path: str, choices: tuple[str, ...]) -> str:
    if not isinstance(value, str) or value not in choices:
        _fail(path, f'must be one of: {", ".join(choices)} (got {value!r})')
    return value


def _text(value: Any, path: str, pattern: re.Pattern, hint: str) -> str:
    if not isinstance(value, str):
        _fail(path, f'must be a string matching {hint} (got {value!r})')
    if not pattern.fullmatch(value):
        _fail(path, f'must match {hint} (got {value!r})')
    return value


def _url(value: Any, path: str, scheme: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value.startswith(scheme) or len(value) <= len(scheme) \
            or not URL_RE.fullmatch(value):
        _fail(path, f'must be a {scheme} URL (got {value!r})')
    return value


def _section(raw: dict, name: str) -> dict:
    value = raw.get(name, _MISSING)
    if value is _MISSING:
        if name in SECTIONS_REQUIRED:
            _fail(name, 'required section is missing')
        value = {}
    return _mapping(value, name)


# --------------------------------------------------------------------------------------
# Section validators. Each returns a NEW normalized dict (input is never mutated).
# --------------------------------------------------------------------------------------
def _validate_app(raw: dict) -> dict:
    p = 'app'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['mode'] = _choice(row['mode'], 'app.mode', APP_MODES)
    out['campaign_id'] = _text(row['campaign_id'], 'app.campaign_id', CAMPAIGN_RE, '[A-Za-z0-9_]{1,32}')
    out['client_prefix'] = _text(row['client_prefix'], 'app.client_prefix', CLIENT_PREFIX_RE, '[a-z0-9]{1,6}')
    state_dir = row['state_dir']
    if not isinstance(state_dir, str) or not state_dir.strip():
        _fail('app.state_dir', 'must be a nonempty path')
    out['state_dir'] = state_dir
    loop = out['loop_seconds'] = _number(row['loop_seconds'], 'app.loop_seconds', gt=0)
    poll = out['poll_seconds'] = _number(row['poll_seconds'], 'app.poll_seconds', gt=0)
    out['request_timeout_seconds'] = _number(row['request_timeout_seconds'], 'app.request_timeout_seconds',
                                             gt=0, le=20)
    out['request_expiry_seconds'] = _number(row['request_expiry_seconds'], 'app.request_expiry_seconds',
                                            ge=5, le=60)
    out['read_error_grace_seconds'] = _number(row['read_error_grace_seconds'], 'app.read_error_grace_seconds',
                                              gt=0)
    out['status_interval_seconds'] = _number(row['status_interval_seconds'], 'app.status_interval_seconds',
                                             gt=0, le=60)
    if poll < 1:
        _fail('app.poll_seconds', 'must be >=1')
    if poll > 60:
        _fail('app.poll_seconds', 'must be <=60')
    if loop > Decimal('0.2'):
        _fail('app.loop_seconds', 'must be <=0.2 for prompt local STOP checks')
    if loop > poll:
        _fail('app.loop_seconds', 'must not exceed app.poll_seconds')
    out['log_level'] = _choice(row['log_level'], 'app.log_level', LOG_LEVELS)
    return out


def _validate_market(raw: dict) -> dict:
    p = 'market'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['currency_pair'] = _text(row['currency_pair'], 'market.currency_pair', PAIR_RE, 'BASE_QUOTE (e.g. STONK_USDT)')
    out['base'] = _text(row['base'], 'market.base', ASSET_RE, '[A-Z0-9]{1,20}')
    out['quote'] = _text(row['quote'], 'market.quote', ASSET_RE, '[A-Z0-9]{1,20}')
    if out['quote'] != 'USDT':
        _fail('market.quote', 'only USDT is supported (sizes, limits and the index are in USDT)')
    if out['currency_pair'] != f"{out['base']}_{out['quote']}":
        _fail('market.currency_pair', f"must equal market.base + '_' + market.quote "
                                      f"({out['base']}_{out['quote']})")
    out['account'] = _choice(row['account'], 'market.account', ACCOUNTS)
    out['rest_url'] = _url(row['rest_url'], 'market.rest_url', 'https://')
    out['spot_ws_url'] = _url(row['spot_ws_url'], 'market.spot_ws_url', 'wss://')
    out['depth_levels'] = _integer(row['depth_levels'], 'market.depth_levels', choices=DEPTH_LEVELS)
    out['depth_interval'] = _choice(row['depth_interval'], 'market.depth_interval', DEPTH_INTERVALS)
    for key in ('expected_precision', 'expected_amount_precision'):
        out[key] = _integer(row[key], f'market.{key}', nullable=True, ge=0, le=18)
    return out


def _validate_fx(raw: Any) -> dict:
    p = 'index.fx'
    raw = _mapping(raw, p)
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['mode'] = _choice(row['mode'], 'index.fx.mode', FX_MODES)
    out['usdc_to_usdt'] = _number(row['usdc_to_usdt'], 'index.fx.usdc_to_usdt', gt=0)
    out['max_depeg_pct'] = _number(row['max_depeg_pct'], 'index.fx.max_depeg_pct', gt=0, lt=100)
    out['max_age_seconds'] = _number(row['max_age_seconds'], 'index.fx.max_age_seconds', gt=0, le=60)
    out['max_spread_pct'] = _number(row['max_spread_pct'], 'index.fx.max_spread_pct', gt=0, lt=100)
    if out['mode'] == 'fixed' and abs(out['usdc_to_usdt'] - ONE) * HUNDRED > out['max_depeg_pct']:
        _fail('index.fx.usdc_to_usdt', 'fixed rate deviates from 1 by more than index.fx.max_depeg_pct '
                                       '(every USDC source would be excluded with FX_DEPEG)')
    return out


def _validate_source(raw: Any, path: str, default_max_age: Decimal) -> dict:
    raw = _mapping(raw, path)
    _keys(raw, path, REQUIRED['source'], tuple(DEFAULTS['source']) + SOURCE_OPTIONAL_SPECIAL)
    row = _with_defaults(raw, DEFAULTS['source'])
    out: dict[str, Any] = {}
    sid = out['id'] = _text(row['id'], f'{path}.id', SOURCE_ID_RE, '[a-z][a-z0-9_]{0,31}')
    if sid in RESERVED_SOURCE_IDS:
        _fail(f'{path}.id', f'{sid!r} is reserved (feed keys {", ".join(RESERVED_SOURCE_IDS)})')
    venue = out['venue'] = _choice(row['venue'], f'{path}.venue', SUPPORTED_VENUES)
    symbol = row['symbol']
    if not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol):
        _fail(f'{path}.symbol', f'must be the exact venue symbol, [A-Za-z0-9_./:-]{{1,40}} (got {symbol!r})')
    out['symbol'] = symbol
    out['quote'] = _choice(row['quote'], f'{path}.quote', SOURCE_QUOTES)
    price = out['price'] = _choice(row['price'], f'{path}.price', PRICE_KINDS)
    out['weight'] = _number(row['weight'], f'{path}.weight', gt=0)
    out['required'] = _boolean(row['required'], f'{path}.required')
    spread = row.get('max_spread_pct', _MISSING)
    if spread is _MISSING:
        if price not in MARK_LAST:
            _fail(f'{path}.max_spread_pct', f'required for price {price} (null is allowed only for mark/last)')
        spread = None
    if spread is None and price not in MARK_LAST:
        _fail(f'{path}.max_spread_pct', f'null is allowed only for price mark/last, not {price}')
    out['max_spread_pct'] = _number(spread, f'{path}.max_spread_pct', nullable=True, gt=0, lt=100)
    out['max_age_seconds'] = _number(row.get('max_age_seconds', default_max_age), f'{path}.max_age_seconds',
                                     gt=0, le=60)
    heartbeat_default = 3 if venue == 'bybit_linear' else 0
    out['heartbeat_seconds'] = _number(row.get('heartbeat_seconds', heartbeat_default),
                                       f'{path}.heartbeat_seconds', ge=0, le=30)
    out['rest_refresh_after_seconds'] = _number(row['rest_refresh_after_seconds'],
                                                f'{path}.rest_refresh_after_seconds', nullable=True, gt=0, le=60)
    out['enabled'] = _boolean(row['enabled'], f'{path}.enabled')
    out['url'] = _url(row['url'], f'{path}.url', 'wss://', nullable=True)
    market_index = _integer(row['market_index'], f'{path}.market_index', nullable=True, ge=0)
    if market_index is not None and venue != 'lighter':
        _fail(f'{path}.market_index', 'is supported only for venue lighter')
    out['market_index'] = market_index
    return out


def _validate_index(raw: dict) -> dict:
    p = 'index'
    _keys(raw, p, REQUIRED[p], tuple(DEFAULTS[p]) + ('fx',))
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['method'] = _choice(row['method'], 'index.method', INDEX_METHODS)
    max_age = out['max_age_seconds'] = _number(row['max_age_seconds'], 'index.max_age_seconds', gt=0, le=60)
    out['max_timestamp_skew_seconds'] = _number(row['max_timestamp_skew_seconds'],
                                                'index.max_timestamp_skew_seconds', gt=0, le=60)
    out['max_source_deviation_pct'] = _number(row['max_source_deviation_pct'], 'index.max_source_deviation_pct',
                                              gt=0, lt=100)
    out['basis_offset_pct'] = _number(row['basis_offset_pct'], 'index.basis_offset_pct', gt=-50, lt=50)
    out['rejoin_snapshots'] = _integer(row['rejoin_snapshots'], 'index.rejoin_snapshots', ge=1, le=100)
    out['observation_interval_seconds'] = _number(row['observation_interval_seconds'],
                                                  'index.observation_interval_seconds', gt=0, le=60)
    sources_raw = row['sources']
    if not isinstance(sources_raw, list) or not sources_raw:
        _fail('index.sources', 'must be a nonempty list of sources')
    sources: list[dict] = []
    seen_ids: dict[str, str] = {}
    seen_feeds: dict[tuple, str] = {}
    for i, item in enumerate(sources_raw):
        path = f'index.sources[{i}]'
        source = _validate_source(item, path, max_age)
        if source['id'] in seen_ids:
            _fail(f'{path}.id', f"duplicate source id {source['id']!r} (also {seen_ids[source['id']]})")
        seen_ids[source['id']] = path
        feed = (source['venue'], source['symbol'], source['price'])
        if feed in seen_feeds:
            _fail(path, f'duplicate source: same venue, symbol and price as {seen_feeds[feed]} '
                        f'(its weight would be counted twice)')
        seen_feeds[feed] = path
        sources.append(source)
    out['sources'] = sources
    enabled = [s for s in sources if s['enabled']]
    if not enabled:
        _fail('index.sources', 'at least one enabled source is required')
    for i, source in enumerate(sources):
        if source['required'] and not source['enabled']:
            _fail(f'index.sources[{i}].required', f"required source {source['id']!r} must be enabled")
    if isinstance(row['min_sources'], int) and not isinstance(row['min_sources'], bool) \
            and not 1 <= row['min_sources'] <= len(enabled):
        _fail('index.min_sources', f'must be between 1 and the number of enabled sources ({len(enabled)}), '
                                   f"got {row['min_sources']}")
    out['min_sources'] = _integer(row['min_sources'], 'index.min_sources', ge=1, le=len(enabled))
    usdc = [s['id'] for s in enabled if s['quote'] == 'USDC']
    if 'fx' in row:
        out['fx'] = _validate_fx(row['fx'])
    elif usdc:
        _fail('index.fx', f'required: enabled source {usdc[0]!r} quotes USDC '
                          f'(configure mode fixed|binance_spot|gate_spot)')
    else:
        out['fx'] = _validate_fx({'mode': 'fixed'})
    return out


def _validate_safety(raw: dict, app: dict) -> dict:
    p = 'safety'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    for key in ('dedicated_account_ack', 'dead_man_switch', 'require_telegram'):
        out[key] = _boolean(row[key], f'safety.{key}')
    min_new = out['min_new_edge_pct'] = _number(row['min_new_edge_pct'], 'safety.min_new_edge_pct', gt=0, lt=100)
    hard = out['hard_min_edge_pct'] = _number(row['hard_min_edge_pct'], 'safety.hard_min_edge_pct', gt=0)
    if hard >= min_new:
        _fail('safety.hard_min_edge_pct', f'must be below safety.min_new_edge_pct ({min_new}): a new order '
                                          f'must not be immediately cancelled by the hard guard')
    timeout = out['dms_timeout_seconds'] = _integer(row['dms_timeout_seconds'], 'safety.dms_timeout_seconds',
                                                    ge=GATE_MIN_COUNTDOWN_SECONDS)
    refresh = out['dms_refresh_seconds'] = _number(row['dms_refresh_seconds'], 'safety.dms_refresh_seconds', gt=0)
    settle = out['terminal_settle_seconds'] = _number(row['terminal_settle_seconds'],
                                                      'safety.terminal_settle_seconds', gt=0, le=60)
    if settle < 1:
        _fail('safety.terminal_settle_seconds', 'must be >=1')
    out['max_clock_skew_ms'] = _integer(row['max_clock_skew_ms'], 'safety.max_clock_skew_ms', ge=1, le=10000)
    margin = out['unknown_create_margin_seconds'] = _number(row['unknown_create_margin_seconds'],
                                                            'safety.unknown_create_margin_seconds', gt=0, le=600)
    if margin * 1000 <= out['max_clock_skew_ms']:
        _fail('safety.unknown_create_margin_seconds', 'must exceed safety.max_clock_skew_ms (x-gate-exptime is '
                                                      'judged by the exchange clock)')
    out['max_open_orders'] = _integer(row['max_open_orders'], 'safety.max_open_orders', ge=1,
                                      le=GATE_MAX_BATCH_CANCEL)
    actions = out['max_signed_actions_per_hour'] = _integer(row['max_signed_actions_per_hour'],
                                                            'safety.max_signed_actions_per_hour', ge=1)
    out['max_order_requests_per_10s'] = _integer(row['max_order_requests_per_10s'],
                                                 'safety.max_order_requests_per_10s', ge=1, le=100)
    out['rate_limit_utilization'] = _number(row['rate_limit_utilization'], 'safety.rate_limit_utilization',
                                            gt=0, le=1)
    out['clock_check_seconds'] = _number(row['clock_check_seconds'], 'safety.clock_check_seconds', gt=0, le=3600)

    renewals = int((Decimal(3600) / refresh).to_integral_value(rounding=ROUND_CEILING))
    if actions < renewals + DMS_ACTION_RESERVE:
        # DMS renewals are counted too; new orders wait 20 actions before the cap and
        # REPRICE pauses at 40, so leave room for creates and cancels as well.
        _fail('safety.max_signed_actions_per_hour',
              f'must be >= {renewals + DMS_ACTION_RESERVE} '
              f'({renewals} DMS renewals per hour + reserve for orders and cancels)')
    # DMS timing (reference formulas; action_expiry -> app.request_expiry_seconds).
    t = Decimal(timeout)
    q, e, poll = app['request_timeout_seconds'], app['request_expiry_seconds'], app['poll_seconds']
    if t < 3 * refresh:
        _fail('safety.dms_timeout_seconds', 'DMS timeout must be >=3 times refresh interval')
    if t <= e + 3 * q + poll:
        _fail('safety.dms_timeout_seconds', f'DMS must outlive the request and polling timing margin: '
                                            f'dms_timeout_seconds must be > request_expiry_seconds + '
                                            f'3*request_timeout_seconds + poll_seconds ({e + 3 * q + poll})')
    left_at_renewal = t - refresh - poll - q
    if left_at_renewal <= max(q + max(q, e) + Decimal('0.5'), q + e + 1):
        _fail('safety.dms_timeout_seconds', 'DMS timing would cancel every order before each renewal: increase '
                                            'dms_timeout_seconds or lower dms_refresh_seconds/request_expiry_seconds')
    return out


def _validate_execution(raw: dict) -> dict:
    p = 'execution'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    tif = row['time_in_force']
    if tif not in TIME_IN_FORCE or not isinstance(tif, str):
        _fail('execution.time_in_force', f'only poc (post-only) is supported (got {tif!r})')
    out['time_in_force'] = tif
    for key in ('use_batch', 'reprice_via_amend', 'iceberg_poc_verified'):
        out[key] = _boolean(row[key], f'execution.{key}')
    return out


def _validate_portfolio(raw: dict) -> dict:
    p = 'portfolio'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['cash_reserve_usdt'] = _number(row['cash_reserve_usdt'], 'portfolio.cash_reserve_usdt', ge=0)
    out['fee_reserve_pct'] = _number(row['fee_reserve_pct'], 'portfolio.fee_reserve_pct', ge=0, lt=100)
    fee = out['fee_estimate_pct'] = _number(row['fee_estimate_pct'], 'portfolio.fee_estimate_pct', ge=0)
    if fee >= 1:
        _fail('portfolio.fee_estimate_pct', 'must be below 1 (percent per execution)')
    tolerance = out['inventory_cap_tolerance_pct'] = _number(row['inventory_cap_tolerance_pct'],
                                                             'portfolio.inventory_cap_tolerance_pct', ge=0)
    if tolerance > 10:
        _fail('portfolio.inventory_cap_tolerance_pct', 'must be 0..10 (percent)')
    return out


def _validate_level(raw: Any, path: str, side: str, side_settings: dict, min_new: Decimal) -> dict:
    raw = _mapping(raw, path)
    _keys(raw, path, REQUIRED['level'], LEVEL_OPTIONAL)
    out: dict[str, Any] = {}
    out['id'] = _text(raw['id'], f'{path}.id', LEVEL_ID_RE, '[a-z0-9]{1,4}')
    # "given" == present and not null for the edge keys.
    edge, low, high = (raw.get(key) for key in LEVEL_EDGE_KEYS)
    if edge is not None and (low is not None or high is not None):
        _fail(path, 'edge_pct (exact level) is exclusive with min_edge_pct/max_edge_pct (range level)')
    if edge is None and (low is None) != (high is None):
        _fail(path, 'a range level needs both min_edge_pct and max_edge_pct')
    if edge is None and low is None:
        _fail(path, 'set edge_pct (exact level) or min_edge_pct + max_edge_pct (range level)')
    upper_cap = 100 if side == 'buy' else 1000
    if edge is not None:
        for key in LEVEL_RANGE_ONLY:
            if key in raw:
                _fail(f'{path}.{key}', 'only for range levels (min_edge_pct + max_edge_pct)')
        value = out['edge_pct'] = _number(edge, f'{path}.edge_pct')
        if value < min_new:
            _fail(f'{path}.edge_pct', f'must be >= safety.min_new_edge_pct ({min_new})')
        if value >= upper_cap:
            _fail(f'{path}.edge_pct', _edge_cap_message(side))
    else:
        low_value = out['min_edge_pct'] = _number(low, f'{path}.min_edge_pct')
        high_value = out['max_edge_pct'] = _number(high, f'{path}.max_edge_pct')
        if low_value < min_new:
            _fail(f'{path}.min_edge_pct', f'must be >= safety.min_new_edge_pct ({min_new})')
        if low_value >= high_value:
            _fail(f'{path}.min_edge_pct', 'must be below max_edge_pct (use edge_pct for an exact level)')
        if high_value >= upper_cap:
            _fail(f'{path}.max_edge_pct', _edge_cap_message(side))
        if 'skip_notional_usdt' not in raw:
            _fail(f'{path}.skip_notional_usdt', 'required for a range level (0 allowed)')
        out['skip_notional_usdt'] = _number(raw['skip_notional_usdt'], f'{path}.skip_notional_usdt', ge=0)
        out['out_of_range'] = _choice(raw.get('out_of_range', 'nearest_index'), f'{path}.out_of_range', OUT_OF_RANGE)
        out['insufficient_depth'] = _choice(raw.get('insufficient_depth', 'clamp'), f'{path}.insufficient_depth',
                                            INSUFFICIENT_DEPTH)
    notional = out['order_notional_usdt'] = _number(raw.get('order_notional_usdt'), f'{path}.order_notional_usdt',
                                                    nullable=True)
    qty = out['order_qty'] = _number(raw.get('order_qty'), f'{path}.order_qty', nullable=True)
    if notional is None and qty is None:
        _fail(path, 'at least one finite order size is required (order_notional_usdt and/or order_qty)')
    for key, value in (('order_notional_usdt', notional), ('order_qty', qty)):
        if value is not None and value <= 0:
            _fail(f'{path}.{key}', 'must be null or positive; 0 would never trade')
    display_qty = out['iceberg_display_qty'] = _number(raw.get('iceberg_display_qty'),
                                                       f'{path}.iceberg_display_qty', nullable=True, gt=0)
    display_pct = out['iceberg_display_pct'] = _number(raw.get('iceberg_display_pct'),
                                                       f'{path}.iceberg_display_pct', nullable=True, gt=0, lt=100)
    if display_qty is not None and display_pct is not None:
        _fail(path, 'iceberg_display_qty and iceberg_display_pct are mutually exclusive')
    if display_qty is not None and qty is not None and display_qty >= qty:
        _fail(f'{path}.iceberg_display_qty', f'must be below order_qty ({qty})')
    # Inherited settings: absent -> side value; present -> override (deeper_tolerance_pct: null disables).
    merged = {key: raw[key] if key in raw else side_settings[key] for key in LEVEL_INHERITED}
    out.update(_validate_tolerances(merged, path))
    return out


def _edge_cap_message(side: str) -> str:
    if side == 'buy':
        return 'edge would allow a nonpositive price (BUY edges must be < 100)'
    return 'must be < 1000 (typo guard)'


def _validate_tolerances(row: dict, path: str) -> dict:
    return {
        'band_tolerance_pct': _number(row['band_tolerance_pct'], f'{path}.band_tolerance_pct', ge=0, lt=100),
        'reprice_threshold_pct': _number(row['reprice_threshold_pct'], f'{path}.reprice_threshold_pct',
                                         ge=0, lt=100),
        'min_reprice_seconds': _number(row['min_reprice_seconds'], f'{path}.min_reprice_seconds', ge=0, le=3600),
        'deeper_tolerance_pct': _number(row['deeper_tolerance_pct'], f'{path}.deeper_tolerance_pct',
                                        nullable=True, ge=0, lt=100),
    }


def _edge_interval(level: dict) -> tuple[Decimal, Decimal]:
    if level.get('edge_pct') is not None:
        return level['edge_pct'], level['edge_pct']
    return level['min_edge_pct'], level['max_edge_pct']


def _validate_side(raw: Any, side: str, enabled: bool, min_new: Decimal, seen_ids: dict[str, str]) -> dict:
    p = f'pair.{side}'
    raw = _mapping(raw, p)
    defaults = {**DEFAULTS['side'], **DEFAULTS[f'side.{side}']}
    _keys(raw, p, REQUIRED['side'], defaults)
    row = _with_defaults(raw, defaults)
    out: dict[str, Any] = _validate_tolerances(row, p)
    for key in ('max_total_qty', 'max_total_notional_usdt', 'max_daily_notional_usdt'):
        out[key] = _number(row[key], f'{p}.{key}', nullable=True, ge=0)
    if side == 'buy':
        out['compensate_base_fee'] = _boolean(row['compensate_base_fee'], 'pair.buy.compensate_base_fee')
    else:
        out['min_profit_pct'] = _number(row['min_profit_pct'], 'pair.sell.min_profit_pct', nullable=True,
                                        gt=-100, lt=1000)
        out['profit_floor_mode'] = _choice(row['profit_floor_mode'], 'pair.sell.profit_floor_mode',
                                           PROFIT_FLOOR_MODES)
    levels_raw = row['levels']
    if not isinstance(levels_raw, list):
        _fail(f'{p}.levels', 'must be a list of levels')
    if enabled and not levels_raw:
        _fail(f'{p}.levels', f'at least one level is required when pair.mode allows {side}')
    levels: list[dict] = []
    previous: tuple[Decimal, Decimal] | None = None
    for i, item in enumerate(levels_raw):
        path = f'{p}.levels[{i}]'
        level = _validate_level(item, path, side, out, min_new)
        if level['id'] in seen_ids:
            _fail(f'{path}.id', f"duplicate level id {level['id']!r} (also {seen_ids[level['id']]}); "
                                f'ids must be unique across both sides')
        seen_ids[level['id']] = path
        low, high = _edge_interval(level)
        if previous is not None:
            prev_low, prev_high = previous
            if low < prev_low or high < prev_high or (low == prev_low and high == prev_high):
                _fail(path, f'levels must be strictly monotone: each level must be deeper than the previous one '
                            f'({p}.levels[{i - 1}]): exact edge_pct strictly increasing; range min_edge_pct and '
                            f'max_edge_pct non-decreasing with at least one strictly')
        previous = (low, high)
        levels.append(level)
    out['levels'] = levels
    return out


def _validate_pair(raw: dict, min_new: Decimal) -> dict:
    p = 'pair'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    mode = out['mode'] = _choice(row['mode'], 'pair.mode', PAIR_MODES)
    out['max_inventory_usdt'] = _number(row['max_inventory_usdt'], 'pair.max_inventory_usdt', nullable=True, ge=0)
    out['max_inventory_qty'] = _number(row['max_inventory_qty'], 'pair.max_inventory_qty', nullable=True, ge=0)
    out['inventory_reserve_qty'] = _number(row['inventory_reserve_qty'], 'pair.inventory_reserve_qty', ge=0)
    out['initial_avg_price'] = _number(row['initial_avg_price'], 'pair.initial_avg_price', nullable=True, gt=0)
    seen_ids: dict[str, str] = {}
    for side in SIDES:
        out[side] = _validate_side(row[side], side, mode in (side, 'both'), min_new, seen_ids)
    return out


def _validate_paper(raw: dict) -> dict:
    p = 'paper'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['initial_usdt'] = _number(row['initial_usdt'], 'paper.initial_usdt', ge=0)
    out['initial_base'] = _number(row['initial_base'], 'paper.initial_base', ge=0)
    touch = _boolean(row['fill_on_touch'], 'paper.fill_on_touch')
    if touch:
        _fail('paper.fill_on_touch', 'PAPER_TOUCH_SIMULATION_NOT_SUPPORTED: use paper.fill_model: trade_through')
    out['fill_on_touch'] = touch
    model = row['fill_model']
    if not isinstance(model, str) or model not in FILL_MODELS:
        _fail('paper.fill_model', f'must be trade_through (got {model!r})')
    out['fill_model'] = model
    out['fee_pct'] = _number(row['fee_pct'], 'paper.fee_pct', nullable=True, ge=0, lt=1)
    return out


def _validate_telegram(raw: dict, app: dict) -> dict:
    p = 'telegram'
    _keys(raw, p, REQUIRED[p], DEFAULTS[p])
    row = _with_defaults(raw, DEFAULTS[p])
    out: dict[str, Any] = {}
    out['enabled'] = _boolean(row['enabled'], 'telegram.enabled')
    unhealthy = out['max_unhealthy_seconds'] = _number(row['max_unhealthy_seconds'], 'telegram.max_unhealthy_seconds',
                                                       gt=0)
    health = out['healthcheck_seconds'] = _number(row['healthcheck_seconds'], 'telegram.healthcheck_seconds', gt=0)
    send_interval = out['send_interval_seconds'] = _number(row['send_interval_seconds'],
                                                           'telegram.send_interval_seconds', gt=0)
    if send_interval < 1:
        _fail('telegram.send_interval_seconds', 'must be >=1')
    out['order_notify_interval_seconds'] = _number(row['order_notify_interval_seconds'],
                                                   'telegram.order_notify_interval_seconds', ge=0)
    if unhealthy < 2 * health + app['request_timeout_seconds']:
        _fail('telegram.max_unhealthy_seconds',
              'must be >= 2*telegram.healthcheck_seconds + app.request_timeout_seconds')
    summary = row['summary_minutes']
    if summary is not None and (isinstance(summary, bool) or not isinstance(summary, int)
                                or not 1 <= summary <= 1440):
        _fail('telegram.summary_minutes', f'must be null or 1..1440 (got {summary!r})')
    out['summary_minutes'] = summary
    out['notify_orders'] = _boolean(row['notify_orders'], 'telegram.notify_orders')
    return out


def _floatify_seconds(section: dict) -> None:
    for key, value in section.items():
        if key.endswith('_seconds') and isinstance(value, Decimal):
            section[key] = float(value)


def validate(data: Any) -> dict:
    """Validate a raw config mapping; return a NEW normalized dict with defaults (input untouched)."""
    data = copy.deepcopy(data)
    if data is None:
        _fail('config', 'is empty; expected the sections of config.example.yaml')
    if not isinstance(data, dict):
        _fail('config', 'must be a mapping of sections (see config.example.yaml)')
    allowed = set(SECTIONS_REQUIRED) | set(SECTIONS_OPTIONAL)
    for key in data:
        if not isinstance(key, str) or key not in allowed:
            _fail(str(key), f'unknown section (allowed: {", ".join(SECTION_ORDER)})')
    app = _validate_app(_section(data, 'app'))
    market = _validate_market(_section(data, 'market'))
    index = _validate_index(_section(data, 'index'))
    safety = _validate_safety(_section(data, 'safety'), app)
    execution = _validate_execution(_section(data, 'execution'))
    portfolio = _validate_portfolio(_section(data, 'portfolio'))
    pair = _validate_pair(_section(data, 'pair'), safety['min_new_edge_pct'])
    paper = _validate_paper(_section(data, 'paper'))
    telegram = _validate_telegram(_section(data, 'telegram'), app)
    level_count = sum(len(pair[side]['levels']) for side in SIDES)
    if safety['max_open_orders'] < level_count:
        _fail('safety.max_open_orders', f'must be >= the number of configured levels ({level_count})')
    for section in (app, index, index['fx'], *index['sources'], safety, telegram):
        _floatify_seconds(section)
    return {'app': app, 'market': market, 'index': index, 'safety': safety, 'execution': execution,
            'portfolio': portfolio, 'pair': pair, 'paper': paper, 'telegram': telegram}


def _check_side(side: Any) -> str:
    if side not in SIDES:
        raise ValueError('side must be buy or sell')
    return side


def _level_config(row: dict, side: str) -> LevelConfig:
    exact = row.get('edge_pct') is not None
    return LevelConfig(
        id=row['id'], side=side, mode='exact' if exact else 'range',
        edge_pct=row.get('edge_pct'), min_edge_pct=row.get('min_edge_pct'), max_edge_pct=row.get('max_edge_pct'),
        skip_notional_usdt=row.get('skip_notional_usdt', ZERO),
        out_of_range=row.get('out_of_range', 'nearest_index'),
        insufficient_depth=row.get('insufficient_depth', 'clamp'),
        order_notional_usdt=row['order_notional_usdt'], order_qty=row['order_qty'],
        iceberg_display_qty=row['iceberg_display_qty'], iceberg_display_pct=row['iceberg_display_pct'],
        band_tolerance_pct=row['band_tolerance_pct'], reprice_threshold_pct=row['reprice_threshold_pct'],
        min_reprice_seconds=row['min_reprice_seconds'], deeper_tolerance_pct=row['deeper_tolerance_pct'])


def _source_config(row: dict) -> SourceConfig:
    refresh = row['rest_refresh_after_seconds']
    return SourceConfig(
        id=row['id'], venue=row['venue'], symbol=row['symbol'], quote=row['quote'], price=row['price'],
        weight=row['weight'], required=row['required'], max_spread_pct=row['max_spread_pct'],
        max_age_seconds=float(row['max_age_seconds']), heartbeat_seconds=float(row['heartbeat_seconds']),
        rest_refresh_after_seconds=None if refresh is None else float(refresh),
        enabled=row['enabled'], url=row['url'], market_index=row['market_index'])


# --------------------------------------------------------------------------------------
# Config object.
# --------------------------------------------------------------------------------------
class Config:
    """Validated configuration.

    ``Config(data, path)`` validates ``data`` (a raw mapping as read from YAML, or a
    previously validated ``cfg.data``) and keeps a deep-copied normalized dict in
    ``self.data``. ``path`` is the config file; if an existing directory is passed it
    is used as ``base_dir`` (convenient in tests). Views (``levels()``, ``sources()``,
    ``index_settings()`` ...) are built from ``self.data`` on each call; treat the
    returned dicts as read-only.
    """

    data: dict
    path: Path
    base_dir: Path
    secrets: Secrets

    def __init__(self, data: dict, path: str | Path, secrets: Secrets | None = None):
        path = Path(path)
        if path.is_dir():
            base_dir = path.resolve()
            path = base_dir / 'config.yaml'
        else:
            path = path.resolve()
            base_dir = path.parent
        self.path = path
        self.base_dir = base_dir
        self.data = validate(data)
        self.secrets = secrets if secrets is not None else secrets_from_env()

    def __repr__(self) -> str:
        app = self.data['app']
        return (f"Config(path={str(self.path)!r}, mode={app['mode']!r}, campaign_id={app['campaign_id']!r}, "
                f"currency_pair={self.data['market']['currency_pair']!r}, secrets={self.secrets!r})")

    __str__ = __repr__

    # Section views (dicts with Decimal numbers; *_seconds may be float).
    @property
    def app(self) -> dict:
        return self.data['app']

    @property
    def market(self) -> dict:
        return self.data['market']

    @property
    def index(self) -> dict:
        return self.data['index']

    @property
    def safety(self) -> dict:
        return self.data['safety']

    @property
    def execution(self) -> dict:
        return self.data['execution']

    @property
    def portfolio(self) -> dict:
        return self.data['portfolio']

    @property
    def pair(self) -> dict:
        return self.data['pair']

    @property
    def paper(self) -> dict:
        return self.data['paper']

    @property
    def telegram(self) -> dict:
        return self.data['telegram']

    # Derived properties.
    @property
    def mode(self) -> str:
        return self.data['app']['mode']

    @property
    def live(self) -> bool:
        return self.data['app']['mode'] == 'live'

    @property
    def currency_pair(self) -> str:
        return self.data['market']['currency_pair']

    @property
    def state_dir(self) -> Path:
        path = Path(self.data['app']['state_dir'])
        return path if path.is_absolute() else self.base_dir / path

    def stop_trading_path(self) -> Path:
        """Global STOP file: its presence cancels every order and blocks new ones."""
        return self.base_dir / 'STOP_TRADING'

    def stop_side_path(self, side: str) -> Path:
        """Per-side STOP file: ``STOP_BUY`` / ``STOP_SELL`` next to the config."""
        return self.base_dir / f'STOP_{_check_side(side).upper()}'

    def identity(self) -> dict:
        """Campaign identity persisted in the DB (a mismatch refuses to start)."""
        return {
            'campaign_id': self.data['app']['campaign_id'],
            'currency_pair': self.data['market']['currency_pair'],
            'mode': self.data['app']['mode'],
            'client_prefix': self.data['app']['client_prefix'],
            'account': self.data['market']['account'],
        }

    # Ladder.
    def side_enabled(self, side: str) -> bool:
        """True if ``pair.mode`` allows new orders on this side."""
        return self.data['pair']['mode'] in (_check_side(side), 'both')

    def side(self, side: str) -> dict:
        """Side settings (without ``levels``), with the cross-side keys present on both sides.

        BUY gets ``min_profit_pct: None`` / ``profit_floor_mode: 'hold'`` and SELL gets
        ``compensate_base_fee: False`` so callers may read any key on either side.
        ``enabled`` reflects ``pair.mode``.
        """
        side = _check_side(side)
        out = {key: value for key, value in self.data['pair'][side].items() if key != 'levels'}
        out.setdefault('compensate_base_fee', False)
        out.setdefault('min_profit_pct', None)
        out.setdefault('profit_floor_mode', 'hold')
        out['side'] = side
        out['enabled'] = self.side_enabled(side)
        return out

    def levels(self, side: str) -> tuple[LevelConfig, ...]:
        """Levels of a side in config order (shallow -> deep), regardless of pair.mode."""
        side = _check_side(side)
        return tuple(_level_config(row, side) for row in self.data['pair'][side]['levels'])

    def all_levels(self) -> tuple[LevelConfig, ...]:
        return self.levels('buy') + self.levels('sell')

    def level(self, side: str, level_id: str) -> LevelConfig:
        for level in self.levels(side):
            if level.id == level_id:
                return level
        raise KeyError(f'Unconfigured level: {side}:{level_id}')

    # Index.
    def sources(self) -> tuple[SourceConfig, ...]:
        """Enabled index sources in config order."""
        return tuple(s for s in self.all_sources() if s.enabled)

    def all_sources(self) -> tuple[SourceConfig, ...]:
        """All configured index sources (disabled included), config order."""
        return tuple(_source_config(row) for row in self.data['index']['sources'])

    def fx_settings(self) -> FxSettings:
        fx = self.data['index']['fx']
        return FxSettings(mode=fx['mode'], usdc_to_usdt=fx['usdc_to_usdt'], max_depeg_pct=fx['max_depeg_pct'],
                          max_age_seconds=float(fx['max_age_seconds']), max_spread_pct=fx['max_spread_pct'])

    def index_settings(self) -> IndexSettings:
        index = self.data['index']
        return IndexSettings(
            method=index['method'], min_sources=index['min_sources'],
            max_age_seconds=float(index['max_age_seconds']),
            max_timestamp_skew_seconds=float(index['max_timestamp_skew_seconds']),
            max_source_deviation_pct=index['max_source_deviation_pct'],
            basis_offset_pct=index['basis_offset_pct'], rejoin_snapshots=index['rejoin_snapshots'],
            observation_interval_seconds=float(index['observation_interval_seconds']),
            fx=self.fx_settings(), sources=self.sources())


# --------------------------------------------------------------------------------------
# Loading.
# --------------------------------------------------------------------------------------
class _UniqueLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys at any depth."""


def _unique_mapping(loader: _UniqueLoader, node: yaml.MappingNode) -> dict:
    out: dict = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        line = key_node.start_mark.line + 1
        try:
            duplicate = key in out
        except TypeError:
            raise ConfigError(f'line {line}: unsupported YAML mapping key') from None
        if duplicate:
            raise ConfigError(f'Duplicate YAML key: {key} (line {line})')
        out[key] = loader.construct_object(value_node, deep=True)
    return out


_UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)


def parse_yaml(text: str) -> Any:
    try:
        return yaml.load(text, Loader=_UniqueLoader)
    except ConfigError:
        raise
    except yaml.YAMLError as exc:
        raise ConfigError(f'invalid YAML: {exc}') from None


def _load_env_file(env_path: Path) -> None:
    """``.env`` next to the config, never overriding the real environment.

    ``GATE_LIVE_CONFIRM`` is deliberately NOT imported: the live confirmation must
    be typed in the shell for the current run (``env_file_has_live_confirm``). The key
    is matched case-insensitively: Windows upper-cases ``os.environ`` keys, so a
    ``gate_live_confirm=`` line would otherwise become ``GATE_LIVE_CONFIRM``.
    """
    if not env_path.is_file():
        return
    try:
        values = dotenv_values(env_path, encoding='utf-8-sig')
    except (OSError, ValueError) as exc:
        raise ConfigError(f'{env_path.name}: cannot read the .env file ({type(exc).__name__})') from None
    for key, value in values.items():
        if _is_live_confirm_key(key) or value is None or key in os.environ:
            continue
        os.environ[key] = value


def _is_live_confirm_key(key: Any) -> bool:
    """``GATE_LIVE_CONFIRM`` in any letter case (Windows environment names are case-insensitive)."""
    return isinstance(key, str) and key.strip().upper() == LIVE_CONFIRM_ENV


def load_config(path: str | Path, *, read_env: bool = True) -> Config:
    """Read YAML (utf-8-sig, unique keys), optionally the sibling ``.env``, validate, build Config."""
    path = Path(path).resolve()
    if read_env:
        _load_env_file(path.parent / '.env')
    try:
        text = path.read_text(encoding='utf-8-sig')
    except FileNotFoundError:
        raise ConfigError(f'{path}: config file not found') from None
    except (OSError, ValueError) as exc:
        raise ConfigError(f'{path}: cannot read the config file ({type(exc).__name__})') from None
    try:
        data = parse_yaml(text)
    except ConfigError as exc:
        raise ConfigError(f'{path.name}: {exc}') from None
    cfg = Config(data, path)
    LOG.debug('config loaded: %s mode=%s pair=%s', path.name, cfg.mode, cfg.currency_pair)
    return cfg


def env_file_has_live_confirm(cfg: Config | str | Path) -> bool:
    """True if the ``.env`` next to the config defines GATE_LIVE_CONFIRM (any value, key in
    any letter case: on Windows ``gate_live_confirm`` IS the same variable).

    Live must refuse in that case: the confirmation has to come from the shell.
    An unreadable ``.env`` counts as True (fail closed). Accepts a Config, the
    config file path or its directory.
    """
    if isinstance(cfg, Config):
        base = cfg.base_dir
    else:
        base = Path(cfg)
        if not base.is_dir():
            base = base.resolve().parent
    env_path = base / '.env'
    if not env_path.exists():
        return False
    try:
        values = dotenv_values(env_path, encoding='utf-8-sig')
    except (OSError, ValueError) as exc:
        LOG.warning('.env next to the config cannot be read (%s); treating live confirmation as present',
                    type(exc).__name__)
        return True
    return any(_is_live_confirm_key(key) for key in values)
