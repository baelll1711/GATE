"""Configuration loading and validation (see DESIGN §3 and config.example.yaml).

SKELETON written by the architect: the dataclasses below are the contract used by
index/pricing/engine. The config implementer fills in ``load_config`` and
``Config`` (validation of every key in config.example.yaml) WITHOUT changing the
public names/fields of these dataclasses (adding fields is allowed).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from .errors import ConfigError

SUPPORTED_VENUES = ('bybit_linear', 'bitget_usdt_futures', 'bingx_swap', 'lighter', 'gate_futures_usdt')
PRICE_KINDS = ('mid', 'microprice', 'mark', 'last')
INDEX_METHODS = ('weighted_median', 'weighted_mean')
FX_MODES = ('fixed', 'binance_spot', 'gate_spot')
LIVE_CONFIRM_VALUE = 'I_UNDERSTAND_SPOT_TRADING'


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


class Config:
    """Validated configuration. Implemented by the config module owner."""

    data: dict
    path: Path
    base_dir: Path
    state_dir: Path
    secrets: Secrets

    def __init__(self, data: dict, path: Path, secrets: Secrets | None = None):
        raise NotImplementedError('config implementer: validate data and build views')


def load_config(path: str | Path, *, read_env: bool = True) -> Config:
    raise ConfigError('load_config is not implemented yet')
