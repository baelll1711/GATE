"""Immutable value types shared by every module (see DESIGN §2).

All money/price/quantity fields are ``Decimal``. Snapshots are frozen and use
tuples so a producer thread can publish them and a consumer can read them
without copying or locking beyond a dict lookup.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .dec import HUNDRED, ZERO, step
from .errors import Unsafe

SIDES = ('buy', 'sell')
Level = tuple[Decimal, Decimal]


@dataclass(frozen=True)
class Book:
    """Order book snapshot: bids descending, asks ascending, best first."""

    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    received: float          # time.monotonic() at receipt
    event_ms: int            # venue event time (ms) where documented, else receipt wall ms
    update_id: int | None = None

    @property
    def bid(self) -> Decimal:
        return self.bids[0][0]

    @property
    def ask(self) -> Decimal:
        return self.asks[0][0]

    @property
    def bid_size(self) -> Decimal:
        return self.bids[0][1]

    @property
    def ask_size(self) -> Decimal:
        return self.asks[0][1]

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2

    @property
    def microprice(self) -> Decimal:
        total = self.bid_size + self.ask_size
        if total <= 0:
            return self.mid
        return (self.bid * self.ask_size + self.ask * self.bid_size) / total

    @property
    def spread_pct(self) -> Decimal:
        return (self.ask - self.bid) / self.mid * HUNDRED

    def check(self, now: float, wall_ms: int, max_age: float, max_spread_pct: Decimal | None) -> None:
        """Validate structure, freshness and spread; first failure wins (reference order)."""
        if not self.bids or not self.asks:
            raise Unsafe('EMPTY_BOOK')
        for levels, ascending in ((self.bids, False), (self.asks, True)):
            previous = None
            for price, qty in levels:
                if (not isinstance(price, Decimal) or not isinstance(qty, Decimal)
                        or not price.is_finite() or not qty.is_finite() or price <= 0 or qty <= 0):
                    raise Unsafe('INVALID_BOOK_LEVEL')
                if previous is not None and ((price <= previous) if ascending else (price >= previous)):
                    raise Unsafe('UNSORTED_OR_DUPLICATE_BOOK_LEVEL')
                previous = price
        if self.bid >= self.ask:
            raise Unsafe('CROSSED_BOOK')
        if not 0 <= now - self.received <= max_age:
            raise Unsafe('STALE_QUOTE')
        if not -2000 <= wall_ms - self.event_ms <= max_age * 1000:
            raise Unsafe('STALE_EVENT_OR_CLOCK_SKEW')
        if max_spread_pct is not None and self.spread_pct > max_spread_pct:
            raise Unsafe('WIDE_SOURCE_SPREAD')


@dataclass(frozen=True)
class Ticker:
    """Mark/last prices from a ticker channel or REST ticker."""

    mark: Decimal | None
    last: Decimal | None
    received: float
    event_ms: int

    def check(self, now: float, wall_ms: int, max_age: float) -> None:
        if not 0 <= now - self.received <= max_age:
            raise Unsafe('STALE_QUOTE')
        if not -2000 <= wall_ms - self.event_ms <= max_age * 1000:
            raise Unsafe('STALE_EVENT_OR_CLOCK_SKEW')


@dataclass(frozen=True)
class SourceSnapshot:
    """What a feed exposes for one index source at one instant."""

    source_id: str
    book: Book | None
    ticker: Ticker | None
    error: str | None        # stored reason when data is missing (CONNECTING, DISCONNECTED, parse code, ...)
    connected: bool


@dataclass(frozen=True)
class FxSnapshot:
    rate: Decimal | None     # USDT per 1 USDC
    mode: str                # fixed | binance_spot | gate_spot
    book: Book | None
    error: str | None


@dataclass(frozen=True)
class IndexComponent:
    source_id: str
    venue: str
    symbol: str
    quote: str
    price_kind: str
    weight: Decimal
    required: bool
    included: bool
    reason: str | None
    native_price: Decimal | None
    usdt_price: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    spread_pct: Decimal | None
    event_ms: int | None
    received_age_s: float | None
    effective_ms: int | None


@dataclass(frozen=True)
class IndexSnapshot:
    ok: bool
    index: Decimal | None        # aggregate in USDT (no basis offset)
    center: Decimal | None       # index * (1 + basis_offset_pct/100); all edges are measured from it
    reason: str | None           # blocking reason when not ok
    components: tuple[IndexComponent, ...]
    fx_rate: Decimal | None
    fx_reason: str | None
    method: str
    at_ms: int
    n_included: int


@dataclass(frozen=True)
class PairMeta:
    """``GET /spot/currency_pairs/{pair}`` (validated)."""

    currency_pair: str
    base: str
    quote: str
    precision: int
    amount_precision: int
    min_base_amount: Decimal | None
    min_quote_amount: Decimal | None
    max_base_amount: Decimal | None
    max_quote_amount: Decimal | None
    trade_status: str
    fee_pct: Decimal | None = None
    raw: dict = field(default_factory=dict, compare=False, repr=False)

    @property
    def tick(self) -> Decimal:
        return step(self.precision)

    @property
    def lot(self) -> Decimal:
        return step(self.amount_precision)


@dataclass(frozen=True)
class Balance:
    total: Decimal               # available + locked
    hold: Decimal                # locked

    @property
    def free(self) -> Decimal:
        return self.total - self.hold


@dataclass(frozen=True)
class VenueOrder:
    """Normalized Gate spot order."""

    order_id: str
    text: str
    currency_pair: str
    side: str
    type: str
    account: str
    time_in_force: str
    price: Decimal
    amount: Decimal
    left: Decimal
    filled_amount: Decimal
    iceberg: Decimal
    status: str                  # open | closed | cancelled
    finish_as: str | None
    create_time_ms: int
    update_time_ms: int
    fee: Decimal | None = None
    fee_currency: str | None = None
    point_fee: Decimal | None = None
    gt_fee: Decimal | None = None
    rebated_fee: Decimal | None = None
    rebated_fee_currency: str | None = None
    amend_text: str | None = None
    raw: dict = field(default_factory=dict, compare=False, repr=False)

    @property
    def is_open(self) -> bool:
        return self.status == 'open'


@dataclass(frozen=True)
class VenueTrade:
    """One own execution (``GET /spot/my_trades`` row)."""

    trade_id: str
    order_id: str
    text: str | None
    currency_pair: str
    side: str
    role: str | None
    amount: Decimal
    price: Decimal
    fee: Decimal
    fee_currency: str
    point_fee: Decimal
    gt_fee: Decimal
    create_time_ms: int
    sequence_id: str | None = None
    raw: dict = field(default_factory=dict, compare=False, repr=False)


@dataclass(frozen=True)
class PublicTrade:
    """Public tape entry (``spot.trades``) used by paper fills and statistics."""

    trade_id: str
    price: Decimal
    amount: Decimal
    side: str                    # taker side
    create_time_ms: int
    received: float
    seq: int                     # local strictly increasing tape position


@dataclass(frozen=True)
class PlaceRequest:
    text: str
    side: str
    price: Decimal
    amount: Decimal
    iceberg: Decimal | None = None
    time_in_force: str = 'poc'


@dataclass(frozen=True)
class PlaceResult:
    text: str
    outcome: str                 # 'accepted' | 'rejected' | 'unknown'
    order: VenueOrder | None = None
    label: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class CancelRequest:
    text: str
    order_id: str | None


@dataclass(frozen=True)
class PriceDecision:
    price: Decimal
    lower: Decimal
    upper: Decimal
    reason: str
    volume_price: Decimal | None = None
    skipped_notional: Decimal = ZERO
    visible_notional: Decimal = ZERO
    own_excluded_notional: Decimal = ZERO
    requested_skip_notional: Decimal = ZERO
    depth_sufficient: bool = False
    skip_satisfied: bool = False
    ahead_volume_complete: bool = False
    volume_pricing_applied: bool = False


@dataclass(frozen=True)
class LevelTarget:
    side: str
    level: str
    decision: PriceDecision | None
    reason: str
    placeable: bool
