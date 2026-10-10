"""Pure sizing helpers (DESIGN §6), ported from the reference ``spot_portfolio/sizing.py``.

Quantities are base-token amounts on Gate's ``amount_precision`` grid and are
always rounded DOWN, except the fee-compensated BUY gross amount, which is
rounded UP to the lot so the net amount after a base-token fee still covers the
target. Limits (balance, inventory, totals) live in the engine's ``quantity()``,
which caps these requested sizes.

Unsafe codes raised here:

``INVALID_ORDER_SIZE``               the level has no positive finite size
``NONPOSITIVE_PRICE_RANGE``          price is not a positive finite number
``INVALID_BUY_SIZE_FEE_ESTIMATE``    BUY fee estimate not in [0, 1) percent
``ICEBERG_DISPLAY_INVALID``          iceberg settings invalid, or the visible part
                                     is not strictly between 0 and the amount
``ICEBERG_DISPLAY_BELOW_MINIMUM``    visible part below the pair minimums
"""
from __future__ import annotations

from decimal import ROUND_CEILING, Decimal
from typing import TYPE_CHECKING

from .dec import HUNDRED, ONE, ZERO, D, floor_qty
from .errors import Unsafe
from .types import SIDES, PairMeta

if TYPE_CHECKING:  # attribute access only
    from .config import LevelConfig

MAX_BUY_FEE_FRACTION = Decimal('0.01')     # fee_estimate_pct must stay below 1 %


def _positive(value, code: str) -> Decimal:
    try:
        result = D(value)
    except (ValueError, TypeError) as exc:
        raise Unsafe(code) from exc
    if result <= 0:
        raise Unsafe(code)
    return result


def requested_size(level: LevelConfig, side: str, price: Decimal, meta: PairMeta, fee_pct: Decimal | None,
                   compensate_base_fee: bool) -> dict:
    """Requested order size before limits.

    ``target_net_qty`` = floor to lot of ``min(order_qty, order_notional_usdt/price)``
    (only the configured ones). For a BUY with ``compensate_base_fee`` and a nonzero
    fee estimate, ``requested_gross_qty = ceil_to_lot(target/(1-fee))`` so the base
    fee deducted from the received tokens still leaves the target. SELL sizes are
    never grossed up and ignore ``fee_pct``. Cumulative limits count BUYs before fee.
    """
    if side not in SIDES:
        raise ValueError('side must be buy or sell')
    if not isinstance(compensate_base_fee, bool):
        raise ValueError('compensate_base_fee must be boolean')
    price = _positive(price, 'NONPOSITIVE_PRICE_RANGE')
    sizes: list[Decimal] = []
    if level.order_qty is not None:
        sizes.append(_positive(level.order_qty, 'INVALID_ORDER_SIZE'))
    if level.order_notional_usdt is not None:
        sizes.append(_positive(level.order_notional_usdt, 'INVALID_ORDER_SIZE') / price)
    if not sizes:
        raise Unsafe('INVALID_ORDER_SIZE')
    target = floor_qty(min(sizes), meta.amount_precision)
    enabled = side == 'buy' and compensate_base_fee
    if side == 'buy':
        try:
            fee = D(fee_pct) / HUNDRED
        except (ValueError, TypeError) as exc:
            raise Unsafe('INVALID_BUY_SIZE_FEE_ESTIMATE') from exc
    else:
        fee = ZERO
    if not ZERO <= fee < MAX_BUY_FEE_FRACTION:
        raise Unsafe('INVALID_BUY_SIZE_FEE_ESTIMATE')
    size_step = meta.lot
    if enabled and fee:
        gross = (target / (ONE - fee) / size_step).to_integral_value(rounding=ROUND_CEILING) * size_step
    else:
        gross = target
    return {
        'enabled': enabled,
        'target_net_qty': target,
        'requested_gross_qty': gross,
        'estimated_base_fee_pct': fee * HUNDRED,
        'size_step': size_step,
    }


def iceberg_visible(level: LevelConfig, amount: Decimal, meta: PairMeta, *,
                    price: Decimal | None = None) -> Decimal | None:
    """Visible part of an iceberg order, or ``None`` for a normal order.

    ``floor_qty(iceberg_display_qty or amount*iceberg_display_pct/100, amount_precision)``.
    Gate cannot hide the whole amount and a visible part equal to the amount is not an
    iceberg, so ``0 < visible < amount`` is required (else ICEBERG_DISPLAY_INVALID).
    The visible part must also satisfy ``min_base_amount`` and, when ``price`` is
    given, ``min_quote_amount`` (else ICEBERG_DISPLAY_BELOW_MINIMUM; Gate does not
    document a minimum for the visible part, so we fail closed).
    """
    display_qty, display_pct = level.iceberg_display_qty, level.iceberg_display_pct
    if display_qty is None and display_pct is None:
        return None
    if display_qty is not None and display_pct is not None:
        raise Unsafe('ICEBERG_DISPLAY_INVALID')
    try:
        amount = D(amount)
        if display_qty is not None:
            display_qty = D(display_qty)
            if display_qty <= 0:
                raise Unsafe('ICEBERG_DISPLAY_INVALID')
            raw = display_qty
        else:
            display_pct = D(display_pct)
            if not ZERO < display_pct < HUNDRED:
                raise Unsafe('ICEBERG_DISPLAY_INVALID')
            raw = amount * display_pct / HUNDRED
    except (ValueError, TypeError) as exc:
        raise Unsafe('ICEBERG_DISPLAY_INVALID') from exc
    visible = floor_qty(raw, meta.amount_precision)
    if not ZERO < visible < amount:
        raise Unsafe('ICEBERG_DISPLAY_INVALID')
    if meta.min_base_amount is not None and visible < meta.min_base_amount:
        raise Unsafe('ICEBERG_DISPLAY_BELOW_MINIMUM')
    if price is not None and meta.min_quote_amount is not None:
        if visible * _positive(price, 'NONPOSITIVE_PRICE_RANGE') < meta.min_quote_amount:
            raise Unsafe('ICEBERG_DISPLAY_BELOW_MINIMUM')
    return visible


def meets_minimums(qty: Decimal, price: Decimal, meta: PairMeta) -> bool:
    """``qty >= min_base_amount`` and ``qty*price >= min_quote_amount`` (null = no limit).

    A nonpositive or invalid quantity/price never meets the minimums.
    """
    try:
        qty, price = D(qty), D(price)
    except (ValueError, TypeError):
        return False
    if qty <= 0 or price <= 0:
        return False
    if meta.min_base_amount is not None and qty < meta.min_base_amount:
        return False
    if meta.min_quote_amount is not None and qty * price < meta.min_quote_amount:
        return False
    return True
