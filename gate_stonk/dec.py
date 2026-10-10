"""Decimal helpers shared by every module.

Money, prices and quantities are always ``Decimal``. ``D`` rejects booleans and
non-finite values so a malformed config or venue payload can never turn into
NaN/Infinity arithmetic. Rounding helpers work on Gate's fixed decimal grids
(``precision`` for prices, ``amount_precision`` for amounts).
"""
from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, Decimal, InvalidOperation

ZERO = Decimal(0)
ONE = Decimal(1)
HUNDRED = Decimal(100)


def D(value) -> Decimal:
    """Strict Decimal conversion: no bool, no NaN/Infinity, no garbage."""
    if isinstance(value, bool):
        raise ValueError("Boolean supplied instead of number")
    if isinstance(value, Decimal):
        result = value
    else:
        try:
            result = Decimal(str(value).strip()) if isinstance(value, str) else Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("Invalid numeric value") from exc
    if not result.is_finite():
        raise ValueError("Non-finite number")
    return result


def step(decimals: int) -> Decimal:
    """Grid step for a number of decimals (``2`` -> ``0.01``)."""
    if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 18:
        raise ValueError("Invalid decimals")
    return ONE.scaleb(-decimals)


def floor_to(value: Decimal, decimals: int) -> Decimal:
    """Largest grid value <= value."""
    unit = step(decimals)
    return (D(value) / unit).to_integral_value(rounding=ROUND_FLOOR) * unit


def ceil_to(value: Decimal, decimals: int) -> Decimal:
    """Smallest grid value >= value."""
    unit = step(decimals)
    return (D(value) / unit).to_integral_value(rounding=ROUND_CEILING) * unit


def floor_qty(value: Decimal, decimals: int) -> Decimal:
    """Quantity rounded toward zero; negative quantities become zero."""
    value = D(value)
    if value <= 0:
        return ZERO
    unit = step(decimals)
    return (value / unit).to_integral_value(rounding=ROUND_DOWN) * unit


def on_grid(value: Decimal, decimals: int) -> bool:
    value = D(value)
    return floor_to(value, decimals) == value


def pct_diff(value: Decimal, reference: Decimal) -> Decimal:
    """(value / reference - 1) * 100."""
    return (D(value) / D(reference) - ONE) * HUNDRED


def fmt(value, places: int | None = None) -> str:
    """Plain (non-scientific) text for logs, status and Telegram."""
    if value is None:
        return "н/д"
    value = D(value)
    if places is not None:
        value = value.quantize(step(places)) if places > 0 else value.quantize(ONE)
    text = format(value.normalize(), "f")
    return "0" if text in ("-0", "") else text


def usd(value) -> str:
    """Two-decimal quote amount, e.g. USDT."""
    if value is None:
        return "н/д"
    return format(D(value).quantize(Decimal("0.01")), "f")


def signed(value) -> str:
    if value is None:
        return "н/д"
    value = D(value).quantize(Decimal("0.01"))
    return ("+" if value > 0 else "") + format(value, "f")
