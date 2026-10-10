"""Pure average-cost ledger built only from the bot's own executions (DESIGN §8).

The ledger answers two questions: what did the bot pay for the base tokens it bought
and still holds, and what did each SELL realize against that cost. Executions are
applied in exchange execution order ``(time_ms, numeric trade id, fill_key)``, never in
the order a REST read happened to deliver them, so a late-visible earlier fill gives
exactly the same result as a timely one (the store replays the pair in that case).

Gate fee rules (a trade may carry several fee components):

* ``fee`` in the BASE currency: on a BUY it reduces the base received; on a SELL it
  increases the base given away.
* ``fee`` in the QUOTE currency: on a BUY it adds to the cost paid; on a SELL it
  reduces the proceeds.
* A negative ``fee`` is a rebate and flows through the same formulas.
* ``point_fee`` (``POINT``), ``gt_fee`` (``GT``) and a ``fee`` in any other currency are
  recorded in ``fees_other`` per currency and are NEVER converted into quote: their
  quote value is unknown here and is not guessed.
* An explicit quote ``rebate`` (credit) lowers the BUY cost / raises the SELL proceeds
  and is accumulated in ``rebates_quote``. Order-level ``rebated_fee`` reported by Gate
  is not allocated to individual fills (not reliable per fill); the store keeps it in a
  separate table for reporting.

Base tokens sold beyond the bot-bought quantity are "previous inventory"
(``unmatched_*``): their cost is unknown unless the operator declares
``initial_avg_price``. Deposits never get a fictitious price; a confirmed withdrawal of
bot-bought tokens is an explicit transfer event removing quantity at average cost
without realizing profit.

No quantization happens in the arithmetic (default Decimal context); rounding is only
for display.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields, replace
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .dec import HUNDRED, ZERO, D, fmt
from .types import SIDES

POINT = 'POINT'
GT = 'GT'

_DIGITS = re.compile(r'[0-9]+')
_INT_FIELDS = frozenset({'last_time_ms', 'fills'})
_STR_FIELDS = frozenset({'last_trade_id', 'last_fill_key'})


@dataclass(frozen=True)
class Position:
    """Average-cost state of the bot's own inventory of one pair.

    ``bought_cost`` / ``sold_proceeds`` are GROSS notionals (``qty * price``, fees
    excluded), exactly like the reference ``bought_usdc`` / ``sold_usdc``; the fee
    effect lives in ``qty``/``cost``/``realized`` and in the ``fees_*`` fields.
    """

    qty: Decimal = ZERO                  # bot-bought base still held, net of base fees
    cost: Decimal = ZERO                 # quote paid for ``qty`` (quote fees included)
    realized: Decimal = ZERO             # quote realized by SELLs against known cost
    bought_qty: Decimal = ZERO           # gross BUY quantity
    bought_cost: Decimal = ZERO          # gross BUY notional (qty * price, fee excluded)
    sold_qty: Decimal = ZERO             # gross SELL quantity
    sold_proceeds: Decimal = ZERO        # gross SELL notional (qty * price, fee excluded)
    unmatched_qty: Decimal = ZERO        # base delivered beyond bot-bought inventory
    unmatched_proceeds: Decimal = ZERO   # net quote received for ``unmatched_qty``
    fees_base: Decimal = ZERO            # signed sum of fees charged in base (negative = rebate)
    fees_quote: Decimal = ZERO           # signed sum of fees charged in quote (negative = rebate)
    fees_other: dict[str, Decimal] = field(default_factory=dict)   # POINT, GT, other currencies
    rebates_quote: Decimal = ZERO        # explicit quote rebates credited (not part of fees_quote)
    last_time_ms: int = 0                # watermark of the newest applied event
    last_trade_id: str = ''
    fills: int = 0
    last_fill_key: str = ''
    transferred_qty: Decimal = ZERO      # bot-bought base withdrawn (removed at average cost)
    transferred_cost: Decimal = ZERO

    @property
    def avg_cost(self) -> Decimal | None:
        return self.cost / self.qty if self.qty > 0 else None

    def watermark(self) -> tuple[int, int, str]:
        """Execution-order key of the newest applied event (see ``event_key``)."""
        return (self.last_time_ms, tid_number(self.last_trade_id), self.last_fill_key)

    def realized_with(self, initial_avg_price) -> Decimal | None:
        """Realized PnL including unmatched sells valued at a declared initial cost."""
        if self.unmatched_qty == 0:
            return self.realized
        if initial_avg_price is None:
            return None
        return self.realized + self.unmatched_proceeds - self.unmatched_qty * D(initial_avg_price)

    def to_json(self) -> str:
        data: dict[str, Any] = {}
        for item in fields(self):
            value = getattr(self, item.name)
            if isinstance(value, Decimal):
                data[item.name] = str(value)
            elif isinstance(value, dict):
                data[item.name] = {str(k): str(v) for k, v in value.items()}
            else:
                data[item.name] = value
        return json.dumps(data, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> 'Position':
        """Unknown keys are ignored and missing keys keep defaults (cache compatibility)."""
        raw = json.loads(text)
        if not isinstance(raw, dict):
            raise ValueError('Invalid position JSON')
        known = {item.name for item in fields(cls)}
        values: dict[str, Any] = {}
        for key, value in raw.items():
            if key not in known:
                continue
            if key in _INT_FIELDS:
                if isinstance(value, bool):
                    raise ValueError('Invalid position integer')
                values[key] = int(value)
            elif key in _STR_FIELDS:
                values[key] = str(value)
            elif key == 'fees_other':
                if not isinstance(value, dict):
                    raise ValueError('Invalid position fees')
                values[key] = {str(k): D(v) for k, v in value.items()}
            else:
                values[key] = D(value)
        return cls(**values)


@dataclass(frozen=True)
class FillEffect:
    """What one execution changed; used by the per-fill notification and reports."""

    matched_qty: Decimal = ZERO
    unmatched_qty: Decimal = ZERO
    realized: Decimal | None = None       # PnL of the matched part (SELL only)
    realized_pct: Decimal | None = None   # vs its cost
    cost_removed: Decimal = ZERO
    net_qty: Decimal = ZERO               # base received (BUY) or delivered (SELL)
    net_quote: Decimal = ZERO             # quote paid (BUY) or received (SELL), fees included


def tid_number(trade_id) -> int:
    """Numeric value of a trade id for ordering; non-numeric ids sort as 0."""
    text = str(trade_id) if trade_id is not None else ''
    return int(text) if _DIGITS.fullmatch(text) else 0


def event_key(time_ms, kind: int, trade_id, key) -> tuple[int, int, int, str]:
    """Deterministic chronology: execution time, fills (0) before same-ms transfers (1),
    numeric trade id, then the stable fill key."""
    return (int(time_ms), int(kind), tid_number(trade_id), str(key))


def _currency(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'Invalid {name}')
    return value.strip()


def _time(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('Invalid execution time')
    return value


def _add(mapping: dict[str, Decimal], currency: str, amount: Decimal) -> None:
    if amount:
        mapping[currency] = mapping.get(currency, ZERO) + amount


def apply_fill_effect(pos: Position, side: str, qty, price, fee, fee_currency: str, base: str, quote: str, *,
                      point_fee=ZERO, gt_fee=ZERO, time_ms: int, trade_id: str, rebate=ZERO,
                      fill_key: str = '') -> tuple[Position, FillEffect]:
    """Apply one execution and describe its effect. ``pos`` is never mutated."""
    if side not in SIDES:
        raise ValueError('Invalid execution side')
    qty, price, fee = D(qty), D(price), D(fee)
    point_fee, gt_fee, rebate = D(point_fee), D(gt_fee), D(rebate)
    if qty <= 0 or price <= 0:
        raise ValueError('Execution quantity and price must be positive')
    fee_currency = _currency(fee_currency, 'fee currency')
    base, quote = _currency(base, 'base currency'), _currency(quote, 'quote currency')
    if base == quote:
        raise ValueError('Base and quote must differ')
    time_ms = _time(time_ms)
    if not isinstance(trade_id, str) or not trade_id:
        raise ValueError('Invalid trade id')

    notional = qty * price
    base_fee = fee if fee_currency == base else ZERO
    quote_fee = fee if fee_currency == quote else ZERO
    fees_other = dict(pos.fees_other)
    if fee_currency not in (base, quote):
        _add(fees_other, fee_currency, fee)
    _add(fees_other, POINT, point_fee)
    _add(fees_other, GT, gt_fee)
    common = dict(
        fees_base=pos.fees_base + base_fee,
        fees_quote=pos.fees_quote + quote_fee,
        fees_other=fees_other,
        rebates_quote=pos.rebates_quote + rebate,
        last_time_ms=time_ms, last_trade_id=trade_id, last_fill_key=str(fill_key),
        fills=pos.fills + 1,
    )
    if side == 'buy':
        net_qty = qty - base_fee
        paid = notional + quote_fee - rebate
        new_qty, new_cost = pos.qty, pos.cost + paid
        if net_qty > 0:
            new_qty = pos.qty + net_qty
        # else: a pathological fee >= the execution; keep cost conservative, qty unchanged
        result = replace(pos, qty=new_qty, cost=new_cost,
                         bought_qty=pos.bought_qty + qty, bought_cost=pos.bought_cost + notional, **common)
        return result, FillEffect(net_qty=net_qty, net_quote=paid)

    delivered = qty + base_fee
    proceeds = notional - quote_fee + rebate
    if delivered <= 0:
        raise ValueError('Base fee rebate exceeds the sold quantity')
    matched = min(delivered, pos.qty) if pos.qty > 0 else ZERO
    unmatched = delivered - matched
    realized = realized_pct = None
    cost_removed = ZERO
    new_qty, new_cost, new_realized = pos.qty, pos.cost, pos.realized
    if matched > 0:
        share = matched / delivered
        cost_removed = pos.cost * matched / pos.qty
        realized = proceeds * share - cost_removed
        realized_pct = realized / cost_removed * HUNDRED if cost_removed > 0 else None
        new_realized += realized
        new_cost -= cost_removed
        new_qty -= matched
        if new_qty == 0:
            new_cost = ZERO          # wipe the residue when flat
    new_unmatched_qty, new_unmatched_proceeds = pos.unmatched_qty, pos.unmatched_proceeds
    if unmatched > 0:
        new_unmatched_qty += unmatched
        new_unmatched_proceeds += proceeds * unmatched / delivered
    result = replace(pos, qty=new_qty, cost=new_cost, realized=new_realized,
                     sold_qty=pos.sold_qty + qty, sold_proceeds=pos.sold_proceeds + notional,
                     unmatched_qty=new_unmatched_qty, unmatched_proceeds=new_unmatched_proceeds, **common)
    return result, FillEffect(matched, unmatched, realized, realized_pct, cost_removed, delivered, proceeds)


def apply_fill(pos: Position, side: str, qty, price, fee, fee_currency: str, base: str, quote: str, *,
               point_fee=ZERO, gt_fee=ZERO, time_ms: int, trade_id: str, rebate=ZERO,
               fill_key: str = '') -> Position:
    """DESIGN §8 entry point: the new position after one execution."""
    return apply_fill_effect(pos, side, qty, price, fee, fee_currency, base, quote, point_fee=point_fee,
                             gt_fee=gt_fee, time_ms=time_ms, trade_id=trade_id, rebate=rebate,
                             fill_key=fill_key)[0]


def apply_transfer_out(pos: Position, qty, time_ms: int) -> tuple[Position, Decimal, Decimal]:
    """Remove withdrawn bot-bought base at average cost; no profit is realized.

    The watermark moves to ``(time_ms + 1, 0, '')`` so that any fill at or before the
    transfer time reaches the cache only through a full replay (never after it).
    Returns ``(position, removed_qty, removed_cost)``.
    """
    qty = D(qty)
    time_ms = _time(time_ms)
    marked = replace(pos, last_time_ms=time_ms + 1, last_trade_id='', last_fill_key='')
    if qty <= 0 or pos.qty <= 0:
        return marked, ZERO, ZERO
    removed = min(qty, pos.qty)
    cost = pos.cost * removed / pos.qty
    new_qty, new_cost = pos.qty - removed, pos.cost - cost
    if new_qty == 0:
        new_cost = ZERO
    return (replace(marked, qty=new_qty, cost=new_cost, transferred_qty=pos.transferred_qty + removed,
                    transferred_cost=pos.transferred_cost + cost), removed, cost)


def _fill_value(row: Mapping[str, Any], key: str, default=ZERO):
    value = row.get(key, default)
    return default if value is None else value


def replay_effects(fills: Iterable[Mapping[str, Any]], base: str, quote: str, *,
                   transfers: Iterable[Mapping[str, Any]] = ()) -> tuple[Position, dict[str, FillEffect]]:
    """Rebuild a position from all its events in execution order.

    ``fills`` rows need ``time_ms, trade_id, fill_key, side, qty, price, fee, fee_currency``
    and optionally ``point_fee, gt_fee, rebate``; ``transfers`` rows need ``id, time_ms,
    qty``. Returns the position and the effect of every fill keyed by ``fill_key``.
    """
    events: list[tuple[tuple, str, Mapping[str, Any]]] = []
    for row in fills:
        events.append((event_key(row['time_ms'], 0, row['trade_id'], row['fill_key']), 'fill', row))
    for row in transfers:
        events.append((event_key(row['time_ms'], 1, '', row['id']), 'transfer', row))
    events.sort(key=lambda item: item[0])
    position, effects = Position(), {}
    for _key, kind, row in events:
        if kind == 'fill':
            position, effect = apply_fill_effect(
                position, row['side'], row['qty'], row['price'], row['fee'], row['fee_currency'], base, quote,
                point_fee=_fill_value(row, 'point_fee'), gt_fee=_fill_value(row, 'gt_fee'),
                time_ms=int(row['time_ms']), trade_id=str(row['trade_id']), rebate=_fill_value(row, 'rebate'),
                fill_key=str(row['fill_key']))
            effects[str(row['fill_key'])] = effect
        else:
            position, _removed, _cost = apply_transfer_out(position, row['qty'], int(row['time_ms']))
    return position, effects


def replay(fills: Iterable[Mapping[str, Any]], base: str, quote: str, *,
           transfers: Iterable[Mapping[str, Any]] = ()) -> Position:
    """Position after all events, sorted by ``(time_ms, trade_id numeric, fill_key)``."""
    return replay_effects(fills, base, quote, transfers=transfers)[0]


def realized_including_initial(effect: FillEffect, initial_price) -> Decimal | None:
    """Profit of one SELL: bot-matched part plus, when the initial cost is declared,
    the part delivered from tokens held before the bot (None when unknown)."""
    if effect.unmatched_qty <= 0:
        return effect.realized
    if initial_price is None or effect.net_qty <= 0:
        return None
    share = effect.net_quote * effect.unmatched_qty / effect.net_qty
    return (effect.realized or ZERO) + share - effect.unmatched_qty * D(initial_price)


# ----- display helpers (Russian UI strings) -----

def money(value, places: int = 4) -> str:
    """Quote amount with at most ``places`` decimals ('н/д' for None)."""
    return fmt(value, places)


def signed(value, places: int = 4) -> str:
    if value is None:
        return 'н/д'
    text = fmt(value, places)
    return text if text.startswith('-') or text == '0' else '+' + text


def pct(value, places: int = 2) -> str:
    return 'н/д' if value is None else signed(value, places) + '%'


def edge_text(side: str, price, index, reference: str = 'индекса') -> str:
    """'ниже индекса на 2.5%' for a BUY discount, with the direction stated in words."""
    if index is None:
        return 'индекс недоступен'
    price, index = D(price), D(index)
    if index <= 0:
        return 'индекс недоступен'
    diff = (index - price) / index * HUNDRED if side == 'buy' else (price - index) / index * HUNDRED
    better, worse = ('ниже', 'выше') if side == 'buy' else ('выше', 'ниже')
    return f'{better if diff >= 0 else worse} {reference} на {fmt(abs(diff), 3)}%'
