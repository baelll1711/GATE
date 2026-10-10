"""Venue protocol, client order ids and shared Gate normalizers (DESIGN §7).

``GateVenue`` (live, venue_gate.py) and ``PaperVenue`` (venue_paper.py) implement the
same ``Venue`` protocol and return the same value types, so the engine is
mode-agnostic. The normalizers below turn Gate JSON rows into those value types:

* structural problems (missing field, non-numeric amount, bad timestamp) raise
  ``ValueError`` — the caller maps it to ``ReadError`` (reads) or ``UnknownResult``
  (replies to writes);
* semantic mismatches (wrong pair, unsupported status, filled order with a remainder,
  unexpected type/time-in-force on our own order) raise ``Unsafe(code)``.
"""
from __future__ import annotations

import json
import re
from decimal import ROUND_FLOOR, Decimal
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

from .dec import ZERO, D
from .errors import BudgetWait, NotSent, Unsafe
from .types import (SIDES, Balance, CancelRequest, PairMeta, PlaceRequest, PlaceResult, VenueOrder,
                    VenueTrade)

ORDER_STATUSES = ('open', 'closed', 'cancelled')
FINISHED_STATUSES = ('closed', 'cancelled')
TRADE_STATUSES = ('tradable', 'buyable', 'sellable', 'untradable')
ACCOUNTS = ('spot', 'unified')
PLACE_OUTCOMES = ('accepted', 'rejected', 'unknown')
CANCEL_OUTCOMES = ('sent', 'rejected', 'unknown')

# Gate label vocabulary shared by live and paper (notes/api_gate_rest.md §11, §13).
POST_ONLY_LABEL = 'POC_FILL_IMMEDIATELY'          # post-only would take: order NOT created
POST_ONLY_FINISH_AS = 'poc'                       # created, then cancelled as taker
NOT_FOUND_LABELS = frozenset({'ORDER_NOT_FOUND', 'CLIENT_ID_NOT_FOUND'})
CANCEL_FINAL_LABELS = frozenset({'ORDER_NOT_FOUND', 'ORDER_CLOSED', 'ORDER_CANCELLED', 'CLIENT_ID_NOT_FOUND'})
NOT_SENT_LABEL = 'NOT_SENT'                       # PlaceResult label of a batch item never sent

TEXT_PREFIX = 't-'
TEXT_BODY_MAX_BYTES = 28
AMEND_TEXT_MAX = 31
BASE36 = '0123456789abcdefghijklmnopqrstuvwxyz'
_TEXT_BODY_RE = re.compile(r'[0-9A-Za-z_.\-]{1,28}')
_PREFIX_RE = re.compile(r'[a-z0-9]{1,6}')
_LEVEL_RE = re.compile(r'[a-z0-9]{1,4}')
_OWN_TEXT_RE = re.compile(r't-([a-z0-9]{1,6})-([a-z0-9]{1,4})-([0-9a-z]{3,})')
_ORDER_ID_RE = re.compile(r'[0-9]{1,32}')
_CURRENCY_RE = re.compile(r'[A-Z0-9]{1,20}')
_PAIR_RE = re.compile(r'[A-Z0-9]{1,20}_[A-Z0-9]{1,20}')
MAX_TIME_MS = 10 ** 14   # year ~5138: anything above is not a millisecond timestamp


class BudgetNotSent(NotSent):
    """A write refused by the local rate limiter before signing (nothing sent).

    It is a ``NotSent`` (the Venue write contract is Rejected | NotSent | UnknownResult)
    and keeps the limiter's ``wait_seconds`` / ``bucket`` for the engine's pacing.
    """

    def __init__(self, wait: BudgetWait):
        super().__init__(str(wait))
        self.wait_seconds = wait.wait_seconds
        self.bucket = wait.bucket


@runtime_checkable
class Venue(Protocol):
    """Implemented by GateVenue (live) and PaperVenue. See DESIGN §7."""

    live: bool
    before_read: Callable[[], None]   # engine checkpoint, called BEFORE each physical read

    def pair_meta(self) -> PairMeta: ...

    def server_time_ms(self) -> int: ...

    def place(self, req: PlaceRequest) -> VenueOrder: ...                       # Rejected | NotSent | UnknownResult

    def place_batch(self, reqs: Sequence[PlaceRequest]) -> list[PlaceResult]: ...

    def cancel_many(self, reqs: Sequence[CancelRequest]) -> dict[str, str]: ...  # text -> sent|rejected|unknown

    def amend(self, order_id: str, text: str, *, price: Decimal | None, amount: Decimal | None,
              amend_text: str) -> VenueOrder: ...

    def countdown(self, timeout_seconds: int) -> int | None: ...

    def open_orders(self) -> list[VenueOrder]: ...

    def get_order(self, *, order_id: str | None = None, text: str | None = None) -> VenueOrder | None: ...

    def finished_orders(self, from_ms: int, to_ms: int) -> list[VenueOrder]: ...

    def my_trades(self, from_ms: int, to_ms: int) -> list[VenueTrade]: ...

    # Account-wide foreign-activity check (customer rule: the account is exclusive to the
    # bot): open orders / trades of OTHER pairs only; the pair reads above cover the pair.
    def account_open_orders(self) -> list[VenueOrder]: ...

    def account_trades(self, from_ms: int, to_ms: int) -> list[VenueTrade]: ...

    def balances(self) -> dict[str, Balance]: ...

    def account_check(self) -> dict: ...

    def cancel_all_pair(self) -> None: ...


# ---------------------------------------------------------------------------- client ids

def base36(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError('base36 needs a non-negative integer')
    if value == 0:
        return '0'
    digits = []
    while value:
        value, rem = divmod(value, 36)
        digits.append(BASE36[rem])
    return ''.join(reversed(digits))


def validate_text(text: Any) -> str:
    """Gate client id: ``t-`` + 1..28 bytes of ``[0-9A-Za-z_.-]``. Returns the text or raises ValueError."""
    if not isinstance(text, str) or not text.startswith(TEXT_PREFIX):
        raise ValueError('INVALID_CLIENT_TEXT')
    body = text[len(TEXT_PREFIX):]
    if len(body.encode('utf-8')) > TEXT_BODY_MAX_BYTES or not _TEXT_BODY_RE.fullmatch(body):
        raise ValueError('INVALID_CLIENT_TEXT')
    return text


def make_text(prefix: str, level: str, now_ms: int, counter: int) -> str:
    """``t-{prefix}-{level}-{base36(now_ms)}{n}``; n = base36(counter mod 36**2), two chars.

    prefix: app.client_prefix ``[a-z0-9]{1,6}``; level: level id ``[a-z0-9]{1,4}``;
    now_ms: wall-clock milliseconds; counter: per-process counter (non-negative int).
    """
    if not isinstance(prefix, str) or not _PREFIX_RE.fullmatch(prefix):
        raise ValueError('INVALID_CLIENT_PREFIX')
    if not isinstance(level, str) or not _LEVEL_RE.fullmatch(level):
        raise ValueError('INVALID_LEVEL_ID')
    if isinstance(now_ms, bool) or not isinstance(now_ms, int) or not 0 < now_ms < MAX_TIME_MS:
        raise ValueError('INVALID_TEXT_TIMESTAMP')
    if isinstance(counter, bool) or not isinstance(counter, int) or counter < 0:
        raise ValueError('INVALID_TEXT_COUNTER')
    n = base36(counter % (36 * 36)).rjust(2, '0')
    return validate_text(f'{TEXT_PREFIX}{prefix}-{level}-{base36(now_ms)}{n}')


def parse_text(text: Any) -> tuple[str, str] | None:
    """(prefix, level) of a text produced by make_text, else None."""
    if not isinstance(text, str):
        return None
    match = _OWN_TEXT_RE.fullmatch(text)
    if match is None:
        return None
    try:
        validate_text(text)
    except ValueError:
        return None
    return match.group(1), match.group(2)


def is_own_text(text: Any, prefix: str) -> bool:
    parsed = parse_text(text)
    return parsed is not None and parsed[0] == prefix


def validate_amend_text(value: Any) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= AMEND_TEXT_MAX
            or not re.fullmatch(r'[0-9A-Za-z_.\-]+', value)):
        raise ValueError('INVALID_AMEND_TEXT')
    return value


def validate_order_id(value: Any) -> str:
    if isinstance(value, bool):
        raise ValueError('INVALID_ORDER_ID')
    if isinstance(value, int):
        value = str(value)
    if not isinstance(value, str) or not _ORDER_ID_RE.fullmatch(value) or int(value) <= 0:
        raise ValueError('INVALID_ORDER_ID')
    return value


# ---------------------------------------------------------------------------- helpers

def json_safe(value: Any) -> Any:
    """Deep copy with Decimal -> str (and tuples -> lists): safe for JSON / SQLite."""
    if isinstance(value, Decimal):
        return format(value, 'f') if value.is_finite() else str(value)
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, float):
        return value if value == value and value not in (float('inf'), float('-inf')) else str(value)
    if value is None or isinstance(value, (str, int, bool)):
        return value
    return str(value)


def wire(value: Decimal) -> str:
    """Plain decimal string for Gate (never exponent notation)."""
    return format(D(value), 'f')


def parse_ms(value: Any, field: str = 'time_ms') -> int:
    """Millisecond timestamp from an int or a decimal string such as ``"1645464610777.399200"``."""
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        raise ValueError(f'Missing {field}')
    if isinstance(value, int):
        result = value
    else:
        result = int(D(value).to_integral_value(rounding=ROUND_FLOOR))
    if not 0 < result < MAX_TIME_MS:
        raise ValueError(f'Invalid {field}')
    return result


def _seconds_ms(value: Any, field: str) -> int:
    seconds = D(value)
    return parse_ms(seconds * 1000, field)


def _str(raw: Mapping, key: str, *, required: bool = True, default: str | None = None) -> str | None:
    value = raw.get(key)
    if value is None or value == '':
        if required:
            raise ValueError(f'Missing {key}')
        return default
    if not isinstance(value, str):
        raise ValueError(f'Invalid {key}')
    return value


def _dec(raw: Mapping, key: str, *, required: bool = True, default: Decimal | None = None) -> Decimal | None:
    value = raw.get(key)
    if value is None or (isinstance(value, str) and value.strip() == ''):
        if required:
            raise ValueError(f'Missing {key}')
        return default
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise ValueError(f'Invalid {key}')
    return D(value)


def _int_field(raw: Mapping, key: str, *, low: int, high: int) -> int:
    value = raw.get(key)
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f'Invalid {key}')
    return value


# ---------------------------------------------------------------------------- orders

def parse_order(raw: Any) -> VenueOrder:
    """Normalize a Gate spot ``Order`` (also a BatchOrder / OrderCancel item)."""
    if not isinstance(raw, Mapping):
        raise ValueError('Order is not an object')
    oid = raw.get('id')
    alt = raw.get('order_id')
    if oid in (None, ''):
        oid = alt
    elif alt not in (None, '') and str(alt) != str(oid):
        raise ValueError('Order id and order_id differ')
    order_id = validate_order_id(oid)
    text = raw.get('text')
    if text is None:
        text = ''
    if not isinstance(text, str):
        raise ValueError('Invalid text')
    status = _str(raw, 'status')
    if status not in ORDER_STATUSES:
        raise Unsafe(f'UNSUPPORTED_ORDER_STATUS:{status[:32]}')
    order_type = _str(raw, 'type')
    side = _str(raw, 'side')
    amount = _dec(raw, 'amount')
    price = _dec(raw, 'price')
    left = _dec(raw, 'left')
    if amount <= 0 or price < 0:
        raise ValueError('Invalid order amount/price')
    if left < 0 or left > amount:
        raise ValueError('Invalid order remaining amount')
    reported = _dec(raw, 'filled_amount', required=False)
    derived = amount - left
    if reported is None:
        filled = derived
    else:
        if reported < 0:
            raise ValueError('Invalid filled_amount')
        if order_type == 'limit' and reported != derived:
            raise Unsafe('ORDER_FILL_AMOUNTS_INCONSISTENT')
        filled = reported
    if status == 'closed' and left != 0:
        raise Unsafe('FILLED_ORDER_HAS_REMAINING')
    iceberg = _dec(raw, 'iceberg', required=False, default=ZERO)
    if iceberg < 0:
        raise ValueError('Invalid iceberg')
    if raw.get('create_time_ms') not in (None, ''):
        create_ms = parse_ms(raw.get('create_time_ms'), 'create_time_ms')
    else:
        create_ms = _seconds_ms(raw.get('create_time'), 'create_time')
    if raw.get('update_time_ms') not in (None, ''):
        update_ms = parse_ms(raw.get('update_time_ms'), 'update_time_ms')
    elif raw.get('update_time') not in (None, ''):
        update_ms = _seconds_ms(raw.get('update_time'), 'update_time')
    else:
        update_ms = create_ms
    finish_as = raw.get('finish_as')
    if finish_as is not None and not isinstance(finish_as, str):
        raise ValueError('Invalid finish_as')
    amend_text = raw.get('amend_text')
    if amend_text is not None and not isinstance(amend_text, str):
        raise ValueError('Invalid amend_text')
    return VenueOrder(
        order_id=order_id,
        text=text,
        currency_pair=_str(raw, 'currency_pair'),
        side=side,
        type=order_type,
        account=_str(raw, 'account', required=False, default=''),
        time_in_force=_str(raw, 'time_in_force', required=False, default=''),
        price=price,
        amount=amount,
        left=left,
        filled_amount=filled,
        iceberg=iceberg,
        status=status,
        finish_as=finish_as or None,
        create_time_ms=create_ms,
        update_time_ms=update_ms,
        fee=_dec(raw, 'fee', required=False),
        fee_currency=_str(raw, 'fee_currency', required=False),
        point_fee=_dec(raw, 'point_fee', required=False),
        gt_fee=_dec(raw, 'gt_fee', required=False),
        rebated_fee=_dec(raw, 'rebated_fee', required=False),
        rebated_fee_currency=_str(raw, 'rebated_fee_currency', required=False),
        amend_text=amend_text or None,
        raw=json_safe(raw),
    )


def check_order(order: VenueOrder, *, currency_pair: str, account: str | None, own: bool,
                order_id: str | None = None, text: str | None = None) -> VenueOrder:
    """Validate a parsed order against what we asked for; Unsafe(code) on mismatch.

    ``own=True`` (our order: lookups by id/text, create/amend replies) also requires
    ``type == 'limit'`` and ``time_in_force == 'poc'``. Listings (open/finished orders)
    may legitimately contain foreign orders of the pair, so they use ``own=False``.
    """
    if order.currency_pair != currency_pair:
        raise Unsafe('UNEXPECTED_ORDER_PAIR')
    if order.side not in SIDES:
        raise Unsafe('UNEXPECTED_ORDER_SIDE')
    if account and order.account and order.account != account:
        raise Unsafe('UNEXPECTED_ORDER_ACCOUNT')
    if order_id is not None and order.order_id != order_id:
        raise Unsafe('ORDER_ID_MISMATCH')
    if text is not None and order.text != text:
        raise Unsafe('ORDER_TEXT_MISMATCH')
    if own:
        if order.type != 'limit':
            raise Unsafe('UNEXPECTED_ORDER_TYPE')
        if order.time_in_force != 'poc':
            raise Unsafe('UNEXPECTED_ORDER_TIME_IN_FORCE')
        if order.price <= 0:
            raise Unsafe('UNEXPECTED_ORDER_PRICE')
    return order


def is_post_only_rejection(order: VenueOrder | None = None, label: str | None = None) -> bool:
    """Both Gate forms of a post-only refusal (DESIGN §15)."""
    if label == POST_ONLY_LABEL:
        return True
    return (order is not None and order.status == 'cancelled' and order.finish_as == POST_ONLY_FINISH_AS
            and order.filled_amount == 0)


def side_allowed(trade_status: str, side: str) -> bool:
    """tradable: both; buyable: BUY only; sellable: SELL only; untradable: none."""
    return (trade_status == 'tradable' or (trade_status == 'buyable' and side == 'buy')
            or (trade_status == 'sellable' and side == 'sell'))


# ---------------------------------------------------------------------------- trades

def parse_trade(raw: Any) -> VenueTrade:
    """Normalize one ``GET /spot/my_trades`` row (create_time_ms is a decimal string)."""
    if not isinstance(raw, Mapping):
        raise ValueError('Trade is not an object')
    trade_id = raw.get('id')
    if isinstance(trade_id, int) and not isinstance(trade_id, bool):
        trade_id = str(trade_id)
    if not isinstance(trade_id, str) or not trade_id or not re.fullmatch(r'[0-9A-Za-z_\-]{1,64}', trade_id):
        raise ValueError('Invalid trade id')
    order_id = validate_order_id(raw.get('order_id'))
    side = _str(raw, 'side')
    if side not in SIDES:
        raise ValueError('Invalid trade side')
    role = raw.get('role')
    if role in (None, ''):
        role = None
    elif role not in ('maker', 'taker'):
        raise ValueError('Invalid trade role')
    amount = _dec(raw, 'amount')
    price = _dec(raw, 'price')
    if amount <= 0 or price <= 0:
        raise ValueError('Invalid trade amount/price')
    if raw.get('create_time_ms') not in (None, ''):
        created = parse_ms(raw.get('create_time_ms'), 'create_time_ms')
    else:
        created = _seconds_ms(raw.get('create_time'), 'create_time')
    text = raw.get('text')
    if text is not None and not isinstance(text, str):
        raise ValueError('Invalid trade text')
    sequence = raw.get('sequence_id')
    if sequence is not None and not isinstance(sequence, (str, int)) or isinstance(sequence, bool):
        raise ValueError('Invalid sequence_id')
    fee_currency = _str(raw, 'fee_currency')
    if not _CURRENCY_RE.fullmatch(fee_currency):
        raise ValueError('Invalid fee_currency')
    return VenueTrade(
        trade_id=trade_id,
        order_id=order_id,
        text=text or None,
        currency_pair=_str(raw, 'currency_pair'),
        side=side,
        role=role,
        amount=amount,
        price=price,
        fee=_dec(raw, 'fee'),
        fee_currency=fee_currency,
        point_fee=_dec(raw, 'point_fee', required=False, default=ZERO),
        gt_fee=_dec(raw, 'gt_fee', required=False, default=ZERO),
        create_time_ms=created,
        sequence_id=None if sequence in (None, '') else str(sequence),
        raw=json_safe(raw),
    )


def trade_sort_key(trade: VenueTrade) -> tuple:
    numeric = int(trade.trade_id) if trade.trade_id.isdigit() else -1
    return trade.create_time_ms, numeric, trade.trade_id


# ---------------------------------------------------------------------------- pair metadata

def _optional_amount(raw: Mapping, key: str) -> Decimal | None:
    value = _dec(raw, key, required=False)
    if value is not None and value < 0:
        raise ValueError(f'Invalid {key}')
    return value


def parse_pair_meta(raw: Any) -> PairMeta:
    """Normalize ``GET /spot/currency_pairs/{pair}``; null minimums/maximums mean "no limit"."""
    if not isinstance(raw, Mapping):
        raise ValueError('Pair metadata is not an object')
    pair = _str(raw, 'id')
    base = _str(raw, 'base')
    quote = _str(raw, 'quote')
    if not _PAIR_RE.fullmatch(pair) or not _CURRENCY_RE.fullmatch(base) or not _CURRENCY_RE.fullmatch(quote):
        raise ValueError('Invalid pair/currency names')
    precision = _int_field(raw, 'precision', low=0, high=18)
    amount_precision = _int_field(raw, 'amount_precision', low=0, high=18)
    trade_status = _str(raw, 'trade_status')
    if trade_status not in TRADE_STATUSES:
        raise ValueError('Unknown trade_status')
    min_base = _optional_amount(raw, 'min_base_amount')
    min_quote = _optional_amount(raw, 'min_quote_amount')
    max_base = _optional_amount(raw, 'max_base_amount')
    max_quote = _optional_amount(raw, 'max_quote_amount')
    for low, high in ((min_base, max_base), (min_quote, max_quote)):
        if low is not None and high is not None and high < low:
            raise ValueError('Maximum below minimum')
    fee = _dec(raw, 'fee', required=False)
    if fee is not None and not 0 <= fee < 100:
        raise ValueError('Invalid fee')
    return PairMeta(
        currency_pair=pair, base=base, quote=quote, precision=precision, amount_precision=amount_precision,
        min_base_amount=min_base, min_quote_amount=min_quote, max_base_amount=max_base,
        max_quote_amount=max_quote, trade_status=trade_status, fee_pct=fee, raw=json_safe(raw))


# ---------------------------------------------------------------------------- balances

def _balance(available: Decimal, locked: Decimal) -> Balance:
    if available < 0 or locked < 0:
        raise Unsafe('NEGATIVE_OR_INCONSISTENT_SPOT_BALANCE')
    total = available + locked
    if locked > total:
        raise Unsafe('NEGATIVE_OR_INCONSISTENT_SPOT_BALANCE')
    return Balance(total=total, hold=locked)


def parse_balances(raw: Any, account: str, base: str, quote: str) -> dict[str, Balance]:
    """Spot (``GET /spot/accounts``) or unified (``GET /unified/accounts``) balances.

    total = available + locked (unified: available + freeze), hold = locked/freeze. The
    managed currencies (base, quote) are validated strictly and default to zero when
    absent; malformed rows of other currencies are skipped (a token appearing in the
    account must not halt the bot).
    """
    if account not in ACCOUNTS:
        raise ValueError('Unknown account type')
    managed = {base, quote}
    result: dict[str, Balance] = {}
    if account == 'spot':
        if not isinstance(raw, list):
            raise ValueError('Spot accounts reply is not a list')
        rows = []
        for row in raw:
            if not isinstance(row, Mapping):
                raise ValueError('Spot account row is not an object')
            rows.append((row.get('currency'), row, 'available', 'locked'))
    else:
        balances = raw.get('balances') if isinstance(raw, Mapping) else None
        if not isinstance(balances, Mapping):
            raise ValueError('Unified accounts reply has no balances object')
        rows = [(currency, row, 'available', 'freeze') for currency, row in balances.items()]
    for currency, row, available_key, locked_key in rows:
        if currency in managed:
            if currency in result:
                raise Unsafe('BALANCE_TOKEN_MISMATCH_OR_DUPLICATE')
            if not isinstance(row, Mapping):
                raise ValueError('Balance row is not an object')
            available = _dec(row, available_key)
            locked = _dec(row, locked_key, required=account == 'spot', default=ZERO)
            result[currency] = _balance(available, locked)
            continue
        try:
            if not isinstance(currency, str) or not _CURRENCY_RE.fullmatch(currency) or currency in result:
                continue
            available = _dec(row, available_key)
            locked = _dec(row, locked_key, required=False, default=ZERO)
            result[currency] = _balance(available, locked)
        except (Unsafe, ValueError, TypeError, AttributeError):
            continue
    for name in managed:
        result.setdefault(name, Balance(ZERO, ZERO))
    return result


def jsonable_order(order: VenueOrder) -> dict:
    """Compact JSON-safe view of an order (status/diagnostics)."""
    return {
        'order_id': order.order_id, 'text': order.text, 'side': order.side, 'price': wire(order.price),
        'amount': wire(order.amount), 'left': wire(order.left), 'status': order.status,
        'finish_as': order.finish_as,
    }


def dumps_compact(value: Any) -> str:
    return json.dumps(json_safe(value), separators=(',', ':'), sort_keys=True)
