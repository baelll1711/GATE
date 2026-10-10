"""Live Gate spot venue for one pair (DESIGN §7, §15; notes/api_gate_rest.md).

Thin object over ``GateRestClient``: the engine never talks HTTP. Every method keeps
the reference transport discipline:

* writes (place / place_batch / amend / countdown / cancel_all_pair) validate
  everything BEFORE sending and raise ``NotSent`` on any local refusal (including
  the rate limiter, as ``BudgetNotSent``); after sending only an explicit Gate
  refusal is ``Rejected``; anything ambiguous — transport failure, 5xx, unparseable
  or mismatching reply — is ``UnknownResult`` (the engine keeps the reservation and
  searches by ``text``);
* ``cancel_many`` never raises on transport errors: per-item 'sent' / 'rejected' /
  'unknown';
* reads call the engine checkpoint (``venue.before_read``) before EVERY physical
  request (each page), raise ``ReadError`` for malformed replies and ``Unsafe`` for
  semantic mismatches (wrong pair, unsupported status, ...).

``market.account`` ('spot' | 'unified') is sent explicitly on every request that
accepts it (create, batch create, cancel, cancel-all, amend, order reads). It is not
sent to ``/spot/my_trades`` (parameter deprecated by Gate; the endpoint returns all
accounts' trades, filtered here by pair).
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from .dec import D, on_grid
from .errors import (BudgetWait, NotSent, NotSentValueError, ReadError, Rejected, UnknownResult, Unsafe,
                     not_sent)
from .gate_rest import RATE_LIMIT_LABELS, UNKNOWN_RESULT_LABELS, GateRestClient
from .ratelimit import CANCEL, ORDER
from .types import SIDES, Balance, CancelRequest, PairMeta, PlaceRequest, PlaceResult, VenueOrder, VenueTrade
from .venue import (ACCOUNTS, FINISHED_STATUSES, MAX_TIME_MS, NOT_FOUND_LABELS, NOT_SENT_LABEL, BudgetNotSent,
                    check_order, json_safe, parse_balances, parse_order, parse_pair_meta, parse_trade, side_allowed,
                    trade_sort_key, validate_amend_text, validate_order_id, validate_text, wire)

LOG = logging.getLogger(__name__)

BATCH_CREATE_MAX = 10          # orders per pair per POST /spot/batch_orders
BATCH_CANCEL_MAX = 20          # items per POST /spot/cancel_batch_orders
ORDERS_PAGE_LIMIT = 100        # GET /spot/orders (status=open: max 100)
TRADES_PAGE_LIMIT = 1000       # GET /spot/my_trades (max 1000)
MAX_PAGES = 50                 # guard per traversal (my_trades: limit*(page-1) <= 100000)
TRADE_WINDOW_SECONDS = 7 * 86400  # my_trades range must be <= 30 days; query in <= 7-day slices
DMS_MIN_TIMEOUT = 5
DMS_MAX_TIMEOUT = 86400
DMS_TRIGGER_TOLERANCE_MS = 60_000
UNIFIED_MODES = ('classic', 'multi_currency', 'portfolio', 'single_currency')
_LABEL_RE = re.compile(r'[A-Z][A-Z0-9_]{0,63}')
_CODE_RE = re.compile(r'[A-Z][A-Z0-9_]*(?::[A-Za-z0-9_.\-:]*)?')
_HEX_RUN_RE = re.compile(r'[0-9a-fA-F]{32,}')
_PAIR_RE = re.compile(r'([A-Z0-9]{1,20})_([A-Z0-9]{1,20})')


def _noop() -> None:
    return None


def _code(exc: BaseException) -> str:
    text = str(exc)
    return text[:120] if _CODE_RE.fullmatch(text or '') else type(exc).__name__


def _safe_message(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return ' '.join(_HEX_RUN_RE.sub('[REDACTED]', value).split())[:160] or None


def _label(value: Any) -> str | None:
    if isinstance(value, str) and _LABEL_RE.fullmatch(value.strip()):
        return value.strip()
    return None


def _int_setting(value: Any, name: str, low: int, high: int) -> int:
    if isinstance(value, Decimal) and value == value.to_integral_value():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValueError(f'{name} must be an integer in [{low}, {high}]')
    return value


@dataclass(frozen=True)
class GateVenueSettings:
    """What the live venue needs from the config (``market.*``)."""

    currency_pair: str
    base: str
    quote: str
    account: str = 'spot'
    expected_precision: int | None = None
    expected_amount_precision: int | None = None
    batch_create_max: int = BATCH_CREATE_MAX
    batch_cancel_max: int = BATCH_CANCEL_MAX
    orders_page_limit: int = ORDERS_PAGE_LIMIT
    trades_page_limit: int = TRADES_PAGE_LIMIT
    max_pages: int = MAX_PAGES
    trade_window_seconds: int = TRADE_WINDOW_SECONDS

    def __post_init__(self) -> None:
        match = _PAIR_RE.fullmatch(self.currency_pair) if isinstance(self.currency_pair, str) else None
        if match is None:
            raise ValueError('currency_pair must look like BASE_QUOTE')
        if (match.group(1), match.group(2)) != (self.base, self.quote):
            raise ValueError('currency_pair must equal base + "_" + quote')
        if self.account not in ACCOUNTS:
            raise ValueError('account must be spot or unified')
        for name in ('expected_precision', 'expected_amount_precision'):
            value = getattr(self, name)
            if value is not None:
                _int_setting(value, name, 0, 18)
        _int_setting(self.batch_create_max, 'batch_create_max', 1, BATCH_CREATE_MAX)
        _int_setting(self.batch_cancel_max, 'batch_cancel_max', 1, BATCH_CANCEL_MAX)
        _int_setting(self.orders_page_limit, 'orders_page_limit', 1, ORDERS_PAGE_LIMIT)
        _int_setting(self.trades_page_limit, 'trades_page_limit', 1, TRADES_PAGE_LIMIT)
        _int_setting(self.max_pages, 'max_pages', 1, 100)
        _int_setting(self.trade_window_seconds, 'trade_window_seconds', 60, 30 * 86400 - 60)

    @classmethod
    def from_config(cls, cfg: Any) -> 'GateVenueSettings':
        market = cfg.market

        def opt(name: str) -> int | None:
            value = market.get(name)
            return None if value is None else _int_setting(value, f'market.{name}', 0, 18)

        return cls(currency_pair=market['currency_pair'], base=market['base'], quote=market['quote'],
                   account=market.get('account', 'spot'), expected_precision=opt('expected_precision'),
                   expected_amount_precision=opt('expected_amount_precision'))


class GateVenue:
    """Live venue (Venue protocol) for ``settings.currency_pair``."""

    live = True

    def __init__(self, settings: GateVenueSettings | Any, client: GateRestClient, meta: PairMeta | None = None):
        if not isinstance(settings, GateVenueSettings):
            if hasattr(settings, 'market'):
                settings = GateVenueSettings.from_config(settings)
            else:
                raise TypeError('GateVenue needs GateVenueSettings or a Config')
        self.settings = settings
        self.client = client
        self.before_read: Callable[[], None] = _noop
        client.before_read = self._checkpoint
        self.meta: PairMeta | None = None
        if meta is not None:
            self.meta = self._validate_meta(meta)
        self.last_dms: dict | None = None
        self.last_cancel_all: dict | None = None
        self.last_trades_read: dict | None = None
        self.last_balance_keys: list[str] | None = None

    # ------------------------------------------------------------------ plumbing
    def _checkpoint(self) -> None:
        self.before_read()

    @property
    def pair(self) -> str:
        return self.settings.currency_pair

    @property
    def account(self) -> str:
        return self.settings.account

    def close(self) -> None:
        self.client.close()

    def diagnostics(self) -> dict:
        return json_safe({
            'currency_pair': self.pair, 'account': self.account,
            'last_dms': self.last_dms, 'last_cancel_all': self.last_cancel_all,
            'last_trades_read': self.last_trades_read, 'client': self.client.diagnostics(),
        })

    def _require_meta(self) -> PairMeta:
        if self.meta is None:
            raise Unsafe('PAIR_METADATA_REQUIRED')
        return self.meta

    def _validate_meta(self, meta: PairMeta) -> PairMeta:
        s = self.settings
        if (meta.currency_pair, meta.base, meta.quote) != (s.currency_pair, s.base, s.quote):
            raise Unsafe('SPOT_PAIR_MAPPING_MISMATCH')
        if s.expected_precision is not None and meta.precision != s.expected_precision:
            raise Unsafe('PAIR_PRECISION_MISMATCH')
        if s.expected_amount_precision is not None and meta.amount_precision != s.expected_amount_precision:
            raise Unsafe('PAIR_AMOUNT_PRECISION_MISMATCH')
        if self.meta is not None and (meta.precision, meta.amount_precision) != (
                self.meta.precision, self.meta.amount_precision):
            raise Unsafe('PAIR_PRECISION_CHANGED')
        return meta

    def _read(self, path: str, params: Any = None, *, public: bool = False) -> Any:
        if public:
            return self.client.public('GET', path, params)
        return self.client.private('GET', path, params)

    def _write(self, method: str, path: str, params: Any = None, body: Any = None, *, what: str) -> Any:
        try:
            return self.client.private(method, path, params, body)
        except BudgetWait as exc:
            raise BudgetNotSent(exc) from None
        except (NotSent, Rejected, UnknownResult):
            raise
        except Exception as exc:  # after the request may have left: never assume it failed
            raise UnknownResult(f'{what}_RESULT_UNKNOWN:{type(exc).__name__}') from None

    def _paged(self, path: str, params: list[tuple[str, Any]], *, limit: int, too_many: str,
               invalid: str) -> list[Any]:
        rows: list[Any] = []
        for page in range(1, self.settings.max_pages + 1):
            reply = self._read(path, params + [('page', page), ('limit', limit)])
            if not isinstance(reply, list):
                raise ReadError(invalid)
            rows.extend(reply)
            if len(reply) < limit:
                return rows
        raise Unsafe(too_many)

    def _parse_listed(self, raw: Any, invalid: str) -> VenueOrder:
        try:
            order = parse_order(raw)
        except Unsafe:
            raise
        except (ValueError, TypeError, ArithmeticError):
            raise ReadError(invalid) from None
        return check_order(order, currency_pair=self.pair, account=self.account, own=False)

    # ------------------------------------------------------------------ metadata / time
    def pair_meta(self) -> PairMeta:
        raw = self._read(f'/spot/currency_pairs/{self.pair}', public=True)
        try:
            meta = parse_pair_meta(raw)
        except Unsafe:
            raise
        except (ValueError, TypeError, ArithmeticError):
            raise ReadError('INVALID_PAIR_METADATA') from None
        self.meta = self._validate_meta(meta)
        return self.meta

    def server_time_ms(self) -> int:
        raw = self._read('/spot/time', public=True)
        value = raw.get('server_time') if isinstance(raw, Mapping) else None
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < MAX_TIME_MS:
            raise ReadError('INVALID_SERVER_TIME')
        return value

    # ------------------------------------------------------------------ order creation
    def _check_size(self, amount: Decimal, price: Decimal, meta: PairMeta) -> None:
        notional = amount * price
        if meta.min_base_amount is not None and amount < meta.min_base_amount:
            raise ValueError('ORDER_SIZE_BELOW_MINIMUM')
        if meta.min_quote_amount is not None and notional < meta.min_quote_amount:
            raise ValueError('ORDER_SIZE_BELOW_MINIMUM')
        if meta.max_base_amount is not None and amount > meta.max_base_amount:
            raise ValueError('ORDER_SIZE_ABOVE_MAXIMUM')
        if meta.max_quote_amount is not None and notional > meta.max_quote_amount:
            raise ValueError('ORDER_SIZE_ABOVE_MAXIMUM')

    def _order_body(self, req: Any) -> dict:
        """Validated create body (pre-send; every failure means nothing was sent)."""
        meta = self._require_meta()
        if not isinstance(req, PlaceRequest):
            raise ValueError('INVALID_PLACE_REQUEST')
        validate_text(req.text)
        if req.side not in SIDES:
            raise ValueError('INVALID_ORDER_SIDE')
        if req.time_in_force != 'poc':
            raise ValueError('ONLY_POST_ONLY_SUPPORTED')
        if not isinstance(req.price, Decimal) or not isinstance(req.amount, Decimal):
            raise ValueError('ORDER_PRICE_AND_AMOUNT_MUST_BE_DECIMAL')
        price, amount = D(req.price), D(req.amount)
        if price <= 0 or amount <= 0:
            raise ValueError('NONPOSITIVE_ORDER_PRICE_OR_AMOUNT')
        if not on_grid(price, meta.precision) or not on_grid(amount, meta.amount_precision):
            raise ValueError('ORDER_PRECISION_INVALID')
        self._check_size(amount, price, meta)
        if not side_allowed(meta.trade_status, req.side):
            raise Unsafe(f'PAIR_NOT_TRADABLE:{meta.trade_status}')
        body = {'text': req.text, 'currency_pair': self.pair, 'type': 'limit', 'account': self.account,
                'side': req.side, 'amount': wire(amount), 'price': wire(price), 'time_in_force': 'poc'}
        if req.iceberg is not None:
            if not isinstance(req.iceberg, Decimal):
                raise ValueError('ICEBERG_DISPLAY_INVALID')
            iceberg = D(req.iceberg)
            if not 0 < iceberg < amount or not on_grid(iceberg, meta.amount_precision):
                raise ValueError('ICEBERG_DISPLAY_INVALID')
            body['iceberg'] = wire(iceberg)
        return body

    def _own_reply(self, raw: Any, req: PlaceRequest) -> VenueOrder:
        order = parse_order(raw)
        check_order(order, currency_pair=self.pair, account=self.account, own=True, text=req.text)
        if order.side != req.side:
            raise Unsafe('UNEXPECTED_ORDER_SIDE')
        if order.price != req.price:
            raise Unsafe('ORDER_PRICE_MISMATCH')
        if order.amount != req.amount:
            raise Unsafe('ORDER_AMOUNT_MISMATCH')
        return order

    def place(self, req: PlaceRequest) -> VenueOrder:
        """POST /spot/orders (post-only). A reply ``status=cancelled, finish_as=poc`` is returned, not raised."""
        try:
            body = self._order_body(req)
        except Exception as exc:
            raise not_sent(exc) from None
        reply = self._write('POST', '/spot/orders', body=body, what='CREATE')
        try:
            return self._own_reply(reply, req)
        except Exception as exc:
            raise UnknownResult(f'CREATE_REPLY_INVALID:{_code(exc)}') from None

    def place_batch(self, reqs: Sequence[PlaceRequest]) -> list[PlaceResult]:
        """POST /spot/batch_orders in chunks of <= 10; results matched by ``text``.

        NotSent if nothing was sent. After the first chunk was sent, a later chunk that
        could not be sent yields 'rejected' items with label NOT_SENT; a transport failure
        of a chunk yields 'unknown' for all its items; a whole-request refusal 'rejected'.
        """
        reqs = list(reqs)
        if not reqs:
            return []
        try:
            bodies = [self._order_body(req) for req in reqs]
            texts = [req.text for req in reqs]
            if len(set(texts)) != len(texts):
                raise ValueError('DUPLICATE_CLIENT_TEXT')
        except Exception as exc:
            raise not_sent(exc) from None
        size = self.settings.batch_create_max
        chunks = [list(range(i, min(i + size, len(reqs)))) for i in range(0, len(reqs), size)]
        limiter = self.client.limiter
        if limiter is not None:
            try:
                wait = limiter.wait(ORDER, len(chunks))
            except ValueError as exc:
                raise not_sent(exc) from None
            if wait > 0:
                raise BudgetNotSent(BudgetWait(wait, ORDER))
        results: dict[str, PlaceResult] = {}
        for number, chunk in enumerate(chunks):
            chunk_reqs = [reqs[i] for i in chunk]
            try:
                reply = self._write('POST', '/spot/batch_orders', body=[bodies[i] for i in chunk],
                                    what='BATCH_CREATE')
            except NotSent as exc:
                if number == 0:
                    raise
                for req in chunk_reqs:
                    results[req.text] = PlaceResult(req.text, 'rejected', None, NOT_SENT_LABEL, _code(exc))
                continue
            except Rejected as exc:
                for req in chunk_reqs:
                    results[req.text] = PlaceResult(req.text, 'rejected', None, exc.label, _safe_message(str(exc)))
                continue
            except UnknownResult as exc:
                for req in chunk_reqs:
                    results[req.text] = PlaceResult(req.text, 'unknown', None, None, _code(exc))
                continue
            results.update(self._batch_results(reply, chunk_reqs))
        out = [results[req.text] for req in reqs]
        for result in out:
            if result.outcome != 'accepted':
                LOG.warning('BATCH_CREATE_ITEM text=%s outcome=%s label=%s', result.text, result.outcome,
                            result.label)
        return out

    def _batch_results(self, reply: Any, chunk_reqs: list[PlaceRequest]) -> dict[str, PlaceResult]:
        out = {req.text: PlaceResult(req.text, 'unknown', None, None, 'BATCH_ITEM_MISSING') for req in chunk_reqs}
        if not isinstance(reply, list):
            return {text: PlaceResult(text, 'unknown', None, None, 'BATCH_REPLY_INVALID') for text in out}
        found: dict[str, Mapping] = {}
        duplicated: set[str] = set()
        for item in reply:
            if not isinstance(item, Mapping):
                continue
            text = item.get('text')
            if not isinstance(text, str) or text not in out:
                continue
            if text in found:
                duplicated.add(text)
                continue
            found[text] = item
        by_text = {req.text: req for req in chunk_reqs}
        for text, item in found.items():
            if text in duplicated:
                out[text] = PlaceResult(text, 'unknown', None, None, 'BATCH_ITEM_DUPLICATE')
            else:
                out[text] = self._batch_item(item, by_text[text])
        return out

    def _batch_item(self, item: Mapping, req: PlaceRequest) -> PlaceResult:
        succeeded = item.get('succeeded')
        label = _label(item.get('label'))
        if succeeded is True:
            try:
                order = self._own_reply(item, req)
            except Exception as exc:
                return PlaceResult(req.text, 'unknown', None, None, f'BATCH_ITEM_INVALID:{_code(exc)}')
            return PlaceResult(req.text, 'accepted', order, None, None)
        if succeeded is False and label is not None:
            if label in UNKNOWN_RESULT_LABELS:
                return PlaceResult(req.text, 'unknown', None, label, _safe_message(item.get('message')))
            if label in RATE_LIMIT_LABELS and self.client.limiter is not None:
                try:
                    self.client.limiter.rate_limited(ORDER)
                except Exception:
                    pass
            return PlaceResult(req.text, 'rejected', None, label, _safe_message(item.get('message')))
        return PlaceResult(req.text, 'unknown', None, label, 'BATCH_ITEM_UNRECOGNIZED')

    # ------------------------------------------------------------------ cancellation
    def cancel_many(self, reqs: Sequence[CancelRequest]) -> dict[str, str]:
        """POST /spot/cancel_batch_orders in chunks of <= 20 (by order_id; by text only without one).

        Returns text -> 'sent' | 'rejected' | 'unknown'; never raises on transport errors.
        """
        reqs = list(reqs)
        if not reqs:
            return {}
        texts = [getattr(req, 'text', None) for req in reqs]
        if any(not isinstance(text, str) or not text for text in texts) or len(set(texts)) != len(texts):
            raise ValueError('Cancellation batch must have distinct texts')
        result: dict[str, str] = {}
        items: list[tuple[str, str]] = []
        for req in reqs:
            try:
                if not isinstance(req, CancelRequest):
                    raise ValueError('INVALID_CANCEL_REQUEST')
                ident = validate_order_id(req.order_id) if req.order_id is not None else validate_text(req.text)
            except Exception:
                LOG.warning('CANCEL_ITEM_INVALID text=%s', req.text if isinstance(req.text, str) else '?')
                result[req.text] = 'unknown'
                continue
            items.append((req.text, ident))
        size = self.settings.batch_cancel_max
        for start in range(0, len(items), size):
            chunk = items[start:start + size]
            body = [{'currency_pair': self.pair, 'id': ident, 'account': self.account} for _, ident in chunk]
            try:
                reply = self.client.private('POST', '/spot/cancel_batch_orders', body=body)
            except Rejected as exc:
                LOG.warning('CANCEL_BATCH_REJECTED label=%s', exc.label)
                result.update({text: 'rejected' for text, _ in chunk})
                continue
            except Exception as exc:  # NotSent, UnknownResult, anything: outcome not proven
                LOG.warning('CANCEL_BATCH_UNKNOWN error=%s', _code(exc))
                result.update({text: 'unknown' for text, _ in chunk})
                continue
            result.update(self._cancel_results(reply, chunk))
        return {text: result[text] for text in texts}

    def _cancel_results(self, reply: Any, chunk: list[tuple[str, str]]) -> dict[str, str]:
        out = {text: 'unknown' for text, _ in chunk}
        if not isinstance(reply, list):
            return out
        by_id: dict[str, Mapping] = {}
        by_text: dict[str, Mapping] = {}
        dup_id: set[str] = set()
        dup_text: set[str] = set()
        for item in reply:
            if not isinstance(item, Mapping):
                continue
            rid = item.get('id')
            if isinstance(rid, int) and not isinstance(rid, bool):
                rid = str(rid)
            if isinstance(rid, str) and rid:
                (dup_id.add(rid) if rid in by_id else by_id.__setitem__(rid, item))
            rtext = item.get('text')
            if isinstance(rtext, str) and rtext:
                (dup_text.add(rtext) if rtext in by_text else by_text.__setitem__(rtext, item))
        for text, ident in chunk:
            if ident in dup_id:
                continue
            item = by_id.get(ident)
            if item is None and text not in dup_text:
                item = by_text.get(text)
            if item is None:
                continue
            pair = item.get('currency_pair')
            if pair not in (None, '', self.pair):
                continue
            succeeded = item.get('succeeded')
            label = _label(item.get('label'))
            if succeeded is True:
                out[text] = 'sent'
            elif succeeded is False and label is not None and label not in UNKNOWN_RESULT_LABELS:
                out[text] = 'rejected'
                if label in RATE_LIMIT_LABELS and self.client.limiter is not None:
                    try:
                        self.client.limiter.rate_limited(CANCEL)
                    except Exception:
                        pass
        return out

    def cancel_all_pair(self) -> None:
        """DELETE /spot/orders?currency_pair=<pair>&account=<account> (cancel-only fallback)."""
        reply = self._write('DELETE', '/spot/orders', params=[('currency_pair', self.pair),
                                                              ('account', self.account)], what='CANCEL_ALL')
        if not isinstance(reply, list):
            raise UnknownResult('CANCEL_ALL_REPLY_INVALID')
        failed: dict[str, int] = {}
        succeeded = 0
        for item in reply:
            if isinstance(item, Mapping) and item.get('succeeded') is True:
                succeeded += 1
            else:
                label = _label(item.get('label')) if isinstance(item, Mapping) else None
                failed[label or 'UNKNOWN'] = failed.get(label or 'UNKNOWN', 0) + 1
        self.last_cancel_all = {'at_ms': int(time.time() * 1000), 'items': len(reply), 'succeeded': succeeded,
                                'failed': failed}
        if failed:
            LOG.warning('CANCEL_ALL_PARTIAL succeeded=%s failed=%s', succeeded, failed)

    # ------------------------------------------------------------------ amend
    def amend(self, order_id: str, text: str, *, price: Decimal | None = None, amount: Decimal | None = None,
              amend_text: str) -> VenueOrder:
        """PATCH /spot/orders/{order_id}: price only, or amount only (never both)."""
        try:
            oid = validate_order_id(order_id)
            validate_text(text)
            validate_amend_text(amend_text)
            if (price is None) == (amount is None):
                raise ValueError('AMEND_PRICE_OR_AMOUNT_ONLY')
            meta = self._require_meta()
            body: dict[str, str] = {'currency_pair': self.pair, 'account': self.account}
            if price is not None:
                if not isinstance(price, Decimal) or price <= 0 or not on_grid(price, meta.precision):
                    raise ValueError('ORDER_PRECISION_INVALID')
                body['price'] = wire(price)
            else:
                if not isinstance(amount, Decimal) or amount <= 0 or not on_grid(amount, meta.amount_precision):
                    raise ValueError('ORDER_PRECISION_INVALID')
                body['amount'] = wire(amount)
            body['amend_text'] = amend_text
        except Exception as exc:
            raise not_sent(exc) from None
        reply = self._write('PATCH', f'/spot/orders/{oid}',
                            params=[('currency_pair', self.pair), ('account', self.account)], body=body,
                            what='AMEND')
        try:
            order = parse_order(reply)
            check_order(order, currency_pair=self.pair, account=self.account, own=True, order_id=oid, text=text)
            if price is not None and order.price != price:
                raise Unsafe('AMEND_PRICE_NOT_APPLIED')
            if amount is not None and order.amount != amount:
                raise Unsafe('AMEND_AMOUNT_NOT_APPLIED')
        except Exception as exc:
            raise UnknownResult(f'AMEND_REPLY_INVALID:{_code(exc)}') from None
        return order

    # ------------------------------------------------------------------ dead man's switch
    def countdown(self, timeout_seconds: int) -> int | None:
        """POST /spot/countdown_cancel_all {timeout, currency_pair}; 0 disarms (returns None).

        Returns Gate's ``triggerTime`` (ms). Anything but a valid acknowledgement —
        transport failure, explicit refusal, missing/invalid triggerTime — is UnknownResult.
        """
        t = timeout_seconds
        if isinstance(t, bool) or not isinstance(t, int) or not (t == 0 or DMS_MIN_TIMEOUT <= t <= DMS_MAX_TIMEOUT):
            raise NotSentValueError('INVALID_DMS_TIMEOUT')
        self.last_dms = None
        sent_ms = int(time.time() * 1000)
        try:
            reply = self.client.private('POST', '/spot/countdown_cancel_all',
                                        body={'timeout': t, 'currency_pair': self.pair})
        except NotSent as exc:
            self._dms_diag(t, {'outcome': 'not_sent', 'error': _code(exc)})
            raise
        except Rejected as exc:
            self._dms_diag(t, {'outcome': 'rejected', 'label': exc.label, 'http_status': exc.status})
            raise UnknownResult(f'DMS_NOT_ACKNOWLEDGED:{exc.label}') from None
        except UnknownResult as exc:
            self._dms_diag(t, {'outcome': 'transport_error', 'error': _code(exc)})
            raise
        except Exception as exc:
            self._dms_diag(t, {'outcome': 'transport_error', 'error': type(exc).__name__})
            raise UnknownResult(f'DMS_RESULT_UNKNOWN:{type(exc).__name__}') from None
        received_ms = int(time.time() * 1000)
        trigger = reply.get('triggerTime') if isinstance(reply, Mapping) else None
        valid = isinstance(trigger, int) and not isinstance(trigger, bool) and 0 <= trigger < MAX_TIME_MS
        if valid and t > 0:
            low = sent_ms + t * 1000 - DMS_TRIGGER_TOLERANCE_MS
            high = received_ms + t * 1000 + DMS_TRIGGER_TOLERANCE_MS
            valid = low <= trigger <= high
        if not valid:
            self._dms_diag(t, {'outcome': 'unexpected_response', 'response_type': type(reply).__name__})
            raise UnknownResult('DMS_NOT_ACKNOWLEDGED:INVALID_TRIGGER_TIME')
        self._dms_diag(t, {'outcome': 'acknowledged', 'trigger_ms': trigger})
        return trigger if t > 0 else None

    def _dms_diag(self, timeout: int, detail: dict) -> None:
        detail = dict(detail, at_ms=int(time.time() * 1000), timeout_seconds=timeout)
        self.last_dms = detail
        if detail['outcome'] != 'acknowledged':
            try:
                LOG.warning('DMS_RESULT %s', json_safe(detail))
            except Exception:
                pass

    def probe_write_permission(self) -> dict:
        """Doctor only (no open orders): harmless signed write ``countdown_cancel_all {timeout: 0}``.

        NOTE: it disarms any running countdown for the pair. Reports the refusal label
        (READ_ONLY / FORBIDDEN / INVALID_KEY / IP_FORBIDDEN ...) if any.
        """
        try:
            reply = self.client.private('POST', '/spot/countdown_cancel_all',
                                        body={'timeout': 0, 'currency_pair': self.pair})
        except Rejected as exc:
            return {'ok': False, 'outcome': 'rejected', 'label': exc.label, 'http_status': exc.status}
        except NotSent as exc:
            return {'ok': False, 'outcome': 'not_sent', 'label': None, 'error': _code(exc)}
        except Exception as exc:
            return {'ok': False, 'outcome': 'unknown', 'label': None, 'error': _code(exc)}
        trigger = reply.get('triggerTime') if isinstance(reply, Mapping) else None
        ok = isinstance(trigger, int) and not isinstance(trigger, bool)
        return {'ok': ok, 'outcome': 'acknowledged' if ok else 'unexpected_response', 'label': None}

    # ------------------------------------------------------------------ order reads
    def open_orders(self) -> list[VenueOrder]:
        """ALL open orders of the pair: GET /spot/orders?status=open paged (limit 100) until a short page."""
        rows = self._paged('/spot/orders', [('currency_pair', self.pair), ('status', 'open'),
                                            ('account', self.account)],
                           limit=self.settings.orders_page_limit, too_many='OPEN_ORDERS_TOO_MANY_PAGES',
                           invalid='INVALID_OPEN_ORDERS')
        orders: dict[str, VenueOrder] = {}
        for raw in rows:
            order = self._parse_listed(raw, 'INVALID_OPEN_ORDER')
            if order.status != 'open':
                raise ReadError('INVALID_OPEN_ORDER_STATUS')
            orders[order.order_id] = order   # the same order may repeat across pages
        return list(orders.values())

    def get_order(self, *, order_id: str | None = None, text: str | None = None) -> VenueOrder | None:
        """GET /spot/orders/{order_id|text}; None when Gate says ORDER_NOT_FOUND / CLIENT_ID_NOT_FOUND.

        By text works only while the order is open (Gate); prefer order_id once bound.
        """
        try:
            if order_id is None and text is None:
                raise ValueError('ORDER_ID_OR_TEXT_REQUIRED')
            oid = validate_order_id(order_id) if order_id is not None else None
            txt = validate_text(text) if text is not None else None
        except Exception as exc:
            raise not_sent(exc) from None
        ident = oid if oid is not None else txt
        try:
            raw = self._read(f'/spot/orders/{ident}', [('currency_pair', self.pair), ('account', self.account)])
        except Rejected as exc:
            if exc.label in NOT_FOUND_LABELS:
                return None
            raise
        try:
            order = parse_order(raw)
        except Unsafe:
            raise
        except (ValueError, TypeError, ArithmeticError):
            raise ReadError('INVALID_ORDER') from None
        return check_order(order, currency_pair=self.pair, account=self.account, own=True, order_id=oid, text=txt)

    @staticmethod
    def _window(from_ms: Any, to_ms: Any) -> tuple[int, int]:
        for value in (from_ms, to_ms):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < MAX_TIME_MS:
                raise ValueError('INVALID_TIME_WINDOW')
        if from_ms > to_ms:
            raise ValueError('INVALID_TIME_WINDOW')
        # Gate takes seconds: widen by one second on both sides, filter/dedupe locally.
        return max(0, from_ms // 1000 - 1), to_ms // 1000 + 1

    def finished_orders(self, from_ms: int, to_ms: int) -> list[VenueOrder]:
        """GET /spot/orders?status=finished&from&to (seconds, by finish time), paged; superset of the window."""
        try:
            from_s, to_s = self._window(from_ms, to_ms)
        except ValueError as exc:
            raise not_sent(exc) from None
        rows = self._paged('/spot/orders', [('currency_pair', self.pair), ('status', 'finished'),
                                            ('from', from_s), ('to', to_s), ('account', self.account)],
                           limit=self.settings.orders_page_limit, too_many='ORDER_HISTORY_TOO_LARGE',
                           invalid='INVALID_FINISHED_ORDERS')
        orders: dict[str, VenueOrder] = {}
        for raw in rows:
            order = self._parse_listed(raw, 'INVALID_FINISHED_ORDER')
            if order.status not in FINISHED_STATUSES:
                raise ReadError('INVALID_FINISHED_ORDER_STATUS')
            orders[order.order_id] = order
        return sorted(orders.values(), key=lambda o: (o.update_time_ms, int(o.order_id)))

    # ------------------------------------------------------------------ trades
    def _collect_trades(self, rows: list[Any], seen: dict[str, VenueTrade], order_id: str | None = None) -> None:
        for raw in rows:
            try:
                trade = parse_trade(raw)
            except (ValueError, TypeError, ArithmeticError):
                raise ReadError('INVALID_TRADE_ROW') from None
            if trade.currency_pair != self.pair:
                raise Unsafe('UNEXPECTED_TRADE_PAIR')
            if order_id is not None and trade.order_id != order_id:
                raise Unsafe('UNEXPECTED_TRADE_ORDER')
            previous = seen.get(trade.trade_id)
            if previous is None:
                seen[trade.trade_id] = trade
            elif _trade_identity(previous) != _trade_identity(trade):
                raise Unsafe('CONFLICTING_TRADE_ROWS')

    def my_trades(self, from_ms: int, to_ms: int) -> list[VenueTrade]:
        """Own trades of the pair with ``from_ms <= create_time_ms <= to_ms``.

        GET /spot/my_trades?currency_pair&from&to&limit=1000&page (seconds; window widened
        by 1 s, sliced to <= trade_window_seconds), complete paging, dedup by trade id,
        sorted by (create_time_ms, id). Rows outside the window are dropped (counted in
        ``last_trades_read['outside_window']``).
        """
        try:
            from_s, to_s = self._window(from_ms, to_ms)
        except ValueError as exc:
            raise not_sent(exc) from None
        seen: dict[str, VenueTrade] = {}
        slices = 0
        start = from_s
        while True:
            end = min(to_s, start + self.settings.trade_window_seconds)
            rows = self._paged('/spot/my_trades', [('currency_pair', self.pair), ('from', start), ('to', end)],
                               limit=self.settings.trades_page_limit, too_many='FILL_HISTORY_TOO_LARGE',
                               invalid='INVALID_FILL_HISTORY')
            self._collect_trades(rows, seen)
            slices += 1
            if end >= to_s:
                break
            start = end   # boundary second read twice; dedup by id
        inside = [t for t in seen.values() if from_ms <= t.create_time_ms <= to_ms]
        inside.sort(key=trade_sort_key)
        self.last_trades_read = {'from_ms': from_ms, 'to_ms': to_ms, 'rows': len(seen), 'returned': len(inside),
                                 'outside_window': len(seen) - len(inside), 'slices': slices}
        return inside

    def order_trades(self, order_id: str) -> list[VenueTrade]:
        """Extra (not in the protocol): all trades of one order (GET /spot/my_trades?order_id, last 7 days)."""
        try:
            oid = validate_order_id(order_id)
        except ValueError as exc:
            raise not_sent(exc) from None
        rows = self._paged('/spot/my_trades', [('currency_pair', self.pair), ('order_id', oid)],
                           limit=self.settings.trades_page_limit, too_many='FILL_HISTORY_TOO_LARGE',
                           invalid='INVALID_FILL_HISTORY')
        seen: dict[str, VenueTrade] = {}
        self._collect_trades(rows, seen, order_id=oid)
        return sorted(seen.values(), key=trade_sort_key)

    # ------------------------------------------------------------------ account
    def balances(self) -> dict[str, Balance]:
        """Spot: GET /spot/accounts; unified: GET /unified/accounts (parsed defensively)."""
        path = '/spot/accounts' if self.account == 'spot' else '/unified/accounts'
        raw = self._read(path)
        try:
            if isinstance(raw, Mapping):
                self.last_balance_keys = sorted(str(k) for k in raw.keys())[:50]
            return parse_balances(raw, self.account, self.settings.base, self.settings.quote)
        except Unsafe:
            raise
        except (ValueError, TypeError, ArithmeticError, AttributeError):
            raise ReadError('INVALID_BALANCES') from None

    @staticmethod
    def _detect_account(key_mode: int | None, unified_mode: str | None) -> str:
        if key_mode == 2:
            return 'unified'
        if key_mode == 1 and unified_mode == 'classic':
            return 'spot'
        if key_mode == 1 and unified_mode in UNIFIED_MODES:
            return 'unified'
        return 'unknown'

    def account_check(self) -> dict:
        """GET /account/detail + GET /unified/unified_mode -> mode / whitelist facts (doctor, live start).

        ``problems`` lists blocking codes: ``ACCOUNT_MODE_MISMATCH:<detected>`` when the
        detected account type differs from ``market.account``; ``PAIR_NOT_IN_KEY_WHITELIST``.
        """
        detail = self._read('/account/detail')
        try:
            key = detail['key']
            key_mode = key.get('mode') if isinstance(key, Mapping) else None
            if isinstance(key_mode, bool) or not isinstance(key_mode, int):
                raise ValueError('key.mode')
            pairs = detail.get('currency_pairs') or []
            ips = detail.get('ip_whitelist') or []
            if not isinstance(pairs, list) or not all(isinstance(p, str) for p in pairs):
                raise ValueError('currency_pairs')
            if not isinstance(ips, list) or not all(isinstance(p, str) for p in ips):
                raise ValueError('ip_whitelist')
            tier = detail.get('tier')
            if tier is not None and (isinstance(tier, bool) or not isinstance(tier, int)):
                raise ValueError('tier')
        except (KeyError, TypeError, ValueError, AttributeError):
            raise ReadError('INVALID_ACCOUNT_DETAIL') from None
        unified_mode = None
        unified_error = None
        try:
            mode_raw = self._read('/unified/unified_mode')
        except Rejected as exc:
            unified_error = exc.label
        else:
            value = mode_raw.get('mode') if isinstance(mode_raw, Mapping) else None
            if isinstance(value, str) and value in UNIFIED_MODES:
                unified_mode = value
            else:
                unified_error = 'INVALID_UNIFIED_MODE'
        detected = self._detect_account(key_mode, unified_mode)
        pair_ok = not pairs or self.pair in pairs
        problems = []
        if detected != self.account:
            problems.append(f'ACCOUNT_MODE_MISMATCH:{detected}')
        if not pair_ok:
            problems.append('PAIR_NOT_IN_KEY_WHITELIST')
        return {
            'key_mode': key_mode, 'unified_mode': unified_mode, 'unified_mode_error': unified_error,
            'detected_account': detected, 'configured_account': self.account, 'account_ok': detected == self.account,
            'pair_whitelist_ok': pair_ok, 'pair_whitelist': list(pairs), 'ip_whitelist': list(ips), 'tier': tier,
            'problems': problems,
        }

    def fee_info(self) -> dict:
        """GET /wallet/fee?currency_pair= (fractions: 0.002 = 0.2 %) + GET /account/debit_fee (GT deduction)."""
        raw = self._read('/wallet/fee', [('currency_pair', self.pair)])
        try:
            maker = D(raw['maker_fee'])
            taker = D(raw['taker_fee'])
            for value in (maker, taker):
                if not Decimal('-0.01') <= value <= Decimal('0.1'):
                    raise ValueError('fee out of range')
            gt_discount = raw.get('gt_discount')
            if gt_discount is not None and not isinstance(gt_discount, bool):
                raise ValueError('gt_discount')
            gt_maker = None if raw.get('gt_maker_fee') in (None, '') else D(raw['gt_maker_fee'])
            gt_taker = None if raw.get('gt_taker_fee') in (None, '') else D(raw['gt_taker_fee'])
            debit_fee = raw.get('debit_fee')
            if debit_fee is not None and (isinstance(debit_fee, bool) or not isinstance(debit_fee, int)):
                raise ValueError('debit_fee')
            point_type = raw.get('point_type')
            point_type = None if point_type is None else str(point_type)
        except (KeyError, TypeError, ValueError, AttributeError, ArithmeticError):
            raise ReadError('INVALID_FEE_INFO') from None
        enabled = None
        debit_error = None
        try:
            debit = self._read('/account/debit_fee')
        except Rejected as exc:
            debit_error = exc.label
        else:
            value = debit.get('enabled') if isinstance(debit, Mapping) else None
            if isinstance(value, bool):
                enabled = value
            else:
                debit_error = 'INVALID_DEBIT_FEE'
        return {
            'maker_fee': maker, 'taker_fee': taker, 'maker_fee_pct': maker * 100, 'taker_fee_pct': taker * 100,
            'gt_discount': gt_discount, 'gt_maker_fee': gt_maker, 'gt_taker_fee': gt_taker, 'debit_fee': debit_fee,
            'point_type': point_type, 'gt_deduction_enabled': enabled, 'debit_fee_error': debit_error,
        }


def _trade_identity(trade: VenueTrade) -> tuple:
    return (trade.order_id, trade.side, trade.amount, trade.price, trade.fee, trade.fee_currency,
            trade.create_time_ms, trade.text)
