"""Lighter (zkLighter) perpetual index source (``venue: lighter``), quoted in USDC.

Wire facts (notes/api_lighter_fx.md part A; Lighter SDK 1.1.6, ccxt, NautilusTrader):

* WS ``wss://mainnet.zklighter.elliot.ai/stream``. The server greets with
  ``{"type":"connected"}`` and sends app-level ``{"type":"ping"}`` which must be answered
  ``{"type":"pong"}`` (handled before full JSON parsing). No client app pings
  (``ping_interval = 0``: the worker sends websocket control pings).
* Subscribe ``{"type":"subscribe","channel":"order_book/{market_index}"}`` - outbound
  channels use ``/``, inbound frames carry ``"channel": "order_book:{market_index}"``
  (``:``). Both spellings are accepted inbound.
* ``subscribed/order_book`` is a full snapshot; ``update/order_book`` frames carry
  ABSOLUTE level sizes (``size == 0`` deletes the level) and are applied to the local
  book. Continuity: every update's ``order_book.begin_nonce`` must equal the ``nonce`` of
  the previously applied frame; a gap raises ``Unsafe('LIGHTER_NONCE_GAP')`` (the worker
  reconnects = resubscribe = new snapshot). ``offset`` is NOT a continuity counter.
  Updates before the snapshot are dropped. A malformed book frame (or a frame without
  nonces) raises ``INVALID_LIGHTER_BOOK``, a crossed local book ``LIGHTER_BOOK_CROSSED``:
  the local book cannot be repaired without a new snapshot, so both are socket-fatal.
* Book freshness (DESIGN §16): ``event_ms`` = receipt wall ms of the last data frame that
  changed or confirmed the book, so a quiet live socket stays fresh only while frames
  arrive. The published book holds the best ``PUBLISH_DEPTH`` levels per side.
* Mark/last (only for those price kinds): ``market_stats/{id}`` ->
  ``{"type":"update/market_stats","market_stats":{"market_id","mark_price","last_trade_price",...}}``;
  ``event_ms`` = receipt wall ms as for the book.
* Errors: ``{"error":{"code":30005,"message":..}}`` or ``{"type":"error","code":30012,..}``.
  30003 (already subscribed) is harmless; anything else is ``SUBSCRIPTION_REJECTED``.
  ``{"type":"shutdown"}`` -> ``LIGHTER_SERVER_SHUTDOWN`` (reconnect early).
* market index: ``source.market_index`` may pin it; otherwise the hub calls
  ``resolve_market_index(http, timeout)`` before starting the worker. It reads
  ``GET /api/v1/orderBookDetails?filter=perp`` (``order_book_details[]``, exact ``symbol``,
  ``market_type == "perp"``). ``on_text``/``subscribe_frames`` refuse to run without it.
* REST snapshot ``GET /api/v1/orderBookOrders?market_id=&limit=`` returns individual orders
  (L3): sizes are aggregated by price; when a side was truncated by ``limit`` its deepest
  aggregated level may be incomplete and is dropped. No timestamp: receipt wall ms.
  Mark/last via ``orderBookDetails?market_id=`` (``mark_price`` string,
  ``last_trade_price`` JSON number parsed exactly).
"""
from __future__ import annotations

import heapq
import logging
import re
from decimal import Decimal
from typing import Any

from ..dec import ONE, ZERO, D
from ..errors import ConfigError, TransientUnsafe, Unsafe
from ..types import Book, Ticker
from ._venue_common import (DATA_ERRORS, AdapterBase, FrameError, Level, ReferenceFailure, as_code, as_int,
                            best_first, detail, dumps, opt_price, plain, positive_decimal)
from .base import Reply, Update

LOG = logging.getLogger(__name__)

LIGHTER_WS = 'wss://mainnet.zklighter.elliot.ai/stream'
LIGHTER_REST = 'https://mainnet.zklighter.elliot.ai'
PONG = dumps({'type': 'pong'})
PING_TEXTS = frozenset({'{"type":"ping"}', '{"type": "ping"}'})
SNAPSHOT = 'subscribed/order_book'
UPDATE = 'update/order_book'
STATS = ('subscribed/market_stats', 'update/market_stats')
PUBLISH_DEPTH = 50
REST_ORDERS_LIMIT = 100          # orderBookOrders accepts 1..250
HARMLESS_ERROR_CODES = frozenset({30003})        # "Already Subscribed"
OK_CODES = frozenset({0, 200})                   # REST bodies carry "code": 200 on success
RATE_LIMIT_CODES = frozenset({23000, 30009})
QUOTE = 'USDC'
_SYMBOL = re.compile(r'[A-Za-z0-9]{1,30}')


def _channel(value: Any) -> str | None:
    return value.replace('/', ':') if isinstance(value, str) else None


def _price(value: Any) -> Decimal:
    """Strict positive price."""
    price = D(value)
    if price <= 0:
        raise ValueError('Non-positive price')
    return price


def _size(value: Any) -> Decimal:
    """Strict non-negative absolute size (0 deletes a level)."""
    size = D(value)
    if size < 0:
        raise ValueError('Negative size')
    return size


def _valid_index(value: Any, source_id: str) -> int:
    try:
        return as_int(value)
    except ValueError as exc:
        raise ConfigError(f'index.sources[{source_id}].market_index: must be a non-negative integer') from exc


class LighterAdapter(AdapterBase):
    VENUE = 'lighter'
    PREFIX = 'LIGHTER'
    DEFAULT_WS_URL = LIGHTER_WS
    DEFAULT_REST_URL = LIGHTER_REST
    ping_interval = 0.0          # server pings {"type":"ping"}; keepalive ~2 min
    idle_timeout = 60.0

    @property
    def rest_min_interval(self) -> float:
        """Seconds between REST polls (read by the hub's refresher). Anonymous access is
        limited to 60 requests per rolling minute; a mark/last poll costs two requests."""
        return 4.0 if self.wants_ticker else 2.0

    def __init__(self, source: Any, *, timeout: float, market_index: int | None = None,
                 rest_url: str | None = None, expected_base: str | None = None) -> None:
        super().__init__(source, timeout=timeout, rest_url=rest_url, expected_base=expected_base)
        pinned = market_index if market_index is not None else getattr(source, 'market_index', None)
        self._market_index: int | None = None if pinned is None else _valid_index(pinned, self.key)
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._nonce: int | None = None
        self._synced = False

    def _check_symbol(self) -> None:
        if not _SYMBOL.fullmatch(self.symbol):
            raise self._symbol_error('Lighter perp symbol is the base coin, e.g. STONK')
        if self.quote != QUOTE:
            raise ConfigError(f'index.sources[{self.key}].quote: Lighter perpetuals are quoted in USDC')

    def _derive_base(self) -> str | None:
        return self.symbol

    @property
    def market_index(self) -> int | None:
        return self._market_index

    def _require_index(self) -> int:
        if self._market_index is None:
            raise self.fatal('LIGHTER_MARKET_INDEX_UNKNOWN')
        return self._market_index

    def resolve_market_index(self, http: Any, timeout: float | None = None) -> int:
        """Look the perp up by exact symbol (``/api/v1/orderBookDetails``) and cache its index.

        Raises ``TransientUnsafe`` for a transient lookup failure and ``Unsafe`` for a
        definitive one (not listed, not an active exact perp, pinned index mismatch).
        """
        info = self.instrument_info(http, self.timeout if timeout is None else timeout)
        if not info['ok']:
            raise TransientUnsafe(info['reason']) if info['transient'] else Unsafe(info['reason'])
        index = info['market_index']
        if self._market_index is not None and index != self._market_index:
            raise Unsafe(f'LIGHTER_MARKET_INDEX_MISMATCH:{self._market_index}')
        self._market_index = index
        return index

    # ------------------------------------------------------------------ websocket
    def subscribe_frames(self) -> list[str]:
        index = self._require_index()
        frames = [dumps({'type': 'subscribe', 'channel': f'order_book/{index}'})]
        if self.wants_ticker:
            frames.append(dumps({'type': 'subscribe', 'channel': f'market_stats/{index}'}))
        return frames

    def ping_frame(self, wall_ms: int) -> str | None:
        return None

    def on_reset(self) -> None:
        self._bids = {}
        self._asks = {}
        self._nonce = None
        self._synced = False

    def _on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        index = self._require_index()
        if len(text) <= 32 and text.strip() in PING_TEXTS:     # cheap check before JSON parsing
            return [Reply(PONG)]
        msg = self.parse(text)
        if msg is None:
            return [self.error('INVALID_WS_MESSAGE')]
        kind = msg.get('type')
        if kind == 'ping':
            return [Reply(PONG)]
        if kind in ('pong', 'connected', 'unsubscribed'):
            return []
        if kind == 'error' or msg.get('error') is not None:
            return self._error_frame(msg)
        if kind == 'shutdown':
            raise self.fatal('LIGHTER_SERVER_SHUTDOWN')
        channel = _channel(msg.get('channel'))
        if kind in (SNAPSHOT, UPDATE):
            if channel != f'order_book:{index}':
                return []
            return self._book_frame(msg, kind, now, wall_ms)
        if kind in STATS and self.wants_ticker and channel == f'market_stats:{index}':
            acks = [self.subscribed()] if kind == STATS[0] else []
            return acks + self.guard(self.ticker_code, self._stats, msg, now, wall_ms, index)
        return []

    def _error_frame(self, msg: dict) -> list[Update]:
        error = msg.get('error')
        code = error.get('code') if isinstance(error, dict) else msg.get('code')
        try:
            value = as_code(code)
        except ValueError:
            value = None
        if value in HARMLESS_ERROR_CODES:
            return []
        raise self.fatal('SUBSCRIPTION_REJECTED')

    @staticmethod
    def _levels(rows: Any) -> list[Level]:
        """``[{"price", "size"}]`` -> Decimal pairs; size is absolute, 0 means delete."""
        if not isinstance(rows, list):
            raise TypeError('levels')
        result = []
        for row in rows:
            if not isinstance(row, dict):
                raise TypeError('level')
            price, size = _price(row['price']), _size(row['size'])
            result.append((price, size))
        return result

    def _book_frame(self, msg: dict, kind: str, now: float, wall_ms: int) -> list[Update]:
        try:
            body = msg['order_book']
            if not isinstance(body, dict) or as_code(body.get('code', 0)) != 0:
                raise ValueError('order_book')
            asks = self._levels(body['asks'])
            bids = self._levels(body['bids'])
            nonce = as_int(body['nonce'])
            begin = as_int(body['begin_nonce']) if kind == UPDATE else None
        except DATA_ERRORS as exc:
            self.on_reset()
            raise self.fatal('INVALID_LIGHTER_BOOK') from exc
        updates: list[Update] = []
        if kind == SNAPSHOT:
            self._bids = {price: size for price, size in bids if size > 0}
            self._asks = {price: size for price, size in asks if size > 0}
            self._nonce = nonce
            self._synced = True
            updates.append(self.subscribed())
        else:
            if not self._synced:
                return []                # increments omit unchanged levels: wait for the snapshot
            if begin != self._nonce:
                self.on_reset()
                raise self.fatal('LIGHTER_NONCE_GAP')
            if nonce < begin:
                self.on_reset()
                raise self.fatal('INVALID_LIGHTER_BOOK')
            for side, rows in ((self._bids, bids), (self._asks, asks)):
                for price, size in rows:
                    if size == 0:
                        side.pop(price, None)
                    else:
                        side[price] = size
            self._nonce = nonce
        if self._bids and self._asks and max(self._bids) >= min(self._asks):
            self.on_reset()
            raise self.fatal('LIGHTER_BOOK_CROSSED')
        if not self._bids or not self._asks:
            updates.append(self.error('EMPTY_BOOK_SIDE'))
            return updates
        book = Book(bids=tuple(heapq.nlargest(PUBLISH_DEPTH, self._bids.items())),
                    asks=tuple(heapq.nsmallest(PUBLISH_DEPTH, self._asks.items())),
                    received=float(now), event_ms=int(wall_ms), update_id=None)
        updates.append(self.book_update(book))
        return updates

    def _stats(self, msg: dict, now: float, wall_ms: int, index: int) -> list[Update]:
        body = msg['market_stats']
        if isinstance(body, dict) and 'market_id' not in body and str(index) in body:
            body = body[str(index)]                  # market_stats:all form {"<id>": {...}}
        if not isinstance(body, dict):
            raise TypeError('market_stats')
        market_id = body.get('market_id')
        if market_id is not None and as_int(market_id) != index:
            raise FrameError('UNEXPECTED_LIGHTER_STATS_SCHEMA')
        ticker = Ticker(mark=opt_price(body.get('mark_price')), last=opt_price(body.get('last_trade_price')),
                        received=float(now), event_ms=int(wall_ms))
        return [self.ticker_update(ticker)]

    # ------------------------------------------------------------------ REST
    @staticmethod
    def _envelope(payload: Any) -> dict:
        if not isinstance(payload, dict):
            raise TypeError('payload')
        code = payload.get('code')
        if code is not None:
            try:
                value = as_code(code)
            except ValueError:
                value = None
            if value not in OK_CODES:
                if value in RATE_LIMIT_CODES:
                    raise TransientUnsafe('LIGHTER_REST_RATE_LIMITED')
                raise TransientUnsafe(f'LIGHTER_REST_ERROR:{detail(code)}')
        return payload

    def _rest_index(self) -> int:
        if self._market_index is None:
            raise Unsafe('LIGHTER_MARKET_INDEX_UNKNOWN')
        return self._market_index

    @staticmethod
    def _aggregate(rows: Any, total: Any, *, bids: bool) -> tuple[Level, ...]:
        """Individual orders -> price levels (sum of remaining sizes), best first."""
        if not isinstance(rows, list) or not rows:
            raise FrameError('EMPTY_BOOK_SIDE')
        levels: dict[Decimal, Decimal] = {}
        for order in rows:
            if not isinstance(order, dict):
                raise TypeError('order')
            price, size = _price(order['price']), _size(order['remaining_base_amount'])
            if size > 0:
                levels[price] = levels.get(price, ZERO) + size
        ordered = best_first(levels.items(), bids=bids)
        try:
            more = total is not None and as_int(total) > len(rows)
        except ValueError:
            more = False
        if (len(rows) >= REST_ORDERS_LIMIT or more) and len(ordered) > 1:
            ordered = ordered[:-1]     # the deepest level may be missing orders beyond the limit
        if not ordered:
            raise FrameError('EMPTY_BOOK_SIDE')
        return ordered

    def _rest_book(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        index = self._rest_index()
        payload = self._envelope(self.rest_get(http, '/api/v1/orderBookOrders',
                                               {'market_id': index, 'limit': REST_ORDERS_LIMIT}, timeout))
        book = Book(bids=self._aggregate(payload['bids'], payload.get('total_bids'), bids=True),
                    asks=self._aggregate(payload['asks'], payload.get('total_asks'), bids=False),
                    received=float(now), event_ms=int(wall_ms), update_id=None)
        return self.book_update(book, 'rest')

    def _rest_ticker(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        index = self._rest_index()
        payload = self._envelope(self.rest_get(http, '/api/v1/orderBookDetails',
                                               {'market_id': index, 'filter': 'perp'}, timeout))
        rows = [row for row in payload['order_book_details']
                if isinstance(row, dict) and row.get('market_id') is not None and as_int(row['market_id']) == index]
        if len(rows) != 1:
            raise FrameError(f'INVALID_REST_TICKER:{self.key}')
        row = rows[0]
        ticker = Ticker(mark=opt_price(row.get('mark_price')), last=opt_price(row.get('last_trade_price')),
                        received=float(now), event_ms=int(wall_ms))
        return self.ticker_update(ticker, 'rest')

    # ------------------------------------------------------------------ doctor
    def _instrument(self, http: Any, timeout: float, info: dict) -> None:
        info['market_index'] = None
        payload = self.ref_get(http, '/api/v1/orderBookDetails', {'filter': 'perp'}, timeout)
        code = payload.get('code')
        try:
            value = as_code(code) if code is not None else 200
        except ValueError:
            value = None
        details = payload.get('order_book_details')
        if value not in OK_CODES or not isinstance(details, list):
            raise ReferenceFailure(f'REFERENCE_LOOKUP_FAILED:INVALID_LIGHTER_DETAILS:{detail(code)}')
        rows = [row for row in details
                if isinstance(row, dict) and row.get('symbol') == self.symbol and row.get('market_type') == 'perp']
        if len(rows) != 1:
            raise ReferenceFailure(self.not_listed)
        row = rows[0]
        problems: list[str] = info['problems']
        info.update(raw_keys=self.raw_keys(row), status=row.get('status'), base=row.get('symbol'), quote=QUOTE,
                    market_index=as_int(row['market_id']))
        self.check_base(row.get('symbol'), problems)
        self.check_equal(problems, 'STATUS', row.get('status'), 'active')
        # ``multiplier`` / ``quote_multiplier`` semantics are undocumented; anything but 1
        # would need a price normalisation we cannot derive -> refuse (fail closed).
        for name, field in (('MULTIPLIER', 'multiplier'), ('QUOTE_MULTIPLIER', 'quote_multiplier')):
            raw = row.get(field)
            if raw is None:
                continue
            if positive_decimal(raw) != ONE:
                problems.append(f'{name}:{detail(raw)}')
        info['multiplier'] = plain(positive_decimal(row.get('multiplier')))
        decimals = row.get('price_decimals', row.get('supported_price_decimals'))
        try:
            places = as_int(decimals)
        except ValueError:
            places = None
        if places is None or places > 18:
            problems.append('TICK')
        else:
            info['tick'] = plain(ONE.scaleb(-places))
        config = row.get('market_config')
        if isinstance(config, dict):
            if config.get('force_reduce_only') is True:
                info['warnings'].append('FORCE_REDUCE_ONLY')
            if config.get('hidden') is True:
                info['warnings'].append('HIDDEN_MARKET')

