"""Gate spot WebSocket adapters: the traded book + public tape of STONK_USDT.

Wire facts (notes/api_gate_ws_futures.md §1-2, notes/api_gate_rest.md, Gate SDK
``models/order_book.py``):

* URL ``wss://api.gateio.ws/ws/v4/``; request
  ``{"time": <sec>, "id": <int>, "channel": ..., "event": "subscribe", "payload": [...]}``.
* App ping ``{"time": <sec>, "channel": "spot.ping"}`` (SDKs ping every 5-10 s);
  the answer arrives on channel ``spot.pong``.
* Subscribe ack ``{"event": "subscribe", "result": {"status": "success"}}``;
  rejection carries ``"error": {"code", "message"}`` and/or ``result.status == "fail"``.
* ``spot.order_book`` payload ``[pair, level, interval]`` pushes FULL snapshots
  ``{t (ms), lastUpdateId, s, l?, bids [[p, q]], asks [[p, q]]}``: every push replaces
  the whole book (never merge, never retain old levels).
* ``spot.trades`` payload ``[pair]`` pushes ``result`` as an object or an array of
  ``{id, create_time, create_time_ms (decimal STRING, e.g. "1648725035923.0"),
  side (taker), currency_pair, amount, price}``.
* REST ``GET /spot/order_book?currency_pair=&limit=&with_id=true`` ->
  ``{id, current (ms), update (ms), asks, bids}``.

Data problems become ``Update(kind='error')`` for this key only; only socket-level
conditions (subscription rejected, non-JSON frame) raise ``Unsafe`` (reconnect).

Freshness of the traded book (deliberate choice): ``event_ms`` = ``t``, the book's own
time, for both the event-age check and regression/cross-transport ordering. The
envelope ``time_ms`` (push time) is NOT used for freshness: it proves only that a push
is recent, not that the book content is (a publisher stuck on an old book would look
fresh), ``Book`` carries a single event time that ordering and the engine's own-order
window also use, and whether Gate re-pushes an unchanged book at all is unconfirmed
(API_NOTES §2.2, stage-1 check §9.1). A quiet book stays usable through the hub's REST
refresher instead: once the stored book's event age passes ``rest_refresh_after_seconds``
(1.5 s, even while pushes keep arriving) it fetches ``/spot/order_book`` (``current`` =
server time), and pushes older than that snapshot are dropped without invalidating it.
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from ..dec import D, step
from ..errors import ConfigError, Unsafe
from ..types import SIDES, Book
from .base import GATE_SPOT_KEY, Reply, Update

LOG = logging.getLogger(__name__)

GATE_SPOT_WS = 'wss://api.gateio.ws/ws/v4/'
GATE_REST = 'https://api.gateio.ws/api/v4'
GATE_PING_SECONDS = 10.0
GATE_IDLE_SECONDS = 60.0
DEPTH_LEVELS = (1, 5, 10, 20, 50, 100)
DEPTH_INTERVALS = ('100ms', '1000ms')
SPOT_REST_REFRESH_AFTER_SECONDS = 1.5
PARSE_ERRORS = (KeyError, TypeError, ValueError, AttributeError, IndexError, OverflowError, ArithmeticError)

Level = tuple[Decimal, Decimal]


# --------------------------------------------------------------------------- parsing helpers
def levels(rows: Any) -> tuple[Level, ...]:
    """``[[price, size], ...]`` -> immutable tuple of Decimal pairs (deep copy)."""
    if not isinstance(rows, list) or not rows:
        raise Unsafe('EMPTY_BOOK_SIDE')
    result = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise Unsafe('INVALID_BOOK_SCHEMA')
        price, size = row
        result.append((D(price), D(size)))
    return tuple(result)


def strict_int(value: Any) -> int:
    """Non-negative integral number (int or integral decimal string); bool refused."""
    if isinstance(value, bool):
        raise ValueError('Invalid integer')
    number = D(value)
    result = int(number)
    if result < 0 or number != result:
        raise ValueError('Invalid integer')
    return result


def ms_from_decimal(value: Any) -> int:
    """Gate ``create_time_ms`` ("1648725035923.0" or "1645464610777.399200") -> int ms (floor)."""
    if isinstance(value, bool):
        raise ValueError('Invalid timestamp')
    number = D(value)
    if number < 0:
        raise ValueError('Invalid timestamp')
    return int(number)


def get_json(http: Any, url: str, params: Mapping[str, Any] | None, timeout: float) -> Any:
    """GET + JSON with reason codes; transport exceptions propagate (callers log type names)."""
    response = http.get(url, params=dict(params or {}), timeout=timeout)
    status = getattr(response, 'status_code', None)
    if status != 200:
        raise Unsafe(f'REST_HTTP_{status}')
    try:
        return response.json()
    except ValueError as exc:
        raise Unsafe('INVALID_REST_RESPONSE') from exc


def _now_s() -> int:
    return int(time.time())


# --------------------------------------------------------------------------- Gate spot WS base
class GateSpotWs:
    """Envelope handling shared by every Gate spot WS adapter (book, tape, FX)."""

    venue = 'gate_spot'
    ping_interval = GATE_PING_SECONDS
    idle_timeout = GATE_IDLE_SECONDS
    rest_min_interval = 1.0
    rest_refresh_after_seconds: float | None = None

    def __init__(self, key: str, pair: str, *, ws_url: str | None = None, rest_url: str | None = None) -> None:
        self.key = key
        self.pair = pair
        self._ws_url = ws_url or GATE_SPOT_WS
        self.rest_url = (rest_url or GATE_REST).rstrip('/')

    # channel -> payload; order is the subscription order
    def subscriptions(self) -> list[tuple[str, list[str]]]:
        raise NotImplementedError

    def ws_url(self) -> str:
        return self._ws_url

    def subscribe_frames(self) -> list[str]:
        now = _now_s()
        return [json.dumps({'time': now, 'id': index, 'channel': channel, 'event': 'subscribe',
                            'payload': payload}, separators=(',', ':'))
                for index, (channel, payload) in enumerate(self.subscriptions(), start=1)]

    def ping_frame(self, wall_ms: int) -> str | None:
        return json.dumps({'time': int(wall_ms) // 1000, 'channel': 'spot.ping'}, separators=(',', ':'))

    def decode(self, raw: bytes | str) -> str:
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw).decode('utf-8')
        return raw

    def on_reset(self) -> None:
        return None     # every Gate spot push used here is a full snapshot: no delta state

    def on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        try:
            msg = json.loads(text)
        except ValueError as exc:
            raise Unsafe('INVALID_WS_MESSAGE') from exc
        if not isinstance(msg, dict):
            raise Unsafe('INVALID_WS_MESSAGE')
        channel = msg.get('channel')
        event = msg.get('event')
        if channel in ('spot.pong', 'spot.ping'):
            return []
        ours = {name for name, _ in self.subscriptions()}
        error = msg.get('error')
        if error is not None and error != {}:
            # Gate error envelope {"error": {"code", "message"}}; never the generic
            # '"code" in msg' rule. A rejected subscription leaves us without data.
            if event == 'subscribe' or channel in ours:
                raise Unsafe('SUBSCRIPTION_REJECTED')
            raise Unsafe('GATE_WS_ERROR')
        if event == 'subscribe':
            result = msg.get('result')
            status = result.get('status') if isinstance(result, dict) else None
            if status == 'fail':
                raise Unsafe('SUBSCRIPTION_REJECTED')
            if status == 'success' and channel in ours:
                return [Update('subscribed', self.key)]
            return []
        if event == 'unsubscribe' or channel not in ours:
            return []
        return self.on_data(channel, event, msg.get('result'), now, wall_ms)

    def on_data(self, channel: str, event: Any, result: Any, now: float, wall_ms: int) -> list[Update | Reply]:
        raise NotImplementedError

    def book_error(self, exc: BaseException) -> Update:
        if isinstance(exc, Unsafe):
            return Update('error', self.key, error=str(exc))
        return Update('error', self.key, error='INVALID_WS_BOOK:' + self.key)

    def rest_order_book(self, http: Any, timeout: float, now: float, limit: int) -> Book:
        payload = get_json(http, f'{self.rest_url}/spot/order_book',
                           {'currency_pair': self.pair, 'limit': limit, 'with_id': 'true'}, timeout)
        if not isinstance(payload, dict):
            raise Unsafe('INVALID_REST_BOOK')
        try:
            # ``current`` = server time of this snapshot (ms); ``id`` only with with_id=true.
            return Book(levels(payload['bids']), levels(payload['asks']), now,
                        strict_int(payload['current']), strict_int(payload['id']))
        except Unsafe:
            raise
        except PARSE_ERRORS as exc:
            raise Unsafe('INVALID_REST_BOOK') from exc

    def pair_info(self, http: Any, timeout: float) -> dict:
        """Doctor: exact pair check via ``GET /spot/currency_pairs/{pair}``."""
        row: dict[str, Any] = {'ok': False, 'symbol': self.pair, 'status': None, 'base': None, 'quote': None,
                               'tick': None, 'contract_size': None, 'raw_keys': [], 'reason': None}
        try:
            payload = get_json(http, f'{self.rest_url}/spot/currency_pairs/{self.pair}', None, timeout)
            if not isinstance(payload, dict):
                raise Unsafe('INVALID_REST_RESPONSE')
            row['raw_keys'] = sorted(str(k) for k in payload)
            row.update(status=payload.get('trade_status'), base=payload.get('base'), quote=payload.get('quote'))
            precision = payload.get('precision')
            if isinstance(precision, int) and not isinstance(precision, bool) and 0 <= precision <= 18:
                row['tick'] = format(step(precision), 'f')
            if payload.get('id') != self.pair:
                row['reason'] = 'GATE_SPOT_PAIR_MISMATCH'
            elif row['status'] != 'tradable':
                row['reason'] = f"GATE_SPOT_NOT_TRADABLE:{row['status']}"
            else:
                row['ok'] = True
        except Unsafe as exc:
            code = str(exc)
            row['reason'] = 'REFERENCE_LOOKUP_FAILED:' + (code[len('REST_'):] if code.startswith('REST_HTTP_') else code)
        except Exception as exc:  # transport: log/report the type name only
            row['reason'] = 'REFERENCE_LOOKUP_FAILED:' + type(exc).__name__
        return row


# --------------------------------------------------------------------------- traded book + tape
class GateSpotAdapter(GateSpotWs):
    """STONK_USDT full-depth snapshots (``spot.order_book``) + public tape (``spot.trades``)."""

    rest_refresh_after_seconds: float | None = SPOT_REST_REFRESH_AFTER_SECONDS

    def __init__(self, cfg: Any, *, timeout: float | None = None) -> None:
        market = cfg.market
        pair = market.get('currency_pair')
        if not isinstance(pair, str) or not pair:
            raise ConfigError('market.currency_pair: required')
        depth = market.get('depth_levels', 20)
        if isinstance(depth, bool) or not isinstance(depth, int) or depth not in DEPTH_LEVELS:
            raise ConfigError(f'market.depth_levels: must be one of {DEPTH_LEVELS}')
        interval = market.get('depth_interval', '100ms')
        if interval not in DEPTH_INTERVALS:
            raise ConfigError(f'market.depth_interval: must be one of {DEPTH_INTERVALS}')
        super().__init__(GATE_SPOT_KEY, pair, ws_url=market.get('spot_ws_url'), rest_url=market.get('rest_url'))
        self.depth_levels = depth
        self.depth_interval = interval
        self.timeout = timeout
        if 'rest_refresh_after_seconds' in market:
            value = market.get('rest_refresh_after_seconds')
            self.rest_refresh_after_seconds = None if value is None else float(value)

    def subscriptions(self) -> list[tuple[str, list[str]]]:
        return [('spot.order_book', [self.pair, str(self.depth_levels), self.depth_interval]),
                ('spot.trades', [self.pair])]

    def on_data(self, channel: str, event: Any, result: Any, now: float, wall_ms: int) -> list[Update | Reply]:
        if channel == 'spot.order_book':
            if event not in ('update', 'all'):
                return []
            return [self._book(result, now)]
        if channel == 'spot.trades':
            if event != 'update':
                return []
            return self._trades(result)
        return []

    def _book(self, result: Any, now: float) -> Update:
        try:
            if not isinstance(result, dict):
                raise Unsafe('INVALID_BOOK_SCHEMA')
            if result.get('s') != self.pair:
                raise Unsafe('GATE_SPOT_SYMBOL_MISMATCH')
            # A full snapshot: the whole book is replaced, nothing is merged.
            book = Book(levels(result['bids']), levels(result['asks']), now,
                        strict_int(result['t']), strict_int(result['lastUpdateId']))
        except Unsafe as exc:
            return self.book_error(exc)
        except PARSE_ERRORS as exc:
            return self.book_error(exc)
        return Update('book', self.key, book=book)

    def _trades(self, result: Any) -> list[Update | Reply]:
        if isinstance(result, dict):
            rows = [result]
        elif isinstance(result, list):
            rows = result
        else:
            LOG.warning('Ignoring malformed spot.trades message')
            return []
        trades = []
        malformed = 0
        for row in rows:
            try:
                trade = self._trade(row)
            except PARSE_ERRORS:
                malformed += 1
                continue
            if trade is not None:
                trades.append(trade)
        if malformed:
            # The tape only drives paper simulation: bad rows never invalidate the book.
            LOG.warning('Ignoring %d malformed spot trade rows', malformed)
        return [Update('trades', self.key, trades=tuple(trades))] if trades else []

    def _trade(self, row: Any) -> dict | None:
        if not isinstance(row, dict):
            raise TypeError('trade row')
        pair = row.get('currency_pair')
        if pair is not None and pair != self.pair:
            return None
        trade_id = row['id']
        if isinstance(trade_id, bool) or not isinstance(trade_id, (int, str)) or str(trade_id).strip() == '':
            raise ValueError('trade id')
        if isinstance(trade_id, int) and trade_id < 0:
            raise ValueError('trade id')
        if row.get('create_time_ms') is not None:
            create_ms = ms_from_decimal(row['create_time_ms'])
        else:
            create_ms = strict_int(row['create_time']) * 1000
        side = row['side']
        if side not in SIDES:
            raise ValueError('trade side')
        price, amount = D(row['price']), D(row['amount'])
        if price <= 0 or amount <= 0:
            raise ValueError('trade size')
        return {'id': str(trade_id), 'price': price, 'amount': amount, 'side': side, 'create_time_ms': create_ms}

    def rest_snapshot(self, http: Any, timeout: float, now: float, wall_ms: int) -> list[Update]:
        return [Update('book', self.key, book=self.rest_order_book(http, timeout, now, self.depth_levels),
                       source='rest')]

    def instrument_info(self, http: Any, timeout: float) -> dict:
        return self.pair_info(http, timeout)
