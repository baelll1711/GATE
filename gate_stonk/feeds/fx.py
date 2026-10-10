"""USDC -> USDT rate sources (index.fx.mode): fixed | binance_spot | gate_spot.

The rate is USDT per 1 USDC (the mid of a USDCUSDT / USDC_USDT book), so a
USDC-quoted index source is converted with ``usdt = native * rate`` (MULTIPLY).
FX is only needed when an enabled index source quotes USDC.

* ``fixed``: :class:`FixedFx`, no worker; the configured ``usdc_to_usdt`` is still
  depeg-checked by the index builder.
* ``binance_spot``: :class:`BinanceFxAdapter`, raw stream
  ``wss://stream.binance.com:9443/ws/usdcusdt@depth5@100ms`` = periodic partial-depth
  snapshots ``{lastUpdateId, bids, asks}`` WITHOUT an exchange timestamp, so
  ``event_ms`` is the receipt wall time (reference); an unchanged lastUpdateId is a
  fresh periodic snapshot. REST fallback ``GET /api/v3/ticker/bookTicker?symbol=USDCUSDT``
  (notes/api_lighter_fx.md B1). Binance pings with control frames; websocket-client
  answers them, so no app ping is sent.
* ``gate_spot``: :class:`GateSpotFxAdapter`, ``spot.book_ticker`` [USDC_USDT]
  ``{t (ms), u, s, b, B, a, A}`` (notes/api_lighter_fx.md B2, api_gate_ws_futures.md 2.1);
  REST fallback ``GET /spot/order_book?currency_pair=USDC_USDT&limit=1&with_id=true``.
"""
from __future__ import annotations

import json
from typing import Any

from ..dec import D
from ..errors import ConfigError, Unsafe
from ..types import Book, FxSnapshot
from .base import FX_KEY, Reply, Update
from .gate_spot import PARSE_ERRORS, GateSpotWs, get_json, levels, strict_int

BINANCE_FX_SYMBOL = 'USDCUSDT'
BINANCE_FX_STREAM = 'usdcusdt@depth5@100ms'
BINANCE_FX_WS = 'wss://stream.binance.com:9443/ws/' + BINANCE_FX_STREAM
BINANCE_REST = 'https://api.binance.com'
GATE_FX_PAIR = 'USDC_USDT'
FX_REST_REFRESH_AFTER_SECONDS = 1.5
FX_MODES = ('fixed', 'binance_spot', 'gate_spot')


class FixedFx:
    """Configured constant rate; no worker, never stale."""

    mode = 'fixed'

    def __init__(self, settings: Any) -> None:
        try:
            rate = D(settings.usdc_to_usdt)
        except (ValueError, AttributeError) as exc:
            raise ConfigError('index.fx.usdc_to_usdt: must be a positive number') from exc
        if rate <= 0:
            raise ConfigError('index.fx.usdc_to_usdt: must be a positive number')
        self.rate = rate

    def snapshot(self) -> FxSnapshot:
        return FxSnapshot(rate=self.rate, mode=self.mode, book=None, error=None)


class BinanceFxAdapter:
    """Binance spot ``usdcusdt@depth5@100ms`` periodic snapshots (+ REST bookTicker)."""

    venue = 'binance_spot'
    key = FX_KEY
    ping_interval = 0.0          # control pings only; the server's pings are auto-answered
    idle_timeout = 60.0
    rest_refresh_after_seconds: float | None = FX_REST_REFRESH_AFTER_SECONDS
    rest_min_interval = 1.0

    def __init__(self, settings: Any = None, *, timeout: float | None = None,
                 ws_url: str | None = None, rest_url: str | None = None) -> None:
        self.settings = settings
        self.timeout = timeout
        self._ws_url = ws_url or BINANCE_FX_WS
        self.rest_url = (rest_url or BINANCE_REST).rstrip('/')

    def ws_url(self) -> str:
        return self._ws_url

    def subscribe_frames(self) -> list[str]:
        return []                # raw stream URL: no subscription request

    def ping_frame(self, wall_ms: int) -> str | None:
        return None

    def decode(self, raw: bytes | str) -> str:
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw).decode('utf-8')
        return raw

    def on_reset(self) -> None:
        return None

    def on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        try:
            msg = json.loads(text)
        except ValueError as exc:
            raise Unsafe('INVALID_WS_MESSAGE') from exc
        if not isinstance(msg, dict):
            raise Unsafe('INVALID_WS_MESSAGE')
        if msg.get('error') is not None:
            raise Unsafe('SUBSCRIPTION_REJECTED')
        if msg.get('e') == 'serverShutdown':
            raise Unsafe('SERVER_SHUTDOWN')
        if 'id' in msg and 'result' in msg:
            return []            # request ack (not used with the raw stream URL)
        if 'stream' in msg and msg['stream'] != BINANCE_FX_STREAM:
            return [Update('error', self.key, error='WRONG_FX_STREAM')]
        data = msg.get('data', msg)
        try:
            if not isinstance(data, dict):
                raise Unsafe('INVALID_BOOK_SCHEMA')
            # Partial-depth snapshots carry no exchange time: receipt time is used
            # explicitly; an unchanged lastUpdateId is a fresh periodic snapshot.
            book = Book(levels(data['bids']), levels(data['asks']), now, int(wall_ms),
                        strict_int(data['lastUpdateId']))
        except Unsafe as exc:
            return [Update('error', self.key, error=str(exc))]
        except PARSE_ERRORS:
            return [Update('error', self.key, error='INVALID_WS_BOOK:' + self.key)]
        return [Update('book', self.key, book=book)]

    def rest_snapshot(self, http: Any, timeout: float, now: float, wall_ms: int) -> list[Update]:
        payload = get_json(http, f'{self.rest_url}/api/v3/ticker/bookTicker', {'symbol': BINANCE_FX_SYMBOL}, timeout)
        if not isinstance(payload, dict):
            raise Unsafe('INVALID_REST_BOOK')
        if payload.get('symbol') != BINANCE_FX_SYMBOL:
            raise Unsafe('WRONG_FX_SYMBOL')
        try:
            book = Book(((D(payload['bidPrice']), D(payload['bidQty'])),),
                        ((D(payload['askPrice']), D(payload['askQty'])),), now, int(wall_ms), None)
        except PARSE_ERRORS as exc:
            raise Unsafe('INVALID_REST_BOOK') from exc
        return [Update('book', self.key, book=book, source='rest')]

    def instrument_info(self, http: Any, timeout: float) -> dict:
        row: dict[str, Any] = {'ok': False, 'symbol': BINANCE_FX_SYMBOL, 'status': None, 'base': None,
                               'quote': None, 'tick': None, 'contract_size': None, 'raw_keys': [], 'reason': None}
        try:
            payload = get_json(http, f'{self.rest_url}/api/v3/exchangeInfo', {'symbol': BINANCE_FX_SYMBOL}, timeout)
            symbols = payload.get('symbols') if isinstance(payload, dict) else None
            if not isinstance(symbols, list):
                raise Unsafe('INVALID_REST_RESPONSE')
            rows = [r for r in symbols if isinstance(r, dict) and r.get('symbol') == BINANCE_FX_SYMBOL]
            if len(rows) != 1:
                row['reason'] = 'FX_SYMBOL_NOT_LISTED:' + BINANCE_FX_SYMBOL
                return row
            info = rows[0]
            row['raw_keys'] = sorted(str(k) for k in info)
            row.update(status=info.get('status'), base=info.get('baseAsset'), quote=info.get('quoteAsset'))
            if row['base'] != 'USDC' or row['quote'] != 'USDT' or row['status'] != 'TRADING':
                row['reason'] = 'FX_SYMBOL_NOT_ACTIVE:' + BINANCE_FX_SYMBOL
            else:
                row['ok'] = True
        except Unsafe as exc:
            code = str(exc)
            row['reason'] = 'REFERENCE_LOOKUP_FAILED:' + (code[len('REST_'):] if code.startswith('REST_HTTP_') else code)
        except Exception as exc:
            row['reason'] = 'REFERENCE_LOOKUP_FAILED:' + type(exc).__name__
        return row


class GateSpotFxAdapter(GateSpotWs):
    """Gate spot ``spot.book_ticker`` USDC_USDT (+ REST order book fallback)."""

    rest_refresh_after_seconds: float | None = FX_REST_REFRESH_AFTER_SECONDS

    def __init__(self, settings: Any = None, *, timeout: float | None = None,
                 ws_url: str | None = None, rest_url: str | None = None) -> None:
        super().__init__(FX_KEY, GATE_FX_PAIR, ws_url=ws_url, rest_url=rest_url)
        self.settings = settings
        self.timeout = timeout

    def subscriptions(self) -> list[tuple[str, list[str]]]:
        return [('spot.book_ticker', [self.pair])]

    def on_data(self, channel: str, event: Any, result: Any, now: float, wall_ms: int) -> list[Update | Reply]:
        if channel != 'spot.book_ticker' or event != 'update':
            return []
        try:
            if not isinstance(result, dict):
                raise Unsafe('INVALID_BOOK_SCHEMA')
            if result.get('s') != self.pair:
                raise Unsafe('WRONG_FX_SYMBOL')
            # Keys are case-sensitive: b/B = bid price/size, a/A = ask price/size.
            book = Book(((D(result['b']), D(result['B'])),), ((D(result['a']), D(result['A'])),), now,
                        strict_int(result['t']), strict_int(result['u']))
        except Unsafe as exc:
            return [self.book_error(exc)]
        except PARSE_ERRORS as exc:
            return [self.book_error(exc)]
        return [Update('book', self.key, book=book)]

    def rest_snapshot(self, http: Any, timeout: float, now: float, wall_ms: int) -> list[Update]:
        return [Update('book', self.key, book=self.rest_order_book(http, timeout, now, 1), source='rest')]

    def instrument_info(self, http: Any, timeout: float) -> dict:
        return self.pair_info(http, timeout)


def fx_needed(sources: Any) -> bool:
    """FX matters only when an enabled index source quotes USDC."""
    return any(getattr(s, 'quote', None) == 'USDC' and getattr(s, 'enabled', True) for s in sources)


def build_fx_adapter(settings: Any, *, timeout: float, ws_url: str | None = None,
                     rest_url: str | None = None) -> BinanceFxAdapter | GateSpotFxAdapter | None:
    """Streaming adapter for the configured FX mode (None for ``fixed``)."""
    mode = settings.mode
    if mode == 'fixed':
        return None
    if mode == 'binance_spot':
        return BinanceFxAdapter(settings, timeout=timeout)
    if mode == 'gate_spot':
        return GateSpotFxAdapter(settings, timeout=timeout, ws_url=ws_url, rest_url=rest_url)
    raise ConfigError(f'index.fx.mode: must be one of {FX_MODES}')
