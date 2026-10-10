"""Bitget V2 (classic) USDT-M perpetual index source (``venue: bitget_usdt_futures``).

Wire facts (notes/api_bitget.md):

* WS ``wss://ws.bitget.com/v2/ws/public``; subscribe
  ``{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books1","instId":"STONKUSDT"}]}``
  (camelCase keys, upper-case instType). Ack ``{"event":"subscribe","arg":{...}}``.
  Errors ``{"event":"error","code":30001,"msg":...}`` -> ``SUBSCRIPTION_REJECTED``
  (socket-fatal). Never the generic ``"code" in msg`` rule.
* Keepalive: the client sends the TEXT frame ``ping`` (not JSON) every 30 s and the server
  answers the text ``pong``; both are handled before JSON parsing. Without pings the
  server closes the socket after 2 minutes.
* ``books1``: every push is a full snapshot (``action == "snapshot"``); when the book does
  not change nothing is pushed (configure ``rest_refresh_after_seconds``). Push:
  ``{"action","arg","data":[{"asks":[[p,s]],"bids":[[p,s]],"checksum","ts":"<ms>"}],"ts":<ms>}``.
  ``event_ms`` = ``data[0].ts`` (top-level ``ts`` as fallback); no update id is relied upon
  (``seq`` is not confirmed for classic v2). Checksums only exist for the incremental
  ``books`` channel. Any other action on ``books1`` is ``UNEXPECTED_BITGET_BOOKS1_SCHEMA``.
* ``ticker`` (only for price kind mark/last): ``data[0]`` with ``markPrice``, ``lastPr``,
  ``ts``.
* REST (envelope ``{"code":"00000","msg","requestTime","data"}``):
  ``/api/v2/mix/market/merge-depth?symbol=&productType=&precision=scale0&limit=1``
  (``data`` object; level elements may be strings or numbers -> parsed exactly);
  ``/api/v2/mix/market/ticker`` (``data`` is an ARRAY); ``/api/v2/mix/market/contracts``
  for the doctor (tick = ``priceEndStep * 10**-pricePlace``; healthy only when
  ``symbolStatus == "normal"``).
"""
from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import Any

from ..errors import ConfigError, TransientUnsafe
from ..types import Ticker
from ._venue_common import (AdapterBase, FrameError, ReferenceFailure, as_int, detail, dumps, make_book,
                            opt_price, parse_levels, plain, positive_decimal)
from .base import Reply, Update

LOG = logging.getLogger(__name__)

BITGET_WS = 'wss://ws.bitget.com/v2/ws/public'
BITGET_REST = 'https://api.bitget.com'
BOOK_CHANNEL = 'books1'
TICKER_CHANNEL = 'ticker'
SUCCESS = '00000'
UNEXPECTED_BOOKS1 = 'UNEXPECTED_BITGET_BOOKS1_SCHEMA'
UNEXPECTED_TICKER = 'UNEXPECTED_BITGET_TICKER_SCHEMA'
RATE_LIMIT_CODES = frozenset({'429', '1001'})
REMOVED_CODES = frozenset({'40309'})          # "The contract has been removed"
PRODUCT_TYPES = {'USDT': 'USDT-FUTURES', 'USDC': 'USDC-FUTURES'}
_SYMBOL = re.compile(r'[A-Z0-9]{2,40}')


class BitgetAdapter(AdapterBase):
    VENUE = 'bitget_usdt_futures'
    PREFIX = 'BITGET'
    DEFAULT_WS_URL = BITGET_WS
    DEFAULT_REST_URL = BITGET_REST
    ping_interval = 30.0
    idle_timeout = 75.0          # books1 may be silent; the pong every 30 s keeps the socket alive

    def __init__(self, source: Any, *, timeout: float, rest_url: str | None = None,
                 expected_base: str | None = None) -> None:
        super().__init__(source, timeout=timeout, rest_url=rest_url, expected_base=expected_base)
        self.product_type = PRODUCT_TYPES[self.quote]

    def _check_symbol(self) -> None:
        if not _SYMBOL.fullmatch(self.symbol):
            raise self._symbol_error('Bitget symbol must be upper-case letters/digits, e.g. STONKUSDT')
        if self.quote not in PRODUCT_TYPES:
            raise ConfigError(f'index.sources[{self.key}].quote: Bitget futures quote must be USDT or USDC')

    def _derive_base(self) -> str | None:
        if self.symbol.endswith(self.quote) and len(self.symbol) > len(self.quote):
            return self.symbol[:-len(self.quote)]
        return None

    def _arg(self, channel: str) -> dict:
        return {'instType': self.product_type, 'channel': channel, 'instId': self.symbol}

    # ------------------------------------------------------------------ websocket
    def subscribe_frames(self) -> list[str]:
        args = [self._arg(BOOK_CHANNEL)]
        if self.wants_ticker:
            args.append(self._arg(TICKER_CHANNEL))
        return [dumps({'op': 'subscribe', 'args': args})]

    def ping_frame(self, wall_ms: int) -> str | None:
        return 'ping'

    def _on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        heartbeat = text.strip()
        if heartbeat == 'pong':
            return []
        if heartbeat == 'ping':
            return [Reply('pong')]
        msg = self.parse(text)
        if msg is None:
            return [self.error('INVALID_WS_MESSAGE')]
        event = msg.get('event')
        if event == 'error':
            raise self.fatal('SUBSCRIPTION_REJECTED')
        arg = msg.get('arg')
        ours = (isinstance(arg, dict) and arg.get('instId') == self.symbol
                and arg.get('instType') == self.product_type)
        if event == 'subscribe':
            return [self.subscribed()] if ours and arg.get('channel') == BOOK_CHANNEL else []
        if event is not None or not ours:
            return []                    # unsubscribe/login acks, foreign instruments
        channel = arg.get('channel')
        if channel == BOOK_CHANNEL:
            return self.guard(self.book_code, self._book, msg, now)
        if channel == TICKER_CHANNEL and self.wants_ticker:
            return self.guard(self.ticker_code, self._ticker, msg, now)
        return []

    @staticmethod
    def _single_row(msg: dict, code: str) -> dict:
        data = msg.get('data')
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise FrameError(code)
        return data[0]

    @staticmethod
    def _event_ms(row: dict, msg: dict) -> int:
        stamp = row.get('ts')
        return as_int(stamp if stamp not in (None, '') else msg['ts'])

    def _book(self, msg: dict, now: float) -> list[Update]:
        # books1 pushes are full snapshots: never merge, never keep an older side.
        if msg.get('action') != 'snapshot':
            raise FrameError(UNEXPECTED_BOOKS1)
        row = self._single_row(msg, UNEXPECTED_BOOKS1)
        book = make_book(parse_levels(row['bids']), parse_levels(row['asks']), now, self._event_ms(row, msg))
        return [self.book_update(book)]

    def _ticker(self, msg: dict, now: float) -> list[Update]:
        row = self._single_row(msg, UNEXPECTED_TICKER)
        inst = row.get('instId', row.get('symbol'))
        if inst is not None and inst != self.symbol:
            raise FrameError(UNEXPECTED_TICKER)
        ticker = Ticker(mark=opt_price(row.get('markPrice')), last=opt_price(row.get('lastPr')),
                        received=float(now), event_ms=self._event_ms(row, msg))
        return [self.ticker_update(ticker)]

    # ------------------------------------------------------------------ REST
    @staticmethod
    def _envelope(payload: Any) -> Any:
        code = payload.get('code')
        if str(code) != SUCCESS or isinstance(code, bool):
            if str(code) in RATE_LIMIT_CODES:
                raise TransientUnsafe('BITGET_REST_RATE_LIMITED')
            raise TransientUnsafe(f'BITGET_REST_ERROR:{detail(code)}')
        return payload['data']

    def _rest_book(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, '/api/v2/mix/market/merge-depth',
                                {'symbol': self.symbol, 'productType': self.product_type,
                                 'precision': 'scale0', 'limit': '1'}, timeout)
        data = self._envelope(payload)
        if not isinstance(data, dict):
            raise TypeError('data')
        stamp = data.get('ts')
        event_ms = as_int(stamp if stamp not in (None, '') else payload['requestTime'])
        book = make_book(parse_levels(data['bids']), parse_levels(data['asks']), now, event_ms)
        return self.book_update(book, 'rest')

    def _rest_ticker(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, '/api/v2/mix/market/ticker',
                                {'symbol': self.symbol, 'productType': self.product_type}, timeout)
        data = self._envelope(payload)
        rows = [row for row in data if isinstance(row, dict) and row.get('symbol') == self.symbol]
        if len(rows) != 1:
            raise FrameError(f'INVALID_REST_TICKER:{self.key}')
        row = rows[0]
        stamp = row.get('ts')
        event_ms = as_int(stamp if stamp not in (None, '') else payload['requestTime'])
        ticker = Ticker(mark=opt_price(row.get('markPrice')), last=opt_price(row.get('lastPr')),
                        received=float(now), event_ms=event_ms)
        return self.ticker_update(ticker, 'rest')

    # ------------------------------------------------------------------ doctor
    def _instrument(self, http: Any, timeout: float, info: dict) -> None:
        payload = self.ref_get(http, '/api/v2/mix/market/contracts',
                               {'productType': self.product_type, 'symbol': self.symbol}, timeout)
        code = payload.get('code')
        if str(code) in REMOVED_CODES:
            raise ReferenceFailure(self.not_listed)
        data = payload.get('data')
        if str(code) != SUCCESS or isinstance(code, bool) or not isinstance(data, list):
            raise ReferenceFailure(f'REFERENCE_LOOKUP_FAILED:INVALID_BITGET_CONTRACTS:{detail(code)}')
        rows = [row for row in data if isinstance(row, dict) and row.get('symbol') == self.symbol]
        if len(rows) != 1:
            raise ReferenceFailure(self.not_listed)
        row = rows[0]
        problems: list[str] = info['problems']
        info.update(raw_keys=self.raw_keys(row), status=row.get('symbolStatus'), base=row.get('baseCoin'),
                    quote=row.get('quoteCoin'))
        self.check_base(row.get('baseCoin'), problems)
        self.check_equal(problems, 'QUOTE', row.get('quoteCoin'), self.quote)
        self.check_equal(problems, 'TYPE', row.get('symbolType'), 'perpetual')
        self.check_equal(problems, 'STATUS', row.get('symbolStatus'), 'normal')
        tick = self._tick(row)
        if tick is None:
            problems.append('TICK')
        info['tick'] = plain(tick)
        info['multiplier'] = plain(positive_decimal(row.get('sizeMultiplier')))
        off_time = row.get('offTime')
        if off_time not in (None, '', '-1', -1):
            info['warnings'].append(f'DELISTING_SCHEDULED:{detail(off_time)}')

    @staticmethod
    def _tick(row: dict) -> Decimal | None:
        """tick = priceEndStep * 10**-pricePlace (e.g. pricePlace 5, priceEndStep 1 -> 0.00001)."""
        end_step = positive_decimal(row.get('priceEndStep'))
        try:
            place = as_int(row.get('pricePlace'))
        except ValueError:
            return None
        if end_step is None or place > 18:
            return None
        return end_step.scaleb(-place)
