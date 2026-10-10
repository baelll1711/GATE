"""BingX USDT-M perpetual (swap V2) index source (``venue: bingx_swap``).

Wire facts (notes/api_bingx.md, official docs-v3 bundle):

* WS ``wss://open-api-swap.bingx.com/swap-market``. EVERY server frame is GZIP-compressed
  binary, heartbeats included: ``decode`` gunzips (bounded); an undecodable frame becomes
  ``BINGX_GZIP_DECODE_ERROR`` for this key (the socket stays up, per the notes).
* Heartbeat: the server sends ``Ping`` about every 5 s and requires the text reply ``Pong``
  (not compressed); handled before JSON parsing, case-insensitively. The spot-style JSON
  ``{"ping": id, "time": t}`` is answered with ``{"pong": id, "time": t}``. No client pings
  (``ping_interval = 0``: the worker only sends websocket control pings).
* Subscribe ``{"id": <uuid>, "reqType": "sub", "dataType": "STONK-USDT@depth5@500ms"}``.
  Ack: ``{"id", "code": 0, "msg", "dataType": ""}`` (``dataType`` empty/missing). An
  ack/envelope with ``code != 0`` is a rejected subscription -> ``SUBSCRIPTION_REJECTED``.
  ``"code": 0`` in DATA frames is NOT an error (never the generic ``"code" in msg`` rule);
  a data frame with a non-zero code becomes ``BINGX_DATA_ERROR:<code>`` for this key.
* ``<SYM>@depth5@500ms``: full top-5 snapshot every 500 ms; ``asks`` are documented in
  DESCENDING order (best ask last) and ``bids`` descending -> both sides are re-sorted
  best-first. ``event_ms`` = top-level ``ts`` (``data.T`` as fallback); no update id.
* Mark/last (only for those price kinds): ``<SYM>@markPrice`` -> ``data {e, E, s, p}``,
  ``<SYM>@lastPrice`` -> ``data {e, E, s, c}``; ``event_ms`` = ``data.E``.
* REST (envelope ``{"code": 0, "msg", "data"}``; ``timestamp`` query always sent):
  ``/openApi/swap/v2/quote/depth?symbol=&limit=5`` (``data.T`` ms);
  ``/openApi/swap/v2/quote/premiumIndex`` (mark) and ``/openApi/swap/v2/quote/ticker``
  (last): their ``updateTime``/``closeTime`` are not documented as the price time, so
  ``event_ms`` = receipt wall ms for REST mark/last (documented fallback).
  ``/openApi/swap/v2/quote/contracts`` for the doctor (code 109425 = symbol unknown).
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from typing import Any

from ..dec import ONE
from ..errors import ConfigError, TransientUnsafe
from ..types import Ticker
from ._venue_common import (AdapterBase, FrameError, ReferenceFailure, as_code, as_int, detail, dumps,
                            make_book, opt_price, parse_levels, plain, positive_decimal)
from .base import Reply, Update

LOG = logging.getLogger(__name__)

BINGX_WS = 'wss://open-api-swap.bingx.com/swap-market'
BINGX_REST = 'https://open-api.bingx.com'
DEPTH_LEVELS = 5
DEPTH_INTERVAL = '500ms'          # 200ms exists only for BTC-USDT/ETH-USDT
REST_DEPTH_LIMIT = 5
RATE_LIMIT_CODES = frozenset({100410})
UNKNOWN_SYMBOL_CODES = frozenset({109425})
ONLINE_STATUS = 1
UNEXPECTED_TICKER = 'UNEXPECTED_BINGX_TICKER_SCHEMA'
_SYMBOL = re.compile(r'([A-Z0-9]{1,30})-([A-Z0-9]{2,10})')


class BingxAdapter(AdapterBase):
    VENUE = 'bingx_swap'
    PREFIX = 'BINGX'
    DEFAULT_WS_URL = BINGX_WS
    DEFAULT_REST_URL = BINGX_REST
    DECODE_ERROR_CODE = 'BINGX_GZIP_DECODE_ERROR'
    ping_interval = 0.0          # the server pings; we only answer
    idle_timeout = 30.0          # server Ping every ~5 s, depth every 500 ms

    def __init__(self, source: Any, *, timeout: float, rest_url: str | None = None,
                 expected_base: str | None = None) -> None:
        super().__init__(source, timeout=timeout, rest_url=rest_url, expected_base=expected_base)
        self.depth_type = f'{self.symbol}@depth{DEPTH_LEVELS}@{DEPTH_INTERVAL}'
        self._depth_prefix = f'{self.symbol}@depth{DEPTH_LEVELS}'
        self.ticker_type = f'{self.symbol}@markPrice' if source.price == 'mark' else f'{self.symbol}@lastPrice'

    def _check_symbol(self) -> None:
        match = _SYMBOL.fullmatch(self.symbol)
        if match is None:
            raise self._symbol_error('BingX symbol must be BASE-QUOTE in upper case, e.g. STONK-USDT')
        if match.group(2) != self.quote:
            raise ConfigError(f'index.sources[{self.key}].quote: must match the symbol quote {match.group(2)}')

    def _derive_base(self) -> str | None:
        return self.symbol.split('-', 1)[0]

    # ------------------------------------------------------------------ websocket
    def subscribe_frames(self) -> list[str]:
        types = [self.depth_type]
        if self.wants_ticker:
            types.append(self.ticker_type)
        return [dumps({'id': str(uuid.uuid4()), 'reqType': 'sub', 'dataType': data_type}) for data_type in types]

    def ping_frame(self, wall_ms: int) -> str | None:
        return None

    def _on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        heartbeat = text.strip().lower()
        if heartbeat == 'ping':
            return [Reply('Pong')]
        if heartbeat == 'pong':
            return []
        msg = self.parse(text)
        if msg is None:
            return [self.error('INVALID_WS_MESSAGE')]
        data_type = msg.get('dataType')
        if 'ping' in msg and not data_type:
            return [Reply(dumps({'pong': msg['ping'], 'time': msg.get('time')}))]
        if not data_type:
            return self._envelope_frame(msg)
        if 'code' in msg:
            try:
                code = as_code(msg['code'])
            except ValueError:
                return [self.error(self.book_code)]
            if code != 0:
                return [self.error(f'BINGX_DATA_ERROR:{code}')]
        if data_type == self.depth_type or (isinstance(data_type, str) and data_type.startswith(self._depth_prefix + '@')):
            return self.guard(self.book_code, self._book, msg, now)
        if data_type == self.ticker_type and self.wants_ticker:
            return self.guard(self.ticker_code, self._ticker, msg, now)
        return []

    def _envelope_frame(self, msg: dict) -> list[Update]:
        """Subscription ack or error envelope (no dataType)."""
        if 'code' in msg:
            try:
                code = as_code(msg['code'])
            except ValueError:
                code = None
            if code != 0:
                raise self.fatal('SUBSCRIPTION_REJECTED')
        return [self.subscribed()] if msg.get('id') else []

    def _book(self, msg: dict, now: float) -> list[Update]:
        data = msg['data']
        if not isinstance(data, dict):
            raise TypeError('data')
        stamp = msg.get('ts')
        if stamp is None:
            stamp = data['T']           # KeyError -> INVALID_WS_BOOK (venue time is mandatory)
        # asks arrive descending (best ask last): make_book re-sorts both sides best-first.
        book = make_book(parse_levels(data['bids']), parse_levels(data['asks']), now, as_int(stamp))
        return [self.book_update(book)]

    def _ticker(self, msg: dict, now: float) -> list[Update]:
        data = msg['data']
        if not isinstance(data, dict):
            raise TypeError('data')
        symbol = data.get('s')
        if symbol is not None and symbol != self.symbol:
            raise FrameError(UNEXPECTED_TICKER)
        stamp = data.get('E')
        if stamp is None:
            stamp = data.get('T')
        if stamp is None:
            stamp = msg.get('ts')
        if stamp is None:
            stamp = msg['timestamp']        # KeyError -> INVALID_WS_TICKER (venue time is mandatory)
        if self.source.price == 'mark':
            ticker = Ticker(mark=opt_price(data['p']), last=None, received=float(now), event_ms=as_int(stamp))
        else:
            ticker = Ticker(mark=None, last=opt_price(data['c']), received=float(now), event_ms=as_int(stamp))
        return [self.ticker_update(ticker)]

    # ------------------------------------------------------------------ REST
    def _params(self, wall_ms: int, **extra: Any) -> dict:
        return {'symbol': self.symbol, **extra, 'timestamp': int(wall_ms)}

    @staticmethod
    def _envelope(payload: Any) -> Any:
        code = payload.get('code')
        try:
            value = as_code(code)
        except ValueError:
            value = None
        if value != 0:
            if value in RATE_LIMIT_CODES:
                raise TransientUnsafe('BINGX_REST_RATE_LIMITED')
            raise TransientUnsafe(f'BINGX_REST_ERROR:{detail(code)}')
        return payload['data']

    def _row(self, data: Any) -> dict:
        """``data`` object, or the row of our symbol when the venue answers with a list."""
        if isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict) and row.get('symbol') == self.symbol]
            if len(rows) != 1:
                raise FrameError(f'INVALID_REST_TICKER:{self.key}')
            return rows[0]
        if not isinstance(data, dict):
            raise TypeError('data')
        symbol = data.get('symbol')
        if symbol is not None and symbol != self.symbol:
            raise FrameError(f'INVALID_REST_TICKER:{self.key}')
        return data

    def _rest_book(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, '/openApi/swap/v2/quote/depth',
                                self._params(wall_ms, limit=REST_DEPTH_LIMIT), timeout)
        data = self._envelope(payload)
        if not isinstance(data, dict):
            raise TypeError('data')
        book = make_book(parse_levels(data['bids']), parse_levels(data['asks']), now, as_int(data['T']))
        return self.book_update(book, 'rest')

    def _rest_ticker(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        # Neither premiumIndex.updateTime nor ticker.closeTime is documented as the price
        # time: receipt wall ms is used for REST mark/last.
        if self.source.price == 'mark':
            payload = self.rest_get(http, '/openApi/swap/v2/quote/premiumIndex', self._params(wall_ms), timeout)
            row = self._row(self._envelope(payload))
            ticker = Ticker(mark=opt_price(row['markPrice']), last=None, received=float(now), event_ms=int(wall_ms))
        else:
            payload = self.rest_get(http, '/openApi/swap/v2/quote/ticker', self._params(wall_ms), timeout)
            row = self._row(self._envelope(payload))
            ticker = Ticker(mark=None, last=opt_price(row['lastPrice']), received=float(now), event_ms=int(wall_ms))
        return self.ticker_update(ticker, 'rest')

    # ------------------------------------------------------------------ doctor
    def _instrument(self, http: Any, timeout: float, info: dict) -> None:
        payload = self.ref_get(http, '/openApi/swap/v2/quote/contracts',
                               {'symbol': self.symbol, 'timestamp': int(time.time() * 1000)}, timeout)
        code = payload.get('code')
        try:
            value = as_code(code)
        except ValueError:
            value = None
        if value in UNKNOWN_SYMBOL_CODES:
            raise ReferenceFailure(self.not_listed)
        data = payload.get('data')
        if value != 0 or not isinstance(data, list):
            raise ReferenceFailure(f'REFERENCE_LOOKUP_FAILED:INVALID_BINGX_CONTRACTS:{detail(code)}')
        rows = [row for row in data if isinstance(row, dict) and row.get('symbol') == self.symbol]
        if len(rows) != 1:
            raise ReferenceFailure(self.not_listed)
        row = rows[0]
        problems: list[str] = info['problems']
        status = row.get('status')
        info.update(raw_keys=self.raw_keys(row), status=status, base=row.get('asset'), quote=row.get('currency'))
        self.check_base(row.get('asset'), problems)
        self.check_equal(problems, 'QUOTE', row.get('currency'), self.quote)
        try:
            online = as_code(status) == ONLINE_STATUS      # int or "1" depending on the doc version
        except ValueError:
            online = False
        if not online:
            problems.append(f'STATUS:{detail(status)}')
        try:
            precision = as_int(row.get('pricePrecision'))
        except ValueError:
            precision = None
        if precision is None or precision > 18:
            problems.append('TICK')
        else:
            info['tick'] = plain(ONE.scaleb(-precision))
        info['multiplier'] = plain(positive_decimal(row.get('size')))
