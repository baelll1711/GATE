"""Bybit V5 linear perpetual index source (``venue: bybit_linear``).

Wire facts (notes/api_bybit.md; reference ``spot_portfolio/feeds.py``):

* WS ``wss://stream.bybit.com/v5/public/linear``; subscribe
  ``{"op":"subscribe","args":[...]}``; ack ``{"success":true,"op":"subscribe",...}``.
  Any reply with ``success: false`` is a rejected request: ``SUBSCRIPTION_REJECTED``
  (socket-fatal, the worker reconnects).
* App ping ``{"op":"ping"}`` every 20 s (Bybit recommendation); the pong arrives as
  ``{"op":"ping","ret_msg":"pong",...}`` (linear) or ``op == "pong"``.
* ``orderbook.1.<SYM>`` is SNAPSHOT-ONLY (since 2025-03-20) and re-sends an unchanged
  snapshot every 3 s with the same ``u`` (quiet-book heartbeat, see
  ``SourceConfig.heartbeat_seconds``). Anything but ``type == "snapshot"`` with
  ``data.s == symbol`` is ``UNEXPECTED_BYBIT_L1_SCHEMA`` for this key: an L1 delta is
  never merged into a partial best-price book (reference rule). ``event_ms`` = top-level
  ``ts``; ``update_id`` = ``data.u`` (``u == 1`` = service restart; the hub accepts it).
* ``tickers.<SYM>`` (subscribed only for price kind mark/last): one snapshot, then deltas
  in which an absent field means "unchanged"; the adapter keeps the merged state.
  ``event_ms`` = top-level ``ts``. A malformed or foreign ticker frame breaks the merge
  chain (a later delta would be merged onto stale fields), so it is socket-fatal: the
  reconnect delivers a fresh snapshot. A delta before the snapshot is ignored.
* REST ``/v5/market/orderbook?category=linear&symbol=&limit=1``: ``ts`` ms; its ``u`` is
  the 1000-level book id and is NOT comparable with the L1 ``u`` -> ``update_id=None``.
  ``/v5/market/tickers``: rows carry no timestamp, ``event_ms`` = envelope ``time``.
  ``/v5/market/instruments-info`` for the doctor (reference checks verbatim).
"""
from __future__ import annotations

import logging
import re
from typing import Any

from ..errors import TransientUnsafe
from ..types import Ticker
from ._venue_common import (DATA_ERRORS, AdapterBase, FrameError, ReferenceFailure, as_int, detail, dumps,
                            make_book, opt_price, parse_levels, plain, positive_decimal)
from .base import Reply, Update

LOG = logging.getLogger(__name__)

BYBIT_WS = 'wss://stream.bybit.com/v5/public/linear'
BYBIT_REST = 'https://api.bybit.com'
CATEGORY = 'linear'
UNEXPECTED_L1 = 'UNEXPECTED_BYBIT_L1_SCHEMA'
UNEXPECTED_TICKER = 'UNEXPECTED_BYBIT_TICKER_SCHEMA'
RATE_LIMIT_CODES = frozenset({10006, 10018})
PARAMS_ERROR = 10001
_SYMBOL = re.compile(r'[A-Z0-9]{2,40}')


class BybitAdapter(AdapterBase):
    VENUE = 'bybit_linear'
    PREFIX = 'BYBIT'
    DEFAULT_WS_URL = BYBIT_WS
    DEFAULT_REST_URL = BYBIT_REST
    ping_interval = 20.0
    idle_timeout = 30.0          # L1 heartbeat every 3 s + pong every 20 s

    def __init__(self, source: Any, *, timeout: float, rest_url: str | None = None,
                 expected_base: str | None = None) -> None:
        super().__init__(source, timeout=timeout, rest_url=rest_url, expected_base=expected_base)
        self.book_topic = f'orderbook.1.{self.symbol}'
        self.ticker_topic = f'tickers.{self.symbol}'
        self._ticker_state: dict | None = None

    def _check_symbol(self) -> None:
        if not _SYMBOL.fullmatch(self.symbol):
            raise self._symbol_error('Bybit linear symbol must be upper-case letters/digits, e.g. STONKUSDT')

    def _derive_base(self) -> str | None:
        if self.symbol.endswith(self.quote) and len(self.symbol) > len(self.quote):
            return self.symbol[:-len(self.quote)]
        return None

    # ------------------------------------------------------------------ websocket
    def subscribe_frames(self) -> list[str]:
        args = [self.book_topic]
        if self.wants_ticker:
            args.append(self.ticker_topic)
        return [dumps({'op': 'subscribe', 'args': args})]

    def ping_frame(self, wall_ms: int) -> str | None:
        return dumps({'op': 'ping'})

    def on_reset(self) -> None:
        self._ticker_state = None      # a new connection starts with a new ticker snapshot

    def _on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        msg = self.parse(text)
        if msg is None:
            return [self.error('INVALID_WS_MESSAGE')]
        if msg.get('success') is False:
            raise self.fatal('SUBSCRIPTION_REJECTED')
        op = msg.get('op')
        if op in ('ping', 'pong') or msg.get('ret_msg') == 'pong':
            return []
        if op == 'subscribe':
            return [self.subscribed()] if msg.get('success') is True else []
        topic = msg.get('topic')
        if topic == self.book_topic:
            return self.guard(self.book_code, self._book, msg, now)
        if topic == self.ticker_topic and self.wants_ticker:
            return self._ticker(msg, now)
        return []                       # other topics/commands are not ours

    def _book(self, msg: dict, now: float) -> list[Update]:
        # Official L1 stream emits snapshots only, including quiet-book repeats. A delta
        # here is a schema change, not permission to keep a side from an older snapshot.
        data = msg.get('data')
        if msg.get('type') != 'snapshot' or not isinstance(data, dict) or data.get('s') != self.symbol:
            raise FrameError(UNEXPECTED_L1)
        book = make_book(parse_levels(data['b']), parse_levels(data['a']), now,
                         as_int(msg['ts']), as_int(data['u']))
        return [self.book_update(book)]

    def _ticker(self, msg: dict, now: float) -> list[Update]:
        try:
            merged = self._merge_ticker(msg, now)
        except FrameError as exc:
            code = exc.code
        except DATA_ERRORS:
            code = self.ticker_code
        else:
            if merged is None:
                return []
            ticker, state = merged
            self._ticker_state = state
            return [self.ticker_update(ticker)]
        self._ticker_state = None
        raise self.fatal(code)

    def _merge_ticker(self, msg: dict, now: float) -> tuple[Ticker, dict] | None:
        kind = msg.get('type')
        data = msg.get('data')
        if not isinstance(data, dict):
            raise FrameError(UNEXPECTED_TICKER)
        symbol = data.get('symbol')
        if kind == 'snapshot':
            if symbol != self.symbol:
                raise FrameError(UNEXPECTED_TICKER)
            state = dict(data)
        elif kind == 'delta':
            if symbol is not None and symbol != self.symbol:
                raise FrameError(UNEXPECTED_TICKER)
            if self._ticker_state is None:
                LOG.debug('%s: ticker delta before snapshot ignored', self.key)
                return None
            state = {**self._ticker_state, **data}
        else:
            raise FrameError(UNEXPECTED_TICKER)
        ticker = Ticker(mark=opt_price(state.get('markPrice')), last=opt_price(state.get('lastPrice')),
                        received=float(now), event_ms=as_int(msg['ts']))
        return ticker, state

    # ------------------------------------------------------------------ REST
    def _envelope(self, payload: Any) -> dict:
        code = payload.get('retCode')
        if isinstance(code, bool) or code != 0:
            if not isinstance(code, bool) and code in RATE_LIMIT_CODES:
                raise TransientUnsafe('BYBIT_REST_RATE_LIMITED')
            raise TransientUnsafe(f'BYBIT_REST_ERROR:{detail(code)}')
        result = payload['result']
        if not isinstance(result, dict):
            raise TypeError('result')
        return result

    def _rest_book(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, '/v5/market/orderbook',
                                {'category': CATEGORY, 'symbol': self.symbol, 'limit': 1}, timeout)
        result = self._envelope(payload)
        if result.get('s') != self.symbol:
            raise FrameError(f'INVALID_REST_BOOK:{self.key}')
        # REST ``u`` belongs to the 1000-level book: not comparable with the L1 ``u``.
        book = make_book(parse_levels(result['b']), parse_levels(result['a']), now, as_int(result['ts']))
        return self.book_update(book, 'rest')

    def _rest_ticker(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, '/v5/market/tickers', {'category': CATEGORY, 'symbol': self.symbol}, timeout)
        result = self._envelope(payload)
        rows = [row for row in result['list'] if isinstance(row, dict) and row.get('symbol') == self.symbol]
        if len(rows) != 1:
            raise FrameError(f'INVALID_REST_TICKER:{self.key}')
        row = rows[0]
        # Ticker rows have no timestamp; the envelope ``time`` (ms) is the server time.
        ticker = Ticker(mark=opt_price(row.get('markPrice')), last=opt_price(row.get('lastPrice')),
                        received=float(now), event_ms=as_int(payload['time']))
        return self.ticker_update(ticker, 'rest')

    # ------------------------------------------------------------------ doctor
    def _instrument(self, http: Any, timeout: float, info: dict) -> None:
        payload = self.ref_get(http, '/v5/market/instruments-info', {'category': CATEGORY, 'symbol': self.symbol},
                               timeout)
        code = payload.get('retCode')
        if not isinstance(code, bool) and isinstance(code, int) and code == PARAMS_ERROR:
            raise ReferenceFailure(self.not_listed)        # unknown symbol -> params error
        result = payload.get('result')
        if (isinstance(code, bool) or code != 0 or not isinstance(result, dict)
                or result.get('category') != CATEGORY or not isinstance(result.get('list'), list)):
            # retCode != 0 is Bybit's error envelope (rate limit, server error).
            raise ReferenceFailure(f'REFERENCE_LOOKUP_FAILED:INVALID_BYBIT_INSTRUMENTS:{self.symbol}')
        rows = [row for row in result['list'] if isinstance(row, dict) and row.get('symbol') == self.symbol]
        if len(rows) != 1:
            raise ReferenceFailure(self.not_listed)
        row = rows[0]
        problems: list[str] = info['problems']
        info.update(raw_keys=self.raw_keys(row), status=row.get('status'), base=row.get('baseCoin'),
                    quote=row.get('quoteCoin'))
        self.check_base(row.get('baseCoin'), problems)
        self.check_equal(problems, 'QUOTE', row.get('quoteCoin'), self.quote)
        self.check_equal(problems, 'SETTLE', row.get('settleCoin'), self.quote)
        self.check_equal(problems, 'TYPE', row.get('contractType'), 'LinearPerpetual')
        self.check_equal(problems, 'STATUS', row.get('status'), 'Trading')
        if row.get('isPreListing', False):
            problems.append('PRELISTING')
        price_filter = row.get('priceFilter')
        tick = positive_decimal(price_filter.get('tickSize')) if isinstance(price_filter, dict) else None
        if tick is None:
            problems.append('TICK')
        info['tick'] = plain(tick)
        delivery = row.get('deliveryTime')
        if delivery not in (None, '', '0', 0):
            # For a perpetual, deliveryTime is the scheduled delisting time.
            info['warnings'].append(f'DELISTING_SCHEDULED:{detail(delivery)}')
