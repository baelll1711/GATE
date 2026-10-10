"""Gate USDT-settled perpetual index source (``venue: gate_futures_usdt``).

Wire facts (notes/api_gate_ws_futures.md §1, §4, §5):

* WS ``wss://fx-ws.gateio.ws/v4/ws/usdt``; request
  ``{"time": <sec>, "channel": ..., "event": "subscribe", "payload": ["STONK_USDT"]}``.
  The handshake must carry ``X-Gate-Size-Decimal: 1`` (``ws_headers``), otherwise sizes
  arrive as integers truncated toward zero (a fractional best size could read as 0).
* App ping ``{"time": <sec>, "channel": "futures.ping"}`` every 10 s; the answer
  ``{"channel": "futures.pong", "error": null, "result": null}`` is ignored (``error:
  null`` is not an error).
* Subscribe ack ``{"event": "subscribe", "error": null, "result": {"status": "success"}}``;
  a rejection carries ``"error": {"code", "message"}`` and/or ``result.status == "fail"``
  -> ``SUBSCRIPTION_REJECTED`` (socket-fatal). An error envelope on one of our data
  channels becomes ``GATE_FUTURES_WS_ERROR`` for this key.
* ``futures.book_ticker`` -> ``result {t (ms), u, s, b, B, a, A}`` (sizes in CONTRACTS; a
  constant ``quanto_multiplier`` cancels out in the microprice). ``event_ms`` = ``t``,
  ``update_id`` = ``u``; ``s`` must be our contract (``UNEXPECTED_GATE_FUTURES_SCHEMA``).
* ``futures.tickers`` (only for price kind mark/last) -> ``result`` ARRAY of
  ``{contract, last, mark_price, index_price, ...}``; rows carry no timestamp, so
  ``event_ms`` = envelope ``time_ms`` (push time; ``time`` seconds as fallback).
* REST (header ``X-Gate-Size-Decimal: 1`` on every request; non-2xx bodies are
  ``{label, message}``): ``/futures/usdt/order_book?contract=&limit=1`` (levels
  ``{"p", "s"}``; ``current`` = snapshot time in float SECONDS -> ``event_ms``; the
  optional book id is not comparable with ``book_ticker.u`` -> ``update_id=None``);
  ``/futures/usdt/tickers?contract=`` (array, no timestamp -> receipt wall ms);
  ``/futures/usdt/contracts/{contract}`` for the doctor (``type == "direct"``,
  ``status == "trading"``, not ``in_delisting``; tick = ``order_price_round``,
  contract size = ``quanto_multiplier``; labels CONTRACT_NOT_FOUND/INVALID_CONTRACT ->
  not listed).
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from ..dec import D
from ..errors import ConfigError
from ..types import Ticker
from ._venue_common import (AdapterBase, FrameError, ReferenceFailure, as_int, detail, dumps, http_get,
                            make_book, ms_from_seconds, opt_price, parse_levels, plain, positive_decimal,
                            reference_payload)
from .base import Reply, Update

LOG = logging.getLogger(__name__)

GATE_FUTURES_WS = 'wss://fx-ws.gateio.ws/v4/ws/usdt'
GATE_REST = 'https://api.gateio.ws/api/v4'
SETTLE = 'usdt'
SIZE_HEADERS = {'X-Gate-Size-Decimal': '1'}
BOOK_CHANNEL = 'futures.book_ticker'
TICKER_CHANNEL = 'futures.tickers'
HEARTBEAT_CHANNELS = frozenset({'futures.ping', 'futures.pong'})
UNEXPECTED = 'UNEXPECTED_GATE_FUTURES_SCHEMA'
NOT_FOUND_LABELS = frozenset({'CONTRACT_NOT_FOUND', 'INVALID_CONTRACT'})
_SYMBOL = re.compile(r'([A-Z0-9]{1,30})_([A-Z0-9]{2,10})')


class GateFuturesAdapter(AdapterBase):
    VENUE = 'gate_futures_usdt'
    PREFIX = 'GATE_FUTURES'
    DEFAULT_WS_URL = GATE_FUTURES_WS
    DEFAULT_REST_URL = GATE_REST
    ping_interval = 10.0
    idle_timeout = 30.0

    def _check_symbol(self) -> None:
        match = _SYMBOL.fullmatch(self.symbol)
        if match is None:
            raise self._symbol_error('Gate futures contract must be BASE_QUOTE in upper case, e.g. STONK_USDT')
        if self.quote != 'USDT' or match.group(2) != 'USDT':
            raise ConfigError(f'index.sources[{self.key}].quote: Gate USDT-settled futures need quote USDT')

    def _derive_base(self) -> str | None:
        return self.symbol.split('_', 1)[0]

    @property
    def channels(self) -> tuple[str, ...]:
        return (BOOK_CHANNEL, TICKER_CHANNEL) if self.wants_ticker else (BOOK_CHANNEL,)

    # ------------------------------------------------------------------ websocket
    def ws_headers(self) -> dict[str, str]:
        return dict(SIZE_HEADERS)

    def subscribe_frames(self) -> list[str]:
        now_s = int(time.time())
        return [dumps({'time': now_s, 'channel': channel, 'event': 'subscribe', 'payload': [self.symbol]})
                for channel in self.channels]

    def ping_frame(self, wall_ms: int) -> str | None:
        return dumps({'time': int(wall_ms) // 1000, 'channel': 'futures.ping'})

    def _on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]:
        msg = self.parse(text)
        if msg is None:
            return [self.error('INVALID_WS_MESSAGE')]
        channel = msg.get('channel')
        event = msg.get('event')
        if channel in HEARTBEAT_CHANNELS:
            return []
        error = msg.get('error')
        has_error = error is not None and error != {}
        if event in ('subscribe', 'unsubscribe'):
            result = msg.get('result')
            status = result.get('status') if isinstance(result, dict) else None
            if has_error or status == 'fail':
                raise self.fatal('SUBSCRIPTION_REJECTED')
            return [self.subscribed()] if event == 'subscribe' and channel == BOOK_CHANNEL else []
        if channel not in self.channels:
            return []
        if has_error:
            return [self.error('GATE_FUTURES_WS_ERROR')]
        if event not in ('update', 'all'):
            return []
        if channel == BOOK_CHANNEL:
            return self.guard(self.book_code, self._book, msg, now)
        return self.guard(self.ticker_code, self._ticker, msg, now)

    def _book(self, msg: dict, now: float) -> list[Update]:
        result = msg['result']
        if not isinstance(result, dict) or result.get('s') != self.symbol:
            raise FrameError(UNEXPECTED)
        bids = parse_levels([[result['b'], result['B']]]) if result['b'] not in ('', None) else None
        asks = parse_levels([[result['a'], result['A']]]) if result['a'] not in ('', None) else None
        if bids is None or asks is None:
            raise FrameError('EMPTY_BOOK_SIDE')
        book = make_book(bids, asks, now, as_int(result['t']), as_int(result['u']))
        return [self.book_update(book)]

    def _ticker(self, msg: dict, now: float) -> list[Update]:
        result = msg['result']
        rows = [result] if isinstance(result, dict) else result
        if not isinstance(rows, list):
            raise TypeError('result')
        mine = [row for row in rows if isinstance(row, dict) and row.get('contract') == self.symbol]
        if len(mine) != 1:
            raise FrameError(UNEXPECTED)
        row = mine[0]
        stamp = msg.get('time_ms')
        event_ms = as_int(stamp) if stamp is not None else as_int(msg['time']) * 1000
        ticker = Ticker(mark=opt_price(row.get('mark_price')), last=opt_price(row.get('last')),
                        received=float(now), event_ms=event_ms)
        return [self.ticker_update(ticker)]

    # ------------------------------------------------------------------ REST
    def _rest_book(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, f'/futures/{SETTLE}/order_book', {'contract': self.symbol, 'limit': 1},
                                timeout, headers=SIZE_HEADERS)
        if not isinstance(payload, dict):
            raise TypeError('payload')
        # ``current`` = generation time of this snapshot, float seconds (spot REST uses ms).
        book = make_book(parse_levels(payload['bids'], 'ps'), parse_levels(payload['asks'], 'ps'), now,
                         ms_from_seconds(payload['current']))
        return self.book_update(book, 'rest')

    def _rest_ticker(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        payload = self.rest_get(http, f'/futures/{SETTLE}/tickers', {'contract': self.symbol}, timeout,
                                headers=SIZE_HEADERS)
        if not isinstance(payload, list):
            raise TypeError('payload')
        rows = [row for row in payload if isinstance(row, dict) and row.get('contract') == self.symbol]
        if len(rows) != 1:
            raise FrameError(f'INVALID_REST_TICKER:{self.key}')
        row = rows[0]
        # REST tickers carry no timestamp: receipt wall ms.
        ticker = Ticker(mark=opt_price(row.get('mark_price')), last=opt_price(row.get('last')),
                        received=float(now), event_ms=int(wall_ms))
        return self.ticker_update(ticker, 'rest')

    # ------------------------------------------------------------------ doctor
    def _instrument(self, http: Any, timeout: float, info: dict) -> None:
        status, payload = http_get(http, f'{self.rest_url}/futures/{SETTLE}/contracts/{self.symbol}',
                                   headers=SIZE_HEADERS, timeout=timeout)
        label = payload.get('label') if isinstance(payload, dict) else None
        if status in (400, 404) and label in NOT_FOUND_LABELS:
            raise ReferenceFailure(self.not_listed)
        payload = reference_payload(status, payload)
        if payload.get('name') != self.symbol:
            raise ReferenceFailure(self.not_listed)
        problems: list[str] = info['problems']
        base, _, settle = self.symbol.partition('_')
        info.update(raw_keys=self.raw_keys(payload), status=payload.get('status'), base=base, quote=settle)
        self.check_base(base, problems)
        self.check_equal(problems, 'TYPE', payload.get('type'), 'direct')
        self.check_equal(problems, 'STATUS', payload.get('status'), 'trading')
        if payload.get('in_delisting') is not False:
            problems.append(f'IN_DELISTING:{detail(payload.get("in_delisting"))}')
        tick = positive_decimal(payload.get('order_price_round'))
        if tick is None:
            problems.append('TICK')
        info['tick'] = plain(tick)
        contract_size = positive_decimal(payload.get('quanto_multiplier'))
        if contract_size is None:
            problems.append('CONTRACT_SIZE')
        info['contract_size'] = plain(contract_size)
        delisting = payload.get('delisting_time')
        try:
            scheduled = delisting is not None and D(delisting) > 0
        except ValueError:
            scheduled = False
        if scheduled:
            info['warnings'].append(f'DELISTING_SCHEDULED:{detail(delisting)}')
