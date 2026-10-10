"""Bybit linear adapter: orderbook.1 snapshots, tickers merge, REST snapshot, instrument check."""
from __future__ import annotations

import dataclasses
import json
from decimal import Decimal

import pytest

from adapter_fakes import NOW, WALL_MS, FakeHttp, FakeResponse, JsonOnlyResponse, fixture_text, reply, source
from gate_stonk.errors import ConfigError, TransientUnsafe, Unsafe
from gate_stonk.feeds.base import Update
from gate_stonk.feeds.bybit import BybitAdapter
from gate_stonk.types import Book, Ticker

V = 'bybit'
D = Decimal


def make(price: str = 'mid', symbol: str = 'STONKUSDT', **kwargs) -> BybitAdapter:
    extra = {key: kwargs.pop(key) for key in ('url',) if key in kwargs}
    return BybitAdapter(source('bybit_linear', symbol, source_id='bybit_perp', price=price, **extra),
                        timeout=5, **kwargs)


def feed(adapter: BybitAdapter, name: str) -> list:
    return adapter.on_text(fixture_text(V, name), NOW, WALL_MS)


def only(items: list) -> Update:
    assert len(items) == 1, items
    return items[0]


# --------------------------------------------------------------------------- identity / subscriptions
def test_identity_urls_pings_and_book_only_subscription():
    adapter = make()
    assert adapter.venue == 'bybit_linear' and adapter.key == 'bybit_perp'
    assert adapter.ws_url() == 'wss://stream.bybit.com/v5/public/linear'
    assert make(url='wss://stream-testnet.bybit.com/v5/public/linear').ws_url() == \
        'wss://stream-testnet.bybit.com/v5/public/linear'
    assert [json.loads(f) for f in adapter.subscribe_frames()] == [
        {'op': 'subscribe', 'args': ['orderbook.1.STONKUSDT']}]
    assert adapter.ping_interval == 20.0
    assert json.loads(adapter.ping_frame(WALL_MS)) == {'op': 'ping'}
    assert adapter.decode(b'{"op":"ping"}') == '{"op":"ping"}'
    assert adapter.ws_headers() == {}


@pytest.mark.parametrize('price', ['mark', 'last'])
def test_ticker_channel_only_for_mark_or_last(price):
    frames = [json.loads(f) for f in make(price).subscribe_frames()]
    assert frames == [{'op': 'subscribe', 'args': ['orderbook.1.STONKUSDT', 'tickers.STONKUSDT']}]


def test_constructor_validation():
    with pytest.raises(ConfigError, match='venue'):
        BybitAdapter(source('bitget_usdt_futures', 'STONKUSDT'), timeout=5)
    with pytest.raises(ConfigError, match='symbol'):
        make(symbol='stonk-usdt')
    with pytest.raises(ConfigError, match='request_timeout_seconds'):
        BybitAdapter(source('bybit_linear', 'STONKUSDT'), timeout=0)


# --------------------------------------------------------------------------- orderbook.1
def test_snapshot_parses_to_immutable_decimal_book():
    update = only(feed(make(), 'ws_ob1_snapshot.json'))
    assert update.kind == 'book' and update.key == 'bybit_perp' and update.source == 'ws'
    book = update.book
    assert isinstance(book, Book)
    assert book.bids == ((D('0.1358'), D('15000')),)
    assert book.asks == ((D('0.1360'), D('9000')),)
    assert all(isinstance(x, Decimal) for level in book.bids + book.asks for x in level)
    assert book.event_ms == 1791640000123          # top-level ts
    assert book.update_id == 4471001                # data.u
    assert book.received == NOW
    assert book.mid == D('0.1359')
    book.check(NOW, 1791640000123 + 500, 3.0, D('1'))     # structurally valid, fresh, tight
    with pytest.raises(dataclasses.FrozenInstanceError):
        book.bids = ()                                   # type: ignore[misc]


def test_quiet_heartbeat_repeats_u_and_ts_may_be_a_string():
    adapter = make()
    first = only(feed(adapter, 'ws_ob1_snapshot.json')).book
    repeat = only(feed(adapter, 'ws_ob1_heartbeat.json')).book
    assert repeat.update_id == first.update_id == 4471001
    assert repeat.event_ms == first.event_ms + 3000
    assert only(feed(adapter, 'ws_ob1_ts_string.json')).book.event_ms == 1791640000123


def test_u1_service_restart_snapshot_is_accepted():
    book = only(feed(make(), 'ws_ob1_restart.json')).book
    assert book.update_id == 1
    assert (book.bid, book.ask) == (D('0.1361'), D('0.1363'))


@pytest.mark.parametrize('name', ['ws_ob1_delta.json', 'ws_ob1_wrong_symbol.json'])
def test_l1_delta_or_foreign_symbol_is_a_schema_error_for_the_key(name):
    update = only(feed(make(), name))
    assert update == Update(kind='error', key='bybit_perp', error='UNEXPECTED_BYBIT_L1_SCHEMA')


@pytest.mark.parametrize('name, code', [
    ('ws_ob1_empty_side.json', 'EMPTY_BOOK_SIDE'),
    ('ws_ob1_malformed.json', 'INVALID_WS_BOOK:bybit_perp'),
    ('ws_not_json.txt', 'INVALID_WS_MESSAGE'),
])
def test_malformed_frames_become_error_updates_not_crashes(name, code):
    update = only(feed(make(), name))
    assert update.kind == 'error' and update.error == code and update.book is None


def test_missing_ts_or_u_is_an_error_update():
    adapter = make()
    frame = json.loads(fixture_text(V, 'ws_ob1_snapshot.json'))
    del frame['ts']
    assert only(adapter.on_text(json.dumps(frame), NOW, WALL_MS)).error == 'INVALID_WS_BOOK:bybit_perp'
    frame = json.loads(fixture_text(V, 'ws_ob1_snapshot.json'))
    frame['data']['u'] = True
    assert only(adapter.on_text(json.dumps(frame), NOW, WALL_MS)).error == 'INVALID_WS_BOOK:bybit_perp'


def test_other_topics_and_pong_are_ignored_ack_is_reported():
    adapter = make()
    assert feed(adapter, 'ws_ob1_other_symbol.json') == []
    assert feed(adapter, 'ws_pong.json') == []
    assert adapter.on_text('{"op":"pong","args":["1"]}', NOW, WALL_MS) == []
    assert feed(adapter, 'ws_sub_ok.json') == [Update(kind='subscribed', key='bybit_perp')]
    # tickers frames are not ours when the price kind is mid
    assert feed(adapter, 'ws_tickers_snapshot.json') == []


def test_rejected_subscription_is_socket_fatal():
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        feed(make(), 'ws_sub_fail.json')


# --------------------------------------------------------------------------- tickers (mark/last)
def test_ticker_snapshot_then_delta_merge():
    adapter = make('mark')
    first = only(feed(adapter, 'ws_tickers_snapshot.json'))
    assert first.kind == 'ticker' and first.key == 'bybit_perp'
    assert first.ticker == Ticker(mark=D('0.13592'), last=D('0.1359'), received=NOW, event_ms=1791640000150)
    second = only(feed(adapter, 'ws_tickers_delta.json')).ticker
    assert (second.mark, second.last, second.event_ms) == (D('0.1361'), D('0.1359'), 1791640000250)
    third = only(feed(adapter, 'ws_tickers_delta_no_symbol.json')).ticker
    assert (third.mark, third.last, third.event_ms) == (D('0.1361'), D('0.1362'), 1791640000350)


def test_ticker_delta_before_snapshot_is_ignored_and_reset_drops_state():
    adapter = make('last')
    assert feed(adapter, 'ws_tickers_delta.json') == []
    assert feed(adapter, 'ws_tickers_snapshot.json')[0].ticker.last == D('0.1359')
    adapter.on_reset()
    assert feed(adapter, 'ws_tickers_delta.json') == []


@pytest.mark.parametrize('name, code', [
    ('ws_tickers_delta_wrong_symbol.json', 'UNEXPECTED_BYBIT_TICKER_SCHEMA'),
    ('ws_tickers_delta_malformed.json', 'INVALID_WS_TICKER:bybit_perp'),
])
def test_broken_ticker_merge_chain_is_socket_fatal(name, code):
    adapter = make('mark')
    feed(adapter, 'ws_tickers_snapshot.json')
    with pytest.raises(Unsafe, match=f'^{code}$'):
        feed(adapter, name)
    # merged state was dropped: later deltas wait for the reconnect snapshot
    assert feed(adapter, 'ws_tickers_delta.json') == []


# --------------------------------------------------------------------------- REST snapshot
def test_rest_snapshot_book_only_for_mid():
    http = FakeHttp({'/v5/market/orderbook': reply(V, 'rest_orderbook.json')})
    updates = make().rest_snapshot(http, 4.0, NOW, WALL_MS)
    update = only(updates)
    assert update.kind == 'book' and update.source == 'rest' and update.key == 'bybit_perp'
    assert update.book.bids == ((D('0.1358'), D('15000')),)
    assert update.book.asks == ((D('0.1360'), D('9000')),)
    assert update.book.event_ms == 1791640000400
    assert update.book.update_id is None          # REST u is the 1000-level id, not comparable with L1 u
    call = http.call('/v5/market/orderbook')
    assert call['url'] == 'https://api.bybit.com/v5/market/orderbook'
    assert call['params'] == {'category': 'linear', 'symbol': 'STONKUSDT', 'limit': 1}
    assert call['timeout'] == 4.0
    assert len(http.calls) == 1


def test_rest_snapshot_adds_ticker_for_mark():
    http = FakeHttp({'/v5/market/orderbook': reply(V, 'rest_orderbook.json'),
                     '/v5/market/tickers': reply(V, 'rest_tickers.json')})
    book, ticker = make('mark').rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert book.kind == 'book' and ticker.kind == 'ticker' and ticker.source == 'rest'
    assert ticker.ticker == Ticker(mark=D('0.13592'), last=D('0.1359'), received=NOW, event_ms=1791640000420)
    assert http.call('/v5/market/tickers')['params'] == {'category': 'linear', 'symbol': 'STONKUSDT'}


def test_rest_snapshot_accepts_json_only_responses():
    body = json.loads(fixture_text(V, 'rest_orderbook.json'))
    http = FakeHttp({'/v5/market/orderbook': JsonOnlyResponse(200, body)})
    assert only(make().rest_snapshot(http, 4.0, NOW, WALL_MS)).book.bid == D('0.1358')


@pytest.mark.parametrize('answer, code', [
    (FakeResponse(429, '{}'), 'BYBIT_REST_RATE_LIMITED'),
    (FakeResponse(503, 'maintenance'), 'BYBIT_REST_HTTP_503'),
    (FakeResponse(200, 'not json'), 'BYBIT_REST_INVALID_JSON'),
    (reply(V, 'rest_rate_limited.json'), 'BYBIT_REST_RATE_LIMITED'),
    (FakeResponse(200, '{"retCode":10001,"retMsg":"params error","result":{}}'), 'BYBIT_REST_ERROR:10001'),
    (TimeoutError('read timed out'), 'BYBIT_REST_TRANSPORT:TimeoutError'),
])
def test_rest_snapshot_transport_and_envelope_failures_raise_transient(answer, code):
    http = FakeHttp({'/v5/market/orderbook': answer})
    with pytest.raises(TransientUnsafe, match=f'^{code}$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)


@pytest.mark.parametrize('body', [
    '{"retCode":0,"result":{"s":"STONKUSDT","a":[["0.1360","9000"]],"b":[],"ts":1}}',
    '{"retCode":0,"result":{"s":"BTCUSDT","a":[["1","1"]],"b":[["0.5","1"]],"ts":1}}',
    '{"retCode":0,"result":{"s":"STONKUSDT","a":[["0.1360","9000"]],"b":[["0.1358","1"]]}}',
    '[]',
])
def test_rest_snapshot_malformed_body_raises_unsafe(body):
    http = FakeHttp({'/v5/market/orderbook': FakeResponse(200, body)})
    with pytest.raises(Unsafe) as info:
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert not isinstance(info.value, TransientUnsafe)
    assert str(info.value) in ('EMPTY_BOOK_SIDE', 'INVALID_REST_BOOK:bybit_perp')


# --------------------------------------------------------------------------- instrument check
def info_for(name: str, adapter: BybitAdapter | None = None, status: int = 200) -> tuple[dict, FakeHttp]:
    http = FakeHttp({'/v5/market/instruments-info': reply(V, name, status)})
    return (adapter or make()).instrument_info(http, 4.0), http


def test_instrument_info_ok():
    info, http = info_for('instruments_info_ok.json')
    assert info['ok'] is True and info['reason'] is None and info['problems'] == []
    assert (info['symbol'], info['status'], info['base'], info['quote']) == ('STONKUSDT', 'Trading', 'STONK', 'USDT')
    assert info['tick'] == '0.0001' and info['transient'] is False
    assert 'priceFilter' in info['raw_keys'] and 'symbol' in info['raw_keys']
    assert http.call('/v5/market/instruments-info')['params'] == {'category': 'linear', 'symbol': 'STONKUSDT'}
    json.dumps(info)        # JSON-safe for the doctor report


@pytest.mark.parametrize('name', ['instruments_info_empty.json', 'instruments_info_params_error.json'])
def test_instrument_not_listed(name):
    info, _ = info_for(name)
    assert info['ok'] is False and info['transient'] is False
    assert info['reason'] == 'BYBIT_REFERENCE_NOT_LISTED:STONKUSDT'


@pytest.mark.parametrize('name, problems', [
    ('instruments_info_prelaunch.json', ['STATUS:PreLaunch', 'PRELISTING']),
    ('instruments_info_futures.json', ['TYPE:LinearFutures']),
])
def test_instrument_not_an_active_exact_perp(name, problems):
    info, _ = info_for(name)
    assert info['ok'] is False
    assert info['reason'] == 'BYBIT_REFERENCE_NOT_ACTIVE_EXACT_PERP:STONKUSDT'
    assert info['problems'] == problems


def test_scaled_contract_is_rejected_and_never_guessed():
    # configured exactly as the scaled contract: still refused
    info, _ = info_for('instruments_info_scaled.json', make(symbol='1000STONKUSDT'))
    assert info['ok'] is False and 'SCALED_CONTRACT:1000STONK' in info['problems']
    info, _ = info_for('instruments_info_scaled.json', make(symbol='1000STONKUSDT', expected_base='STONK'))
    assert {'SCALED_CONTRACT:1000STONK', 'BASE:1000STONK'} <= set(info['problems'])
    # configured STONKUSDT while only 1000STONKUSDT exists: exact symbol only
    info, _ = info_for('instruments_info_scaled.json')
    assert info['reason'] == 'BYBIT_REFERENCE_NOT_LISTED:STONKUSDT'


def test_scheduled_delisting_is_a_warning():
    info, _ = info_for('instruments_info_delisting.json')
    assert info['ok'] is True
    assert info['warnings'] == ['DELISTING_SCHEDULED:1792000000000']


@pytest.mark.parametrize('answer, reason, transient', [
    (FakeResponse(403, 'access too frequent'), 'REFERENCE_LOOKUP_FAILED:HTTP_403', True),
    (FakeResponse(502, ''), 'REFERENCE_LOOKUP_FAILED:HTTP_502', True),
    (FakeResponse(404, '{}'), 'REFERENCE_HTTP_ERROR', False),
    (FakeResponse(200, '[]'), 'REFERENCE_LOOKUP_FAILED:INVALID_RESPONSE', True),
    (reply(V, 'rest_rate_limited.json'), 'REFERENCE_LOOKUP_FAILED:INVALID_BYBIT_INSTRUMENTS:STONKUSDT', True),
    (TimeoutError('slow'), 'REFERENCE_LOOKUP_FAILED:TimeoutError', True),
])
def test_instrument_lookup_failures(answer, reason, transient):
    http = FakeHttp({'/v5/market/instruments-info': answer})
    info = make().instrument_info(http, 4.0)
    assert info['ok'] is False and info['reason'] == reason and info['transient'] is transient
