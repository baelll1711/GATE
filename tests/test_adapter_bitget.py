"""Bitget USDT-M adapter: text ping/pong, books1 snapshots, ticker, REST, contracts check."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from adapter_fakes import NOW, WALL_MS, FakeHttp, FakeResponse, JsonOnlyResponse, fixture_text, reply, source
from gate_stonk.errors import ConfigError, TransientUnsafe, Unsafe
from gate_stonk.feeds.base import Reply, Update
from gate_stonk.feeds.bitget import BitgetAdapter
from gate_stonk.types import Ticker

V = 'bitget'
D = Decimal


def make(price: str = 'mid', symbol: str = 'STONKUSDT', **kwargs) -> BitgetAdapter:
    extra = {key: kwargs.pop(key) for key in ('url', 'quote') if key in kwargs}
    return BitgetAdapter(source('bitget_usdt_futures', symbol, source_id='bitget_perp', price=price, **extra),
                         timeout=5, **kwargs)


def feed(adapter: BitgetAdapter, name: str) -> list:
    return adapter.on_text(fixture_text(V, name), NOW, WALL_MS)


def only(items: list) -> Update:
    assert len(items) == 1, items
    return items[0]


# --------------------------------------------------------------------------- identity / subscriptions
def test_identity_subscriptions_and_text_ping():
    adapter = make()
    assert adapter.venue == 'bitget_usdt_futures' and adapter.key == 'bitget_perp'
    assert adapter.ws_url() == 'wss://ws.bitget.com/v2/ws/public'
    assert make(url='wss://wspap.bitget.com/v2/ws/public').ws_url() == 'wss://wspap.bitget.com/v2/ws/public'
    assert [json.loads(f) for f in adapter.subscribe_frames()] == [
        {'op': 'subscribe', 'args': [{'instType': 'USDT-FUTURES', 'channel': 'books1', 'instId': 'STONKUSDT'}]}]
    assert adapter.ping_interval == 30.0 and adapter.ping_frame(WALL_MS) == 'ping'     # text, not JSON
    assert adapter.idle_timeout > 2 * adapter.ping_interval


@pytest.mark.parametrize('price', ['mark', 'last'])
def test_ticker_channel_only_for_mark_or_last(price):
    (frame,) = [json.loads(f) for f in make(price).subscribe_frames()]
    assert [arg['channel'] for arg in frame['args']] == ['books1', 'ticker']


def test_usdc_quote_maps_to_usdc_futures_and_bad_quote_rejected():
    assert make(symbol='STONKUSDC', quote='USDC').product_type == 'USDC-FUTURES'
    with pytest.raises(ConfigError, match='quote'):
        make(quote='EUR')
    with pytest.raises(ConfigError, match='symbol'):
        make(symbol='STONK-USDT')


def test_heartbeats_are_handled_before_json():
    adapter = make()
    assert feed(adapter, 'ws_pong.txt') == []
    assert adapter.on_text(' pong\n', NOW, WALL_MS) == []
    assert adapter.on_text('ping', NOW, WALL_MS) == [Reply('pong')]


def test_subscribe_ack_and_error_frame():
    adapter = make('mark')
    assert feed(adapter, 'ws_sub_ack.json') == [Update(kind='subscribed', key='bitget_perp')]
    assert feed(adapter, 'ws_sub_ack_ticker.json') == []        # one 'subscribed' per connection
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        feed(adapter, 'ws_sub_error.json')


# --------------------------------------------------------------------------- books1
def test_books1_snapshot_parses_to_book():
    update = only(feed(make(), 'ws_books1_push.json'))
    assert update.kind == 'book' and update.key == 'bitget_perp' and update.source == 'ws'
    book = update.book
    assert book.bids == ((D('0.13588'), D('12000')),)
    assert book.asks == ((D('0.13595'), D('8500')),)
    assert book.event_ms == 1791619200120           # data[0].ts (string)
    assert book.update_id is None
    assert book.received == NOW
    book.check(NOW, 1791619200120 + 100, 3.0, D('1'))


def test_books1_falls_back_to_top_level_ts():
    frame = json.loads(fixture_text(V, 'ws_books1_push.json'))
    del frame['data'][0]['ts']
    assert only(make().on_text(json.dumps(frame), NOW, WALL_MS)).book.event_ms == 1791619200125


@pytest.mark.parametrize('name, code', [
    ('ws_books1_update_action.json', 'UNEXPECTED_BITGET_BOOKS1_SCHEMA'),
    ('ws_books1_malformed.json', 'INVALID_BOOK_SCHEMA'),
    ('ws_not_json.txt', 'INVALID_WS_MESSAGE'),
])
def test_malformed_frames_become_error_updates(name, code):
    update = only(feed(make(), name))
    assert update.kind == 'error' and update.error == code and update.key == 'bitget_perp'


def test_books1_data_shape_and_bad_numbers():
    adapter = make()
    frame = json.loads(fixture_text(V, 'ws_books1_push.json'))
    frame['data'] = frame['data'] * 2
    assert only(adapter.on_text(json.dumps(frame), NOW, WALL_MS)).error == 'UNEXPECTED_BITGET_BOOKS1_SCHEMA'
    frame = json.loads(fixture_text(V, 'ws_books1_push.json'))
    frame['data'][0]['asks'] = [['Infinity', '1']]
    assert only(adapter.on_text(json.dumps(frame), NOW, WALL_MS)).error == 'INVALID_WS_BOOK:bitget_perp'


def test_foreign_instruments_and_unsubscribed_channels_are_ignored():
    adapter = make()
    assert feed(adapter, 'ws_books1_other_inst.json') == []
    assert feed(adapter, 'ws_ticker_push.json') == []       # ticker not subscribed for mid


# --------------------------------------------------------------------------- ticker
@pytest.mark.parametrize('price', ['mark', 'last'])
def test_ticker_push_carries_mark_and_last(price):
    update = only(feed(make(price), 'ws_ticker_push.json'))
    assert update.kind == 'ticker'
    assert update.ticker == Ticker(mark=D('0.13593'), last=D('0.13592'), received=NOW, event_ms=1791619200120)


def test_ticker_for_other_instrument_inside_our_arg_is_an_error():
    frame = json.loads(fixture_text(V, 'ws_ticker_push.json'))
    frame['data'][0]['instId'] = 'BTCUSDT'
    update = only(make('mark').on_text(json.dumps(frame), NOW, WALL_MS))
    assert update.error == 'UNEXPECTED_BITGET_TICKER_SCHEMA'


# --------------------------------------------------------------------------- REST
@pytest.mark.parametrize('name', ['rest_merge_depth.json', 'rest_merge_depth_numbers.json'])
def test_rest_merge_depth_strings_or_numbers_parse_exactly(name):
    http = FakeHttp({'/api/v2/mix/market/merge-depth': reply(V, name)})
    update = only(make().rest_snapshot(http, 4.0, NOW, WALL_MS))
    assert update.kind == 'book' and update.source == 'rest'
    book = update.book
    assert book.bids == ((D('0.13588'), D('12000')), (D('0.13580'), D('30000')))
    assert book.asks == ((D('0.13595'), D('8500')), (D('0.13600'), D('20000')))
    assert book.event_ms == 1791619200120 and book.update_id is None
    call = http.call('/api/v2/mix/market/merge-depth')
    assert call['url'] == 'https://api.bitget.com/api/v2/mix/market/merge-depth'
    assert call['params'] == {'symbol': 'STONKUSDT', 'productType': 'USDT-FUTURES', 'precision': 'scale0',
                              'limit': '1'}


def test_rest_numbers_through_json_only_response_stay_exact():
    body = json.loads(fixture_text(V, 'rest_merge_depth_numbers.json'))       # floats, as requests returns
    http = FakeHttp({'/api/v2/mix/market/merge-depth': JsonOnlyResponse(200, body)})
    book = only(make().rest_snapshot(http, 4.0, NOW, WALL_MS)).book
    assert book.bid == D('0.13588') and book.ask == D('0.13595')


def test_rest_snapshot_with_ticker():
    http = FakeHttp({'/api/v2/mix/market/merge-depth': reply(V, 'rest_merge_depth.json'),
                     '/api/v2/mix/market/ticker': reply(V, 'rest_ticker.json')})
    book, ticker = make('last').rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert book.kind == 'book' and ticker.kind == 'ticker' and ticker.source == 'rest'
    assert ticker.ticker == Ticker(mark=D('0.13593'), last=D('0.13592'), received=NOW, event_ms=1791619200120)
    assert http.call('/api/v2/mix/market/ticker')['params'] == {'symbol': 'STONKUSDT', 'productType': 'USDT-FUTURES'}


@pytest.mark.parametrize('answer, code', [
    (reply(V, 'rest_error_removed.json'), 'BITGET_REST_ERROR:40309'),
    (reply(V, 'rest_rate_limited.json'), 'BITGET_REST_RATE_LIMITED'),
    (FakeResponse(429, ''), 'BITGET_REST_RATE_LIMITED'),
    (FakeResponse(500, '{"code":"50000"}'), 'BITGET_REST_HTTP_500'),
    (ConnectionResetError(), 'BITGET_REST_TRANSPORT:ConnectionResetError'),
])
def test_rest_failures_raise_transient(answer, code):
    http = FakeHttp({'/api/v2/mix/market/merge-depth': answer})
    with pytest.raises(TransientUnsafe, match=f'^{code}$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)


def test_rest_malformed_body_raises_unsafe():
    http = FakeHttp({'/api/v2/mix/market/merge-depth':
                     FakeResponse(200, '{"code":"00000","data":{"asks":[["0.1","1"]],"bids":"x","ts":"1"}}')})
    with pytest.raises(Unsafe, match='^EMPTY_BOOK_SIDE$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)


# --------------------------------------------------------------------------- contracts check
def info_for(name: str, adapter: BitgetAdapter | None = None) -> dict:
    http = FakeHttp({'/api/v2/mix/market/contracts': reply(V, name)})
    return (adapter or make()).instrument_info(http, 4.0)


def test_contracts_ok_and_tick_from_price_place():
    info = info_for('rest_contracts_ok.json')
    assert info['ok'] is True and info['reason'] is None
    assert (info['status'], info['base'], info['quote']) == ('normal', 'STONK', 'USDT')
    assert info['tick'] == '0.00001' and info['multiplier'] == '1'
    assert 'pricePlace' in info['raw_keys']
    json.dumps(info)


def test_tick_uses_price_end_step():
    assert info_for('rest_contracts_tick5.json')['tick'] == '0.0005'


@pytest.mark.parametrize('name', ['rest_contracts_empty.json', 'rest_error_removed.json'])
def test_contracts_not_listed(name):
    info = info_for(name)
    assert info['ok'] is False and info['reason'] == 'BITGET_REFERENCE_NOT_LISTED:STONKUSDT'


def test_contracts_maintenance_is_not_active():
    info = info_for('rest_contracts_maintain.json')
    assert info['reason'] == 'BITGET_REFERENCE_NOT_ACTIVE_EXACT_PERP:STONKUSDT'
    assert info['problems'] == ['STATUS:maintain']


def test_scaled_contract_rejected():
    info = info_for('rest_contracts_scaled.json', make(symbol='1000STONKUSDT'))
    assert info['ok'] is False and 'SCALED_CONTRACT:1000STONK' in info['problems']
    assert info_for('rest_contracts_scaled.json')['reason'] == 'BITGET_REFERENCE_NOT_LISTED:STONKUSDT'


def test_contracts_transient_failures():
    http = FakeHttp({'/api/v2/mix/market/contracts': reply(V, 'rest_rate_limited.json')})
    info = make().instrument_info(http, 4.0)
    assert info['reason'] == 'REFERENCE_LOOKUP_FAILED:INVALID_BITGET_CONTRACTS:429' and info['transient'] is True
    http = FakeHttp({'/api/v2/mix/market/contracts': FakeResponse(429, '')})
    assert make().instrument_info(http, 4.0)['reason'] == 'REFERENCE_LOOKUP_FAILED:HTTP_429'
