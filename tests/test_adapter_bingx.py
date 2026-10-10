"""BingX swap adapter: gzip frames, Ping/Pong, depth5 with descending asks, mark/last, REST, contracts."""
from __future__ import annotations

import gzip
import json
from decimal import Decimal

import pytest

from adapter_fakes import NOW, WALL_MS, FakeHttp, FakeResponse, JsonOnlyResponse, fixture_text, gz, reply, source
from gate_stonk.errors import ConfigError, TransientUnsafe, Unsafe
from gate_stonk.feeds.base import Reply, Update
from gate_stonk.feeds.bingx import BingxAdapter
from gate_stonk.types import Ticker

V = 'bingx'
D = Decimal


def make(price: str = 'mid', symbol: str = 'STONK-USDT', **kwargs) -> BingxAdapter:
    extra = {key: kwargs.pop(key) for key in ('url', 'quote') if key in kwargs}
    return BingxAdapter(source('bingx_swap', symbol, source_id='bingx_perp', price=price, **extra),
                        timeout=5, **kwargs)


def feed(adapter: BingxAdapter, name: str) -> list:
    """Frames go through the real path: gzip on the wire -> decode -> on_text."""
    return adapter.on_text(adapter.decode(gz(fixture_text(V, name))), NOW, WALL_MS)


def only(items: list) -> Update:
    assert len(items) == 1, items
    return items[0]


# --------------------------------------------------------------------------- identity / transport
def test_identity_subscriptions_and_no_client_ping():
    adapter = make()
    assert adapter.venue == 'bingx_swap' and adapter.key == 'bingx_perp'
    assert adapter.ws_url() == 'wss://open-api-swap.bingx.com/swap-market'
    (frame,) = [json.loads(f) for f in adapter.subscribe_frames()]
    assert frame['reqType'] == 'sub' and frame['dataType'] == 'STONK-USDT@depth5@500ms' and frame['id']
    assert adapter.ping_interval == 0 and adapter.ping_frame(WALL_MS) is None


@pytest.mark.parametrize('price, data_type', [('mark', 'STONK-USDT@markPrice'), ('last', 'STONK-USDT@lastPrice')])
def test_ticker_subscription_only_for_mark_or_last(price, data_type):
    frames = [json.loads(f) for f in make(price).subscribe_frames()]
    assert [f['dataType'] for f in frames] == ['STONK-USDT@depth5@500ms', data_type]
    assert len({f['id'] for f in frames}) == 2


def test_symbol_and_quote_validation():
    with pytest.raises(ConfigError, match='symbol'):
        make(symbol='STONKUSDT')
    with pytest.raises(ConfigError, match='quote'):
        make(quote='USDC')


def test_gzip_round_trip_and_text_passthrough():
    adapter = make()
    text = fixture_text(V, 'ws_depth5.json')
    assert adapter.decode(gzip.compress(text.encode())) == text
    assert adapter.decode(bytearray(gzip.compress(b'Ping'))) == 'Ping'
    assert adapter.decode(text) == text                      # a text frame is used as is
    assert adapter.decode(b'plain utf-8') == 'plain utf-8'


@pytest.mark.parametrize('raw', [b'\x1f\x8b\x08\x00garbage', gzip.compress(b'x' * 64)[:-6], b'\xff\xfe\x00'])
def test_undecodable_binary_frame_is_an_error_update_not_a_disconnect(raw):
    adapter = make()
    assert only(adapter.on_text(adapter.decode(raw), NOW, WALL_MS)) == \
        Update(kind='error', key='bingx_perp', error='BINGX_GZIP_DECODE_ERROR')


def test_oversized_gzip_frame_is_refused():
    adapter = make()
    bomb = gzip.compress(b'0' * (5 * 1024 * 1024))
    assert only(adapter.on_text(adapter.decode(bomb), NOW, WALL_MS)).error == 'BINGX_GZIP_DECODE_ERROR'


def test_ping_pong_before_json():
    adapter = make()
    assert feed(adapter, 'ws_ping.txt') == [Reply('Pong')]
    assert adapter.on_text('ping', NOW, WALL_MS) == [Reply('Pong')]
    assert adapter.on_text('Pong', NOW, WALL_MS) == []
    (pong,) = feed(adapter, 'ws_ping_json.json')
    assert json.loads(pong.text) == {'pong': '5f2b6c1a', 'time': '2026-10-10T14:33:20.000+0000'}


# --------------------------------------------------------------------------- acks / errors
@pytest.mark.parametrize('name', ['ws_ack.json', 'ws_ack_alt.json'])
def test_subscription_ack_code_zero_is_not_an_error(name):
    assert feed(make(), name) == [Update(kind='subscribed', key='bingx_perp')]


@pytest.mark.parametrize('name', ['ws_sub_error.json', 'ws_error_no_id.json'])
def test_non_zero_code_ack_is_socket_fatal(name):
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        feed(make(), name)


# --------------------------------------------------------------------------- depth5
def test_depth5_descending_asks_are_sorted_best_first():
    update = only(feed(make(), 'ws_depth5.json'))
    assert update.kind == 'book' and update.key == 'bingx_perp' and update.source == 'ws'
    book = update.book
    assert book.ask == D('0.13600') and book.bid == D('0.13590')
    assert [price for price, _ in book.asks] == [D('0.13600'), D('0.13605'), D('0.13610'), D('0.13615'),
                                                 D('0.13620')]
    assert [price for price, _ in book.bids] == [D('0.13590'), D('0.13585'), D('0.13580'), D('0.13575'),
                                                 D('0.13570')]
    assert book.asks[0] == (D('0.13600'), D('14000'))                 # size travels with its price
    assert book.event_ms == 1791626000500 and book.update_id is None    # top-level ts
    assert book.mid == D('0.13595')
    book.check(NOW, 1791626000500 + 200, 3.0, D('1'))                  # passes the hub's structure check


def test_depth5_time_from_data_T_and_missing_time_is_an_error():
    adapter = make()
    assert only(feed(adapter, 'ws_depth5_data_T.json')).book.event_ms == 1791626000499
    assert only(feed(adapter, 'ws_depth5_no_time.json')).error == 'INVALID_WS_BOOK:bingx_perp'


@pytest.mark.parametrize('name, code', [
    ('ws_depth5_malformed.json', 'INVALID_BOOK_SCHEMA'),
    ('ws_depth5_data_error.json', 'BINGX_DATA_ERROR:80012'),
])
def test_malformed_or_failed_data_frames_become_error_updates(name, code):
    assert only(feed(make(), name)) == Update(kind='error', key='bingx_perp', error=code)


def test_duplicate_levels_stay_visible_to_book_check():
    frame = json.loads(fixture_text(V, 'ws_depth5.json'))
    frame['data']['asks'].append(['0.13600', '1'])
    book = only(make().on_text(json.dumps(frame), NOW, WALL_MS)).book
    with pytest.raises(Unsafe, match='UNSORTED_OR_DUPLICATE_BOOK_LEVEL'):
        book.check(NOW, 1791626000500, 3.0, None)


def test_non_json_text_and_foreign_streams():
    adapter = make()
    assert only(adapter.on_text('<html>', NOW, WALL_MS)).error == 'INVALID_WS_MESSAGE'
    assert feed(adapter, 'ws_depth5_other_symbol.json') == []
    assert feed(adapter, 'ws_mark_price.json') == []          # not subscribed for mid


# --------------------------------------------------------------------------- mark / last
def test_mark_price_stream():
    update = only(feed(make('mark'), 'ws_mark_price.json'))
    assert update.ticker == Ticker(mark=D('0.13594'), last=None, received=NOW, event_ms=1791626000800)


def test_last_price_stream():
    adapter = make('last')
    update = only(feed(adapter, 'ws_last_price.json'))
    assert update.ticker == Ticker(mark=None, last=D('0.13592'), received=NOW, event_ms=1791626000900)
    assert feed(adapter, 'ws_mark_price.json') == []


def test_ticker_symbol_mismatch_and_missing_price():
    frame = json.loads(fixture_text(V, 'ws_mark_price.json'))
    frame['data']['s'] = 'BTC-USDT'
    assert only(make('mark').on_text(json.dumps(frame), NOW, WALL_MS)).error == 'UNEXPECTED_BINGX_TICKER_SCHEMA'
    frame = json.loads(fixture_text(V, 'ws_mark_price.json'))
    del frame['data']['p']
    assert only(make('mark').on_text(json.dumps(frame), NOW, WALL_MS)).error == 'INVALID_WS_TICKER:bingx_perp'


# --------------------------------------------------------------------------- REST
def test_rest_depth_snapshot():
    http = FakeHttp({'/openApi/swap/v2/quote/depth': reply(V, 'rest_depth.json')})
    update = only(make().rest_snapshot(http, 4.0, NOW, WALL_MS))
    assert update.kind == 'book' and update.source == 'rest'
    assert (update.book.bid, update.book.ask, update.book.event_ms) == (D('0.13590'), D('0.13600'), 1791626000123)
    call = http.call('/openApi/swap/v2/quote/depth')
    assert call['url'] == 'https://open-api.bingx.com/openApi/swap/v2/quote/depth'
    assert call['params'] == {'symbol': 'STONK-USDT', 'limit': 5, 'timestamp': WALL_MS}


@pytest.mark.parametrize('price, path, name, expected', [
    ('mark', '/openApi/swap/v2/quote/premiumIndex', 'rest_premium_index.json', (D('0.13594'), None)),
    ('last', '/openApi/swap/v2/quote/ticker', 'rest_ticker.json', (None, D('0.13592'))),
])
def test_rest_mark_or_last_use_receipt_time(price, path, name, expected):
    http = FakeHttp({'/openApi/swap/v2/quote/depth': reply(V, 'rest_depth.json'), path: reply(V, name)})
    book, ticker = make(price).rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert (ticker.ticker.mark, ticker.ticker.last) == expected
    assert ticker.ticker.event_ms == WALL_MS and ticker.source == 'rest'
    assert http.call(path)['params'] == {'symbol': 'STONK-USDT', 'timestamp': WALL_MS}


def test_rest_premium_index_list_form_and_float_numbers():
    body = {'code': 0, 'msg': '', 'data': [{'symbol': 'BTC-USDT', 'markPrice': '69836.0'},
                                           {'symbol': 'STONK-USDT', 'markPrice': 0.13594}]}
    http = FakeHttp({'/openApi/swap/v2/quote/depth': reply(V, 'rest_depth.json'),
                     '/openApi/swap/v2/quote/premiumIndex': JsonOnlyResponse(200, body)})
    _, ticker = make('mark').rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert ticker.ticker.mark == D('0.13594')


@pytest.mark.parametrize('answer, code', [
    (reply(V, 'rest_rate_limited.json'), 'BINGX_REST_RATE_LIMITED'),
    (reply(V, 'rest_error_symbol.json'), 'BINGX_REST_ERROR:109425'),
    (FakeResponse(418, ''), 'BINGX_REST_HTTP_418'),
    (OSError('boom'), 'BINGX_REST_TRANSPORT:OSError'),
])
def test_rest_failures_raise_transient(answer, code):
    http = FakeHttp({'/openApi/swap/v2/quote/depth': answer})
    with pytest.raises(TransientUnsafe, match=f'^{code}$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)


# --------------------------------------------------------------------------- contracts check
def info_for(name: str, adapter: BingxAdapter | None = None) -> tuple[dict, FakeHttp]:
    http = FakeHttp({'/openApi/swap/v2/quote/contracts': reply(V, name)})
    return (adapter or make()).instrument_info(http, 4.0), http


@pytest.mark.parametrize('name', ['rest_contracts_ok.json', 'rest_contracts_string_types.json'])
def test_contracts_ok_int_or_string_types(name):
    info, http = info_for(name)
    assert info['ok'] is True and info['reason'] is None
    assert (info['base'], info['quote'], info['tick'], info['multiplier']) == ('STONK', 'USDT', '0.00001', '1')
    params = http.call('/openApi/swap/v2/quote/contracts')['params']
    assert params['symbol'] == 'STONK-USDT' and isinstance(params['timestamp'], int)
    json.dumps(info)


def test_contracts_offline_status():
    info, _ = info_for('rest_contracts_offline.json')
    assert info['reason'] == 'BINGX_REFERENCE_NOT_ACTIVE_EXACT_PERP:STONK-USDT'
    assert info['problems'] == ['STATUS:0']


@pytest.mark.parametrize('name', ['rest_error_symbol.json', 'rest_contracts_lookalike.json'])
def test_contracts_not_listed_exact_symbol_only(name):
    info, _ = info_for(name)
    assert info['ok'] is False and info['reason'] == 'BINGX_REFERENCE_NOT_LISTED:STONK-USDT'


def test_scaled_contract_rejected():
    info, _ = info_for('rest_contracts_scaled.json', make(symbol='1000STONK-USDT'))
    assert info['ok'] is False and 'SCALED_CONTRACT:1000STONK' in info['problems']


def test_contracts_rate_limited_is_transient():
    info, _ = info_for('rest_rate_limited.json')
    assert info['reason'] == 'REFERENCE_LOOKUP_FAILED:INVALID_BINGX_CONTRACTS:100410' and info['transient'] is True
