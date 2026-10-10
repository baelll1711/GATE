"""Gate USDT futures adapter: book_ticker, tickers (mark/last), {p,s} REST levels, contract check."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from adapter_fakes import (NOW, WALL_MS, FakeHttp, FakeResponse, JsonOnlyResponse, fixture_json, fixture_text,
                           reply, source)
from gate_stonk.errors import ConfigError, TransientUnsafe, Unsafe
from gate_stonk.feeds.base import Update
from gate_stonk.feeds.gate_futures import GateFuturesAdapter
from gate_stonk.types import Ticker

V = 'gate_futures'
D = Decimal
CONTRACT = '/futures/usdt/contracts/STONK_USDT'
BOOK = '/futures/usdt/order_book'
TICKERS = '/futures/usdt/tickers'


def make(price: str = 'mid', symbol: str = 'STONK_USDT', **kwargs) -> GateFuturesAdapter:
    extra = {key: kwargs.pop(key) for key in ('url', 'quote') if key in kwargs}
    return GateFuturesAdapter(source('gate_futures_usdt', symbol, source_id='gate_perp', price=price, **extra),
                              timeout=5, **kwargs)


def feed(adapter: GateFuturesAdapter, name: str) -> list:
    return adapter.on_text(fixture_text(V, name), NOW, WALL_MS)


def only(items: list) -> Update:
    assert len(items) == 1, items
    return items[0]


# --------------------------------------------------------------------------- identity / subscriptions
def test_identity_headers_subscriptions_and_ping(clock):
    adapter = make()
    assert adapter.venue == 'gate_futures_usdt' and adapter.key == 'gate_perp'
    assert adapter.ws_url() == 'wss://fx-ws.gateio.ws/v4/ws/usdt'
    assert adapter.ws_headers() == {'X-Gate-Size-Decimal': '1'}
    (frame,) = [json.loads(f) for f in adapter.subscribe_frames()]
    assert frame == {'time': int(clock[0]), 'channel': 'futures.book_ticker', 'event': 'subscribe',
                     'payload': ['STONK_USDT']}
    assert [json.loads(f)['channel'] for f in make('mark').subscribe_frames()] == ['futures.book_ticker',
                                                                                    'futures.tickers']
    assert adapter.ping_interval == 10.0
    assert json.loads(adapter.ping_frame(1791640000999)) == {'time': 1791640000, 'channel': 'futures.ping'}


def test_constructor_validation():
    with pytest.raises(ConfigError, match='symbol'):
        make(symbol='STONKUSDT')
    with pytest.raises(ConfigError, match='quote'):
        make(symbol='STONK_USDC', quote='USDC')


# --------------------------------------------------------------------------- control frames
def test_pong_with_null_error_is_ignored_and_ack_reported():
    adapter = make()
    assert feed(adapter, 'ws_pong.json') == []
    assert feed(adapter, 'ws_sub_ack.json') == [Update(kind='subscribed', key='gate_perp')]


def test_subscription_error_is_socket_fatal():
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        feed(make(), 'ws_sub_error.json')
    frame = fixture_json(V, 'ws_sub_ack.json')
    frame['result'] = {'status': 'fail'}
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        make().on_text(json.dumps(frame), NOW, WALL_MS)


def test_error_on_a_data_channel_invalidates_the_key_only():
    assert only(feed(make(), 'ws_channel_error.json')) == Update(kind='error', key='gate_perp',
                                                                  error='GATE_FUTURES_WS_ERROR')


# --------------------------------------------------------------------------- book_ticker
def test_book_ticker_parses_contract_sizes_exactly():
    update = only(feed(make(), 'ws_book_ticker.json'))
    assert update.kind == 'book' and update.key == 'gate_perp' and update.source == 'ws'
    book = update.book
    assert book.bids == ((D('0.1354'), D('1200.5')),) and book.asks == ((D('0.1356'), D('800')),)
    assert book.event_ms == 1791640000049 and book.update_id == 5213871234       # t / u
    assert book.microprice == (D('0.1354') * D('800') + D('0.1356') * D('1200.5')) / (D('1200.5') + D('800'))
    book.check(NOW, 1791640000049 + 10, 3.0, D('1'))


def test_book_ticker_integer_sizes_without_size_decimal_header():
    book = only(feed(make(), 'ws_book_ticker_int_sizes.json')).book
    assert book.bid_size == D('1200') and book.ask_size == D('800')


@pytest.mark.parametrize('name, code', [
    ('ws_book_ticker_wrong_contract.json', 'UNEXPECTED_GATE_FUTURES_SCHEMA'),
    ('ws_book_ticker_empty_bid.json', 'EMPTY_BOOK_SIDE'),
    ('ws_book_ticker_malformed.json', 'INVALID_WS_BOOK:gate_perp'),
])
def test_malformed_book_ticker_frames_become_error_updates(name, code):
    assert only(feed(make(), name)) == Update(kind='error', key='gate_perp', error=code)


def test_non_json_and_unsubscribed_channels():
    adapter = make()
    assert only(adapter.on_text('pong?', NOW, WALL_MS)).error == 'INVALID_WS_MESSAGE'
    assert feed(adapter, 'ws_tickers.json') == []            # tickers not subscribed for mid
    frame = fixture_json(V, 'ws_book_ticker.json')
    frame['channel'] = 'futures.trades'
    assert adapter.on_text(json.dumps(frame), NOW, WALL_MS) == []


# --------------------------------------------------------------------------- tickers
@pytest.mark.parametrize('price', ['mark', 'last'])
def test_tickers_mark_price_and_last(price):
    update = only(feed(make(price), 'ws_tickers.json'))
    assert update.ticker == Ticker(mark=D('0.13552'), last=D('0.1355'), received=NOW, event_ms=1791640000500)


def test_tickers_time_fallback_and_foreign_contract():
    frame = fixture_json(V, 'ws_tickers.json')
    del frame['time_ms']
    assert only(make('mark').on_text(json.dumps(frame), NOW, WALL_MS)).ticker.event_ms == 1791640000000
    frame = fixture_json(V, 'ws_tickers.json')
    frame['result'][0]['contract'] = 'BTC_USDT'
    assert only(make('mark').on_text(json.dumps(frame), NOW, WALL_MS)).error == 'UNEXPECTED_GATE_FUTURES_SCHEMA'


# --------------------------------------------------------------------------- REST
def test_rest_order_book_ps_levels_and_seconds_timestamp():
    http = FakeHttp({BOOK: reply(V, 'rest_order_book.json')})
    update = only(make().rest_snapshot(http, 4.0, NOW, WALL_MS))
    assert update.kind == 'book' and update.source == 'rest'
    book = update.book
    assert book.bids == ((D('0.1354'), D('1200.5')), (D('0.1353'), D('203')))
    assert book.asks == ((D('0.1356'), D('800')), (D('0.1357'), D('40')))
    assert book.event_ms == 1791640000123 and book.update_id is None           # current (s) -> ms
    call = http.call(BOOK)
    assert call['url'] == 'https://api.gateio.ws/api/v4/futures/usdt/order_book'
    assert call['params'] == {'contract': 'STONK_USDT', 'limit': 1}
    assert call['headers'] == {'X-Gate-Size-Decimal': '1'}


def test_rest_order_book_float_seconds_via_json_only_response():
    body = fixture_json(V, 'rest_order_book.json')       # current parsed as float by requests
    http = FakeHttp({BOOK: JsonOnlyResponse(200, body)})
    assert only(make().rest_snapshot(http, 4.0, NOW, WALL_MS)).book.event_ms == 1791640000123


def test_rest_snapshot_with_tickers_uses_receipt_time():
    http = FakeHttp({BOOK: reply(V, 'rest_order_book.json'), TICKERS: reply(V, 'rest_tickers.json')})
    _, ticker = make('mark').rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert ticker.ticker == Ticker(mark=D('0.13552'), last=D('0.1355'), received=NOW, event_ms=WALL_MS)
    assert http.call(TICKERS)['params'] == {'contract': 'STONK_USDT'}
    assert http.call(TICKERS)['headers'] == {'X-Gate-Size-Decimal': '1'}


@pytest.mark.parametrize('answer, code', [
    (reply(V, 'rest_error_429.json', status=429), 'GATE_FUTURES_REST_RATE_LIMITED'),
    (reply(V, 'rest_contract_not_found.json', status=400), 'GATE_FUTURES_REST_HTTP_400:CONTRACT_NOT_FOUND'),
    (FakeResponse(502, '<html>'), 'GATE_FUTURES_REST_HTTP_502'),
    (TimeoutError(), 'GATE_FUTURES_REST_TRANSPORT:TimeoutError'),
])
def test_rest_failures_raise_transient(answer, code):
    with pytest.raises(TransientUnsafe, match=f'^{code}$'):
        make().rest_snapshot(FakeHttp({BOOK: answer}), 4.0, NOW, WALL_MS)


def test_rest_malformed_body_raises_unsafe():
    http = FakeHttp({BOOK: FakeResponse(200, '{"current":1.5,"asks":[["0.1356","800"]],"bids":[{"p":"0.1","s":"1"}]}')})
    with pytest.raises(Unsafe, match='^INVALID_BOOK_SCHEMA$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)
    http = FakeHttp({BOOK: FakeResponse(200, '{"asks":[{"p":"0.2","s":"1"}],"bids":[{"p":"0.1","s":"1"}]}')})
    with pytest.raises(Unsafe, match='^INVALID_REST_BOOK:gate_perp$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)


# --------------------------------------------------------------------------- contract check
def info_for(name: str, status: int = 200, adapter: GateFuturesAdapter | None = None,
             path: str = CONTRACT) -> tuple[dict, FakeHttp]:
    http = FakeHttp({path: reply(V, name, status)})
    return (adapter or make()).instrument_info(http, 4.0), http


def test_contract_ok():
    info, http = info_for('rest_contract_ok.json')
    assert info['ok'] is True and info['reason'] is None
    assert (info['status'], info['base'], info['quote'], info['tick'], info['contract_size']) == \
        ('trading', 'STONK', 'USDT', '0.0001', '10')
    assert http.call(CONTRACT)['headers'] == {'X-Gate-Size-Decimal': '1'}
    assert 'quanto_multiplier' in info['raw_keys']
    json.dumps(info)


@pytest.mark.parametrize('status', [400, 404])
def test_contract_not_found_label(status):
    info, _ = info_for('rest_contract_not_found.json', status)
    assert info['ok'] is False and info['reason'] == 'GATE_FUTURES_REFERENCE_NOT_LISTED:STONK_USDT'


def test_contract_in_delisting_is_not_active():
    info, _ = info_for('rest_contract_delisting.json')
    assert info['reason'] == 'GATE_FUTURES_REFERENCE_NOT_ACTIVE_EXACT_PERP:STONK_USDT'
    assert info['problems'] == ['STATUS:delisting', 'IN_DELISTING:True']
    assert info['warnings'] == ['DELISTING_SCHEDULED:1792000000']


def test_scaled_contract_rejected():
    adapter = make(symbol='1000STONK_USDT')
    info, _ = info_for('rest_contract_scaled.json', adapter=adapter, path='/futures/usdt/contracts/1000STONK_USDT')
    assert info['ok'] is False and 'SCALED_CONTRACT:1000STONK' in info['problems']


def test_contract_lookup_rate_limited_is_transient():
    info, _ = info_for('rest_error_429.json', 429)
    assert info['reason'] == 'REFERENCE_LOOKUP_FAILED:HTTP_429' and info['transient'] is True


# --------------------------------------------------------------------------- with the real worker
def test_worker_passes_size_decimal_header_on_connect():
    from gate_stonk.feeds.ws import WsWorker

    class Conn:
        def __init__(self):
            self.frames = [fixture_text(V, 'ws_sub_ack.json'), fixture_text(V, 'ws_book_ticker.json')]
            self.sent = []

        def settimeout(self, value):
            pass

        def send(self, text):
            self.sent.append(text)

        def recv(self):
            return self.frames.pop(0) if self.frames else ''

        def ping(self):
            pass

        def close(self):
            pass

    class Sink:
        def __init__(self):
            self.published = []

        def publish(self, update):
            self.published.append(update)

        def invalidate(self, key, reason):
            pass

        def set_connected(self, key, connected):
            pass

        def connection_reset(self, keys):
            pass

        def should_reconnect(self, stream, now, keys):
            return False

    conn, sink, opened = Conn(), Sink(), []

    def connect(url, timeout, **options):
        opened.append((url, options))
        return conn

    WsWorker(make(), sink, timeout=5, connect=connect, wait=lambda seconds: True).run()
    assert opened == [('wss://fx-ws.gateio.ws/v4/ws/usdt', {'header': {'X-Gate-Size-Decimal': '1'}})]
    assert [u.kind for u in sink.published] == ['book']
    assert sink.published[0].book.update_id == 5213871234
