"""FX: FixedFx, Binance depth5 / Gate book_ticker adapters, REST fallbacks and hub.fx_snapshot()."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from gate_stonk.config import FxSettings, IndexSettings, SourceConfig
from gate_stonk.errors import ConfigError, Unsafe
from gate_stonk.feeds.base import FX_KEY, Update
from gate_stonk.feeds.fx import (BINANCE_FX_WS, BinanceFxAdapter, FixedFx, GateSpotFxAdapter, build_fx_adapter,
                                 fx_needed)
from gate_stonk.feeds.hub import NO_DATA, FeedHub
from gate_stonk.types import Book, FxSnapshot

NOW = 500.25
WALL_MS = 1_800_000_000_123


def fx_settings(mode: str = 'binance_spot', rate: str = '1') -> FxSettings:
    return FxSettings(mode=mode, usdc_to_usdt=Decimal(rate), max_depeg_pct=Decimal('0.5'), max_age_seconds=3.0,
                      max_spread_pct=Decimal('0.2'))


def source(sid: str, quote: str) -> SourceConfig:
    return SourceConfig(id=sid, venue='lighter' if quote == 'USDC' else 'bybit_linear', symbol='STONK', quote=quote,
                        price='mid', weight=Decimal(1), required=False, max_spread_pct=Decimal(1), max_age_seconds=3.0)


class Cfg:
    def __init__(self, mode: str = 'binance_spot', rate: str = '1', quotes=('USDC', 'USDT')) -> None:
        self.app = {'request_timeout_seconds': 4}
        self.market = {'currency_pair': 'STONK_USDT', 'depth_levels': 20, 'depth_interval': '100ms'}
        sources = tuple(source(f's{i}', quote) for i, quote in enumerate(quotes))
        self._settings = IndexSettings(method='weighted_median', min_sources=1, max_age_seconds=3.0,
                                       max_timestamp_skew_seconds=2.0, max_source_deviation_pct=Decimal(1),
                                       basis_offset_pct=Decimal(0), rejoin_snapshots=3,
                                       observation_interval_seconds=0.2, fx=fx_settings(mode, rate),
                                       sources=sources)

    def index_settings(self):
        return self._settings


def load(fixtures_dir, name: str) -> dict:
    data = json.loads((fixtures_dir / 'fx' / name).read_text())
    assert 'SYNTHETIC' in data.pop('_comment')
    return data


class Response:
    def __init__(self, status: int, payload) -> None:
        self.status_code = status
        self.payload = payload

    def json(self):
        return self.payload


class Http:
    def __init__(self, response) -> None:
        self.response = response
        self.calls: list[tuple] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return self.response


def fx_book(now: float, bid: str, ask: str, *, update_id: int | None = 1, event_ms: int | None = None) -> Book:
    return Book(((Decimal(bid), Decimal(1000)),), ((Decimal(ask), Decimal(1000)),), now,
                int(now * 1000) if event_ms is None else event_ms, update_id)


# ---------------------------------------------------------------------------- fixed
def test_fixed_fx_snapshot_and_validation():
    snap = FixedFx(fx_settings('fixed', '0.999')).snapshot()
    assert snap == FxSnapshot(rate=Decimal('0.999'), mode='fixed', book=None, error=None)
    for bad in ('0', '-1'):
        with pytest.raises(ConfigError):
            FixedFx(fx_settings('fixed', bad))


def test_hub_fixed_mode_has_no_worker_and_returns_the_configured_rate(clock):
    hub = FeedHub(Cfg('fixed', '1.0005'), start_threads=False)
    assert FX_KEY not in hub.specs
    assert hub.fx_snapshot() == FxSnapshot(rate=Decimal('1.0005'), mode='fixed', book=None, error=None)
    # depeg is the index builder's decision, not the feed's: an off-peg fixed rate is still reported
    assert FeedHub(Cfg('fixed', '1.02'), start_threads=False).fx_snapshot().rate == Decimal('1.02')


def test_fx_not_needed_without_usdc_sources(clock):
    assert FeedHub(Cfg('binance_spot', quotes=('USDT',)), start_threads=False).fx_snapshot() is None
    assert not fx_needed([source('a', 'USDT')]) and fx_needed([source('a', 'USDT'), source('b', 'USDC')])


# ---------------------------------------------------------------------------- binance
def test_binance_depth5_snapshot_uses_receipt_time(fixtures_dir):
    adapter = BinanceFxAdapter(fx_settings(), timeout=4)
    assert adapter.key == FX_KEY and adapter.ws_url() == BINANCE_FX_WS
    assert adapter.ws_url() == 'wss://stream.binance.com:9443/ws/usdcusdt@depth5@100ms'
    assert adapter.subscribe_frames() == [] and adapter.ping_frame(WALL_MS) is None and adapter.ping_interval == 0
    frame = load(fixtures_dir, 'binance_depth5.json')['frame']
    (update,) = adapter.on_text(json.dumps(frame), NOW, WALL_MS)
    book = update.book
    assert update.kind == 'book' and update.source == 'ws'
    assert (book.received, book.event_ms, book.update_id) == (NOW, WALL_MS, 160)
    assert book.bid == Decimal('0.99990000') and book.ask == Decimal('1.00000000') and len(book.bids) == 5
    assert book.mid == Decimal('0.99995')


def test_binance_combined_stream_wrapper_and_wrong_stream(fixtures_dir):
    adapter = BinanceFxAdapter(timeout=4)
    data = load(fixtures_dir, 'binance_depth5.json')['frame']
    (ok,) = adapter.on_text(json.dumps({'stream': 'usdcusdt@depth5@100ms', 'data': data}), NOW, WALL_MS)
    assert ok.kind == 'book'
    (bad,) = adapter.on_text(json.dumps({'stream': 'usdcusdt@depth20', 'data': data}), NOW, WALL_MS)
    assert (bad.kind, bad.error) == ('error', 'WRONG_FX_STREAM')


@pytest.mark.parametrize('payload, code', [
    ({'lastUpdateId': 1, 'bids': [], 'asks': [['1', '1']]}, 'EMPTY_BOOK_SIDE'),
    ({'lastUpdateId': 1, 'bids': [['1', '1', '1']], 'asks': [['1', '1']]}, 'INVALID_BOOK_SCHEMA'),
    ({'bids': [['1', '1']], 'asks': [['1.1', '1']]}, 'INVALID_WS_BOOK:fx'),
    ({'lastUpdateId': -3, 'bids': [['1', '1']], 'asks': [['1.1', '1']]}, 'INVALID_WS_BOOK:fx'),
    ({'lastUpdateId': 1, 'bids': [['x', '1']], 'asks': [['1.1', '1']]}, 'INVALID_WS_BOOK:fx'),
    ({'e': 'trade'}, 'INVALID_WS_BOOK:fx'),
])
def test_binance_malformed_frames_become_error_updates(payload, code):
    (update,) = BinanceFxAdapter(timeout=4).on_text(json.dumps(payload), NOW, WALL_MS)
    assert (update.kind, update.key, update.error) == ('error', FX_KEY, code)


def test_binance_socket_level_frames():
    adapter = BinanceFxAdapter(timeout=4)
    assert adapter.on_text('{"result": null, "id": 1}', NOW, WALL_MS) == []
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        adapter.on_text('{"error": {"code": 2, "msg": "Invalid request"}, "id": 1}', NOW, WALL_MS)
    with pytest.raises(Unsafe, match='^SERVER_SHUTDOWN$'):
        adapter.on_text('{"e": "serverShutdown", "E": 1800000000000}', NOW, WALL_MS)
    for text in ('nope', '[]'):
        with pytest.raises(Unsafe, match='^INVALID_WS_MESSAGE$'):
            adapter.on_text(text, NOW, WALL_MS)


def test_binance_unchanged_last_update_id_is_a_fresh_periodic_snapshot(clock, fixtures_dir):
    hub = FeedHub(Cfg(), start_threads=False)
    adapter = BinanceFxAdapter(timeout=4)
    text = json.dumps(load(fixtures_dir, 'binance_depth5.json')['frame'])
    for update in adapter.on_text(text, clock[0], int(clock[0] * 1000)):
        hub.publish(update)
    clock[0] += 2.5
    for update in adapter.on_text(text, clock[0], int(clock[0] * 1000)):   # same lastUpdateId 160
        hub.publish(update)
    clock[0] += 2.5
    snap = hub.fx_snapshot()
    assert snap.error is None and snap.rate == Decimal('0.99995') and snap.book.update_id == 160


def test_binance_rest_book_ticker_fallback(fixtures_dir):
    adapter = BinanceFxAdapter(timeout=4)
    assert adapter.rest_refresh_after_seconds == 1.5 and adapter.rest_min_interval == 1.0
    http = Http(Response(200, load(fixtures_dir, 'binance_book_ticker.json')['response']))
    (update,) = adapter.rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert http.calls == [('https://api.binance.com/api/v3/ticker/bookTicker', {'symbol': 'USDCUSDT'}, 4.0)]
    assert (update.kind, update.source, update.key) == ('book', 'rest', FX_KEY)
    assert (update.book.bid, update.book.ask) == (Decimal('0.99980000'), Decimal('0.99990000'))
    assert (update.book.event_ms, update.book.received, update.book.update_id) == (WALL_MS, NOW, None)
    wrong = dict(load(fixtures_dir, 'binance_book_ticker.json')['response'], symbol='USDCBUSD')
    with pytest.raises(Unsafe, match='^WRONG_FX_SYMBOL$'):
        adapter.rest_snapshot(Http(Response(200, wrong)), 4.0, NOW, WALL_MS)
    with pytest.raises(Unsafe, match='^INVALID_REST_BOOK$'):
        adapter.rest_snapshot(Http(Response(200, {'symbol': 'USDCUSDT', 'bidPrice': 'x'})), 4.0, NOW, WALL_MS)
    with pytest.raises(Unsafe, match='^REST_HTTP_418$'):
        adapter.rest_snapshot(Http(Response(418, {})), 4.0, NOW, WALL_MS)


def test_binance_instrument_info():
    adapter = BinanceFxAdapter(timeout=4)
    row = {'symbol': 'USDCUSDT', 'status': 'TRADING', 'baseAsset': 'USDC', 'quoteAsset': 'USDT'}
    assert adapter.instrument_info(Http(Response(200, {'symbols': [row]})), 4.0)['ok']
    halted = adapter.instrument_info(Http(Response(200, {'symbols': [dict(row, status='BREAK')]})), 4.0)
    assert halted['reason'] == 'FX_SYMBOL_NOT_ACTIVE:USDCUSDT'
    assert adapter.instrument_info(Http(Response(200, {'symbols': []})), 4.0)['reason'] == \
        'FX_SYMBOL_NOT_LISTED:USDCUSDT'
    assert adapter.instrument_info(Http(Response(451, {})), 4.0)['reason'] == 'REFERENCE_LOOKUP_FAILED:HTTP_451'


def test_rest_refresher_keeps_fx_alive_when_the_stream_is_quiet(clock, fixtures_dir):
    adapter = BinanceFxAdapter(timeout=4)
    http = Http(Response(200, load(fixtures_dir, 'binance_book_ticker.json')['response']))
    hub = FeedHub(Cfg(), adapters=[adapter], start_threads=False, http=http)
    hub.publish(Update('book', FX_KEY, book=fx_book(clock[0], '0.9999', '1.0000', update_id=7)))
    clock[0] += 1.0
    assert hub.refresh_once() == []
    clock[0] += 1.0
    assert hub.refresh_once() == [FX_KEY]
    snap = hub.fx_snapshot()
    assert snap.error is None and snap.rate == Decimal('0.99985') and snap.book.update_id is None
    assert hub.diagnostics()[FX_KEY]['source'] == 'rest'


# ---------------------------------------------------------------------------- hub.fx_snapshot
def test_hub_live_fx_snapshot_states(clock):
    hub = FeedHub(Cfg(), start_threads=False)
    assert hub.fx_snapshot() == FxSnapshot(rate=None, mode='binance_spot', book=None, error=NO_DATA)
    hub.publish(Update('book', FX_KEY, book=fx_book(clock[0], '1.0045', '1.0055')))
    snap = hub.fx_snapshot()
    assert snap.rate == Decimal('1.005') and snap.error is None and snap.mode == 'binance_spot'
    clock[0] += 3.1
    stale = hub.fx_snapshot()
    assert (stale.rate, stale.error) == (None, 'STALE_QUOTE') and stale.book is not None
    hub.publish(Update('book', FX_KEY, book=fx_book(clock[0], '0.9950', '1.0000', update_id=2)))
    assert hub.fx_snapshot().error == 'WIDE_SOURCE_SPREAD' and hub.fx_snapshot().rate is None
    hub.publish(Update('book', FX_KEY, book=fx_book(clock[0], '1.0199', '1.0201', update_id=3)))
    assert hub.fx_snapshot().rate == Decimal('1.02')      # depeg is checked by the index builder
    hub.invalidate(FX_KEY, 'DISCONNECTED')
    assert hub.fx_snapshot() == FxSnapshot(rate=None, mode='binance_spot', book=None, error='DISCONNECTED')


# ---------------------------------------------------------------------------- gate spot FX
def test_gate_spot_fx_book_ticker(clock, fixtures_dir):
    adapter = GateSpotFxAdapter(fx_settings('gate_spot'), timeout=4)
    assert adapter.key == FX_KEY and adapter.rest_refresh_after_seconds == 1.5
    (sub,) = [json.loads(f) for f in adapter.subscribe_frames()]
    assert sub == {'time': 1_800_000_000, 'id': 1, 'channel': 'spot.book_ticker', 'event': 'subscribe',
                   'payload': ['USDC_USDT']}
    assert json.loads(adapter.ping_frame(WALL_MS))['channel'] == 'spot.ping'
    frame = load(fixtures_dir, 'gate_book_ticker.json')['frame']
    (update,) = adapter.on_text(json.dumps(frame), NOW, WALL_MS)
    book = update.book
    assert (book.bid, book.bid_size, book.ask, book.ask_size) == (Decimal('0.9998'), Decimal(250000),
                                                                  Decimal('0.9999'), Decimal(180000))
    assert (book.event_ms, book.update_id, book.received) == (1_799_999_999_990, 9_793_320_464, NOW)


@pytest.mark.parametrize('field, value, code', [
    ('s', 'BTC_USDT', 'WRONG_FX_SYMBOL'),
    ('b', 'abc', 'INVALID_WS_BOOK:fx'),
    ('B', None, 'INVALID_WS_BOOK:fx'),
    ('t', -1, 'INVALID_WS_BOOK:fx'),
    ('u', None, 'INVALID_WS_BOOK:fx'),
])
def test_gate_spot_fx_malformed_ticker(fixtures_dir, field, value, code):
    frame = load(fixtures_dir, 'gate_book_ticker.json')['frame']
    frame['result'][field] = value
    (update,) = GateSpotFxAdapter(timeout=4).on_text(json.dumps(frame), NOW, WALL_MS)
    assert (update.kind, update.error) == ('error', code)


def test_gate_spot_fx_rest_and_errors():
    adapter = GateSpotFxAdapter(timeout=4, rest_url='https://api.gateio.invalid/api/v4')
    http = Http(Response(200, {'id': 5, 'current': 1_800_000_000_000, 'update': 1_799_999_999_000,
                               'bids': [['0.9998', '10']], 'asks': [['0.9999', '10']]}))
    (update,) = adapter.rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert http.calls[0] == ('https://api.gateio.invalid/api/v4/spot/order_book',
                             {'currency_pair': 'USDC_USDT', 'limit': 1, 'with_id': 'true'}, 4.0)
    assert (update.source, update.book.event_ms, update.book.update_id) == ('rest', 1_800_000_000_000, 5)
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        adapter.on_text(json.dumps({'channel': 'spot.book_ticker', 'event': 'subscribe',
                                    'error': {'code': 2, 'message': 'unknown currency pair: USDC_USDT'},
                                    'result': {'status': 'fail'}}), NOW, WALL_MS)


def test_build_fx_adapter_by_mode():
    assert build_fx_adapter(fx_settings('fixed'), timeout=4) is None
    assert isinstance(build_fx_adapter(fx_settings('binance_spot'), timeout=4), BinanceFxAdapter)
    gate = build_fx_adapter(fx_settings('gate_spot'), timeout=4, ws_url='wss://x.invalid/ws/v4/')
    assert isinstance(gate, GateSpotFxAdapter) and gate.ws_url() == 'wss://x.invalid/ws/v4/'
    with pytest.raises(ConfigError):
        build_fx_adapter(fx_settings('kraken'), timeout=4)
