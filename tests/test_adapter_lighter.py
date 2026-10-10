"""Lighter adapter: local book (snapshot + absolute updates, nonce continuity), market index, REST."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from adapter_fakes import (NOW, WALL_MS, FakeHttp, FakeResponse, JsonOnlyResponse, fixture_json, fixture_text,
                           reply, source)
from gate_stonk.errors import ConfigError, TransientUnsafe, Unsafe
from gate_stonk.feeds.base import Reply, Update
from gate_stonk.feeds.lighter import PUBLISH_DEPTH, LighterAdapter
from gate_stonk.types import Ticker

V = 'lighter'
D = Decimal
DETAILS = '/api/v1/orderBookDetails'
ORDERS = '/api/v1/orderBookOrders'


def make(price: str = 'mid', symbol: str = 'STONK', market_index: int | None = 4100, **kwargs) -> LighterAdapter:
    extra = {key: kwargs.pop(key) for key in ('url', 'quote') if key in kwargs}
    extra.setdefault('quote', 'USDC')
    return LighterAdapter(source('lighter', symbol, source_id='lighter_perp', price=price,
                                 market_index=market_index, **extra), timeout=5, **kwargs)


def feed(adapter: LighterAdapter, name: str, wall_ms: int = WALL_MS, now: float = NOW) -> list:
    return adapter.on_text(fixture_text(V, name), now, wall_ms)


def books(items: list) -> list:
    return [item.book for item in items if isinstance(item, Update) and item.kind == 'book']


def synced() -> LighterAdapter:
    adapter = make()
    feed(adapter, 'ws_ob_snapshot.json')
    return adapter


# --------------------------------------------------------------------------- identity / subscriptions
def test_identity_and_subscribe_uses_slash_channel():
    adapter = make()
    assert adapter.venue == 'lighter' and adapter.key == 'lighter_perp' and adapter.market_index == 4100
    assert adapter.ws_url() == 'wss://mainnet.zklighter.elliot.ai/stream'
    assert [json.loads(f) for f in adapter.subscribe_frames()] == [
        {'type': 'subscribe', 'channel': 'order_book/4100'}]
    assert [json.loads(f)['channel'] for f in make('mark').subscribe_frames()] == ['order_book/4100',
                                                                                    'market_stats/4100']
    assert adapter.ping_interval == 0 and adapter.ping_frame(WALL_MS) is None


def test_constructor_validation():
    with pytest.raises(ConfigError, match='quote'):
        make(quote='USDT')
    with pytest.raises(ConfigError, match='market_index'):
        make(market_index=-1)
    with pytest.raises(ConfigError, match='symbol'):
        make(symbol='STONK/USDC')
    assert LighterAdapter(source('lighter', 'STONK', quote='USDC'), timeout=5, market_index=7).market_index == 7


def test_on_text_and_subscribe_refuse_to_run_without_market_index():
    adapter = make(market_index=None)
    with pytest.raises(Unsafe, match='^LIGHTER_MARKET_INDEX_UNKNOWN$'):
        adapter.subscribe_frames()
    with pytest.raises(Unsafe, match='^LIGHTER_MARKET_INDEX_UNKNOWN$'):
        feed(adapter, 'ws_ob_snapshot.json')


# --------------------------------------------------------------------------- heartbeats / control frames
@pytest.mark.parametrize('name', ['ws_ping.json', 'ws_ping_spaced.json'])
def test_server_ping_gets_json_pong(name):
    (pong,) = feed(make(), name)
    assert isinstance(pong, Reply) and json.loads(pong.text) == {'type': 'pong'}


def test_connected_pong_and_harmless_error_are_ignored():
    adapter = make()
    assert feed(adapter, 'ws_connected.json') == []
    assert adapter.on_text('{"type":"pong"}', NOW, WALL_MS) == []
    assert feed(adapter, 'ws_error_30003.json') == []


@pytest.mark.parametrize('name', ['ws_error_30005.json', 'ws_error_type_30012.json'])
def test_subscription_errors_in_both_formats_are_socket_fatal(name):
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        feed(make(), name)


def test_shutdown_notice_reconnects_early():
    with pytest.raises(Unsafe, match='^LIGHTER_SERVER_SHUTDOWN$'):
        feed(make(), 'ws_shutdown.json')


def test_non_json_frame_is_an_error_update():
    assert make().on_text('not json', NOW, WALL_MS) == [Update(kind='error', key='lighter_perp',
                                                                error='INVALID_WS_MESSAGE')]


# --------------------------------------------------------------------------- local book
def test_snapshot_builds_sorted_book_with_receipt_time():
    items = feed(make(), 'ws_ob_snapshot.json', wall_ms=WALL_MS + 7)
    assert items[0] == Update(kind='subscribed', key='lighter_perp')
    (book,) = books(items)
    assert book.bids == ((D('0.13590'), D('12000.0')), (D('0.13580'), D('8000.0')))
    assert book.asks == ((D('0.13610'), D('2500.0')), (D('0.13620'), D('5000.0')))     # re-sorted best first
    assert book.event_ms == WALL_MS + 7 and book.received == NOW and book.update_id is None
    book.check(NOW, WALL_MS + 7, 3.0, D('1'))


def test_absolute_size_updates_and_zero_deletes():
    adapter = synced()
    (after_update,) = books(feed(adapter, 'ws_ob_update_ok.json', wall_ms=WALL_MS + 100, now=NOW + 0.1))
    assert after_update.asks[0] == (D('0.13605'), D('1000.0'))           # new best ask inserted
    assert after_update.bids[0] == (D('0.13590'), D('11000.0'))          # absolute size replaced, not added
    assert after_update.event_ms == WALL_MS + 100 and after_update.received == NOW + 0.1
    (after_delete,) = books(feed(adapter, 'ws_ob_update_delete.json'))
    assert after_delete.ask == D('0.13610')                              # size "0.0000" removed the level
    assert after_delete.bids == after_update.bids
    # the earlier snapshot object is immutable and unaffected by later updates
    assert after_update.asks[0] == (D('0.13605'), D('1000.0'))


def test_nonce_gap_raises_and_resets_local_book():
    adapter = synced()
    with pytest.raises(Unsafe, match='^LIGHTER_NONCE_GAP$'):
        feed(adapter, 'ws_ob_update_gap.json')
    # local state dropped: further updates wait for the reconnect snapshot
    assert feed(adapter, 'ws_ob_update_ok.json') == []


def test_gap_after_a_valid_chain():
    adapter = synced()
    feed(adapter, 'ws_ob_update_ok.json')            # nonce 904845 -> 904850
    stale = fixture_json(V, 'ws_ob_update_ok.json')    # begin_nonce 904845 again (replayed frame)
    with pytest.raises(Unsafe, match='^LIGHTER_NONCE_GAP$'):
        adapter.on_text(json.dumps(stale), NOW, WALL_MS)


def test_updates_before_snapshot_are_dropped_and_reset_clears_book():
    adapter = make()
    assert feed(adapter, 'ws_ob_update_ok.json') == []
    feed(adapter, 'ws_ob_snapshot.json')
    adapter.on_reset()
    assert feed(adapter, 'ws_ob_update_ok.json') == []


@pytest.mark.parametrize('name, code', [
    ('ws_ob_update_no_begin_nonce.json', 'INVALID_LIGHTER_BOOK'),
    ('ws_ob_update_malformed.json', 'INVALID_LIGHTER_BOOK'),
    ('ws_ob_update_cross.json', 'LIGHTER_BOOK_CROSSED'),
])
def test_unrepairable_book_frames_are_socket_fatal(name, code):
    adapter = synced()
    with pytest.raises(Unsafe, match=f'^{code}$'):
        feed(adapter, name)
    assert feed(adapter, 'ws_ob_update_ok.json') == []      # state was dropped


def test_channel_colon_inbound_slash_tolerated_and_other_markets_ignored():
    adapter = make()
    assert books(feed(adapter, 'ws_ob_snapshot_slash_channel.json'))
    assert feed(adapter, 'ws_ob_update_other_market.json') == []
    assert books(feed(adapter, 'ws_ob_update_ok.json'))          # chain intact after the foreign frame


def test_one_sided_book_is_an_error_update_but_state_is_kept():
    adapter = make()
    items = feed(adapter, 'ws_ob_snapshot_one_sided.json')
    assert items == [Update(kind='subscribed', key='lighter_perp'),
                     Update(kind='error', key='lighter_perp', error='EMPTY_BOOK_SIDE')]
    frame = fixture_json(V, 'ws_ob_update_ok.json')       # begin 904845 == snapshot nonce; adds a bid
    (book,) = books(adapter.on_text(json.dumps(frame), NOW, WALL_MS))
    assert book.bid == D('0.13590') and book.ask == D('0.13605')


def test_published_depth_is_bounded():
    frame = fixture_json(V, 'ws_ob_snapshot.json')
    frame['order_book']['bids'] = [{'price': str(D('0.1') - D('0.0001') * i), 'size': '1'} for i in range(80)]
    (book,) = books(make().on_text(json.dumps(frame), NOW, WALL_MS))
    assert len(book.bids) == PUBLISH_DEPTH and book.bid == D('0.1')
    book.check(NOW, WALL_MS, 3.0, None)


# --------------------------------------------------------------------------- market_stats (mark/last)
@pytest.mark.parametrize('price', ['mark', 'last'])
def test_market_stats_ticker(price):
    (update,) = feed(make(price), 'ws_market_stats.json', wall_ms=WALL_MS + 3)
    assert update.ticker == Ticker(mark=D('0.13593'), last=D('0.13592'), received=NOW, event_ms=WALL_MS + 3)


def test_market_stats_ignored_for_mid_and_validated_for_mark():
    assert feed(make(), 'ws_market_stats.json') == []
    frame = fixture_json(V, 'ws_market_stats.json')
    frame['market_stats']['market_id'] = 7
    frame['channel'] = 'market_stats:4100'
    (update,) = make('mark').on_text(json.dumps(frame), NOW, WALL_MS)
    assert update.error == 'UNEXPECTED_LIGHTER_STATS_SCHEMA'
    frame = fixture_json(V, 'ws_market_stats.json')
    frame['market_stats']['mark_price'] = 'oops'
    (update,) = make('mark').on_text(json.dumps(frame), NOW, WALL_MS)
    assert update.error == 'INVALID_WS_TICKER:lighter_perp'


def test_market_stats_all_form_and_empty_strings():
    frame = fixture_json(V, 'ws_market_stats.json')
    stats = dict(frame['market_stats'])
    del stats['market_id']
    stats['last_trade_price'] = ''
    frame['market_stats'] = {'4100': stats}
    (update,) = make('mark').on_text(json.dumps(frame), NOW, WALL_MS)
    assert update.ticker.mark == D('0.13593') and update.ticker.last is None


# --------------------------------------------------------------------------- market index / instrument
def test_resolve_market_index_by_exact_symbol():
    http = FakeHttp({DETAILS: reply(V, 'rest_order_book_details.json')})
    adapter = make(market_index=None)
    assert adapter.resolve_market_index(http, 4.0) == 4100
    assert adapter.market_index == 4100
    call = http.call(DETAILS)
    assert call['url'] == 'https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails'
    assert call['params'] == {'filter': 'perp'} and call['timeout'] == 4.0
    assert json.loads(adapter.subscribe_frames()[0])['channel'] == 'order_book/4100'


def test_resolve_market_index_failures():
    http = FakeHttp({DETAILS: reply(V, 'rest_order_book_details.json')})
    with pytest.raises(Unsafe, match='^LIGHTER_MARKET_INDEX_MISMATCH:7$'):
        make(market_index=7).resolve_market_index(http, 4.0)
    with pytest.raises(Unsafe, match='^LIGHTER_REFERENCE_NOT_LISTED:PEPE$'):
        make(symbol='PEPE', market_index=None).resolve_market_index(http, 4.0)
    http = FakeHttp({DETAILS: FakeResponse(503, '')})
    with pytest.raises(TransientUnsafe, match='^REFERENCE_LOOKUP_FAILED:HTTP_503$'):
        make(market_index=None).resolve_market_index(http, 4.0)
    http = FakeHttp({DETAILS: reply(V, 'rest_order_book_details_inactive.json')})
    with pytest.raises(Unsafe, match='^LIGHTER_REFERENCE_NOT_ACTIVE_EXACT_PERP:STONK$'):
        make(market_index=None).resolve_market_index(http, 4.0)


@pytest.mark.parametrize('name', ['rest_order_book_details.json', 'rest_order_book_details_minimal.json'])
def test_instrument_info_ok(name):
    info = make().instrument_info(FakeHttp({DETAILS: reply(V, name)}), 4.0)
    assert info['ok'] is True and info['reason'] is None
    assert (info['base'], info['quote'], info['status'], info['tick']) == ('STONK', 'USDC', 'active', '0.00001')
    assert info['market_index'] == 4100
    json.dumps(info)


def test_instrument_spot_market_and_lookalike_are_not_matched():
    body = fixture_json(V, 'rest_order_book_details.json')
    stonk = next(row for row in body['order_book_details'] if row['symbol'] == 'STONK')
    body['order_book_details'] = [{**stonk, 'symbol': 'STONKBROKER', 'market_id': 4200},
                                  {**stonk, 'symbol': 'STONK', 'market_type': 'spot'}]
    info = make().instrument_info(FakeHttp({DETAILS: FakeResponse(200, json.dumps(body))}), 4.0)
    assert info['ok'] is False and info['reason'] == 'LIGHTER_REFERENCE_NOT_LISTED:STONK'
    assert info['market_index'] is None


@pytest.mark.parametrize('name, adapter_kwargs, problem', [
    ('rest_order_book_details_inactive.json', {}, 'STATUS:inactive'),
    ('rest_order_book_details_multiplier.json', {}, 'MULTIPLIER:1000.000000000000000000'),
    ('rest_order_book_details_scaled.json', {'symbol': '1000STONK'}, 'SCALED_CONTRACT:1000STONK'),
])
def test_instrument_not_an_active_exact_perp(name, adapter_kwargs, problem):
    adapter = make(**adapter_kwargs)
    info = adapter.instrument_info(FakeHttp({DETAILS: reply(V, name)}), 4.0)
    assert info['ok'] is False and problem in info['problems']
    assert info['reason'] == f'LIGHTER_REFERENCE_NOT_ACTIVE_EXACT_PERP:{adapter.symbol}'


def test_instrument_error_body_is_transient_lookup_failure():
    info = make().instrument_info(FakeHttp({DETAILS: reply(V, 'rest_error_400.json')}), 4.0)
    assert info['reason'] == 'REFERENCE_LOOKUP_FAILED:INVALID_LIGHTER_DETAILS:20001' and info['transient'] is True
    info = make().instrument_info(FakeHttp({DETAILS: reply(V, 'rest_error_400.json', status=400)}), 4.0)
    assert info['reason'] == 'REFERENCE_HTTP_ERROR'


# --------------------------------------------------------------------------- REST snapshot
def test_rest_snapshot_aggregates_orders_by_price():
    http = FakeHttp({ORDERS: reply(V, 'rest_order_book_orders.json')})
    (update,) = make().rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert update.kind == 'book' and update.source == 'rest'
    book = update.book
    assert book.asks == ((D('0.13605'), D('1500.0')), (D('0.13610'), D('2500.0')))
    assert book.bids == ((D('0.13590'), D('12000.0')), (D('0.13580'), D('8000.0')))
    assert book.event_ms == WALL_MS and book.update_id is None             # no venue timestamp
    assert http.call(ORDERS)['params'] == {'market_id': 4100, 'limit': 100}
    book.check(NOW, WALL_MS, 3.0, D('1'))


def test_rest_snapshot_drops_possibly_incomplete_deepest_level_when_truncated():
    http = FakeHttp({ORDERS: reply(V, 'rest_order_book_orders_truncated.json')})
    (update,) = make().rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert update.book.asks == ((D('0.13605'), D('1000.0')),)
    assert len(update.book.bids) == 2


def test_rest_snapshot_with_mark_from_details():
    http = FakeHttp({ORDERS: reply(V, 'rest_order_book_orders.json'),
                     DETAILS: reply(V, 'rest_order_book_details.json')})
    _, ticker = make('mark').rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert ticker.ticker == Ticker(mark=D('0.13593'), last=D('0.13592'), received=NOW, event_ms=WALL_MS)
    assert http.call(DETAILS)['params'] == {'market_id': 4100, 'filter': 'perp'}


def test_rest_last_trade_price_float_from_json_only_response():
    body = fixture_json(V, 'rest_order_book_details_minimal.json')
    http = FakeHttp({ORDERS: reply(V, 'rest_order_book_orders.json'), DETAILS: JsonOnlyResponse(200, body)})
    _, ticker = make('last').rest_snapshot(http, 4.0, NOW, WALL_MS)
    assert ticker.ticker.last == D('0.13592') and ticker.ticker.mark is None


def test_rest_snapshot_failures():
    with pytest.raises(Unsafe, match='^LIGHTER_MARKET_INDEX_UNKNOWN$'):
        make(market_index=None).rest_snapshot(FakeHttp({}), 4.0, NOW, WALL_MS)
    http = FakeHttp({ORDERS: FakeResponse(200, '{"code":23000,"message":"Too Many Requests"}')})
    with pytest.raises(TransientUnsafe, match='^LIGHTER_REST_RATE_LIMITED$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)
    http = FakeHttp({ORDERS: FakeResponse(200, '{"code":200,"asks":[],"bids":[]}')})
    with pytest.raises(Unsafe, match='^EMPTY_BOOK_SIDE$'):
        make().rest_snapshot(http, 4.0, NOW, WALL_MS)


def test_rest_poll_interval_respects_anonymous_rate_limit():
    assert make().rest_min_interval == 2.0
    assert make('mark').rest_min_interval == 4.0


def test_prepare_resolves_market_index_before_subscribing():
    adapter = make(market_index=None)
    calls = []

    def fake_resolve(http, timeout=None):
        calls.append(http)
        adapter._market_index = 4100
        return 4100

    adapter.resolve_market_index = fake_resolve
    adapter.prepare(http='fake-http')
    assert calls == ['fake-http'] and adapter.market_index == 4100
    adapter.prepare(http='other')            # already resolved: no second lookup
    assert calls == ['fake-http']
    assert 'order_book/4100' in adapter.subscribe_frames()[0]
