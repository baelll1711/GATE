"""GateSpotAdapter: spot.order_book snapshots, spot.trades tape, envelopes, REST snapshot (synthetic fixtures)."""
from __future__ import annotations

import json
import logging
from decimal import Decimal

import pytest

from gate_stonk.config import FxSettings, IndexSettings, SourceConfig
from gate_stonk.errors import ConfigError, Unsafe
from gate_stonk.feeds.base import GATE_SPOT_KEY, Update
from gate_stonk.feeds.gate_spot import GateSpotAdapter, levels, ms_from_decimal, strict_int
from gate_stonk.feeds.hub import FeedHub

NOW = 12345.5            # monotonic receipt time passed by the worker
WALL_MS = 1_800_000_000_000


def load(fixtures_dir, name: str) -> dict:
    data = json.loads((fixtures_dir / 'gate_spot' / name).read_text())
    assert 'SYNTHETIC' in data.pop('_comment')
    return data


def frame_text(fixtures_dir, name: str, mutate=None) -> str:
    frame = load(fixtures_dir, name)['frame']
    if mutate is not None:
        mutate(frame)
    return json.dumps(frame)


class Cfg:
    def __init__(self, **market) -> None:
        self.app = {'request_timeout_seconds': 5}
        self.market = {'currency_pair': 'STONK_USDT', 'rest_url': 'https://api.gateio.invalid/api/v4/',
                       'spot_ws_url': 'wss://api.gateio.invalid/ws/v4/', 'depth_levels': 20,
                       'depth_interval': '100ms', **market}
        fx = FxSettings(mode='fixed', usdc_to_usdt=Decimal(1), max_depeg_pct=Decimal('0.5'), max_age_seconds=3.0,
                        max_spread_pct=Decimal('0.2'))
        sources = (SourceConfig(id='bitget_perp', venue='bitget_usdt_futures', symbol='STONKUSDT', quote='USDT',
                                price='mid', weight=Decimal(1), required=False, max_spread_pct=Decimal(1),
                                max_age_seconds=3.0),)
        self._settings = IndexSettings(method='weighted_median', min_sources=1, max_age_seconds=3.0,
                                       max_timestamp_skew_seconds=2.0, max_source_deviation_pct=Decimal(1),
                                       basis_offset_pct=Decimal(0), rejoin_snapshots=3,
                                       observation_interval_seconds=0.2, fx=fx, sources=sources)

    def index_settings(self):
        return self._settings


class Response:
    def __init__(self, status: int, payload=None, bad_json: bool = False) -> None:
        self.status_code = status
        self.payload = payload
        self.bad_json = bad_json

    def json(self):
        if self.bad_json:
            raise ValueError('not json')
        return self.payload


class Http:
    def __init__(self, response=None, error: BaseException | None = None) -> None:
        self.response = response
        self.error = error
        self.calls: list[tuple] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        if self.error is not None:
            raise self.error
        return self.response


@pytest.fixture
def adapter() -> GateSpotAdapter:
    return GateSpotAdapter(Cfg())


# ---------------------------------------------------------------------------- protocol frames
def test_adapter_contract_and_subscriptions(adapter, clock):
    assert adapter.key == GATE_SPOT_KEY and adapter.venue == 'gate_spot'
    assert 10 <= adapter.ping_interval <= 15 and adapter.idle_timeout == 60.0
    assert adapter.ws_url() == 'wss://api.gateio.invalid/ws/v4/'
    frames = [json.loads(f) for f in adapter.subscribe_frames()]
    assert frames == [
        {'time': 1_800_000_000, 'id': 1, 'channel': 'spot.order_book', 'event': 'subscribe',
         'payload': ['STONK_USDT', '20', '100ms']},
        {'time': 1_800_000_000, 'id': 2, 'channel': 'spot.trades', 'event': 'subscribe', 'payload': ['STONK_USDT']},
    ]
    assert json.loads(adapter.ping_frame(1_800_000_001_999)) == {'time': 1_800_000_001, 'channel': 'spot.ping'}
    assert adapter.decode(b'{"a":1}') == '{"a":1}' and adapter.decode('x') == 'x'
    assert adapter.on_reset() is None


def test_defaults_and_config_validation():
    plain = GateSpotAdapter(Cfg(spot_ws_url=None, rest_url=None))
    assert plain.ws_url() == 'wss://api.gateio.ws/ws/v4/' and plain.rest_url == 'https://api.gateio.ws/api/v4'
    assert plain.rest_refresh_after_seconds == 1.5
    assert GateSpotAdapter(Cfg(rest_refresh_after_seconds=None)).rest_refresh_after_seconds is None
    assert GateSpotAdapter(Cfg(rest_refresh_after_seconds=2)).rest_refresh_after_seconds == 2.0
    for bad in ({'depth_levels': 7}, {'depth_levels': True}, {'depth_levels': '20'}, {'depth_interval': '20ms'},
                {'currency_pair': ''}):
        with pytest.raises(ConfigError):
            GateSpotAdapter(Cfg(**bad))


# ---------------------------------------------------------------------------- order book
def test_order_book_snapshot_is_parsed_exactly(adapter, fixtures_dir):
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json'), NOW, WALL_MS)
    assert update.kind == 'book' and update.key == GATE_SPOT_KEY and update.source == 'ws'
    book = update.book
    assert book.bids == ((Decimal('0.13580'), Decimal('1520.5')), (Decimal('0.13570'), Decimal('800')),
                         (Decimal('0.13560'), Decimal('50')))
    assert book.asks[0] == (Decimal('0.13600'), Decimal('300')) and len(book.asks) == 3
    assert (book.received, book.event_ms, book.update_id) == (NOW, 1_799_999_999_900, 48_791_820)
    assert isinstance(book.bids, tuple) and all(isinstance(level, tuple) for level in book.bids)


def test_all_event_is_also_a_full_snapshot(adapter, fixtures_dir):
    def to_all(frame):
        frame['event'] = 'all'
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', to_all), NOW, WALL_MS)
    assert update.kind == 'book'


def _set(path, value):
    def mutate(frame):
        target = frame['result']
        for part in path[:-1]:
            target = target[part]
        if value is KeyError:
            del target[path[-1]]
        else:
            target[path[-1]] = value
    return mutate


@pytest.mark.parametrize('mutate, code', [
    (_set(['bids'], []), 'EMPTY_BOOK_SIDE'),
    (_set(['asks'], None), 'EMPTY_BOOK_SIDE'),
    (_set(['bids', 0], ['0.1358', '1', '2']), 'INVALID_BOOK_SCHEMA'),
    (_set(['bids', 0], {'p': '0.1358', 's': '1'}), 'INVALID_BOOK_SCHEMA'),
    (_set(['bids', 0, 0], 'NaN'), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['asks', 0, 1], 'abc'), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['asks', 0, 1], True), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['t'], KeyError), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['t'], -5), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['t'], True), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['t'], '1799999999900.5'), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['lastUpdateId'], KeyError), 'INVALID_WS_BOOK:gate_spot'),
    (_set(['s'], 'BTC_USDT'), 'GATE_SPOT_SYMBOL_MISMATCH'),
    (_set(['s'], KeyError), 'GATE_SPOT_SYMBOL_MISMATCH'),
])
def test_malformed_snapshot_becomes_an_error_update(adapter, fixtures_dir, mutate, code):
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', mutate), NOW, WALL_MS)
    assert (update.kind, update.key, update.error) == ('error', GATE_SPOT_KEY, code)


def test_result_that_is_not_an_object_is_a_schema_error(adapter, fixtures_dir):
    def mutate(frame):
        frame['result'] = [1, 2]
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', mutate), NOW, WALL_MS)
    assert update.error == 'INVALID_BOOK_SCHEMA'


def test_new_snapshot_replaces_the_whole_book_in_the_hub(adapter, fixtures_dir, clock):
    hub = FeedHub(Cfg(), adapters=[adapter], start_threads=False)
    now, wall = clock[0], int(clock[0] * 1000)

    def first(frame):
        frame['result']['t'] = wall
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', first), now, wall)
    hub.publish(update)

    def second(frame):     # fewer levels, size change without a best-price change
        frame['result'].update(t=wall + 100, lastUpdateId=48791821, bids=[['0.13580', '1000']],
                               asks=[['0.13600', '300'], ['0.13615', '9']])
    clock[0] += 0.1
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', second), clock[0], wall + 100)
    hub.publish(update)
    book = hub.spot_book()
    assert book.bids == ((Decimal('0.13580'), Decimal('1000')),)
    assert book.asks == ((Decimal('0.13600'), Decimal('300')), (Decimal('0.13615'), Decimal('9')))
    assert hub.spot_book_error() is None


class ServerTimeRest:
    """Fake HTTP for ``GET /spot/order_book``: each answer is stamped with the (fake)
    server time of the request (``current``) and a new snapshot id."""

    def __init__(self, clock, payload: dict) -> None:
        self.clock = clock
        self.payload = payload
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return Response(200, dict(self.payload, current=int(self.clock[0] * 1000), id=self.payload['id'] + self.calls))


def test_quiet_book_repushed_with_frozen_t_stays_usable_with_the_rest_refresher(adapter, fixtures_dir, clock):
    """Illiquid pair: Gate may keep re-pushing the unchanged spot.order_book every 100 ms with
    the book's last-change ``t`` (API_NOTES §2.2, not confirmed). The REST refresher (ticking
    every 250 ms) keeps the traded book usable, and no stale push deletes the REST snapshot."""
    http = ServerTimeRest(clock, load(fixtures_dir, 'rest_order_book.json')['response'])
    hub = FeedHub(Cfg(), adapters=[adapter], start_threads=False, http=http)
    frozen_t = int(clock[0] * 1000)

    def quiet(frame):
        frame['result']['t'] = frozen_t
    start, step = clock[0], 0
    while clock[0] - start < 10.0:
        if step % 2 == 0:                       # a 100 ms push of the same snapshot
            (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', quiet), clock[0],
                                        int(clock[0] * 1000))
            hub.publish(update)                 # never rejected (raises Unsafe when it is)
        if step % 5 == 0:                       # refresher tick
            hub.refresh_once()
        assert hub.spot_book_error() is None, (round(clock[0] - start, 2), hub.spot_book_error())
        clock[0] += 0.05
        step += 1
    counters = hub.diagnostics()[GATE_SPOT_KEY]['counters']
    assert counters['rejected'] == 0 and counters['dropped_out_of_order'] >= 75   # pushes after the 1st poll
    assert 5 <= http.calls <= 7                 # ~every 1.75 s from 1.75 s on, never faster than 1 s
    assert hub.diagnostics()[GATE_SPOT_KEY]['source'] == 'rest'


def test_zero_size_and_crossed_snapshots_are_rejected_by_the_hub(adapter, fixtures_dir, clock):
    hub = FeedHub(Cfg(), adapters=[adapter], start_threads=False)
    wall = int(clock[0] * 1000)
    for mutate, code in ((_set(['bids', 0, 1], '0'), 'INVALID_BOOK_LEVEL'),
                         (_set(['bids'], [['0.13600', '1']]), 'CROSSED_BOOK')):
        def with_time(frame, mutate=mutate):
            mutate(frame)
            frame['result']['t'] = wall
        (update,) = adapter.on_text(frame_text(fixtures_dir, 'order_book_update.json', with_time), clock[0], wall)
        with pytest.raises(Unsafe, match=f'^{code}$'):
            hub.publish(update)
        assert hub.spot_book() is None and hub.spot_book_error() == code


# ---------------------------------------------------------------------------- trades
def test_trades_array_is_normalized(adapter, fixtures_dir):
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'trades_update_array.json'), NOW, WALL_MS)
    assert update.kind == 'trades' and update.key == GATE_SPOT_KEY
    assert update.trades == (
        {'id': '3130257995', 'price': Decimal('0.13580'), 'amount': Decimal('116.5'), 'side': 'sell',
         'create_time_ms': 1_799_999_999_923},
        {'id': '3130257996', 'price': Decimal('0.13600'), 'amount': Decimal('20'), 'side': 'buy',
         'create_time_ms': 1_800_000_000_010},
    )


def test_trades_object_form_and_create_time_fallback(adapter, fixtures_dir):
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'trades_update_object.json'), NOW, WALL_MS)
    assert [t['create_time_ms'] for t in update.trades] == [1_800_000_000_100]

    def no_ms(frame):
        del frame['result']['create_time_ms']
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'trades_update_object.json', no_ms), NOW, WALL_MS)
    assert update.trades[0]['create_time_ms'] == 1_800_000_000_000


def test_malformed_and_foreign_trade_rows_are_skipped(adapter, fixtures_dir, caplog):
    caplog.set_level(logging.WARNING)

    def mutate(frame):
        good = frame['result'][1]
        frame['result'] = [
            dict(good, id='a1', side='ask'), dict(good, id='a2', price='0'), dict(good, id='a3', amount='-1'),
            dict(good, id=True), dict(good, id='a5', create_time_ms='soon'), 'junk',
            dict(good, id='a7', currency_pair='BTC_USDT'), dict(good, id='ok'),
        ]
    (update,) = adapter.on_text(frame_text(fixtures_dir, 'trades_update_array.json', mutate), NOW, WALL_MS)
    assert [t['id'] for t in update.trades] == ['ok']
    assert 'Ignoring 6 malformed spot trade rows' in caplog.text

    def all_bad(frame):
        frame['result'] = [{'id': 1}]
    assert adapter.on_text(frame_text(fixtures_dir, 'trades_update_array.json', all_bad), NOW, WALL_MS) == []

    def not_rows(frame):
        frame['result'] = 'nope'
    assert adapter.on_text(frame_text(fixtures_dir, 'trades_update_array.json', not_rows), NOW, WALL_MS) == []


def test_trades_reach_the_hub_tape_with_dedup(adapter, fixtures_dir, clock):
    hub = FeedHub(Cfg(), adapters=[adapter], start_threads=False)
    start = hub.last_trade_seq()
    for _ in range(2):      # a resubscription repeats recent trades
        for update in adapter.on_text(frame_text(fixtures_dir, 'trades_update_array.json'), clock[0], WALL_MS):
            hub.publish(update)
    tape = hub.trades_since(start)
    assert [(t.trade_id, t.side, t.seq) for t in tape] == [('3130257995', 'sell', start + 1),
                                                         ('3130257996', 'buy', start + 2)]


# ---------------------------------------------------------------------------- envelopes
def test_heartbeat_ack_and_unknown_frames(adapter, fixtures_dir):
    assert adapter.on_text(frame_text(fixtures_dir, 'pong.json'), NOW, WALL_MS) == []
    assert adapter.on_text(frame_text(fixtures_dir, 'subscribe_ack.json'), NOW, WALL_MS) == [
        Update('subscribed', GATE_SPOT_KEY)]
    assert adapter.on_text(json.dumps({'channel': 'spot.tickers', 'event': 'update', 'result': {}}), NOW, WALL_MS) == []
    assert adapter.on_text(json.dumps({'channel': 'spot.trades', 'event': 'unsubscribe'}), NOW, WALL_MS) == []
    assert adapter.on_text(json.dumps({'channel': 'spot.order_book', 'event': 'subscribe', 'result': None}),
                           NOW, WALL_MS) == []
    # Gate data frames carry no "code" field; a generic '"code" in msg' rule is never applied.
    assert adapter.on_text(json.dumps({'channel': 'spot.trades', 'event': 'update', 'code': 0, 'result': []}),
                           NOW, WALL_MS) == []


def test_subscription_errors_are_socket_fatal(adapter, fixtures_dir):
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        adapter.on_text(frame_text(fixtures_dir, 'subscribe_error.json'), NOW, WALL_MS)

    def fail_without_error(frame):
        del frame['error']
    with pytest.raises(Unsafe, match='^SUBSCRIPTION_REJECTED$'):
        adapter.on_text(frame_text(fixtures_dir, 'subscribe_error.json', fail_without_error), NOW, WALL_MS)
    with pytest.raises(Unsafe, match='^GATE_WS_ERROR$'):
        adapter.on_text(json.dumps({'channel': 'spot.other', 'event': 'update', 'error': {'code': 3}}), NOW, WALL_MS)


@pytest.mark.parametrize('text', ['not json', '[1, 2]', '"pong"', ''])
def test_non_object_frames_reconnect(adapter, text):
    with pytest.raises(Unsafe, match='^INVALID_WS_MESSAGE$'):
        adapter.on_text(text, NOW, WALL_MS)


# ---------------------------------------------------------------------------- REST
def test_rest_snapshot_uses_current_and_id(adapter, fixtures_dir):
    http = Http(Response(200, load(fixtures_dir, 'rest_order_book.json')['response']))
    (update,) = adapter.rest_snapshot(http, 5.0, NOW, WALL_MS)
    assert http.calls == [('https://api.gateio.invalid/api/v4/spot/order_book',
                           {'currency_pair': 'STONK_USDT', 'limit': 20, 'with_id': 'true'}, 5.0)]
    assert (update.kind, update.source) == ('book', 'rest')
    assert (update.book.event_ms, update.book.update_id, update.book.received) == (1_800_000_000_050, 48_791_830, NOW)
    assert update.book.asks[0] == (Decimal('0.13600'), Decimal('310'))


@pytest.mark.parametrize('response, code', [
    (Response(500, {}), 'REST_HTTP_500'),
    (Response(429, {}), 'REST_HTTP_429'),
    (Response(200, bad_json=True), 'INVALID_REST_RESPONSE'),
    (Response(200, []), 'INVALID_REST_BOOK'),
    (Response(200, {'current': 1, 'bids': [['1', '1']], 'asks': [['2', '1']]}), 'INVALID_REST_BOOK'),
    (Response(200, {'id': 1, 'current': 1, 'bids': [], 'asks': [['2', '1']]}), 'EMPTY_BOOK_SIDE'),
])
def test_rest_snapshot_failures_are_reason_codes(adapter, response, code):
    with pytest.raises(Unsafe, match=f'^{code}$'):
        adapter.rest_snapshot(Http(response), 5.0, NOW, WALL_MS)


def test_instrument_info(adapter, fixtures_dir):
    payload = load(fixtures_dir, 'currency_pair.json')['response']
    info = adapter.instrument_info(Http(Response(200, payload)), 5.0)
    assert info['ok'] and info['symbol'] == 'STONK_USDT' and info['status'] == 'tradable'
    assert (info['base'], info['quote'], info['tick'], info['reason']) == ('STONK', 'USDT', '0.00001', None)
    assert 'amount_precision' in info['raw_keys']
    halted = adapter.instrument_info(Http(Response(200, dict(payload, trade_status='sellable'))), 5.0)
    assert not halted['ok'] and halted['reason'] == 'GATE_SPOT_NOT_TRADABLE:sellable'
    other = adapter.instrument_info(Http(Response(200, dict(payload, id='STONK_USDC'))), 5.0)
    assert other['reason'] == 'GATE_SPOT_PAIR_MISMATCH'
    assert adapter.instrument_info(Http(Response(503, {})), 5.0)['reason'] == 'REFERENCE_LOOKUP_FAILED:HTTP_503'
    failed = adapter.instrument_info(Http(error=TimeoutError('https://secret')), 5.0)
    assert failed['reason'] == 'REFERENCE_LOOKUP_FAILED:TimeoutError' and not failed['ok']


# ---------------------------------------------------------------------------- helpers
def test_parsing_helpers():
    assert levels([['1.5', '2'], [3, 4]]) == ((Decimal('1.5'), Decimal(2)), (Decimal(3), Decimal(4)))
    with pytest.raises(Unsafe, match='EMPTY_BOOK_SIDE'):
        levels([])
    assert strict_int('17') == 17 and strict_int(17) == 17
    for bad in (True, -1, '1.5', 'x', None):
        with pytest.raises(ValueError):
            strict_int(bad)
    assert ms_from_decimal('1645464610777.399200') == 1_645_464_610_777
    assert ms_from_decimal('1648725035923.0') == 1_648_725_035_923
    for bad in (True, '-1', 'NaN', 'Infinity'):
        with pytest.raises(ValueError):
            ms_from_decimal(bad)
