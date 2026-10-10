"""FeedHub ingestion, regressions, trade tape, diagnostics, REST refresher, QuoteHealth, build_hub."""
from __future__ import annotations

import copy
import dataclasses
import json
import logging
import sys
import threading
import time
import types
from decimal import Decimal

import pytest
import websocket

from gate_stonk import feeds
from gate_stonk.config import FxSettings, IndexSettings, SourceConfig
from gate_stonk.errors import ConfigError, Unsafe
from gate_stonk.feeds.base import FX_KEY, GATE_SPOT_KEY, Update
from gate_stonk.feeds.fx import BinanceFxAdapter, GateSpotFxAdapter
from gate_stonk.feeds.gate_spot import GateSpotAdapter
from gate_stonk.feeds.health import QuoteHealth
from gate_stonk.feeds.hub import NO_DATA, TRADE_HISTORY, FeedHub
from gate_stonk.types import Book, PublicTrade, Ticker


# ---------------------------------------------------------------------------- helpers
def src(sid: str, venue: str = 'bitget_usdt_futures', *, quote: str = 'USDT', price: str = 'mid',
        max_spread: str | None = '1', heartbeat: float = 0.0, rest: float | None = None,
        max_age: float = 3.0) -> SourceConfig:
    return SourceConfig(id=sid, venue=venue, symbol='STONKUSDT', quote=quote, price=price, weight=Decimal(1),
                        required=False, max_spread_pct=None if max_spread is None else Decimal(max_spread),
                        max_age_seconds=max_age, heartbeat_seconds=heartbeat, rest_refresh_after_seconds=rest)


class FakeConfig:
    def __init__(self, sources=None, *, fx_mode: str = 'fixed', fx_rate: str = '1', market: dict | None = None) -> None:
        if sources is None:
            sources = [src('bybit_perp', 'bybit_linear', heartbeat=3.0), src('bitget_perp', rest=1.5),
                       src('gate_perp', 'gate_futures_usdt', price='mark', max_spread=None)]
        self.app = {'request_timeout_seconds': 5}
        self.market = {'currency_pair': 'STONK_USDT', 'rest_url': 'https://api.gateio.invalid/api/v4',
                       'spot_ws_url': 'wss://api.gateio.invalid/ws/v4/', 'depth_levels': 20,
                       'depth_interval': '100ms', **(market or {})}
        fx = FxSettings(mode=fx_mode, usdc_to_usdt=Decimal(fx_rate), max_depeg_pct=Decimal('0.5'),
                        max_age_seconds=3.0, max_spread_pct=Decimal('0.2'))
        self._settings = IndexSettings(method='weighted_median', min_sources=1, max_age_seconds=3.0,
                                       max_timestamp_skew_seconds=2.0, max_source_deviation_pct=Decimal(1),
                                       basis_offset_pct=Decimal(0), rejoin_snapshots=3,
                                       observation_interval_seconds=0.2, fx=fx, sources=tuple(sources))

    def index_settings(self) -> IndexSettings:
        return self._settings

    def sources(self):
        return self._settings.sources


def lv(*pairs):
    return tuple((Decimal(p), Decimal(q)) for p, q in pairs)


def book(now: float, *, age_ms: int = 0, update_id: int | None = None, bids=(('100', '1'),),
         asks=(('101', '1'),), received: float | None = None) -> Book:
    return Book(lv(*bids), lv(*asks), now if received is None else received, int(now * 1000) - age_ms, update_id)


def ticker(now: float, *, mark: str | None = '100', last: str | None = '100.5', age_ms: int = 0) -> Ticker:
    return Ticker(None if mark is None else Decimal(mark), None if last is None else Decimal(last), now,
                  int(now * 1000) - age_ms)


def put(hub: FeedHub, key: str, item, source: str = 'ws') -> None:
    kind = 'book' if isinstance(item, Book) else 'ticker'
    hub.publish(Update(kind, key, **{kind: item}, source=source))


def state(hub: FeedHub) -> tuple:
    return (dict(hub._books), dict(hub._tickers), dict(hub._errors), dict(hub._updates), dict(hub._origin),
            set(hub._connected), copy.deepcopy(hub._counters), list(hub._tape), hub._tape_seq)


@pytest.fixture
def hub(clock):
    return FeedHub(FakeConfig(), start_threads=False)


# ---------------------------------------------------------------------------- construction
def test_keys_cover_sources_spot_and_live_fx_only_when_needed(clock):
    assert set(FeedHub(FakeConfig(), start_threads=False).specs) == {'bybit_perp', 'bitget_perp', 'gate_perp',
                                                                     GATE_SPOT_KEY}
    usdc = [src('lighter_perp', 'lighter', quote='USDC')]
    assert FX_KEY in FeedHub(FakeConfig(usdc, fx_mode='binance_spot'), start_threads=False).specs
    assert FX_KEY not in FeedHub(FakeConfig(usdc, fx_mode='fixed'), start_threads=False).specs
    assert FX_KEY not in FeedHub(FakeConfig(fx_mode='binance_spot'), start_threads=False).specs


def test_constructor_rejects_bad_adapters_and_reserved_ids(clock):
    cfg = FakeConfig()
    with pytest.raises(ValueError, match='not a configured feed key'):
        FeedHub(cfg, adapters=[types.SimpleNamespace(key='nope')], start_threads=False)
    with pytest.raises(ValueError, match='duplicate adapter'):
        FeedHub(cfg, adapters=[types.SimpleNamespace(key='bitget_perp'), types.SimpleNamespace(key='bitget_perp')],
                start_threads=False)
    with pytest.raises(ConfigError):
        FeedHub(FakeConfig([src('fx')]), start_threads=False)


def test_initial_snapshots_report_missing_data(hub):
    snap = hub.source_snapshot('bitget_perp')
    assert (snap.book, snap.ticker, snap.error, snap.connected) == (None, None, NO_DATA, False)
    assert hub.source_snapshot('nope').error == 'UNKNOWN_SOURCE'
    assert hub.spot_book() is None and hub.spot_book_error() == NO_DATA
    assert hub.fx_snapshot() is None          # no USDC-quoted source: FX irrelevant
    assert hub.last_trade_seq() > 0 and hub.trades_since(0) == []


def test_start_without_threads_starts_nothing(hub):
    before = threading.active_count()
    hub.start()
    hub.start()
    assert threading.active_count() == before and hub._workers == [] and hub._refresher is None
    hub.stop()


# ---------------------------------------------------------------------------- ingestion
def test_publish_stores_immutable_snapshot_and_sets_wake(hub, clock):
    hub.wake.clear()
    snapshot = book(clock[0], update_id=7)
    put(hub, 'bitget_perp', snapshot)
    assert hub.wake.is_set()
    got = hub.source_snapshot('bitget_perp')
    assert got.book is snapshot and got.error is None and got.ticker is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        got.book.bids = ()


def test_mutable_levels_are_refused(hub, clock):
    mutable = Book([(Decimal(100), Decimal(1))], ((Decimal(101), Decimal(1)),), clock[0], int(clock[0] * 1000))
    with pytest.raises(Unsafe, match='^INVALID_BOOK_SCHEMA$'):
        put(hub, 'bitget_perp', mutable)
    assert hub.source_snapshot('bitget_perp').error == 'INVALID_BOOK_SCHEMA'


@pytest.mark.parametrize('bad, code', [
    (dict(bids=(), asks=(('101', '1'),)), 'EMPTY_BOOK'),
    (dict(bids=(('100', '0'),)), 'INVALID_BOOK_LEVEL'),
    (dict(bids=(('100', '1'), ('100.5', '1'))), 'UNSORTED_OR_DUPLICATE_BOOK_LEVEL'),
    (dict(bids=(('100', '1'), ('100', '1'))), 'UNSORTED_OR_DUPLICATE_BOOK_LEVEL'),
    (dict(bids=(('101', '1'),)), 'CROSSED_BOOK'),
    (dict(age_ms=10_000), 'STALE_EVENT_OR_CLOCK_SKEW'),
    (dict(age_ms=-2_100), 'STALE_EVENT_OR_CLOCK_SKEW'),
])
def test_invalid_book_never_reuses_the_previous_snapshot(hub, clock, bad, code):
    put(hub, 'bitget_perp', book(clock[0], update_id=1))
    put(hub, 'bybit_perp', book(clock[0]))
    clock[0] += 0.1
    with pytest.raises(Unsafe, match=f'^{code}$'):
        put(hub, 'bitget_perp', book(clock[0], update_id=2, **bad))
    snap = hub.source_snapshot('bitget_perp')
    assert snap.book is None and snap.error == code
    assert hub.source_snapshot('bybit_perp').book is not None        # other keys untouched
    put(hub, 'bitget_perp', book(clock[0], update_id=1))               # recovery accepts any sequence
    assert hub.source_snapshot('bitget_perp').error is None


def test_ingestion_is_structural_wide_spread_is_an_index_time_check(hub, clock):
    wide = book(clock[0], bids=(('90', '1'),), asks=(('110', '1'),))
    put(hub, 'bitget_perp', wide)                                      # 20% spread accepted at ingestion
    put(hub, GATE_SPOT_KEY, wide)
    diag = hub.diagnostics()
    assert diag['bitget_perp']['error'] == 'WIDE_SOURCE_SPREAD' and not diag['bitget_perp']['ok']
    assert diag[GATE_SPOT_KEY]['ok'] and diag[GATE_SPOT_KEY]['error'] is None


def test_bad_types_are_schema_errors(hub, clock):
    good = book(clock[0])
    for broken in (dataclasses.replace(good, event_ms=True), dataclasses.replace(good, update_id=-1),
                   dataclasses.replace(good, received=float('nan')),
                   Book(((Decimal(100), Decimal(1), Decimal(0)),), good.asks, clock[0], int(clock[0] * 1000))):
        with pytest.raises(Unsafe, match='^INVALID_BOOK_SCHEMA$'):
            put(hub, 'bitget_perp', broken)
    with pytest.raises(Unsafe, match='^INVALID_BOOK_SCHEMA$'):
        hub.publish(Update('book', 'bitget_perp', book=None))


def test_event_regression_invalidates_but_equal_event_is_accepted(hub, clock):
    put(hub, 'bitget_perp', book(clock[0]))
    clock[0] += 0.5
    put(hub, 'bitget_perp', book(clock[0], age_ms=500))               # same event time: allowed
    with pytest.raises(Unsafe, match='^BOOK_EVENT_REGRESSION$'):
        put(hub, 'bitget_perp', book(clock[0], age_ms=501))
    assert hub.source_snapshot('bitget_perp').book is None
    assert hub.source_snapshot('bitget_perp').error == 'BOOK_EVENT_REGRESSION'


def test_repeated_update_id_refreshes_and_lower_id_is_a_regression(hub, clock):
    put(hub, 'bitget_perp', book(clock[0], update_id=10))
    clock[0] += 1
    repeat = book(clock[0], update_id=10)
    put(hub, 'bitget_perp', repeat)
    assert hub.source_snapshot('bitget_perp').book.received == clock[0]
    with pytest.raises(Unsafe, match='^BOOK_UPDATE_REGRESSION$'):
        put(hub, 'bitget_perp', book(clock[0], update_id=9))
    put(hub, 'bitget_perp', book(clock[0], update_id=3))                # after invalidation: any sequence
    assert hub.source_snapshot('bitget_perp').book.update_id == 3


def test_bybit_u1_restart_reset_is_accepted_only_for_bybit(hub, clock):
    put(hub, 'bybit_perp', book(clock[0], update_id=500))
    put(hub, 'bitget_perp', book(clock[0], update_id=500))
    clock[0] += 1
    restarted = book(clock[0], update_id=1, bids=(('99', '2'),))
    put(hub, 'bybit_perp', restarted)
    assert hub.source_snapshot('bybit_perp').book is restarted
    with pytest.raises(Unsafe, match='^BOOK_UPDATE_REGRESSION$'):
        put(hub, 'bitget_perp', book(clock[0], update_id=1))
    with pytest.raises(Unsafe, match='^BOOK_UPDATE_REGRESSION$'):
        put(hub, 'bybit_perp', book(clock[0], update_id=0))


def test_cross_transport_ordering_keeps_the_newer_snapshot(hub, clock):
    put(hub, 'bitget_perp', book(clock[0], update_id=900))
    clock[0] += 0.2
    with_lower_id = book(clock[0], update_id=5)
    put(hub, 'bitget_perp', with_lower_id, source='rest')              # REST ids are a separate sequence
    assert hub.source_snapshot('bitget_perp').book is with_lower_id
    late_ws = book(clock[0], age_ms=150, update_id=901)                # older than the stored REST book
    put(hub, 'bitget_perp', late_ws)
    assert hub.source_snapshot('bitget_perp').book is with_lower_id   # dropped, not invalidated
    assert hub._counters['bitget_perp']['dropped_out_of_order'] == 1
    with pytest.raises(Unsafe, match='^BOOK_UPDATE_REGRESSION$'):
        put(hub, 'bitget_perp', book(clock[0], update_id=4), source='rest')


def test_error_update_invalidates_only_that_key(hub, clock):
    put(hub, 'bitget_perp', book(clock[0]))
    put(hub, 'bybit_perp', book(clock[0]))
    hub.wake.clear()
    hub.publish(Update('error', 'bitget_perp', error='UNEXPECTED_SCHEMA'))
    assert hub.wake.is_set()
    assert hub.source_snapshot('bitget_perp').error == 'UNEXPECTED_SCHEMA'
    assert hub.source_snapshot('bitget_perp').book is None
    assert hub.source_snapshot('bybit_perp').book is not None
    hub.publish(Update('error', 'bybit_perp', error=None))
    assert hub.source_snapshot('bybit_perp').error == 'INVALID_UPDATE'


def test_unknown_keys_kinds_and_sources(hub, clock, caplog):
    caplog.set_level(logging.WARNING)
    hub.publish(Update('book', 'nope', book=book(clock[0])))
    assert 'unknown key' in caplog.text
    hub.publish(Update('subscribed', 'bitget_perp'))
    hub.publish(Update('ignore', 'bitget_perp'))
    with pytest.raises(ValueError):
        hub.publish(Update('weird', 'bitget_perp'))
    with pytest.raises(ValueError):
        hub.publish(Update('book', 'bitget_perp', book=book(clock[0]), source='carrier-pigeon'))
    with pytest.raises(TypeError):
        hub.publish({'kind': 'book'})
    assert hub.source_snapshot('bitget_perp').error == NO_DATA


def test_tickers_validation_and_regression(hub, clock):
    put(hub, 'gate_perp', ticker(clock[0]))
    assert hub.source_snapshot('gate_perp').ticker.mark == Decimal(100)
    with pytest.raises(Unsafe, match='^TICKER_EVENT_REGRESSION$'):
        put(hub, 'gate_perp', ticker(clock[0], age_ms=1))
    assert hub.source_snapshot('gate_perp').ticker is None
    for bad in (ticker(clock[0], mark='0'), ticker(clock[0], mark=None, last=None), ticker(clock[0], mark='NaN')):
        with pytest.raises(Unsafe, match='^INVALID_TICKER$'):
            put(hub, 'gate_perp', bad)
    with pytest.raises(Unsafe, match='^STALE_EVENT_OR_CLOCK_SKEW$'):
        put(hub, 'gate_perp', ticker(clock[0], age_ms=3500))
    put(hub, 'gate_perp', ticker(clock[0], mark=None))
    assert hub.diagnostics()['gate_perp']['error'] == 'MISSING_PRICE:mark'


def test_invalidate_stop_and_restart_states(clock):
    hub = FeedHub(FakeConfig(), start_threads=False)
    hub.start()
    put(hub, 'bitget_perp', book(clock[0]))
    hub.set_connected('bitget_perp', True)
    assert hub.source_snapshot('bitget_perp').connected
    hub.invalidate('bitget_perp', 'DISCONNECTED')
    assert hub.source_snapshot('bitget_perp').error == 'DISCONNECTED'
    hub.invalidate('nope', 'DISCONNECTED')                              # ignored
    hub.stop()
    assert {hub.source_snapshot(k).error for k in ('bitget_perp', 'bybit_perp', 'gate_perp')} == {'STOPPED'}
    assert hub.spot_book_error() == 'STOPPED'
    put(hub, 'bitget_perp', book(clock[0]))                             # late publish after stop: ignored
    hub.invalidate('bitget_perp', 'CONNECTING')                         # a late worker cannot hide STOPPED
    hub.set_connected('bitget_perp', True)
    snap = hub.source_snapshot('bitget_perp')
    assert snap.book is None and snap.error == 'STOPPED' and not snap.connected
    hub.start()
    put(hub, 'bitget_perp', book(clock[0]))
    assert hub.source_snapshot('bitget_perp').error is None


def test_spot_book_error_reports_freshness(hub, clock):
    put(hub, GATE_SPOT_KEY, book(clock[0]))
    assert hub.spot_book() is not None and hub.spot_book_error() is None
    clock[0] += 3.1
    assert hub.spot_book() is not None and hub.spot_book_error() == 'STALE_QUOTE'


# ---------------------------------------------------------------------------- trade tape
def trade(tid, price='0.1358', amount='10', side='buy', ms=1_800_000_000_000):
    return {'id': tid, 'price': Decimal(price), 'amount': Decimal(amount), 'side': side, 'create_time_ms': ms}


def test_trade_tape_dedup_seq_and_trades_since(hub, clock):
    start = hub.last_trade_seq()
    hub.publish(Update('trades', GATE_SPOT_KEY, trades=(trade(1), trade(2, side='sell'))))
    boundary = hub.last_trade_seq()
    assert boundary == start + 2
    hub.publish(Update('trades', GATE_SPOT_KEY, trades=(trade(2), trade('3', ms='1800000000001.7'))))
    tape = hub.trades_since(start)
    assert [t.trade_id for t in tape] == ['1', '2', '3']
    assert [t.seq for t in tape] == [start + 1, start + 2, start + 3]
    assert all(isinstance(t, PublicTrade) for t in tape)
    assert tape[2].create_time_ms == 1_800_000_000_001 and tape[1].side == 'sell'
    assert [t.trade_id for t in hub.trades_since(boundary)] == ['3']   # placed at boundary: only later trades
    assert hub.trades_since(hub.last_trade_seq()) == []


def test_malformed_trade_rows_are_skipped_and_never_invalidate_books(hub, clock):
    put(hub, GATE_SPOT_KEY, book(clock[0]))
    rows = (trade(1, price='0'), trade(2, side='ask'), {'id': True, 'price': '1', 'amount': '1', 'side': 'buy',
                                                        'create_time_ms': 1}, trade(3, ms=-1), 'junk', trade(4))
    hub.publish(Update('trades', GATE_SPOT_KEY, trades=rows))
    assert [t.trade_id for t in hub.trades_since(0)] == ['4']
    assert hub.spot_book() is not None and hub.spot_book_error() is None
    assert hub.diagnostics()[GATE_SPOT_KEY]['trade_tape']['rejected_rows'] == 5
    hub.publish(Update('trades', 'bitget_perp', trades=(trade(5),)))  # only the traded pair has a tape
    assert [t.trade_id for t in hub.trades_since(0)] == ['4']


def test_trade_tape_is_bounded(hub):
    hub.publish(Update('trades', GATE_SPOT_KEY, trades=tuple(trade(i) for i in range(TRADE_HISTORY + 5))))
    assert len(hub._tape) == TRADE_HISTORY and len(hub._tape_id_set) == TRADE_HISTORY
    assert hub.trades_since(0)[0].trade_id == '5'


# ---------------------------------------------------------------------------- diagnostics
def test_diagnostics_shape_ages_and_json_safety(clock):
    hub = FeedHub(FakeConfig([src('lighter_perp', 'lighter', quote='USDC'), src('bybit_perp', 'bybit_linear',
                                                                                  heartbeat=3.0)],
                             fx_mode='binance_spot'), start_threads=False)
    put(hub, FX_KEY, book(clock[0], bids=(('0.9999', '1'),), asks=(('1.0000', '1'),)))
    hub.set_connected(FX_KEY, True)
    clock[0] += 0.75
    put(hub, 'bybit_perp', book(clock[0]))
    hub.invalidate(GATE_SPOT_KEY, 'DISCONNECTED')
    clock[0] += 3.5
    put(hub, 'lighter_perp', book(clock[0]))
    diag = hub.diagnostics()
    fx_row = {k: v for k, v in diag[FX_KEY].items() if k not in ('counters', 'quote_diagnostics')}
    assert fx_row == {'ok': False, 'error': 'STALE_QUOTE', 'received_age_seconds': 4.25,
                      'event_age_seconds': 4.25, 'max_age_seconds': 3.0, 'connected': True, 'source': 'ws'}
    assert diag['bybit_perp']['ok'] and diag['bybit_perp']['max_age_seconds'] == 4.0     # heartbeat allowance
    assert diag['lighter_perp']['ok'] and not diag['lighter_perp']['connected']
    assert diag[GATE_SPOT_KEY]['error'] == 'DISCONNECTED'                # stored reason preserved
    assert diag[GATE_SPOT_KEY]['received_age_seconds'] is None
    assert json.loads(json.dumps(diag, allow_nan=False)) == diag


def test_diagnostics_show_negative_receipt_age_and_quote_faults(hub, clock):
    put(hub, 'bitget_perp', book(clock[0], received=clock[0] + 0.000123))
    with pytest.raises(Unsafe):
        put(hub, 'bybit_perp', book(clock[0], age_ms=10_000))
    diag = hub.diagnostics()
    assert diag['bitget_perp']['received_age_seconds'] == -0.000123
    assert diag['bitget_perp']['error'] == 'STALE_QUOTE'
    faults = diag['bybit_perp']['quote_diagnostics']
    assert faults['error'] == 'STALE_EVENT_OR_CLOCK_SKEW' and faults['event_age_ms'] == 10_000


def test_diagnostics_do_not_mutate_state_or_set_wake(hub, clock):
    put(hub, 'bitget_perp', book(clock[0], update_id=3))
    put(hub, 'gate_perp', ticker(clock[0]))
    hub.publish(Update('trades', GATE_SPOT_KEY, trades=(trade(1),)))
    hub.set_connected('bitget_perp', True)
    hub.invalidate('bybit_perp', 'DISCONNECTED')
    clock[0] += 10
    before = state(hub)
    health = hub.quote_health.snapshot()
    hub.wake.clear()
    first = hub.diagnostics()
    first['bitget_perp']['error'] = 'MUTATED'
    first['gate_perp']['counters']['accepted'] = 99
    second = hub.diagnostics()
    assert state(hub) == before and hub.quote_health.snapshot() == health
    assert not hub.wake.is_set()
    assert second['bitget_perp']['error'] == 'STALE_QUOTE' and second['gate_perp']['counters']['accepted'] == 1


def test_diagnostics_survive_a_broken_health_snapshot(hub, clock, monkeypatch):
    def broken():
        raise RuntimeError('boom')
    monkeypatch.setattr(hub.quote_health, 'snapshot', broken)
    put(hub, 'bitget_perp', book(clock[0]))
    assert hub.diagnostics()['bitget_perp']['ok']


# ---------------------------------------------------------------------------- REST refresher
class RestAdapter:
    venue = 'fake'

    def __init__(self, key: str, *, min_interval: float = 0.0, fail: BaseException | None = None,
                 after: float | None = None) -> None:
        self.key = key
        self.calls: list[tuple] = []
        self.fail = fail
        self.rest_min_interval = min_interval
        self.rest_refresh_after_seconds = after
        self.payload = None

    def rest_snapshot(self, http, timeout, now, wall_ms):
        self.calls.append((http, timeout, now, wall_ms))
        if self.fail is not None:
            raise self.fail
        if self.payload is not None:
            return self.payload
        return [Update('book', self.key, book=Book(lv(('100', '1')), lv(('101', '1')), now, wall_ms, None))]


def test_rest_refresher_polls_only_after_the_receipt_age_threshold(clock):
    adapter = RestAdapter('bitget_perp')
    hub = FeedHub(FakeConfig(), adapters=[adapter], start_threads=False, http='HTTP')
    put(hub, 'bitget_perp', book(clock[0]))
    clock[0] += 1.5
    assert hub.refresh_once() == [] and adapter.calls == []            # age == threshold: not exceeded
    clock[0] += 0.01
    assert hub.refresh_once() == ['bitget_perp']
    assert adapter.calls[0][:2] == ('HTTP', 5.0)
    diag = hub.diagnostics()['bitget_perp']
    assert diag['source'] == 'rest' and diag['ok'] and diag['rest_refresh']['polls'] == 1


def test_rest_refresher_respects_min_interval_and_lighter_two_seconds(clock):
    fast, slow = RestAdapter('bitget_perp', fail=OSError('down')), RestAdapter('bybit_perp', min_interval=2.0)
    cfg = FakeConfig([src('bitget_perp', rest=0.5), src('bybit_perp', 'bybit_linear', rest=0.5)])
    hub = FeedHub(cfg, adapters=[fast, slow], start_threads=False, http=object())
    assert hub.refresh_once() == ['bitget_perp', 'bybit_perp']         # no data at all: poll now
    clock[0] += 0.6
    assert hub.refresh_once() == []                                     # 1 s floor for everyone
    clock[0] += 0.4
    assert hub.refresh_once() == ['bitget_perp']                        # bybit (2 s) still waits; its REST
    clock[0] += 1.0                                                     # book is 1.0 s old (> 0.5)
    assert hub.refresh_once() == ['bitget_perp', 'bybit_perp']
    assert len(fast.calls) == 3 and len(slow.calls) == 2
    row = hub.diagnostics()['bitget_perp']
    assert row['rest_refresh']['failures'] == 3 and row['rest_refresh']['last_error'] == 'OSError'
    assert row['error'] == NO_DATA                                      # a failed poll never invalidates


def test_rest_refresher_lighter_floor_applies_without_adapter_declaration(clock):
    adapter = RestAdapter('lighter_perp', min_interval=0.0)
    hub = FeedHub(FakeConfig([src('lighter_perp', 'lighter', quote='USDC', rest=0.5)]), adapters=[adapter],
                  start_threads=False, http=object())
    assert hub.refresh_once() == ['lighter_perp']
    clock[0] += 1.9
    assert hub.refresh_once() == []
    clock[0] += 0.1
    assert hub.refresh_once() == ['lighter_perp']
    assert hub.diagnostics()['lighter_perp']['rest_refresh']['min_interval_seconds'] == 2.0


def test_rest_refresher_skips_disabled_sources_and_adapters_without_rest(clock):
    no_rest = types.SimpleNamespace(key='bybit_perp')                  # bybit: rest_refresh_after None
    disabled = RestAdapter('gate_perp')
    hub = FeedHub(FakeConfig(), adapters=[no_rest, disabled], start_threads=False, http=object())
    assert hub.refresh_once() == [] and disabled.calls == []


def test_rest_refresher_uses_adapter_default_for_spot_and_fx(clock):
    cfg = FakeConfig([src('lighter_perp', 'lighter', quote='USDC')], fx_mode='binance_spot')
    spot, fx = RestAdapter(GATE_SPOT_KEY, after=1.5), RestAdapter(FX_KEY, after=None)
    hub = FeedHub(cfg, adapters=[spot, fx], start_threads=False, http=object())
    assert hub.refresh_once() == [GATE_SPOT_KEY]


def test_rest_refresher_rejects_foreign_keys_and_records_rejections(clock):
    adapter = RestAdapter('bitget_perp')
    hub = FeedHub(FakeConfig(), adapters=[adapter], start_threads=False, http=object())
    adapter.payload = [Update('book', 'bybit_perp', book=book(clock[0]))]
    hub.refresh_once()
    assert hub.source_snapshot('bybit_perp').book is None
    assert hub._rest['bitget_perp']['last_error'] == 'INVALID_ADAPTER_OUTPUT'
    clock[0] += 1.0
    adapter.payload = [Update('book', 'bitget_perp', book=book(clock[0], bids=(('102', '1'),)))]
    hub.refresh_once()
    assert hub._rest['bitget_perp']['last_error'] == 'CROSSED_BOOK'
    assert hub.source_snapshot('bitget_perp').error == 'CROSSED_BOOK'


# ---------------------------------------------------------------------------- real threads
class LoopAdapter:
    """Publishes one book per connection, then idles until the socket closes."""

    venue = 'fake'
    ping_interval = 0.0
    idle_timeout = 60.0

    def __init__(self, key):
        self.key = key

    def ws_url(self):
        return 'wss://fake.invalid'

    def subscribe_frames(self):
        return []

    def ping_frame(self, wall_ms):
        return None

    def decode(self, raw):
        return raw

    def on_reset(self):
        pass

    def on_text(self, text, now, wall_ms):
        return [Update('book', self.key, book=Book(lv(('100', '1')), lv(('101', '1')), now, wall_ms, None))]


class IdleConn:
    def __init__(self):
        self.frames = ['book']
        self.closed = threading.Event()

    def settimeout(self, value):
        pass

    def send(self, text):
        pass

    def ping(self):
        pass

    def close(self):
        self.closed.set()

    def recv(self):
        if self.frames:
            return self.frames.pop()
        if self.closed.wait(0.02):
            raise websocket.WebSocketConnectionClosedException('closed')
        raise websocket.WebSocketTimeoutException('timeout')


def test_threads_start_publish_and_stop_cleanly():
    conns = []

    def connect(url, timeout=None, **options):
        conn = IdleConn()
        conns.append(conn)
        return conn

    hub = FeedHub(FakeConfig(), adapters=[LoopAdapter('bitget_perp'), LoopAdapter(GATE_SPOT_KEY)],
                  start_threads=True, connect=connect)
    hub.start()
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and (hub.source_snapshot('bitget_perp').book is None
                                               or hub.spot_book() is None):
            hub.wake.wait(0.05)
            hub.wake.clear()
        assert hub.source_snapshot('bitget_perp').book is not None and hub.spot_book() is not None
        assert hub.source_snapshot('bitget_perp').connected
        assert hub.diagnostics()['bitget_perp']['stream']['connects'] == 1
    finally:
        started = time.monotonic()
        hub.stop()
    assert time.monotonic() - started < 1.5
    assert all(conn.closed.is_set() for conn in conns)
    assert hub.source_snapshot('bitget_perp').error == 'STOPPED' and hub._workers == []


# ---------------------------------------------------------------------------- QuoteHealth (reference pins)
def test_stale_warning_flood_is_bounded_and_keeps_every_counter(caplog):
    caplog.set_level(logging.WARNING)
    health = QuoteHealth()
    for i in range(1000):
        health.observe('src', 'STALE_EVENT_OR_CLOCK_SKEW', 100 + i / 1000, 100000 + i, 90000, 3)
    assert len(caplog.records) == 1 and 'QUOTE_FAULT' in caplog.text
    row = health.snapshot()['src']
    assert row['invalid_total'] == 1000 and row['suppressed'] == 999
    assert row['event_age_ms'] == 10999 and row['received_ms'] == 100999
    assert 'next_log' not in row and 'stale_since' not in row
    health.observe('src', None, 102, 102000, 102000, 3)
    assert len(caplog.records) == 2 and 'QUOTE_RECOVERED' in caplog.text
    assert not health.should_reconnect('src', 110)


def test_clock_jump_recorded_and_reconnections_bounded_across_reset():
    h = QuoteHealth()
    h.observe('x', None, 0, 100000, 100000, 3)
    h.observe('x', 'STALE_EVENT_OR_CLOCK_SKEW', 1, 106000, 101000, 3)
    assert h.snapshot()['x']['wall_clock_step_ms'] == 5000
    assert h.snapshot()['x']['max_abs_wall_clock_step_ms'] == 5000
    for start in (1, 40, 80, 120):
        for now in (start, start + 2, start + 6):
            h.observe('x', 'STALE_EVENT_OR_CLOCK_SKEW', now, 200000, 100000, 3)
        assert h.should_reconnect('x', start + 6) == (start != 120)
        assert not h.should_reconnect('x', start + 7)                   # 30 s cooldown
        h.connection_reset('x')
    assert len(h.reconnects['x']) == 3
    for now in (301, 303, 307):                                         # window of 300 s frees one slot
        h.observe('x', 'STALE_EVENT_OR_CLOCK_SKEW', now, 200000, 100000, 3)
    assert h.should_reconnect('x', 307)


def test_reconnect_needs_streak_duration_and_the_stale_reason():
    h = QuoteHealth()
    for now in (0, 1, 2):
        h.observe('x', 'STALE_EVENT_OR_CLOCK_SKEW', now, 1000, 0, 3)
    assert not h.should_reconnect('x', 4.9)                             # streak 3 but only 4.9 s
    assert h.should_reconnect('x', 5.0)
    h2 = QuoteHealth()
    for now in (0, 3, 6):
        h2.observe('y', 'CROSSED_BOOK', now, 1000, 0, 3)
    assert not h2.should_reconnect('y', 10)                             # malformed is not "stale"
    h3 = QuoteHealth()
    for now in (0, 6):
        h3.observe('z', 'STALE_EVENT_OR_CLOCK_SKEW', now, 1000, 0, 3)
    assert not h3.should_reconnect('z', 10)                             # streak of 2
    assert not h3.should_reconnect('other', 10, keys=('other',))        # keys select rows


def test_health_logging_failure_never_raises(monkeypatch):
    h = QuoteHealth()

    def broken(*args, **kwargs):
        raise RuntimeError('log down')
    monkeypatch.setattr('gate_stonk.feeds.health.LOG.warning', broken)
    h.observe('x', 'STALE_QUOTE', 0, 1000, 0, 3)
    h.observe('x', None, 1, 2000, 2000, 3)
    assert h.snapshot()['x']['error'] is None


def test_observe_market_bounded_logs_and_recovery(caplog):
    caplog.set_level(logging.WARNING)
    h = QuoteHealth()
    h.observe_market('STONK', None, lambda: {'unused': 1}, 0)
    assert h.market_snapshot() == {}
    for i in range(100):
        h.observe_market('STONK', 'SOURCE_TIMESTAMP_SKEW', lambda: {'raw_timestamp_skew_ms': 2500}, i / 100)
    row = h.market_snapshot()['STONK']
    assert row['invalid_checks'] == 100 and row['suppressed'] == 99 and row['raw_timestamp_skew_ms'] == 2500
    h.observe_market('STONK', None, lambda: {}, 2)
    row = h.market_snapshot()['STONK']
    assert row['recovered_from'] == 'SOURCE_TIMESTAMP_SKEW' and row['error'] is None
    assert caplog.text.count('MARKET_SOURCE_FAULT') == 1 and caplog.text.count('MARKET_SOURCE_RECOVERED') == 1
    snapshot = h.market_snapshot()
    snapshot['STONK']['error'] = 'MUTATED'
    assert h.market_snapshot()['STONK']['error'] is None


# ---------------------------------------------------------------------------- build_hub / registry
def test_registry_names_match_the_adapter_contract():
    assert feeds.VENUE_ADAPTERS == {
        'bybit_linear': ('bybit', 'BybitAdapter'),
        'bitget_usdt_futures': ('bitget', 'BitgetAdapter'),
        'bingx_swap': ('bingx', 'BingxAdapter'),
        'lighter': ('lighter', 'LighterAdapter'),
        'gate_futures_usdt': ('gate_futures', 'GateFuturesAdapter'),
    }
    with pytest.raises(ConfigError):
        feeds.adapter_class('binance_perp')


class FakeVenueAdapter:
    def __init__(self, source, *, timeout):
        self.source = source
        self.key = source.id
        self.timeout = timeout


@pytest.mark.parametrize('fx_mode, fx_class', [('binance_spot', BinanceFxAdapter), ('gate_spot', GateSpotFxAdapter),
                                               ('fixed', None)])
def test_build_hub_constructs_adapters_lazily(clock, monkeypatch, fx_mode, fx_class):
    module = types.ModuleType('gate_stonk.feeds.fakevenue')
    module.FakeVenueAdapter = FakeVenueAdapter
    monkeypatch.setitem(sys.modules, 'gate_stonk.feeds.fakevenue', module)
    monkeypatch.setitem(feeds.VENUE_ADAPTERS, 'lighter', ('fakevenue', 'FakeVenueAdapter'))
    monkeypatch.setitem(feeds.VENUE_ADAPTERS, 'bitget_usdt_futures', ('fakevenue', 'FakeVenueAdapter'))
    cfg = FakeConfig([src('lighter_perp', 'lighter', quote='USDC'), src('bitget_perp')], fx_mode=fx_mode)
    hub = feeds.build_hub(cfg, start_threads=False)
    adapters = hub._adapter_list
    assert [type(a) for a in adapters[:3]] == [FakeVenueAdapter, FakeVenueAdapter, GateSpotAdapter]
    assert adapters[0].source.id == 'lighter_perp' and adapters[0].timeout == 5.0
    if fx_class is None:
        assert len(adapters) == 3 and hub.fx_snapshot().mode == 'fixed'
    else:
        assert type(adapters[3]) is fx_class and hub._adapters[FX_KEY] is adapters[3]
    if fx_class is GateSpotFxAdapter:
        assert adapters[3].ws_url() == 'wss://api.gateio.invalid/ws/v4/'
