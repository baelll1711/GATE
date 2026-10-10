"""WsWorker: reconnect/backoff/idle/ping/reply semantics with fake sockets (no network)."""
from __future__ import annotations

import logging
import threading
import time
from decimal import Decimal

import pytest
import websocket

from gate_stonk.config import FxSettings, IndexSettings, SourceConfig
from gate_stonk.errors import Unsafe
from gate_stonk.feeds.base import Reply, Update
from gate_stonk.feeds.hub import FeedHub
from gate_stonk.feeds.ws import (BACKOFF_MAX_SECONDS, CONTROL_PING_SECONDS, RECV_TIMEOUT_SECONDS, WsWorker,
                                 safe_reason)
from gate_stonk.types import Book

TIMEOUT = object()      # script item: recv timeout (advances the fake clock by 1 s)
T0 = 1_800_000_000.0


def make_book(received: float, event_ms: int, update_id: int | None = None, bid: str = '100', ask: str = '101') -> Book:
    return Book(((Decimal(bid), Decimal('1')),), ((Decimal(ask), Decimal('1')),), received, event_ms, update_id)


class FakeAdapter:
    venue = 'fake'

    def __init__(self, key: str = 'src', *, ping_interval: float = 0.0, idle_timeout: float = 60.0,
                 ping_returns_none: bool = False) -> None:
        self.key = key
        self.ping_interval = ping_interval
        self.idle_timeout = idle_timeout
        self.ping_returns_none = ping_returns_none
        self.resets = 0
        self.texts: list[str] = []

    def ws_url(self) -> str:
        return 'wss://fake.invalid/ws'

    def subscribe_frames(self) -> list[str]:
        return ['SUB']

    def ping_frame(self, wall_ms: int) -> str | None:
        return None if self.ping_returns_none else f'PING {wall_ms}'

    def decode(self, raw):
        return raw.decode('utf-8') if isinstance(raw, bytes) else raw

    def on_reset(self) -> None:
        self.resets += 1

    def on_text(self, text: str, now: float, wall_ms: int):
        self.texts.append(text)
        if text == 'ping':
            return [Reply('pong')]
        if text.startswith('book'):
            age_ms = int(text.split(':')[1]) if ':' in text else 0
            return [Update('book', self.key, book=make_book(now, wall_ms - age_ms))]
        if text.startswith('err:'):
            return [Update('error', self.key, error=text[4:])]
        if text == 'fatal':
            raise Unsafe('SUBSCRIPTION_REJECTED')
        if text == 'bad':
            raise ValueError('cannot parse secret payload')
        if text == 'junk':
            return ['not an update']
        if text == 'ack':
            return [Update('subscribed', self.key), Update('ignore', self.key)]
        return []

    def rest_snapshot(self, http, timeout, now, wall_ms):
        return []

    def instrument_info(self, http, timeout):
        return {}


class FakeConn:
    def __init__(self, script, clock=None, *, blocking: bool = False) -> None:
        self.script = list(script)
        self.clock = clock
        self.sent: list[str] = []
        self.pings = 0
        self.closed = False
        self.timeouts: list[float] = []
        self.blocking = blocking
        self._closed_event = threading.Event()

    def settimeout(self, value: float) -> None:
        self.timeouts.append(value)

    def send(self, text: str) -> None:
        self.sent.append(text)

    def ping(self) -> None:
        self.pings += 1

    def close(self) -> None:
        self.closed = True
        self._closed_event.set()

    def recv(self):
        if self.blocking:
            # Real-time blocking recv: only close() unblocks it (like a socket).
            if self._closed_event.wait(5):
                raise websocket.WebSocketConnectionClosedException('closed')
            raise websocket.WebSocketTimeoutException('timeout')
        if not self.script:
            return ''
        item = self.script.pop(0)
        if item is TIMEOUT:
            if self.clock is not None:
                self.clock[0] += 1.0
            raise websocket.WebSocketTimeoutException('timed out')
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            return item()
        return item


class FakeSink:
    def __init__(self, *, reject: str | None = None, reconnect_after: int | None = None) -> None:
        self.events: list[tuple] = []
        self.published: list[Update] = []
        self.reject = reject
        self.reconnect_after = reconnect_after
        self.reconnect_checks = 0

    def publish(self, update: Update) -> None:
        self.events.append(('publish', update.kind))
        self.published.append(update)
        if self.reject and update.kind != 'error':
            raise Unsafe(self.reject)

    def invalidate(self, key: str, reason: str) -> None:
        self.events.append(('invalidate', key, reason))

    def set_connected(self, key: str, connected: bool) -> None:
        self.events.append(('connected', key, connected))

    def connection_reset(self, keys) -> None:
        self.events.append(('reset', tuple(keys)))

    def should_reconnect(self, stream, now, keys) -> bool:
        self.reconnect_checks += 1
        return self.reconnect_after is not None and self.reconnect_checks >= self.reconnect_after


class Harness:
    """Drive ``WsWorker.run()`` synchronously: scripted connections, recorded waits."""

    def __init__(self, adapter, sink, plan, clock=None, timeout: float = 5.0) -> None:
        self.plan = list(plan)
        self.calls: list[tuple] = []
        self.waits: list[float] = []
        self.clock = clock
        self.reasons: list[str | None] = []     # last_error after every connection cycle
        self.worker = WsWorker(adapter, sink, timeout=timeout, connect=self.connect, wait=self.wait)
        original = self.worker._cycle

        def cycle() -> None:
            original()
            self.reasons.append(self.worker.last_error)
        self.worker._cycle = cycle

    def connect(self, url, timeout=None, **options):
        self.calls.append((url, timeout, options))
        if not self.plan:
            self.worker.request_stop()
            raise ConnectionRefusedError('plan exhausted')
        item = self.plan.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def wait(self, seconds: float) -> bool:
        self.waits.append(seconds)
        if self.clock is not None:
            self.clock[0] += seconds
        return self.worker.stopped

    def run(self) -> WsWorker:
        self.worker.run()
        return self.worker


# ---------------------------------------------------------------------------- basics
def test_connect_subscribe_publish_reply_and_lifecycle(clock):
    adapter, sink = FakeAdapter(), FakeSink()
    conn = FakeConn(['ping', 'book', b'ack', ''], clock)
    harness = Harness(adapter, sink, [conn], clock)
    worker = harness.run()
    assert harness.calls[0] == ('wss://fake.invalid/ws', 5.0, {})
    assert conn.timeouts == [RECV_TIMEOUT_SECONDS]
    assert conn.sent == ['SUB', 'pong']               # subscription first, then the Reply
    assert [u.kind for u in sink.published] == ['book']   # 'subscribed'/'ignore' are not published
    assert conn.closed
    first = sink.events[:2]
    assert first == [('invalidate', 'src', 'CONNECTING'), ('reset', ('src',))]
    assert ('connected', 'src', True) in sink.events
    connected_at = sink.events.index(('connected', 'src', True))
    published_at = sink.events.index(('publish', 'book'))
    disconnected_at = sink.events.index(('invalidate', 'src', 'DISCONNECTED'))
    assert connected_at < published_at < disconnected_at
    assert sink.events[disconnected_at - 1] == ('connected', 'src', False)
    assert adapter.resets == 2                        # one per connection attempt (2nd attempt refused)
    assert worker.last_error == 'ConnectionRefusedError'
    assert worker.connects == 1 and worker.attempts == 2 and worker.disconnects == 1
    assert harness.waits == [1.0]                     # closed socket -> backoff before the next attempt


def test_empty_or_none_frame_is_websocket_closed(clock):
    harness = Harness(FakeAdapter(), FakeSink(), [FakeConn([''], clock), FakeConn([lambda: None], clock)], clock)
    harness.run()
    assert harness.reasons == ['WEBSOCKET_CLOSED', 'WEBSOCKET_CLOSED', 'ConnectionRefusedError']
    assert harness.waits == [1.0, 2.0]


def test_backoff_doubles_caps_at_30_and_resets_after_a_long_lived_connection(clock):
    plan = [ConnectionRefusedError('x')] * 7
    plan.append(FakeConn([TIMEOUT] * 31 + [''], clock))      # lives 31 s > 30 s
    plan.append(ConnectionRefusedError('y'))
    harness = Harness(FakeAdapter(), FakeSink(), plan, clock)
    harness.run()
    assert harness.waits == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 1.0, 2.0]
    assert max(harness.waits) == BACKOFF_MAX_SECONDS


def test_short_lived_connection_does_not_reset_backoff(clock):
    plan = [ConnectionRefusedError('x'), FakeConn([TIMEOUT] * 5 + [''], clock), ConnectionRefusedError('y')]
    harness = Harness(FakeAdapter(), FakeSink(), plan, clock)
    harness.run()
    assert harness.waits == [1.0, 2.0, 4.0]


# ---------------------------------------------------------------------------- pings / idle
def test_app_ping_sent_every_ping_interval(clock):
    adapter = FakeAdapter(ping_interval=3.0)
    conn = FakeConn([TIMEOUT] * 7 + [''], clock)
    Harness(adapter, FakeSink(), [conn], clock).run()
    pings = [frame for frame in conn.sent if frame.startswith('PING')]
    assert pings == [f'PING {int((T0 + 3) * 1000)}', f'PING {int((T0 + 6) * 1000)}']
    assert conn.pings == 0


def test_control_ping_when_adapter_has_no_app_ping(clock):
    conn = FakeConn([TIMEOUT] * 16 + [''], clock)
    Harness(FakeAdapter(ping_interval=0.0), FakeSink(), [conn], clock).run()
    assert conn.pings == 1 and conn.sent == ['SUB']
    assert CONTROL_PING_SECONDS == 15.0


def test_app_ping_falls_back_to_control_ping_when_adapter_returns_none(clock):
    conn = FakeConn([TIMEOUT] * 4 + [''], clock)
    Harness(FakeAdapter(ping_interval=2.0, ping_returns_none=True), FakeSink(), [conn], clock).run()
    assert conn.pings == 2 and conn.sent == ['SUB']


def test_idle_timeout_reconnects(clock):
    adapter, sink = FakeAdapter(idle_timeout=5.0), FakeSink()
    first = FakeConn([TIMEOUT] * 20, clock)
    harness = Harness(adapter, sink, [first], clock)
    harness.run()
    reasons = harness.reasons
    assert reasons[0] == 'WEBSOCKET_IDLE'
    assert first.closed
    assert len(first.script) == 20 - 6               # idle detected after > 5 s of silence
    assert ('invalidate', 'src', 'DISCONNECTED') in sink.events


def test_any_frame_including_heartbeats_resets_idle(clock):
    adapter = FakeAdapter(idle_timeout=5.0)
    conn = FakeConn(['ping'] + [TIMEOUT] * 4 + ['ping'] + [TIMEOUT] * 4 + ['ping', ''], clock)
    harness = Harness(adapter, FakeSink(), [conn], clock)
    harness.run()
    reasons = harness.reasons
    assert reasons[0] == 'WEBSOCKET_CLOSED'
    assert conn.sent.count('pong') == 3


# ---------------------------------------------------------------------------- errors
def test_per_key_error_update_keeps_the_socket(clock):
    adapter, sink = FakeAdapter(), FakeSink()
    conn = FakeConn(['err:CROSSED_BOOK', 'book', ''], clock)
    harness = Harness(adapter, sink, [conn], clock)
    worker = harness.run()
    assert [(u.kind, u.error) for u in sink.published] == [('error', 'CROSSED_BOOK'), ('book', None)]
    assert worker.connects == 1                       # the book after the error used the same socket
    assert sink.reconnect_checks == 1                 # the error asked the bounded stale policy only


def test_adapter_unsafe_is_socket_fatal_and_reconnects(clock):
    adapter, sink = FakeAdapter(), FakeSink()
    first = FakeConn(['fatal', 'book', ''], clock)
    second = FakeConn(['book', ''], clock)
    harness = Harness(adapter, sink, [first, second], clock)
    harness.run()
    reasons = harness.reasons
    assert reasons[0] == 'SUBSCRIPTION_REJECTED'
    assert first.closed and first.script == ['book', '']     # nothing after the fatal frame was read
    assert len(sink.published) == 1                   # only the book from the second connection
    assert adapter.resets == 3                        # on_reset at every connection attempt


def test_non_unsafe_adapter_exception_reconnects_and_logs_type_only(clock, caplog):
    caplog.set_level(logging.WARNING)
    adapter, sink = FakeAdapter(), FakeSink()
    harness = Harness(adapter, sink, [FakeConn(['bad'], clock)], clock)
    harness.run()
    reasons = harness.reasons
    assert reasons[0] == 'ValueError'
    assert 'ValueError' in caplog.text
    assert 'secret' not in caplog.text and 'payload' not in caplog.text


def test_connect_failure_logs_only_the_exception_type(clock, caplog):
    caplog.set_level(logging.WARNING)
    error = ConnectionRefusedError('wss://user:token123@secret-host:443 refused')
    harness = Harness(FakeAdapter(), FakeSink(), [error], clock)
    harness.run()
    assert 'ConnectionRefusedError' in caplog.text
    assert 'token123' not in caplog.text and 'secret-host' not in caplog.text


def test_invalid_adapter_output_reconnects(clock):
    harness = Harness(FakeAdapter(), FakeSink(), [FakeConn(['junk', 'book'], clock)], clock)
    harness.run()
    reasons = harness.reasons
    assert reasons[0] == 'INVALID_ADAPTER_OUTPUT'


def test_rejected_publish_reconnects_only_when_policy_allows(clock):
    adapter = FakeAdapter()
    sink = FakeSink(reject='STALE_EVENT_OR_CLOCK_SKEW', reconnect_after=3)
    first = FakeConn(['book', 'book', 'book', 'book', ''], clock)
    harness = Harness(adapter, sink, [first], clock)
    harness.run()
    reasons = harness.reasons
    assert reasons[0] == 'STALE_STREAM_RECONNECT'
    assert first.script == ['book', '']               # reconnected right after the 3rd rejection
    assert sink.reconnect_checks == 3


def test_ws_headers_are_passed_to_connect(clock):
    adapter = FakeAdapter()
    adapter.ws_headers = lambda: {'X-Gate-Size-Decimal': '1'}
    harness = Harness(adapter, FakeSink(), [FakeConn([''], clock)], clock)
    harness.run()
    assert harness.calls[0][2] == {'header': {'X-Gate-Size-Decimal': '1'}}


def test_worker_validates_timeout():
    with pytest.raises(ValueError):
        WsWorker(FakeAdapter(), FakeSink(), timeout=0)


def test_safe_reason_only_exposes_our_codes():
    assert safe_reason(Unsafe('WEBSOCKET_IDLE')) == 'WEBSOCKET_IDLE'
    assert safe_reason(Unsafe('INVALID_WS_BOOK:gate_spot')) == 'INVALID_WS_BOOK:gate_spot'
    assert safe_reason(Unsafe('connect to wss://x?token=1 failed')) == 'Unsafe'
    assert safe_reason(OSError('secret')) == 'OSError'


# ---------------------------------------------------------------------------- real threads
def test_stop_interrupts_the_backoff_wait():
    attempts = []

    def connect(url, timeout=None, **options):
        attempts.append(time.monotonic())
        raise ConnectionRefusedError('down')

    worker = WsWorker(FakeAdapter(), FakeSink(), timeout=1.0, connect=connect)
    worker.start()
    deadline = time.monotonic() + 2
    while not attempts and time.monotonic() < deadline:
        time.sleep(0.005)
    started = time.monotonic()
    worker.stop()
    assert not worker.is_alive()
    assert time.monotonic() - started < 0.9           # did not sit out the 1 s backoff
    assert len(attempts) == 1


def test_stop_closes_a_blocked_socket_and_joins():
    sink = FakeSink()
    conn = FakeConn([], blocking=True)
    connected = threading.Event()

    def connect(url, timeout=None, **options):
        connected.set()
        return conn

    worker = WsWorker(FakeAdapter(), sink, timeout=1.0, connect=connect)
    worker.start()
    assert connected.wait(2)
    deadline = time.monotonic() + 2
    while ('connected', 'src', True) not in sink.events and time.monotonic() < deadline:
        time.sleep(0.005)
    started = time.monotonic()
    worker.stop()
    assert not worker.is_alive() and conn.closed
    assert time.monotonic() - started < 1.0
    assert sink.events[-1] == ('invalidate', 'src', 'DISCONNECTED')


# ---------------------------------------------------------------------------- with the real hub
class HubConfig:
    def __init__(self) -> None:
        self.app = {'request_timeout_seconds': 5}
        self.market = {'currency_pair': 'STONK_USDT', 'depth_levels': 20, 'depth_interval': '100ms'}
        fx = FxSettings(mode='fixed', usdc_to_usdt=Decimal(1), max_depeg_pct=Decimal('0.5'), max_age_seconds=3.0,
                        max_spread_pct=Decimal('0.2'))
        sources = tuple(SourceConfig(id=sid, venue='bitget_usdt_futures', symbol='STONKUSDT', quote='USDT',
                                     price='mid', weight=Decimal(1), required=False, max_spread_pct=Decimal(1),
                                     max_age_seconds=3.0) for sid in ('src', 'other'))
        self._settings = IndexSettings(method='weighted_median', min_sources=1, max_age_seconds=3.0,
                                       max_timestamp_skew_seconds=2.0, max_source_deviation_pct=Decimal(1),
                                       basis_offset_pct=Decimal(0), rejoin_snapshots=3,
                                       observation_interval_seconds=0.2, fx=fx, sources=sources)

    def index_settings(self):
        return self._settings


def test_worker_reconnects_after_sustained_stale_messages_through_the_hub(clock):
    hub = FeedHub(HubConfig(), start_threads=False)
    adapter = FakeAdapter('src')

    def stale():
        clock[0] += 2
        return 'book:10000'                           # event time 10 s old at receipt
    first = FakeConn([stale] * 10, clock)
    second = FakeConn([''], clock)
    harness = Harness(adapter, hub, [first, second], clock)
    harness.run()
    assert harness.worker.connects == 2               # bounded stale policy forced a reconnect
    assert len(first.script) == 10 - 4                # streak >= 3 and stale for >= 5 s (t=2,4,6,8)
    snap = hub.source_snapshot('src')
    assert snap.book is None and snap.error == 'DISCONNECTED'
    row = hub.quote_health.snapshot()['src']
    assert row['error'] == 'STALE_EVENT_OR_CLOCK_SKEW' and row['invalid_total'] == 4
    assert len(hub.quote_health.reconnects['src']) == 1


def test_worker_bad_data_invalidates_only_its_key_and_disconnect_drops_the_book(clock):
    hub = FeedHub(HubConfig(), start_threads=False)
    hub.publish(Update('book', 'other', book=make_book(clock[0], int(clock[0] * 1000))))
    observed = []

    def inspect():
        observed.append((hub.source_snapshot('src').error, hub.source_snapshot('src').connected,
                         hub.source_snapshot('other').book is not None))
        return ''
    conn = FakeConn(['book', 'err:INVALID_BOOK_LEVEL', inspect], clock)
    Harness(FakeAdapter('src'), hub, [conn], clock).run()
    assert observed == [('INVALID_BOOK_LEVEL', True, True)]
    after = hub.source_snapshot('src')
    assert after.book is None and after.error == 'DISCONNECTED' and not after.connected
    assert hub.source_snapshot('other').book is not None      # another key is untouched


def test_prepare_hook_runs_before_connect_and_failure_backs_off(clock):
    """Adapters may resolve metadata (Lighter market index) before subscribing; failures retry with backoff."""
    class PreparingAdapter(FakeAdapter):
        def __init__(self):
            super().__init__()
            self.prepared = 0

        def prepare(self):
            self.prepared += 1
            if self.prepared == 1:
                raise ConnectionError('lookup failed')

    adapter, sink = PreparingAdapter(), FakeSink()
    harness = Harness(adapter, sink, [FakeConn([''], clock)], clock)
    harness.run()
    assert adapter.prepared == 3                       # failed, succeeded (connected), then plan exhausted
    assert harness.reasons[0] == 'ConnectionError'     # first attempt never reached connect()
    assert len(harness.calls) == 2 and harness.waits[0] == 1.0
