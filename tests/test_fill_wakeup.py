"""fill_wakeup: Gate private WS auth payload and wake-up on own events only (offline)."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging

import pytest

from gate_stonk.config import load_config
from gate_stonk.errors import Unsafe
from gate_stonk.fill_wakeup import FillWakeup, sign_subscription, subscribe_request, subscription_message

from cli_fakes import live_env, write_config

KEY, SECRET = 'key-ABCDEFGH-123456', 'secret-ZYXWVUT-987654'


@pytest.fixture
def wakeup(tmp_path, monkeypatch):
    live_env(monkeypatch, confirm=False)
    cfg = load_config(write_config(tmp_path), read_env=False)
    calls = []
    fw = FillWakeup(cfg, on_wake=lambda: calls.append('wake'))
    fw.callbacks = calls       # type: ignore[attr-defined]
    return fw


def frame(channel, rows, event='update'):
    return json.dumps({'time': 1800000000, 'time_ms': 1800000000123, 'channel': channel, 'event': event,
                       'result': rows})


def test_signature_matches_gate_formula():
    expected = hmac.new(SECRET.encode(), b'channel=spot.orders&event=subscribe&time=1760000000',
                        hashlib.sha512).hexdigest()
    assert subscription_message('spot.orders', 'subscribe', 1760000000) == \
        'channel=spot.orders&event=subscribe&time=1760000000'
    assert sign_subscription(SECRET, 'spot.orders', 'subscribe', 1760000000) == expected
    request = subscribe_request(KEY, SECRET, 'spot.orders', ['STONK_USDT'], 1760000000, 10)
    assert request == {'time': 1760000000, 'id': 10, 'channel': 'spot.orders', 'event': 'subscribe',
                       'payload': ['STONK_USDT'],
                       'auth': {'method': 'api_key', 'KEY': KEY, 'SIGN': expected}}


def test_subscribe_frames_cover_orders_and_usertrades(wakeup):
    frames = [json.loads(f) for f in wakeup.subscribe_frames(ts=1760000001)]
    assert [f['channel'] for f in frames] == ['spot.orders', 'spot.usertrades']
    for f in frames:
        assert f['payload'] == ['STONK_USDT'] and f['time'] == 1760000001 and f['event'] == 'subscribe'
        assert f['auth']['KEY'] == KEY
        assert f['auth']['SIGN'] == sign_subscription(SECRET, f['channel'], 'subscribe', 1760000001)


def test_wake_on_own_events_only(wakeup):
    foreign = {'id': '1', 'text': 'apiv4', 'currency_pair': 'STONK_USDT', 'event': 'put', 'left': '1', 'amount': '1'}
    other_bot = {'id': '2', 'text': 't-zzz-b1-abc01', 'currency_pair': 'STONK_USDT', 'event': 'put'}
    other_pair = {'id': '3', 'text': 't-stk1-b1-abc01', 'currency_pair': 'BTC_USDT', 'event': 'put'}
    assert wakeup.handle_text(frame('spot.orders', [foreign, other_bot, other_pair])) is None
    assert not wakeup.wake.is_set() and wakeup.consume() is None and wakeup.callbacks == []

    own_put = {'id': '4', 'text': 't-stk1-b1-abc01', 'currency_pair': 'STONK_USDT', 'event': 'put',
               'filled_amount': '0', 'finish_as': 'open'}
    assert wakeup.handle_text(frame('spot.orders', [own_put])) == 'event'
    assert wakeup.wake.is_set() and wakeup.callbacks == ['wake']
    assert wakeup.consume() == 'event' and not wakeup.wake.is_set() and wakeup.consume() is None

    own_partial = dict(own_put, event='update', filled_amount='3.5')
    assert wakeup.handle_text(frame('spot.orders', own_partial)) == 'fill'      # object result accepted
    trade = {'id': 99, 'order_id': '4', 'text': 't-stk1-b1-abc01', 'currency_pair': 'STONK_USDT', 'amount': '1',
             'price': '0.13', 'role': 'maker'}
    assert wakeup.handle_text(frame('spot.usertrades', [trade])) == 'fill'
    assert wakeup.consume() == 'fill'
    assert wakeup.counters['own_events'] == 3 and wakeup.counters['own_fills'] == 2


def test_fill_outranks_plain_event_until_consumed(wakeup):
    own = {'text': 't-stk1-s1-abc02', 'currency_pair': 'STONK_USDT', 'event': 'put'}
    wakeup.handle_text(frame('spot.usertrades', [own]))
    wakeup.handle_text(frame('spot.orders', [own]))
    assert wakeup.consume() == 'fill'


def test_acks_pongs_and_garbage_are_ignored(wakeup):
    assert wakeup.handle_text('not json') is None
    assert wakeup.handle_text(json.dumps({'channel': 'spot.pong', 'result': None})) is None
    assert wakeup.handle_text(json.dumps({'channel': 'spot.orders', 'event': 'subscribe',
                                          'result': {'status': 'success'}})) is None
    assert not wakeup.wake.is_set()


def test_refused_subscription_raises_code_without_secrets(wakeup):
    msg = json.dumps({'channel': 'spot.orders', 'event': 'subscribe', 'result': None,
                      'error': {'code': 4, 'message': '{"label":"INVALID_KEY"}'}})
    with pytest.raises(Unsafe) as info:
        wakeup.handle_text(msg)
    assert str(info.value) == 'FILL_WAKEUP_SUBSCRIBE_FAILED:4'
    status = json.dumps(wakeup.status())
    assert KEY not in status and SECRET not in status and KEY not in repr(wakeup)


def test_requires_keys(tmp_path):
    cfg = load_config(write_config(tmp_path), read_env=False)
    with pytest.raises(Unsafe):
        FillWakeup(cfg)


class FakeConn:
    def __init__(self, frames, stopper):
        self.frames = list(frames)
        self.sent: list[str] = []
        self.stopper = stopper
        self.closed = False

    def settimeout(self, value):
        pass

    def send(self, text):
        self.sent.append(text)

    def ping(self):
        pass

    def recv(self):
        if self.frames:
            return self.frames.pop(0)
        self.stopper()
        return ''

    def close(self):
        self.closed = True


def test_worker_end_to_end_with_fake_socket(tmp_path, monkeypatch, caplog):
    live_env(monkeypatch, confirm=False)
    cfg = load_config(write_config(tmp_path), read_env=False)
    holder = {}
    own = {'text': 't-stk1-b2-abc03', 'currency_pair': 'STONK_USDT', 'event': 'finish', 'finish_as': 'filled'}

    def connect(url, timeout, **kwargs):
        holder['url'] = url
        conn = FakeConn([frame('spot.orders', [own])], lambda: holder['fw'].worker.request_stop())
        holder['conn'] = conn
        return conn

    fw = FillWakeup(cfg, connect=connect)
    holder['fw'] = fw
    caplog.set_level(logging.DEBUG)
    fw.make_worker().run()                 # synchronous: one connection, one frame, then stop
    assert holder['url'] == 'wss://api.gateio.ws/ws/v4/'
    sent = [json.loads(text) for text in holder['conn'].sent]
    assert [s['channel'] for s in sent[:2]] == ['spot.orders', 'spot.usertrades']
    assert all('auth' in s for s in sent[:2])
    assert fw.consume() == 'fill'
    for record in caplog.records:
        assert KEY not in record.getMessage() and SECRET not in record.getMessage()
    fw.stop()
