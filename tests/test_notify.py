"""Telegram notifier: durable outbox delivery with a fake HTTP session (no sockets).

The fake session mimics the Bot API reply shapes (``{"ok": true, "result": ...}``;
HTTP 429 ``{"ok": false, "error_code": 429, "parameters": {"retry_after": N}}``).
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from decimal import Decimal
from pathlib import Path

import pytest
import requests
import yaml

from gate_stonk import notify
from gate_stonk.config import Config, Secrets
from gate_stonk.ledger import Position
from gate_stonk.notify import Notifier, TelegramAPI, TelegramError, split_text
from gate_stonk.store import Store
from gate_stonk.types import Balance

D = Decimal
ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / 'config.example.yaml'
TOKEN = '123456:TOP_SECRET_TOKEN'
CHAT = '777'
START_MS = 1_799_999_000_000
IDENTITY = {'campaign_id': 'tg', 'currency_pair': 'STONK_USDT', 'mode': 'paper', 'client_prefix': 'stk1'}


class FakeResponse:
    def __init__(self, status: int = 200, data=None, *, bad_json: bool = False):
        self.status_code = status
        self._data = {'ok': True, 'result': {'message_id': 1}} if data is None else data
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError(f'not json from https://api.telegram.org/bot{TOKEN}/sendMessage')
        return self._data


class FakeSession:
    """Records every POST; ``script`` items (responses or exceptions) are consumed per call."""

    def __init__(self):
        self.calls: list[dict] = []
        self.script: list = []
        self.fail: dict = {}          # method -> response/exception returned on EVERY call
        self.closed = False
        self.lock = threading.Lock()

    def post(self, url, json=None, timeout=None):
        with self.lock:
            method = url.rsplit('/', 1)[-1]
            self.calls.append({'url': url, 'method': method, 'json': json, 'timeout': timeout})
            if method in self.fail:
                item = self.fail[method]
            else:
                item = self.script.pop(0) if self.script else FakeResponse()
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self):
        self.closed = True

    def methods(self):
        return [c['method'] for c in self.calls]

    def texts(self):
        return [c['json']['text'] for c in self.calls if c['method'] == 'sendMessage']


def make_cfg(tmp_path, *, enabled=True, token=TOKEN, chat=CHAT, **telegram):
    data = yaml.safe_load(EXAMPLE.read_text(encoding='utf-8'))
    data['telegram']['enabled'] = enabled
    data['telegram'].update(telegram)
    return Config(data, tmp_path, secrets=Secrets(telegram_token=token, telegram_chat=chat))


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / 'state.sqlite3')
    s.initialize(IDENTITY, START_MS)
    yield s
    s.close()


def make(tmp_path, store, session=None, **kw):
    session = session if session is not None else FakeSession()
    notifier = Notifier(make_cfg(tmp_path, **kw), store, session=session, start_thread=False)
    return notifier, session


def rows(store):
    return {r['key']: r for r in store.outbox_rows(1000)}


def now_ms(clock):
    return int(clock[0] * 1000)


# ----------------------------------------------------------------------------- delivery

def test_ordered_delivery_with_event_suffix_prefix_and_pacing(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    assert n.event('state:1', 'первое') is True
    assert n.event('state:1', 'дубликат') is False          # idempotent by key
    store.outbox_add('fill:abc', '[PAPER] STONK BUY [b1]: исполнено', now_ms(clock))   # store-written event
    n.event('state:2', 'третье')
    n.pump()
    assert s.texts() == ['[PAPER] первое\n[event state:1]']
    n.pump()                                                  # within send_interval_seconds (1.1 s)
    assert len(s.texts()) == 1
    clock[0] += 1.2
    n.pump()
    clock[0] += 1.2
    n.pump()
    assert s.texts() == ['[PAPER] первое\n[event state:1]',
                         '[PAPER] STONK BUY [b1]: исполнено\n[event fill:abc]',
                         '[PAPER] третье\n[event state:2]']
    call = s.calls[0]
    assert call['url'] == f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    assert call['json']['chat_id'] == CHAT and call['json']['link_preview_options'] == {'is_disabled': True}
    assert call['timeout'] == 5.0
    assert all(r['status'] == 'sent' and r['sent_ms'] for r in rows(store).values())
    st = n.status()
    assert st['pending'] == 0 and st['healthy'] is True and st['last_error_type'] is None
    assert st['last_sent_ms'] == now_ms(clock)


def test_live_events_have_no_prefix_and_existing_prefix_kept(tmp_path, store):
    cfg = make_cfg(tmp_path)
    n = Notifier(cfg, store, session=FakeSession(), start_thread=False)
    n.live = True
    n.event('a:1', 'live text')
    n.live = False
    n.event('a:2', '[LIVE] старт')
    n.event('a:3', '[PAPER] старт')
    texts = {r['key']: r['text'] for r in store.outbox_rows()}
    assert texts == {'a:1': 'live text', 'a:2': '[LIVE] старт', 'a:3': '[PAPER] старт'}


def test_long_text_is_chunked_and_acknowledged_after_last_chunk(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    body = '\n'.join(f'строка {i:04d} ' + 'x' * 60 for i in range(130))     # ~9 KB
    n.event('summary:1', body)
    full = '[PAPER] ' + body + '\n[event summary:1]'
    for _ in range(5):
        n.pump()
        clock[0] += 1.2
    chunks = s.texts()
    assert len(chunks) == 3
    assert ''.join(chunks) == full
    assert max(len(c) for c in chunks) <= 3900
    assert all(c.endswith('\n') for c in chunks[:-1])           # split at line breaks
    assert rows(store)['summary:1']['status'] == 'sent'


def test_event_pending_until_all_chunks_then_retry_resumes_from_failed_chunk(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    n.event('summary:1', 'y' * 5000)
    n.pump()                                                     # chunk 1 delivered
    assert rows(store)['summary:1']['sent_ms'] is None
    s.script = [requests.ConnectionError('reset')]
    clock[0] += 1.2
    n.pump()                                                     # chunk 2 fails
    row = rows(store)['summary:1']
    assert row['sent_ms'] is None and row['attempts'] == 1 and row['last_error'] == 'ConnectionError'
    assert row['next_ms'] == now_ms(clock) + 2000
    clock[0] += 2.0
    n.pump()
    texts = s.texts()
    assert len(texts) == 3 and texts[1] == texts[2]              # only the failed chunk was resent
    assert texts[0] + texts[2] == '[PAPER] ' + 'y' * 5000 + '\n[event summary:1]'
    assert rows(store)['summary:1']['status'] == 'sent'


def test_retry_backoff_is_durable_and_blocks_newer_events(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    n.event('a:1', 'first')
    n.event('a:2', 'second')
    s.script = [FakeResponse(500, {'ok': False, 'error_code': 500})]
    n.pump()
    assert rows(store)['a:1']['next_ms'] == now_ms(clock) + 2000       # 2**1 s
    assert n.status()['last_error_type'] == 'TELEGRAM_HTTP_500'
    assert n.status()['next_attempt_ms'] == now_ms(clock) + 2000
    clock[0] += 1.5
    n.pump()                                                            # head not due: no send (getMe only)
    assert s.texts() == ['[PAPER] first\n[event a:1]']
    s.script = [FakeResponse(502, {'ok': False}, bad_json=True)]
    clock[0] += 0.6
    n.pump()                                                            # second failure -> 4 s
    assert rows(store)['a:1']['attempts'] == 2
    assert rows(store)['a:1']['next_ms'] == now_ms(clock) + 4000
    # A restarted process honours the stored backoff and the FIFO order.
    s2 = FakeSession()
    n2 = Notifier(make_cfg(tmp_path), store, session=s2, start_thread=False)
    assert n2.delivery_failed is True and n2.status()['last_error_type'] == 'TELEGRAM_HTTP_502'
    clock[0] += 3.9
    n2.pump()
    assert s2.texts() == []
    clock[0] += 0.2
    n2.pump()
    clock[0] += 1.2
    n2.pump()
    assert s2.texts() == ['[PAPER] first\n[event a:1]', '[PAPER] second\n[event a:2]']


def test_429_retry_after_blocks_all_calls(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    n.event('a:1', 'first')
    s.script = [FakeResponse(429, {'ok': False, 'error_code': 429, 'description': 'Too Many Requests: retry after 7',
                                   'parameters': {'retry_after': 7}})]
    n.pump()
    assert rows(store)['a:1']['next_ms'] == now_ms(clock) + 7000       # max(retry_after, 2)
    assert n.status()['last_error_type'] == 'TELEGRAM_HTTP_429'
    for _ in range(6):
        clock[0] += 1
        n.pump()
    assert s.methods() == ['sendMessage']                               # not even getMe while blocked
    clock[0] += 1.01
    n.pump()
    assert s.texts() == ['[PAPER] first\n[event a:1]'] * 2
    assert rows(store)['a:1']['status'] == 'sent' and n.healthy()


def test_healthy_transitions(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    n.pump()                                                    # getMe alone never verifies the chat
    assert s.methods() == ['getMe'] and not n.healthy()
    n.event('a:1', 'hello')
    n.pump()
    assert n.healthy() and n.chat_verified
    t0 = clock[0]
    clock[0] += 100
    n.pump()                                                    # getMe refreshes while nothing failed
    assert s.methods()[-1] == 'getMe' and n.last_good == t0 + 100
    clock[0] += 100
    assert n.healthy()                                          # 100 s since the getMe
    n.event('a:2', 'world')
    s.fail['sendMessage'] = FakeResponse(403, {'ok': False, 'error_code': 403})
    n.pump()                                                    # delivery fails (chat permission)
    assert n.delivery_failed and n.healthy()                    # one failure is not an outage
    clock[0] += 1
    n.pump()                                                    # row in backoff -> getMe succeeds ...
    assert s.methods()[-1] == 'getMe'
    assert n.last_good == t0 + 100 and n.delivery_failed        # ... but cannot clear the failed send
    assert n.healthy()                                          # 101 s since the last good contact
    clock[0] += 20
    assert not n.healthy()                                      # > 120 s without confirmed contact
    assert n.status()['healthy'] is False and n.status()['last_error_type'] == 'TELEGRAM_HTTP_403'
    del s.fail['sendMessage']
    clock[0] += 1
    n.pump()
    assert n.healthy() and not n.delivery_failed and rows(store)['a:2']['status'] == 'sent'
    assert n.healthy(now=n.last_good + 120) and not n.healthy(now=n.last_good + 121)


def test_failed_getme_keeps_last_good_until_window_expires(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    n.event('a:1', 'hello')
    n.pump()
    s.script = [requests.Timeout('t')] * 10
    clock[0] += 31
    n.pump()
    assert n.healthy() and n.status()['last_error_type'] == 'Timeout'
    clock[0] += 90
    assert not n.healthy()


def test_disabled_mode_records_and_skips_without_network(tmp_path, store, clock, monkeypatch):
    created = []
    monkeypatch.setattr(requests, 'Session', lambda: created.append(1))
    n = Notifier(make_cfg(tmp_path, enabled=False), store, start_thread=False)
    assert n.api is None and n.healthy()
    assert n.event('state:1', 'остановка') is True
    store.outbox_add('fill:x', 'fill text', now_ms(clock))       # written by the store directly
    assert n.status()['pending'] == 1
    n.pump()
    st = n.status()
    assert st == {**st, 'enabled': False, 'healthy': True, 'pending': 0, 'skipped': 2}
    assert {r['status'] for r in store.outbox_rows()} == {'skipped'}
    assert rows(store)['state:1']['text'] == '[PAPER] остановка'
    assert n.flush(1) == 0 and created == []
    n.close()


def test_token_never_in_logs_exceptions_status_or_repr(tmp_path, store, clock, caplog):
    leak = f'HTTPSConnectionPool: Max retries exceeded with url: /bot{TOKEN}/sendMessage'
    n, s = make(tmp_path, store)
    n.event('a:1', 'fill')
    s.script = [requests.ConnectionError(leak), RuntimeError(leak)]
    with caplog.at_level(logging.DEBUG):
        n.pump()
        clock[0] += 2.5
        n.pump()
        clock[0] += 31
        s.script = [FakeResponse(200, None, bad_json=True)]
        n.pump()
    text = caplog.text + repr(n) + repr(n.api) + str(n.status()) + repr(store.outbox_rows())
    assert 'TOP_SECRET' not in text and TOKEN not in text
    assert 'failed (ConnectionError)' in caplog.text and 'failed (RuntimeError)' in caplog.text
    row = rows(store)['a:1']
    assert row['last_error'] == 'TELEGRAM_BAD_REPLY' and row['attempts'] == 3 and row['sent_ms'] is None
    api = TelegramAPI(TOKEN, CHAT, timeout=1, session=FakeSession())
    api._session.script = [requests.ConnectionError(leak)]
    with pytest.raises(TelegramError) as info:
        api.call('getMe')
    err = info.value
    assert str(err) == 'ConnectionError' and err.__context__ is None and err.__cause__ is None
    assert 'TOP_SECRET' not in repr(err)


def test_missing_credentials_never_touch_the_network(tmp_path, store, clock):
    n, s = make(tmp_path, store, token=None)
    n.event('a:1', 'x')
    n.pump()
    assert s.calls == [] and n.status()['last_error_type'] == 'MISSING_TELEGRAM_BOT_TOKEN'
    assert n.flush(5) == 1
    n2, s2 = make(tmp_path, store, chat=None)
    clock[0] += 3
    n2.pump()
    assert s2.calls == [] and n2.last_error == 'MISSING_TELEGRAM_CHAT_ID'


def test_unsupported_method_refused():
    api = TelegramAPI(TOKEN, CHAT, timeout=1, session=FakeSession())
    with pytest.raises(ValueError):
        api.call('deleteMessage')


# ----------------------------------------------------------------------------- flush / thread

def test_flush_drains_queue_in_order(tmp_path, store, clock, monkeypatch):
    monkeypatch.setattr(time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    n, s = make(tmp_path, store)
    for i in range(4):
        n.event(f'a:{i}', f'msg {i}')
    assert n.flush(15) == 0
    assert s.texts() == [f'[PAPER] msg {i}\n[event a:{i}]' for i in range(4)]


def test_flush_gives_up_when_backoff_exceeds_deadline(tmp_path, store, clock, monkeypatch):
    monkeypatch.setattr(time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    n, s = make(tmp_path, store)
    n.event('a:1', 'x')
    s.script = [FakeResponse(429, {'ok': False, 'parameters': {'retry_after': 60}})]
    started = clock[0]
    assert n.flush(5) == 1
    assert len(s.calls) == 1 and clock[0] - started < 5


def test_flush_with_frozen_clock_is_bounded_in_real_time(tmp_path, store, clock):
    n, s = make(tmp_path, store)
    n.event('a:1', 'one')
    n.event('a:2', 'two')                                    # needs 1.1 s of (frozen) monotonic time
    started = time.perf_counter()
    assert n.flush(0.3) == 1
    assert time.perf_counter() - started < 2


def test_background_thread_delivers_and_close_stops_it(tmp_path, store):
    session = FakeSession()
    n = Notifier(make_cfg(tmp_path), store, session=session, start_thread=True)
    try:
        with pytest.raises(RuntimeError):
            n.start()
        n.event('a:1', 'фон')
        deadline = time.monotonic() + 5
        while store.outbox_pending_count() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert '[PAPER] фон\n[event a:1]' in session.texts()
        assert n.healthy()
    finally:
        n.close()
    assert n._thread is None and session.closed is False     # an injected session is not ours to close


def test_close_closes_own_session(tmp_path, store, monkeypatch):
    class Own(FakeSession):
        pass
    own = Own()
    monkeypatch.setattr(requests, 'Session', lambda: own)
    n = Notifier(make_cfg(tmp_path), store, start_thread=False)
    n.event('a:1', 'x')
    n.pump()
    assert own.texts() and not own.closed
    n.close()
    assert own.closed


def test_event_never_raises(tmp_path, store, caplog):
    n, _ = make(tmp_path, store)
    store.close()
    with caplog.at_level(logging.WARNING):
        assert n.event('a:1', 'x') is False
    assert 'not queued' in caplog.text
    assert n.status()['status_error']


# ----------------------------------------------------------------------------- summary bucket

def test_periodic_summary_once_per_bucket_across_restarts(tmp_path, store, clock):
    n, _ = make(tmp_path, store, summary_minutes=60)
    built = []

    def build():
        built.append(1)
        return 'Сводка'

    t = 1_800_000_000_000
    assert n.maybe_summary(t, build) is True
    assert n.maybe_summary(t + 1000, build) is False
    n2, _ = make(tmp_path, store, summary_minutes=60)
    assert n2.maybe_summary(t + 2000, build) is False
    assert n2.maybe_summary(t + 3_600_000, build) is True
    assert built == [1, 1]
    keys = [r['key'] for r in store.outbox_rows()]
    assert keys == [f'summary:{t // 3_600_000}', f'summary:{t // 3_600_000 + 1}']
    n3, _ = make(tmp_path, store, summary_minutes=None)
    assert n3.maybe_summary(t + 7_200_000, build) is False


def test_summary_builder_failure_is_swallowed(tmp_path, store):
    n, _ = make(tmp_path, store)

    def broken():
        raise KeyError('x')

    assert n.maybe_summary(1_800_000_000_000, broken) is False
    assert store.outbox_rows() == []


# ----------------------------------------------------------------------------- chunking helper

def test_split_text_rejoins_exactly_and_respects_utf16_units():
    text = 'a' * 4100 + ' комиссия и позиция'
    parts = split_text(text)
    assert ''.join(parts) == text and max(len(p) for p in parts) <= 3900
    emoji = '😀' * 2500                                         # 2 UTF-16 units each
    parts = split_text(emoji)
    assert ''.join(parts) == emoji
    assert all(len(p.encode('utf-16-le')) // 2 <= 3900 for p in parts)
    assert split_text('short') == ['short']
    lines = 'x' * 3000 + '\n' + 'y' * 2000
    assert split_text(lines) == ['x' * 3000 + '\n', 'y' * 2000]


def test_event_key_unique_with_frozen_clock(clock):
    keys = {notify.event_key('state') for _ in range(50)}
    assert len(keys) == 50 and all(k.startswith('state:') for k in keys)
    assert notify.event_key('reject', 't-stk1-b1-x') == 'reject:t-stk1-b1-x'


# ----------------------------------------------------------------------------- message helpers

def test_message_texts_in_russian():
    assert notify.started_text(live=True, pair='STONK_USDT', levels={'buy': ['b1', 'b2'], 'sell': ['s1']}) == (
        '[LIVE] gate_stonk: STONK_USDT, уровни BUY b1, b2; SELL s1. Проверка сохранённых заявок и баланса. '
        'Доставка событий — минимум один раз.')
    assert notify.stopped_text('KeyboardInterrupt', live=True).startswith(
        '[LIVE] Процесс бота остановлен (KeyboardInterrupt). Сохранённые заявки отменяются; проверьте биржу.')
    assert notify.stopped_text('EXIT', live=False).startswith('[PAPER] Процесс бота остановлен (EXIT).')
    assert notify.state_text('STOP_TRADING', pair='STONK_USDT') == (
        'STONK_USDT: состояние STOP_TRADING — найден файл STOP_TRADING; удалите его, чтобы продолжить.')
    assert notify.state_text('BAD_MARKET:INSUFFICIENT_SOURCES:1/2', pair='P').startswith(
        'P: состояние BAD_MARKET:INSUFFICIENT_SOURCES:1/2 — индекс или стакан недоступен')
    assert notify.state_text('SOMETHING_NEW', pair='P') == 'P: состояние SOMETHING_NEW.'
    assert notify.resumed_text('CLOCK_SKEW', pair='P') == (
        'P: причина остановки устранена (CLOCK_SKEW); торговля возобновляется.')
    assert notify.unknown_text(side='buy', level='b1', qty=D('10'), price=D('1.98'), base='STONK', quote='USDT',
                               text='t-stk1-b1-abc') == (
        'STONK BUY [b1]: результат выставления неизвестен. 10 STONK по 1.98 USDT, резерв сохранён; '
        'повтор запрещён до сверки. text: t-stk1-b1-abc.')
    assert notify.reject_text(side='sell', level='s2', label='BALANCE_NOT_ENOUGH', message='Not  enough\nbalance',
                              qty=D('5'), price=D('2.5'), base='STONK', quote='USDT', retry_seconds=30) == (
        'STONK SELL [s2]: заявка отклонена (BALANCE_NOT_ENOUGH: Not enough balance). Размер 5 STONK, цена 2.5, '
        'сумма 12.50 USDT. Повтор этого уровня не раньше чем через 30 с.')
    assert notify.reject_text(side='buy', level='b1', label=None, message=None, qty=1, price=1, base='S',
                              quote='USDT').endswith('(без метки). Размер 1 S, цена 1, сумма 1.00 USDT. '
                                                     'Повтор на следующем шаге.')
    assert notify.cancel_text(side='buy', level='b1', reason='STOP_TRADING', result='unknown', base='STONK') == (
        'STONK BUY [b1]: отмена STOP_TRADING; ответ unknown. Резерв сохраняется до сверки.')
    assert notify.terminal_text(side='buy', level='b2', status='cancelled', executed=D('1.5'), qty=D('3'),
                                base='STONK', reason=None, finish_as='stp') == (
        'STONK BUY [b2]: cancelled (stp); исполнено 1.5/3 STONK; причина биржа. Резерв освобождён после сверки.')
    order = notify.order_text(side='buy', level='b1', qty=D('10'), price=D('1.98'), center=D('2'), lower=D('1.98'),
                              upper=D('1.98'), rule='FIXED_EDGE', base='STONK', quote='USDT', suppressed=3)
    assert order.splitlines() == [
        'STONK BUY [b1]: новая заявка 10 STONK по 1.98 = 19.80 USDT.',
        'Центр индекса 2; цена ниже центра на 1%; полоса 1.98–1.98; правило FIXED_EDGE.',
        'Перестановок без отдельного сообщения с прошлого уведомления: 3.']


def test_startup_sizes_text():
    text = notify.startup_sizes_text([
        {'side': 'buy', 'level': 'b1', 'qty': D('10'), 'price': D('1.98'), 'index': D('2')},
        {'side': 'sell', 'level': 's1', 'qty': D('1'), 'price': D('2.02'), 'below_minimum': True,
         'minimum_text': 'min_quote_amount 3 USDT'},
        {'side': 'sell', 'level': 's2', 'error': 'INSUFFICIENT_SOURCES:1/2'},
    ], base='STONK', quote='USDT')
    assert text.splitlines() == [
        'Размер заявок из конфига (до ограничений баланса и лимитов):',
        'BUY [b1]: 10 STONK × 1.98 = 19.80 USDT; по индексу ≈ 20.00 USDT.',
        'SELL [s1]: 1 STONK × 2.02 = 2.02 USDT. МЕНЬШЕ МИНИМУМА БИРЖИ (min_quote_amount 3 USDT).',
        'SELL [s2]: оценка недоступна (INSUFFICIENT_SOURCES:1/2); сообщу после восстановления котировок.']


def test_position_and_summary_texts():
    pos = Position(qty=D('10'), cost=D('19.5'), realized=D('1.25'), unmatched_qty=D('2'),
                   unmatched_proceeds=D('4.2'))
    balances = {'USDT': Balance(D('480.5'), D('19.8')), 'STONK': Balance(D('12'), D('2'))}
    t = 1_800_000_000_000
    text = notify.position_text(now_ms=t, base='STONK', quote='USDT', balances=balances, position=pos,
                                index=D('2'))
    assert text.splitlines() == [
        'После сверки исполнений (2027-01-15 08:00:00 UTC): USDT всего 480.50, свободно 460.70.',
        'STONK: остаток 12, свободно 10; оценка по индексу 24.00 USDT; куплено ботом 10 по средней 1.95, '
        'нереализовано +0.5 USDT; реализовано +1.25 USDT; продано из прежнего запаса 2 (себестоимость не учтена).']
    assert 'оценка по индексу недоступна' in notify.position_text(
        now_ms=t, base='STONK', quote='USDT', balances={}, position=Position(), index=None)
    summary = notify.summary_text(
        now_ms=t, pair='STONK_USDT', base='STONK', quote='USDT', state='RUNNING', balances=balances, position=pos,
        index=D('2'), orders=[{'side': 'buy', 'level': 'b1', 'qty': D('10'), 'price': D('1.98'),
                               'filled_qty': D('1')}],
        slot_states={'buy:b1': 'SLOT_ACTIVE', 'sell:s1': 'REJECT_BACKOFF'},
        day_buy=(D('1'), D('1.98')), day_sell=(D('0'), D('0')), initial_avg_price=D('1.5'))
    assert summary.splitlines() == [
        'Сводка STONK_USDT на 2027-01-15 08:00:00 UTC. Состояние: RUNNING.',
        'USDT: всего 480.50, свободно 460.70, в заявках BUY 17.82.',
        'STONK: 12 STONK ≈ 24.00 USDT (индекс 2); куплено ботом 10 по средней 1.95, нереализовано +0.5 USDT; '
        'реализовано +1.25 USDT; продано из прежнего запаса 2 (с учётом initial_avg_price всего +2.45 USDT).',
        'Заявки: BUY [b1] 9 по 1.98 (17.82 USDT).',
        'Ожидание: sell:s1 — REJECT_BACKOFF.',
        'Портфель ≈ 504.50 USDT.',
        'Реализовано ботом +1.25 USDT; нереализовано по купленному ботом +0.5 USDT.',
        'Сегодня (UTC): куплено на 1.98 USDT, продано на 0.00 USDT.']
    empty = notify.summary_text(now_ms=t, pair='P', base='STONK', quote='USDT', state=None, balances={},
                                position=Position(), index=None)
    assert 'Активных заявок нет.' in empty and 'Состояние: н/д.' in empty


def test_fill_text_is_reexported_from_store():
    from gate_stonk.store import fill_text
    assert notify.fill_text is fill_text


def test_worker_error_logged_once_per_type(tmp_path, store, caplog):
    n, _ = make(tmp_path, store)
    calls = []

    def broken():
        calls.append(1)
        if len(calls) >= 3:
            n._stop.set()
        raise sqlite3.OperationalError('database is locked')

    n.pump = broken
    with caplog.at_level(logging.ERROR, logger='gate_stonk.notify'):
        n._run()
    assert len(calls) == 3
    assert caplog.text.count('Telegram worker cannot advance: OperationalError') == 1
    assert n.delivery_failed and n.status()['last_error_type'] == 'OperationalError'
