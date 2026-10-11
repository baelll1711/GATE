"""``smoke-test`` (gate_stonk.smoke) offline: the real CLI, GateVenue and GateRestClient over a
stateful fake Gate spot REST (``FakeGate``), a FakeHub for the index and a fake clock.

Every reply here is SYNTHETIC; the shapes follow tests/fixtures/gate (themselves synthetic).
"""
from __future__ import annotations

import copy
import json
import re
import signal
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pytest
import yaml

from gate_stonk.cli import Deps, ProcessLocks, main
from gate_stonk.config import load_config
from gate_stonk.errors import NotSent, Rejected
from gate_stonk.gate_rest import EXPTIME_HEADER, GateRestClient
from gate_stonk.smoke import (FALLBACK_QUOTE_MIN, MIN_BID_GAP_PCT, RecordingSession, SmokeOptions, option_refusal,
                              price_base, price_guard, sanitize, smoke_price, smoke_quantity)
from gate_stonk.types import Book
from gate_stonk.venue import parse_pair_meta

from cli_fakes import FakeHub, GATE_PAIR, Output, advance, live_env, make_live, write_config

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / 'tests' / 'fixtures' / 'gate'
PAIR = 'STONK_USDT'
KEY, SECRET, TG = 'key-ABCDEFGH-123456', 'secret-ZYXWVUT-987654', '12345:token-QWERTY'
CENTER = Decimal('0.1356')
EXPECTED_PRICE = Decimal('0.12204')        # floor(0.1356 * 0.9) at precision 5
EXPECTED_QTY = Decimal('25.1')             # ceil(3 * 1.02 / 0.12204) at amount_precision 1


def fx(name: str):
    return copy.deepcopy(json.loads((FIX / name).read_text(encoding='utf-8'))['data'])


# ============================================================================ fake Gate

class Resp:
    def __init__(self, status: int, body=None, *, raw: bytes | None = None, headers: dict | None = None):
        self.status_code = status
        self.content = raw if raw is not None else json.dumps(body).encode('utf-8')
        self.headers = headers or {}


class FakeGate:
    """Stateful Gate spot REST for one pair. ``create``: open | poc_cancelled | reject_poc |
    reject_balance | unknown (order created, reply lost) | unknown_lost (nothing created)."""

    def __init__(self, *, create: str = 'open', cancel_effect: bool = True, dms_fires: bool = True,
                 usdt: str = '100', skew_ms: int = 0, zero_reply=None, text_after_finish: bool = False,
                 key_mode: int = 1) -> None:
        self.create = create
        self.cancel_effect = cancel_effect
        self.dms_fires = dms_fires
        self.usdt = Decimal(usdt)
        self.skew_ms = skew_ms
        self.zero_reply = {'triggerTime': 0} if zero_reply is None else zero_reply
        self.text_after_finish = text_after_finish
        self.key_mode = key_mode
        self.calls: list[dict] = []
        self.orders: dict[str, dict] = {}
        self.foreign: list[dict] = []          # open rows of our pair that are not ours
        self.other_pairs: list[dict] = []      # open rows of other pairs
        self.dms_trigger: int | None = None
        self.dms_log: list[tuple[str, int]] = []
        self.next_id = 912345678901
        self.hooks: dict = {}                  # (METHOD, route) -> callable(call) -> Resp | None

    # ----- helpers
    @staticmethod
    def now_ms() -> int:
        import time
        return int(time.time() * 1000)

    def row(self, oid: str, text: str, price: str, amount: str, *, pair: str = PAIR, status: str = 'open',
            finish_as: str = 'open') -> dict:
        now = self.now_ms()
        row = fx('order_open.json')
        row.update(id=oid, text=text, currency_pair=pair, price=price, amount=amount, left=amount, status=status,
                   finish_as=finish_as, create_time_ms=now, update_time_ms=now, create_time=str(now // 1000),
                   update_time=str(now // 1000))
        return row

    def open_rows(self) -> list[dict]:
        return [r for r in self.orders.values() if r['status'] == 'open'] + list(self.foreign)

    def locked(self) -> Decimal:
        return sum((Decimal(r['price']) * Decimal(r['left']) for r in self.orders.values() if r['status'] == 'open'),
                   Decimal(0))

    def finish(self, row: dict, finish_as: str, at_ms: int | None = None) -> None:
        at = at_ms or self.now_ms()
        row.update(status='cancelled', finish_as=finish_as, update_time_ms=at, update_time=str(at // 1000))

    def _fire_dms(self) -> None:
        if self.dms_trigger is not None and self.dms_fires and self.now_ms() >= self.dms_trigger:
            for row in self.open_rows():
                self.finish(row, 'cancelled', self.dms_trigger)
            self.dms_log.append(('fired', self.dms_trigger))
            self.dms_trigger = None

    def creates(self) -> list[dict]:
        return [c for c in self.calls if c['method'] == 'POST' and c['path'] == '/spot/orders']

    def countdowns(self) -> list[int]:
        return [c['body']['timeout'] for c in self.calls if c['path'] == '/spot/countdown_cancel_all']

    def writes(self) -> list[dict]:
        return [c for c in self.calls if c['method'] != 'GET']

    def index_of(self, predicate) -> int:
        return next(i for i, c in enumerate(self.calls) if predicate(c))

    # ----- session protocol
    def request(self, method, url, data=None, headers=None, timeout=None, allow_redirects=True):
        parts = urlsplit(url)
        path = parts.path[len('/api/v4'):]
        call = {'method': method, 'path': path, 'params': dict(parse_qsl(parts.query)),
                'body': None if data is None else json.loads(data), 'headers': dict(headers or {})}
        self.calls.append(call)
        self._fire_dms()
        route = re.sub(r'/spot/orders/[^/]+$', '/spot/orders/{id}', path)
        hook = self.hooks.get((method, route))
        if hook is not None:
            answer = hook(call)
            if isinstance(answer, BaseException):
                raise answer
            if answer is not None:
                return answer
        return self.route(method, route, path, call)

    def close(self) -> None:
        pass

    def route(self, method: str, route: str, path: str, call: dict) -> Resp:
        p, body = call['params'], call['body']
        if (method, route) == ('GET', '/spot/time'):
            return Resp(200, {'server_time': self.now_ms() + self.skew_ms})
        if (method, route) == ('GET', f'/spot/currency_pairs/{PAIR}'):
            return Resp(200, dict(GATE_PAIR))
        if (method, route) == ('GET', '/account/detail'):
            detail = fx('account_detail_classic.json')
            detail['key']['mode'] = self.key_mode
            detail['ip_whitelist'] = ['203.0.113.7']
            return Resp(200, detail)
        if (method, route) == ('GET', '/unified/unified_mode'):
            return Resp(200, {'mode': 'classic'})
        if (method, route) == ('GET', '/wallet/fee'):
            return Resp(200, fx('wallet_fee.json'))
        if (method, route) == ('GET', '/account/debit_fee'):
            return Resp(200, {'enabled': False})
        if (method, route) == ('GET', '/spot/accounts'):
            locked = self.locked()
            return Resp(200, [{'currency': 'USDT', 'available': format(self.usdt - locked, 'f'),
                               'locked': format(locked, 'f'), 'update_id': 7},
                              {'currency': 'STONK', 'available': '0', 'locked': '0', 'update_id': 3}])
        if (method, route) == ('GET', '/spot/open_orders'):
            if p.get('page') != '1':
                return Resp(200, [])
            groups: dict[str, list] = {}
            for row in self.other_pairs + self.open_rows():
                groups.setdefault(row['currency_pair'], []).append(row)
            return Resp(200, [{'currency_pair': k, 'total': len(v), 'orders': v} for k, v in groups.items()])
        if (method, route) == ('GET', '/spot/orders'):
            if p.get('page') != '1':
                return Resp(200, [])
            if p.get('status') == 'open':
                return Resp(200, self.open_rows())
            low, high = int(p['from']) * 1000, int(p['to']) * 1000 + 999
            return Resp(200, [r for r in self.orders.values()
                              if r['status'] != 'open' and low <= r['update_time_ms'] <= high])
        if (method, route) == ('GET', '/spot/orders/{id}'):
            ident = path.rsplit('/', 1)[1]
            row = self.orders.get(ident)
            if row is None and ident.startswith('t-'):
                row = next((r for r in self.orders.values() if r['text'] == ident
                            and (r['status'] == 'open' or self.text_after_finish)), None)
            if row is None:
                return Resp(404, {'label': 'ORDER_NOT_FOUND', 'message': 'Order not found'})
            return Resp(200, row)
        if (method, route) == ('GET', '/spot/my_trades'):
            return Resp(200, [])
        if (method, route) == ('POST', '/spot/orders'):
            return self.create_order(body)
        if (method, route) == ('POST', '/spot/cancel_batch_orders'):
            out = []
            for item in body:
                row = self.orders.get(str(item['id'])) or next(
                    (r for r in self.orders.values() if r['text'] == item['id']), None)
                if row is not None and row['status'] == 'open':
                    if self.cancel_effect:
                        self.finish(row, 'cancelled')
                    out.append({'currency_pair': PAIR, 'id': row['id'], 'text': row['text'], 'succeeded': True,
                                'label': '', 'message': '', 'account': ''})
                else:
                    out.append({'currency_pair': PAIR, 'id': str(item['id']), 'text': '', 'succeeded': False,
                                'label': 'ORDER_NOT_FOUND', 'message': 'Order not found', 'account': ''})
            return Resp(200, out)
        if (method, route) == ('DELETE', '/spot/orders/{id}'):
            exptime = int(call['headers'][EXPTIME_HEADER])
            if exptime < self.now_ms():
                return Resp(400, {'label': 'FAKE_REQUEST_EXPIRED', 'message': 'request expired (fake)'})
            return Resp(404, {'label': 'ORDER_NOT_FOUND', 'message': 'Order not found'})
        if (method, route) == ('POST', '/spot/countdown_cancel_all'):
            timeout = body['timeout']
            assert body['currency_pair'] == PAIR
            if timeout == 0:
                self.dms_trigger = None
                self.dms_log.append(('disarm', self.now_ms()))
                return Resp(200, self.zero_reply)
            self.dms_trigger = self.now_ms() + timeout * 1000
            self.dms_log.append(('arm', timeout))
            return Resp(200, {'triggerTime': self.dms_trigger})
        return Resp(404, {'label': 'NOT_FOUND', 'message': f'no route {method} {path}'})

    def create_order(self, body: dict) -> Resp:
        assert body['time_in_force'] == 'poc' and 'iceberg' not in body and body['side'] == 'buy'
        mode = self.create
        if mode == 'reject_poc':
            return Resp(400, {'label': 'POC_FILL_IMMEDIATELY',
                              'message': 'Order would match and take immediately so its cancelled'})
        if mode == 'reject_balance':
            return Resp(400, {'label': 'BALANCE_NOT_ENOUGH', 'message': 'Not enough balance'})
        if mode == 'unknown_lost':
            return Resp(502, raw=b'<html>bad gateway</html>')
        oid = str(self.next_id)
        self.next_id += 1
        row = self.row(oid, body['text'], body['price'], body['amount'])
        row['account'] = body['account']
        self.orders[oid] = row
        if mode == 'poc_cancelled':
            self.finish(row, 'poc')
        if mode == 'unknown':
            return Resp(504, raw=b'gateway timeout')
        return Resp(201, row, headers={'X-Gate-RateLimit-Requests-Remain': '9', 'X-Gate-RateLimit-Limit': '10'})


# ============================================================================ harness

def _live(data: dict) -> None:
    make_live(data)


def setup(tmp_path, monkeypatch, *, mutate=None, env: bool = True, confirm: bool = True) -> Path:
    def change(data):
        _live(data)
        if mutate is not None:
            mutate(data)
    if env:
        live_env(monkeypatch, confirm=confirm)
    return write_config(tmp_path, change, two_sources=True)


def make_hub(clock, *, center=CENTER, spot=(Decimal('0.1350'), Decimal('0.1360'))):
    return FakeHub(clock, {'bybit_perp': center, 'bitget_perp': center}, spot=spot)


def smoke(path: Path, clock, gate: FakeGate, *extra: str, hub=None, live: bool = True, signals: bool = False):
    out = Output()
    hub = hub or make_hub(clock)
    deps = Deps(out=out, err=out, http=gate, hub_factory=lambda cfg: hub, sleep=advance(clock), logging=False,
                signals=signals)
    args = ['smoke-test', '--config', str(path), *(['--live'] if live else []), *extra]
    return main(args, deps=deps), out, hub


def load_report(tmp_path: Path) -> tuple[dict, str]:
    reports = sorted((tmp_path / 'state' / 'smoke').glob('smoke_*.json'))
    assert len(reports) == 1, reports
    text = reports[0].read_text(encoding='utf-8')
    return json.loads(text), text


def step(report: dict, name: str) -> dict:
    return next(s for s in report['steps'] if s['name'] == name)


def assert_no_secrets(text: str) -> None:
    for secret in (KEY, SECRET, TG, '10406147', '203.0.113.7'):
        assert secret not in text
    assert not re.search(r'[0-9a-f]{64,}', text)        # no signature


# ============================================================================ happy path

def test_happy_path_one_order_dms_first_cancel_verify_disarm(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, hub = smoke(path, clock, gate)
    assert rc == 0, out.text
    # exactly one create, post-only, far, minimal, no iceberg, our text
    creates = gate.creates()
    assert len(creates) == 1
    body = creates[0]['body']
    assert body['time_in_force'] == 'poc' and body['side'] == 'buy' and body['account'] == 'spot'
    assert Decimal(body['price']) == EXPECTED_PRICE and Decimal(body['amount']) == EXPECTED_QTY
    assert 'iceberg' not in body and body['text'].startswith('t-stk1-smk-')
    assert Decimal(body['price']) <= CENTER * Decimal('0.9')
    # DMS armed before the create, renewed once, disarmed after the cancel; final read last
    i_arm = gate.index_of(lambda c: c['path'] == '/spot/countdown_cancel_all')
    i_create = gate.index_of(lambda c: c['method'] == 'POST' and c['path'] == '/spot/orders')
    i_cancel = gate.index_of(lambda c: c['path'] == '/spot/cancel_batch_orders')
    i_disarm = gate.index_of(lambda c: c['path'] == '/spot/countdown_cancel_all' and c['body']['timeout'] == 0)
    assert i_arm < i_create < i_cancel < i_disarm
    assert gate.countdowns() == [45, 45, 0]
    last = gate.calls[-1]
    assert last['method'] == 'GET' and last['path'] == '/spot/orders' and last['params']['status'] == 'open'
    # no write before the read-only prechecks
    first_write = gate.index_of(lambda c: c['method'] != 'GET')
    reads_before = {c['path'] for c in gate.calls[:first_write]}
    assert {'/spot/time', f'/spot/currency_pairs/{PAIR}', '/account/detail', '/spot/orders', '/spot/open_orders',
            '/spot/accounts'} <= reads_before
    # cancel by order id; nothing left open, DMS off
    cancel = next(c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders')
    oid = next(iter(gate.orders))
    assert cancel['body'] == [{'currency_pair': PAIR, 'id': oid, 'account': 'spot'}]
    assert not gate.open_rows() and gate.dms_trigger is None
    assert hub.started == 1 and hub.stopped == 1
    # verification reads after placing
    after = gate.calls[i_create + 1:i_cancel]
    assert any(c['path'] == f'/spot/orders/{oid}' for c in after)
    assert any(c['path'] == f"/spot/orders/{body['text']}" for c in after)
    assert any(c['path'] == '/spot/accounts' for c in after)
    after_cancel = gate.calls[i_cancel + 1:i_disarm]
    assert any(c['path'] == f'/spot/orders/{oid}' for c in after_cancel)
    assert any(c['path'] == '/spot/my_trades' and c['params'].get('order_id') == oid for c in after_cancel)
    assert any(c['path'] == '/spot/orders' and c['params'].get('status') == 'finished' for c in after_cancel)
    # human output
    assert 'ПРОЙДЕН' in out.text and 'ОШИБКА' not in out.text
    assert '--- Статус smoke-заявки ---' in out.text and 'JSON отчёта:' in out.text
    assert re.search(r'^ ?\d+\. Создание post-only BUY: принята.* — OK$', out.text, re.M)
    # report
    report, text = load_report(tmp_path)
    assert report['result'] == 'PASS' and report['exit_code'] == 0
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'
    assert all(s['status'] in ('PASS', 'INFO') for s in report['steps'])
    facts = report['facts']
    assert facts['finish_as_values'] == {'main_create': 'open', 'after_cancel': 'cancelled'}
    assert facts['create_reply']['account_echo'] == 'spot' and facts['create_reply']['time_in_force_echo'] == 'poc'
    assert facts['create_reply']['http_status'] == 201 and facts['amend_text_default'] == '-'
    assert facts['get_order_by_text_while_open']['found'] is True
    assert facts['get_order_by_text_after_finish'] == {'found': False, 'status': None, 'order_id_matches': None,
                                                       'http_status': 404, 'label': 'ORDER_NOT_FOUND'}
    assert facts['finished_orders_window_contains_order']['contains'] is True
    zero = facts['countdown_zero_reply'][0]
    assert zero['http_status'] == 200 and zero['trigger_time_present'] is True and zero['reply'] == {'triggerTime': 0}
    assert facts['rate_limit_headers']['seen'] is True
    assert facts['post_only_rejection_path'] == 'не наблюдался'
    assert facts['exptime_expired'] == {'probed': False} and facts['dms_fire'] == {'tested': False}
    assert facts['cancel_batch_item']['outcome'] == 'sent' and facts['cancel_batch_item']['item']['succeeded'] is True
    # requests recorded with step tags, latency, no request headers
    requests = report['requests']
    assert len(requests) == len(gate.calls)
    create_rec = next(r for r in requests if r['method'] == 'POST' and r['endpoint'] == '/spot/orders')
    assert create_rec['http_status'] == 201 and create_rec['latency_ms'] is not None
    assert create_rec['exptime_offset_ms'] == 10_000 and create_rec['signed'] is True
    assert create_rec['rate_limit_headers'] == {'x-gate-ratelimit-requests-remain': '9',
                                                'x-gate-ratelimit-limit': '10'}
    assert all('headers' not in r for r in requests)
    assert step(report, 'create')['requests'] == [create_rec]
    detail = next(r for r in requests if r['endpoint'] == '/account/detail')
    assert detail['reply']['user_id'] == '***' and detail['reply']['ip_whitelist'] == ['***']
    assert_no_secrets(text)
    # fixtures
    fixtures = tmp_path / 'state' / 'smoke' / 'fixtures'
    assert (fixtures / 'README.md').is_file()
    files = sorted(fixtures.glob('*/*.json'))
    assert len(files) == sum(1 for r in requests if r['reply'] is not None)
    for file in files:
        raw = file.read_text(encoding='utf-8')
        assert_no_secrets(raw)
        data = json.loads(raw)
        assert data['_comment'].startswith('RECORDED') and 'data' in data
    assert any('post_spot_orders' in f.name for f in files)


def test_telegram_is_not_required(tmp_path, monkeypatch, clock):
    def no_telegram(data):
        data['telegram']['enabled'] = False
    path = setup(tmp_path, monkeypatch, mutate=no_telegram)
    monkeypatch.delenv('TELEGRAM_BOT_TOKEN')
    rc, out, _ = smoke(path, clock, FakeGate())
    assert rc == 0, out.text


# ============================================================================ refusals

GATES = {
    'live_without_flag': ({}, False, 'LIVE_CONFIG_REQUIRES_EXPLICIT_RUN_LIVE_FLAG'),
    'flag_with_paper': ({'paper': True}, True, 'LIVE_FLAG_REQUIRES_APP_MODE_LIVE'),
    'paper_without_flag': ({'paper': True}, False, 'SMOKE_TEST_LIVE_ONLY'),
    'missing_confirm': ({'confirm': False}, True, 'LIVE_CONFIRM_MISSING'),
    'confirm_in_env_file': ({'dotenv': True}, True, 'LIVE_CONFIRM_IN_ENV_FILE'),
    'no_ack': ({'ack': False}, True, 'DEDICATED_ACCOUNT_ACK_REQUIRED'),
    'missing_keys': ({'keys': False}, True, 'GATE_API_KEYS_MISSING'),
}


@pytest.mark.parametrize('case', sorted(GATES))
def test_live_gates_refuse_before_any_file_or_request(tmp_path, monkeypatch, clock, case):
    opts, flag, code = GATES[case]

    def mutate(data):
        if opts.get('paper'):
            data['app']['mode'] = 'paper'
        if opts.get('ack') is False:
            data['safety']['dedicated_account_ack'] = False
    path = setup(tmp_path, monkeypatch, mutate=mutate, confirm=opts.get('confirm', True))
    if opts.get('keys') is False:
        monkeypatch.delenv('GATE_API_KEY')
    if opts.get('dotenv'):
        (tmp_path / '.env').write_text('gate_live_confirm=I_UNDERSTAND_SPOT_TRADING\n', encoding='utf-8')
    gate = FakeGate()
    rc, out, hub = smoke(path, clock, gate, live=flag)
    assert rc == 2
    assert code in out.text and 'Smoke-тест отклонён' in out.text
    assert gate.calls == [] and hub.started == 0
    assert not (tmp_path / 'state').exists()


def test_gate_order_matches_run(tmp_path, monkeypatch, clock):
    """Missing confirmation is reported before the missing ack, like run."""
    path = setup(tmp_path, monkeypatch, mutate=lambda d: d['safety'].update(dedicated_account_ack=False),
                 confirm=False)
    rc, out, _ = smoke(path, clock, FakeGate())
    assert rc == 2 and 'LIVE_CONFIRM_MISSING' in out.text and 'DEDICATED_ACCOUNT_ACK_REQUIRED' not in out.text


@pytest.mark.parametrize('extra, code', [
    (['--edge-pct', '9.99'], 'SMOKE_EDGE_TOO_SMALL'),
    (['--edge-pct', '-5'], 'SMOKE_EDGE_TOO_SMALL'),
    (['--edge-pct', 'abc'], 'SMOKE_ARGUMENT_INVALID'),
    (['--max-notional', '0'], 'SMOKE_MAX_NOTIONAL_INVALID'),
    (['--max-notional', '1000'], 'SMOKE_MAX_NOTIONAL_INVALID'),
    (['--index-seconds', '0'], 'SMOKE_INDEX_SECONDS_INVALID'),
])
def test_argument_refusals(tmp_path, monkeypatch, clock, extra, code):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, *extra)
    assert rc == 2 and code in out.text
    assert gate.calls == [] and not (tmp_path / 'state').exists()


def test_level_id_conflict_refuses(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch, mutate=lambda d: d['pair']['buy']['levels'][1].update(id='smk'))
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 2 and 'SMOKE_LEVEL_ID_CONFLICT' in out.text and gate.calls == []


def test_lock_held_refuses(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    cfg = load_config(path, read_env=False)
    gate = FakeGate()
    with ProcessLocks(cfg, live=True):
        rc, out, _ = smoke(path, clock, gate)
    assert rc == 2 and 'LOCK_HELD' in out.text and gate.calls == []


def _refused(tmp_path, monkeypatch, clock, gate, *extra, hub=None, mutate=None):
    path = setup(tmp_path, monkeypatch, mutate=mutate)
    rc, out, hub = smoke(path, clock, gate, *extra, hub=hub)
    assert rc == 2, out.text
    assert gate.writes() == []                       # nothing written: no DMS, no order, no cancel
    report, text = load_report(tmp_path)
    assert report['result'] == 'REFUSED' and report['exit_code'] == 2
    assert report['dms_state'] == 'never_armed' and report['orders'] == []
    assert_no_secrets(text)
    assert 'Заявки не отправлялись' in out.text
    return report, out


def test_refuses_when_pair_not_empty(tmp_path, monkeypatch, clock):
    gate = FakeGate()
    gate.foreign.append(gate.row('777000000001', 't-other-1', '0.1', '50'))
    report, out = _refused(tmp_path, monkeypatch, clock, gate)
    assert report['refusal']['code'] == 'SMOKE_PAIR_NOT_EMPTY' and 'SMOKE_PAIR_NOT_EMPTY' in out.text
    assert step(report, 'pair_open_orders')['status'] == 'REFUSED'
    assert gate.foreign[0]['status'] == 'open'           # a foreign order is never touched


def test_refuses_when_account_not_empty(tmp_path, monkeypatch, clock):
    gate = FakeGate()
    gate.other_pairs.append(gate.row('777000000002', 't-x', '50000', '0.001', pair='BTC_USDT'))
    report, _ = _refused(tmp_path, monkeypatch, clock, gate)
    assert report['refusal']['code'] == 'SMOKE_ACCOUNT_NOT_EMPTY'
    assert report['prechecks']['account_open_orders']['pairs'] == ['BTC_USDT']


@pytest.mark.parametrize('kind', ['sources', 'spot'])
def test_refuses_when_index_or_book_invalid(tmp_path, monkeypatch, clock, kind):
    if kind == 'sources':
        hub = FakeHub(clock, {'bybit_perp': None, 'bitget_perp': None})
    else:
        hub = make_hub(clock, spot=None)
    report, _ = _refused(tmp_path, monkeypatch, clock, FakeGate(), '--index-seconds', '5', hub=hub)
    assert report['refusal']['code'] == 'SMOKE_INDEX_INVALID'
    assert report['prechecks']['index']['waited_seconds'] >= 5
    assert hub.stopped == 1
    assert ('SPOT_BOOK' if kind == 'spot' else 'INDEX') in report['prechecks']['index']['problem']


def test_refuses_notional_above_cap(tmp_path, monkeypatch, clock):
    report, out = _refused(tmp_path, monkeypatch, clock, FakeGate(), '--max-notional', '3')
    assert report['refusal']['code'] == 'SMOKE_NOTIONAL_CAP' and '3.06' in out.text


def test_refuses_insufficient_usdt(tmp_path, monkeypatch, clock):
    report, _ = _refused(tmp_path, monkeypatch, clock, FakeGate(usdt='3'))
    assert report['refusal']['code'] == 'SMOKE_INSUFFICIENT_USDT'


def test_refuses_clock_skew_and_account_mode(tmp_path, monkeypatch, clock):
    report, _ = _refused(tmp_path, monkeypatch, clock, FakeGate(skew_ms=5000))
    assert report['refusal']['code'] == 'CLOCK_SKEW'


def test_refuses_account_mode_mismatch(tmp_path, monkeypatch, clock):
    report, _ = _refused(tmp_path, monkeypatch, clock, FakeGate(key_mode=2))
    assert report['refusal']['code'] == 'ACCOUNT_MODE_MISMATCH:unified'


def test_refuses_when_price_guard_fails_right_before_create(tmp_path, monkeypatch, clock):
    """The index moved down between the plan and the create: refused, DMS disarmed after the
    pair was read empty, no order sent."""
    calls = {'n': 0}

    def center(elapsed):
        calls['n'] += 1
        return CENTER if calls['n'] <= 2 else Decimal('0.12')     # one sample (two sources) for the plan
    hub = FakeHub(clock, {'bybit_perp': center, 'bitget_perp': center})
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, hub=hub)
    assert rc == 2, out.text
    assert gate.creates() == [] and gate.countdowns() == [45, 0]
    report, _ = load_report(tmp_path)
    assert report['refusal']['code'] == 'SMOKE_PRICE_GUARD' and report['dms_state'] == 'disarmed'
    assert step(report, 'cleanup')['status'] == 'PASS'


def _moving_hub(clock, later: Decimal) -> FakeHub:
    calls = {'n': 0}

    def center(elapsed):
        calls['n'] += 1
        return CENTER if calls['n'] <= 2 else later              # one sample (two sources) for the plan
    return FakeHub(clock, {'bybit_perp': center, 'bitget_perp': center})


def test_small_index_drop_before_create_lowers_the_price_instead_of_refusing(tmp_path, monkeypatch, clock):
    """An ordinary tick down between the plan and the create: the price is recomputed from the
    fresh index (only ever lower), the run passes and the read checks expect the sent price."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, hub=_moving_hub(clock, Decimal('0.13555')))
    assert rc == 0, out.text
    creates = gate.creates()
    assert len(creates) == 1
    lowered = Decimal('0.12199')                                   # floor(0.13555 * 0.9) at precision 5
    assert Decimal(creates[0]['body']['price']) == lowered < EXPECTED_PRICE
    assert Decimal(creates[0]['body']['amount']) == EXPECTED_QTY   # the planned quantity is kept
    report, _ = load_report(tmp_path)
    assert Decimal(report['plan']['price']) == lowered
    assert Decimal(report['plan']['index']) == Decimal('0.13555')
    fact = report['facts']['price_lowered_before_create']
    assert len(fact) == 1 and Decimal(fact[0]['planned_price']) == EXPECTED_PRICE
    assert Decimal(fact[0]['price']) == lowered
    assert lowered <= Decimal('0.13555') * Decimal('0.9')
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'


def test_index_rise_before_create_never_raises_the_price(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, hub=_moving_hub(clock, Decimal('0.1370')))
    assert rc == 0, out.text
    assert [Decimal(c['body']['price']) for c in gate.creates()] == [EXPECTED_PRICE]
    report, _ = load_report(tmp_path)
    assert 'price_lowered_before_create' not in report['facts']


def test_lower_price_below_the_pair_quote_minimum_refuses(tmp_path, monkeypatch, clock):
    """A drop large enough that the planned quantity no longer meets min_quote_amount at the
    lower price is still a refusal before any order (exit 2)."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, hub=_moving_hub(clock, Decimal('0.1310')))
    assert rc == 2, out.text
    assert gate.creates() == []
    report, _ = load_report(tmp_path)
    assert report['refusal']['code'] == 'SMOKE_PRICE_GUARD'
    assert step(report, 'pre_create_guard')['data']['guard'] == 'QTY_BELOW_PAIR_MINIMUM_AT_LOWER_PRICE'


# ============================================================================ create outcomes

@pytest.mark.parametrize('mode, label, path_text', [
    ('reject_poc', 'POC_FILL_IMMEDIATELY', 'POC_FILL_IMMEDIATELY'),
    ('reject_balance', 'BALANCE_NOT_ENOUGH', 'не наблюдался'),
])
def test_create_rejected(tmp_path, monkeypatch, clock, mode, label, path_text):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(create=mode)
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1, out.text
    assert len(gate.creates()) == 1
    assert not any(c['path'] == '/spot/cancel_batch_orders' for c in gate.calls)
    assert gate.countdowns() == [45, 0]                  # disarmed only after the pair was read empty
    i_disarm = gate.index_of(lambda c: c['path'] == '/spot/countdown_cancel_all' and c['body']['timeout'] == 0)
    assert gate.calls[i_disarm - 1]['params'].get('status') == 'open'
    report, text = load_report(tmp_path)
    assert report['result'] == 'FAIL' and report['dms_state'] == 'disarmed' and report['pair_verified_empty']
    assert step(report, 'create')['status'] == 'FAIL'
    assert report['facts']['create_rejections'][0]['label'] == label
    assert report['facts']['create_rejections'][0]['http_status'] == 400
    assert path_text in report['facts']['post_only_rejection_path']
    assert 'ОШИБКА' in out.text and 'НЕ ПРОЙДЕН' in out.text
    assert_no_secrets(text)


def test_create_reply_cancelled_poc_is_the_second_post_only_path(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(create='poc_cancelled')
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    assert not any(c['path'] == '/spot/cancel_batch_orders' for c in gate.calls)
    report, _ = load_report(tmp_path)
    assert 'finish_as=poc' in report['facts']['post_only_rejection_path']
    assert report['facts']['finish_as_values']['main_create'] == 'poc'
    assert report['dms_state'] == 'disarmed' and report['pair_verified_empty'] is True


def test_create_unknown_then_found_by_text_and_cancelled(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(create='unknown')
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1, out.text                              # the create step itself failed
    assert len(gate.creates()) == 1                       # never re-sent
    oid = next(iter(gate.orders))
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert len(cancels) == 1 and cancels[0]['body'][0]['id'] == oid
    assert gate.orders[oid]['status'] == 'cancelled' and gate.dms_trigger is None
    report, _ = load_report(tmp_path)
    create = step(report, 'create')
    assert create['status'] == 'FAIL' and 'найдена по text' in create['detail']
    assert step(report, 'verify_cancel')['status'] == 'PASS'
    assert report['dms_state'] == 'disarmed' and report['pair_verified_empty'] is True


def test_create_reply_unparseable_is_recorded_and_order_found_by_text(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()

    def odd_reply(call):
        reply = gate.create_order(call['body'])
        row = json.loads(reply.content)
        row.pop('status')
        return Resp(201, row)
    gate.hooks[('POST', '/spot/orders')] = odd_reply
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1 and len(gate.creates()) == 1
    report, _ = load_report(tmp_path)
    unparsed = report['facts']['create_replies_unparsed'][0]
    assert unparsed['http_status'] == 201 and 'CREATE_REPLY_INVALID' in unparsed['error']
    assert 'status' not in unparsed['reply']
    assert report['dms_state'] == 'disarmed' and report['pair_verified_empty'] is True
    assert all(r['status'] == 'cancelled' for r in gate.orders.values())


def test_create_unknown_never_found_waits_for_the_exptime_window(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(create='unknown_lost')
    started = clock[0]
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    assert len(gate.creates()) == 1
    assert clock[0] - started >= 10 + 30                  # request_expiry + unknown_create_margin
    report, _ = load_report(tmp_path)
    assert report['orders'][0]['state'] == 'not_found'
    assert report['dms_state'] == 'disarmed' and report['pair_verified_empty'] is True


# ============================================================================ verification failures

def test_cancel_without_effect_keeps_dms_armed_and_fails(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(cancel_effect=False, dms_fires=False)
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    assert 0 not in gate.countdowns()                     # never disarmed while our order may be open
    assert gate.dms_trigger is not None
    report, _ = load_report(tmp_path)
    assert report['dms_state'] == 'left_armed' and report['pair_verified_empty'] is False
    assert step(report, 'verify_cancel')['status'] == 'FAIL'
    assert step(report, 'cleanup')['status'] == 'FAIL'
    assert 'ОСТАВЛЕН ВКЛЮЧЁННЫМ' in out.text
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert len(cancels) > 1                               # cleanup retried the cancel by id
    assert all(item['id'] == next(iter(gate.orders)) for c in cancels for item in c['body'])


def test_unreadable_open_orders_after_cancel_keeps_dms_armed(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    state = {'cancelled': False}

    def cancel_hook(call):
        state['cancelled'] = True
        return None

    def open_hook(call):
        if state['cancelled'] and call['params'].get('status') == 'open':
            return Resp(500, {'label': 'SERVER_ERROR', 'message': 'boom'})
        return None
    gate.hooks[('POST', '/spot/cancel_batch_orders')] = cancel_hook
    gate.hooks[('GET', '/spot/orders')] = open_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    assert 0 not in gate.countdowns()
    report, _ = load_report(tmp_path)
    assert report['dms_state'] == 'left_armed'
    assert 'НЕ подтверждена пустой' in out.text


def test_verify_open_mismatch_fails_but_still_cancels_and_disarms(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()

    def get_hook(call):
        ident = call['path'].rsplit('/', 1)[1]
        row = gate.orders.get(ident)
        if row is not None and row['status'] == 'open':
            return Resp(200, dict(row, price='0.12205'))      # price differs from what we sent
        return None
    gate.hooks[('GET', '/spot/orders/{id}')] = get_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    report, _ = load_report(tmp_path)
    assert step(report, 'verify_open')['status'] == 'FAIL' and 'цена' in step(report, 'verify_open')['detail']
    assert step(report, 'verify_cancel')['status'] == 'PASS'
    assert report['dms_state'] == 'disarmed' and report['pair_verified_empty'] is True


def test_ctrl_c_while_the_order_is_open_cancels_it_then_disarms(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    fired = {'n': 0}

    def interrupt_once(call):
        if fired['n'] == 0:
            fired['n'] += 1
            raise KeyboardInterrupt()
        return None
    gate.hooks[('GET', '/spot/orders/{id}')] = interrupt_once
    rc, out, hub = smoke(path, clock, gate)
    assert rc == 1
    assert 'Прервано (Ctrl+C)' in out.text
    oid = next(iter(gate.orders))
    assert gate.orders[oid]['status'] == 'cancelled' and gate.dms_trigger is None
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert [item['id'] for c in cancels for item in c['body']] == [oid]
    assert gate.countdowns() == [45, 0] and hub.stopped == 1
    report, _ = load_report(tmp_path)
    assert report['interrupted'] is True and report['pair_verified_empty'] is True
    assert report['dms_state'] == 'disarmed' and step(report, 'cleanup')['status'] == 'PASS'


def test_countdown_zero_reply_without_trigger_time_is_recorded(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(zero_reply={})
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 0, out.text
    report, _ = load_report(tmp_path)
    zero = report['facts']['countdown_zero_reply'][0]
    assert zero['trigger_time_present'] is False and zero['reply'] == {}
    assert zero['outcome'] == 'accepted_2xx_without_valid_trigger_time'


# ============================================================================ optional flags

def test_dms_fire_second_order_cancelled_by_gate(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, '--dms-fire')
    assert rc == 0, out.text
    assert 'ВНИМАНИЕ (--dms-fire)' in out.text
    creates = gate.creates()
    assert len(creates) == 2 and creates[0]['body']['text'] != creates[1]['body']['text']
    assert gate.countdowns() == [45, 45, 0, 45, 10, 0]
    # the second order was cancelled by the (fake) DMS, not by us
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert len(cancels) == 1
    i_second = [i for i, c in enumerate(gate.calls) if c['method'] == 'POST' and c['path'] == '/spot/orders'][1]
    i_fire_arm = [i for i, c in enumerate(gate.calls) if c['path'] == '/spot/countdown_cancel_all'][3]
    assert i_fire_arm < i_second                          # DMS armed before the second create too
    assert ('fired', gate.dms_log[[k for k, _ in gate.dms_log].index('fired')][1]) in gate.dms_log
    report, _ = load_report(tmp_path)
    fire = report['facts']['dms_fire']
    assert fire['tested'] is True and fire['status'] == 'cancelled' and fire['finish_as'] == 'cancelled'
    assert fire['update_time_minus_trigger_ms'] == 0
    assert 10_000 <= fire['observed_after_arm_ms'] <= 14_000
    assert report['facts']['finish_as_values']['after_dms'] == 'cancelled'
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'


def test_dms_fire_not_firing_is_cancelled_and_fails(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(dms_fires=False)
    rc, out, _ = smoke(path, clock, gate, '--dms-fire')
    assert rc == 1
    second = list(gate.orders.values())[1]
    assert second['status'] == 'cancelled'                 # cleanup cancelled it by id
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert cancels[-1]['body'][0]['id'] == second['id']
    report, _ = load_report(tmp_path)
    assert step(report, 'dms_fire_wait')['status'] == 'FAIL'
    assert report['dms_state'] == 'disarmed' and report['pair_verified_empty'] is True


def test_probe_exptime_records_label_before_any_order(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    rc, out, _ = smoke(path, clock, gate, '--probe-exptime')
    assert rc == 0, out.text
    deletes = [c for c in gate.calls if c['method'] == 'DELETE']
    assert [c['path'] for c in deletes] == ['/spot/orders/1', '/spot/orders/1']
    sent = [int(c['headers'][EXPTIME_HEADER]) for c in deletes]
    assert sent[0] < clock[0] * 1000 - 50_000 and sent[1] > clock[0] * 1000
    assert all(c['params'] == {'currency_pair': PAIR, 'account': 'spot'} for c in deletes)
    i_delete = gate.index_of(lambda c: c['method'] == 'DELETE')
    i_arm = gate.index_of(lambda c: c['path'] == '/spot/countdown_cancel_all')
    assert i_delete < i_arm
    report, _ = load_report(tmp_path)
    probe = report['facts']['exptime_expired']
    assert probe['probed'] is True
    assert probe['expired']['label'] == 'FAKE_REQUEST_EXPIRED' and probe['expired']['http_status'] == 400
    assert probe['expired']['exptime_offset_ms'] < -50_000
    assert probe['control']['label'] == 'ORDER_NOT_FOUND' and probe['control']['http_status'] == 404
    assert step(report, 'probe_exptime')['status'] == 'INFO'


# ============================================================================ review regressions

def _open_list_reads(records: list[dict]) -> list[dict]:
    return [r for r in records if r['method'] == 'GET' and r['endpoint'] == '/spot/orders'
            and r['query'].get('status') == 'open']


def test_cleanup_cancels_a_bound_order_by_id_although_the_open_list_lags(tmp_path, monkeypatch, clock):
    """Ctrl+C right after an accepted create while the open-orders LIST does not show the new
    order yet. One empty list read is no proof: cleanup cancels by order_id, confirms with
    get_order(order_id), needs two empty reads terminal_settle_seconds apart, then disarms."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    state = {'interrupted': False}

    def list_hook(call):
        if call['params'].get('status') == 'open' and gate.orders:
            return Resp(200, list(gate.foreign))          # the list lags behind the create all along
        return None

    def get_hook(call):
        if not state['interrupted'] and gate.orders:
            state['interrupted'] = True
            raise KeyboardInterrupt()
        return None
    gate.hooks[('GET', '/spot/orders')] = list_hook
    gate.hooks[('GET', '/spot/orders/{id}')] = get_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1, out.text
    oid = next(iter(gate.orders))
    text = gate.orders[oid]['text']
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert [item['id'] for c in cancels for item in c['body']] == [oid]       # by order_id, once
    assert gate.orders[oid]['status'] == 'cancelled' and gate.dms_trigger is None
    assert gate.countdowns() == [45, 0]
    i_cancel = gate.index_of(lambda c: c['path'] == '/spot/cancel_batch_orders')
    i_disarm = gate.index_of(lambda c: c['path'] == '/spot/countdown_cancel_all' and c['body']['timeout'] == 0)
    assert any(c['path'] == f'/spot/orders/{oid}' for c in gate.calls[i_cancel + 1:i_disarm])
    report, _ = load_report(tmp_path)
    clean = step(report, 'cleanup')
    assert clean['status'] == 'PASS'
    assert clean['data']['order_reads'][text] == {'by': 'order_id', 'found': True, 'order_id': oid,
                                                  'status': 'cancelled', 'finish_as': 'cancelled',
                                                  'left': str(EXPECTED_QTY)}
    reads = _open_list_reads(clean['requests'])
    assert len(reads) == 2 and reads[1]['sent_at_ms'] - reads[0]['sent_at_ms'] >= 2000   # terminal_settle_seconds
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'
    assert report['orders'][0]['state'] == 'finished'


def test_cleanup_never_disarms_while_get_order_says_open_and_the_list_is_empty(tmp_path, monkeypatch, clock):
    """The cancel has no effect (get_order keeps saying open) but the list read is empty: the
    direct read wins, the DMS stays armed, the pair is not called clean."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(cancel_effect=False, dms_fires=False)
    state = {'cancelled': False}

    def cancel_hook(call):
        state['cancelled'] = True
        return None

    def list_hook(call):
        if state['cancelled'] and call['params'].get('status') == 'open':
            return Resp(200, [])
        return None
    gate.hooks[('POST', '/spot/cancel_batch_orders')] = cancel_hook
    gate.hooks[('GET', '/spot/orders')] = list_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    oid = next(iter(gate.orders))
    assert gate.orders[oid]['status'] == 'open'
    assert 0 not in gate.countdowns() and gate.dms_trigger is not None
    report, _ = load_report(tmp_path)
    assert step(report, 'verify_cancel')['status'] == 'FAIL'
    assert 'status open' in step(report, 'verify_cancel')['detail']
    clean = step(report, 'cleanup')
    assert clean['status'] == 'FAIL' and clean['data']['open_but_not_listed'] == [gate.orders[oid]['text']]
    assert report['dms_state'] == 'left_armed' and report['pair_verified_empty'] is False
    assert report['orders'][0]['state'] == 'cancel_sent'
    assert 'ОСТАВЛЕН ВКЛЮЧЁННЫМ' in out.text and 'get_order показывает open' in out.text
    assert 'проверена чтением, открытых заявок нет' not in out.text
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert len(cancels) > 2 and all(item['id'] == oid for c in cancels for item in c['body'])


def test_cleanup_keeps_dms_armed_when_our_order_cannot_be_read(tmp_path, monkeypatch, clock):
    """Ctrl+C after the create; afterwards get_order(order_id) fails and the list is empty:
    the cancel is still sent by id, but nothing proves the order gone, so the DMS stays armed."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate(dms_fires=False)
    state = {'n': 0}

    def get_hook(call):
        if gate.orders:
            state['n'] += 1
            if state['n'] == 1:
                raise KeyboardInterrupt()
            return Resp(500, {'label': 'SERVER_ERROR', 'message': 'boom'})
        return None

    def list_hook(call):
        if call['params'].get('status') == 'open' and gate.orders:
            return Resp(200, [])
        return None
    gate.hooks[('GET', '/spot/orders/{id}')] = get_hook
    gate.hooks[('GET', '/spot/orders')] = list_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    oid = next(iter(gate.orders))
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert cancels and all(item['id'] == oid for c in cancels for item in c['body'])
    assert 0 not in gate.countdowns() and gate.dms_trigger is not None
    report, _ = load_report(tmp_path)
    assert report['dms_state'] == 'left_armed' and report['pair_verified_empty'] is False
    assert report['orders'][0]['state'] != 'finished'
    assert 'error' in step(report, 'cleanup')['data']['order_reads'][gate.orders[oid]['text']]
    assert 'ОСТАВЛЕН ВКЛЮЧЁННЫМ' in out.text


def test_ctrl_c_during_the_create_cancels_by_text_and_confirms_in_finished(tmp_path, monkeypatch, clock):
    """Gate accepted the create but the reply never arrived (Ctrl+C): no id is known. Cleanup
    cancels by text, finds the order by text in the finished list, binds it, then disarms."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()

    def create_hook(call):
        gate.create_order(call['body'])
        raise KeyboardInterrupt()
    gate.hooks[('POST', '/spot/orders')] = create_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 1
    oid = next(iter(gate.orders))
    text = gate.orders[oid]['text']
    cancels = [c for c in gate.calls if c['path'] == '/spot/cancel_batch_orders']
    assert cancels[0]['body'] == [{'currency_pair': PAIR, 'id': text, 'account': 'spot'}]
    assert gate.orders[oid]['status'] == 'cancelled' and gate.dms_trigger is None
    assert gate.countdowns() == [45, 0]
    report, _ = load_report(tmp_path)
    assert report['orders'][0]['order_id'] == oid and report['orders'][0]['state'] == 'finished'
    assert step(report, 'cleanup')['data']['order_reads'][text]['by'] == 'finished_orders'
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'


def test_positive_basis_offset_still_prices_at_most_index_minus_10(tmp_path, monkeypatch, clock):
    """centre = index * 1.15: the price is taken from the lower index, not from the centre."""
    path = setup(tmp_path, monkeypatch, mutate=lambda d: d['index'].update(basis_offset_pct=15))
    gate = FakeGate()
    hub = make_hub(clock, spot=(Decimal('0.1600'), Decimal('0.1610')))     # spot trades at a premium
    rc, out, _ = smoke(path, clock, gate, hub=hub)
    assert rc == 0, out.text
    price = Decimal(gate.creates()[0]['body']['price'])
    assert price == EXPECTED_PRICE and price <= CENTER * Decimal('0.9')
    report, _ = load_report(tmp_path)
    plan = report['prechecks']['plan']
    assert plan['price_base'] == 'index' and Decimal(plan['center']) > CENTER
    assert Decimal(plan['index']) == CENTER and Decimal(plan['max_price_by_index']) == CENTER * Decimal('0.9')
    assert report['plan']['price_base'] == 'index' and Decimal(report['plan']['actual_edge_pct']) >= 10
    assert 'от индекса' in out.text


def test_refuses_when_spot_trades_far_below_the_index(tmp_path, monkeypatch, clock):
    """Spot ~9 % under the index: index -10 % would sit just under the spot bid, which is not
    far from the market. Refused before any write."""
    hub = make_hub(clock, spot=(Decimal('0.1234'), Decimal('0.1240')))
    report, _ = _refused(tmp_path, monkeypatch, clock, FakeGate(), hub=hub)
    assert report['refusal']['code'] == 'SMOKE_PRICE_GUARD'
    assert 'PRICE_TOO_CLOSE_TO_BEST_BID' in report['refusal']['message']


def test_larger_edge_passes_when_spot_trades_below_the_index(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    hub = make_hub(clock, spot=(Decimal('0.1234'), Decimal('0.1240')))
    rc, out, _ = smoke(path, clock, gate, '--edge-pct', '15', hub=hub)
    assert rc == 0, out.text
    price = Decimal(gate.creates()[0]['body']['price'])
    assert price <= CENTER * Decimal('0.85') and price <= Decimal('0.1234') * Decimal('0.95')


def test_sigterm_while_the_order_is_open_runs_cleanup_and_writes_the_report(tmp_path, monkeypatch, clock):
    """SIGTERM / SIGBREAK (kill, Ctrl+Break, closing the console on Windows) is handled like
    Ctrl+C: cancel, verification, DMS, report. A repeated signal during cleanup is only recorded;
    the previous handlers are restored afterwards."""
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    before = signal.getsignal(signal.SIGTERM)
    seen: dict = {}

    def get_hook(call):
        if gate.orders and 'handler' not in seen:
            handler = signal.getsignal(signal.SIGTERM)
            seen['handler'] = handler
            if hasattr(signal, 'SIGBREAK'):
                seen['sigbreak'] = signal.getsignal(signal.SIGBREAK)
            if callable(handler):
                handler(signal.SIGTERM, None)          # what the OS delivers while the order is open
        return None

    def cancel_hook(call):
        seen['handler'](signal.SIGTERM, None)         # a second stop during cleanup must not abort it
        seen['second'] = True
        return None
    gate.hooks[('GET', '/spot/orders/{id}')] = get_hook
    gate.hooks[('POST', '/spot/cancel_batch_orders')] = cancel_hook
    rc, out, hub = smoke(path, clock, gate, signals=True)
    assert callable(seen['handler']) and seen['handler'] is not before
    if 'sigbreak' in seen:
        assert seen['sigbreak'] == seen['handler']
    assert seen.get('second') is True
    assert signal.getsignal(signal.SIGTERM) == before     # restored
    assert rc == 1
    assert 'Остановлено сигналом SIGTERM' in out.text
    oid = next(iter(gate.orders))
    assert gate.orders[oid]['status'] == 'cancelled' and gate.dms_trigger is None
    assert gate.countdowns() == [45, 0] and hub.stopped == 1
    report, _ = load_report(tmp_path)
    assert report['interrupted'] is True and report['interrupt_reason'] == 'SIGTERM'
    assert report['stop_signal'] == 'SIGTERM'
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'
    assert step(report, 'cleanup')['status'] == 'PASS'
    assert 'прервано (SIGTERM)' in out.text


def test_signals_are_not_touched_without_deps_signals(tmp_path, monkeypatch, clock):
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    before = signal.getsignal(signal.SIGTERM)
    seen = {}

    def get_hook(call):
        seen.setdefault('handler', signal.getsignal(signal.SIGTERM))
        return None
    gate.hooks[('GET', '/spot/orders/{id}')] = get_hook
    rc, out, _ = smoke(path, clock, gate)
    assert rc == 0, out.text
    assert seen['handler'] == before


def test_dms_fire_recheck_failure_after_the_main_flow_is_not_a_refusal(tmp_path, monkeypatch, clock):
    """The index falls after the main flow: the --dms-fire second order is not sent. A real
    order was placed and cancelled, so this is no "refusal" (exit 2 = no order ever sent): exit
    1, the main-flow PASS is kept and stated, the re-armed DMS is disarmed after the pair is
    verified empty."""
    holder: dict = {}

    def center(elapsed):
        g = holder.get('gate')
        if g is not None and any(c['path'] == '/spot/cancel_batch_orders' for c in g.calls):
            return Decimal('0.12')
        return CENTER
    hub = FakeHub(clock, {'bybit_perp': center, 'bitget_perp': center})
    path = setup(tmp_path, monkeypatch)
    gate = FakeGate()
    holder['gate'] = gate
    rc, out, _ = smoke(path, clock, gate, '--dms-fire', hub=hub)
    assert rc == 1, out.text
    assert len(gate.creates()) == 1
    assert gate.countdowns() == [45, 45, 0, 45, 0] and gate.dms_trigger is None
    report, _ = load_report(tmp_path)
    assert report['result'] == 'FAIL' and report['exit_code'] == 1 and report['refusal'] is None
    assert report['main_passed'] is True and 'SMOKE_PRICE_GUARD' in report['dms_fire_not_performed']
    assert step(report, 'verify_cancel')['status'] == 'PASS'
    guard = step(report, 'dms_fire_guard')
    assert guard['status'] == 'SKIP' and guard['mandatory'] is True
    assert report['facts']['dms_fire']['tested'] is False
    assert report['pair_verified_empty'] is True and report['dms_state'] == 'disarmed'
    assert step(report, 'cleanup')['status'] == 'PASS'
    assert 'ОТКЛОНЁН' not in out.text and 'Заявки не отправлялись' not in out.text
    assert 'основная проверка ПРОЙДЕНА' in out.text and 'НЕ ВЫПОЛНЕНА' in out.text


# ============================================================================ gate_rest probe scope

def _client(session) -> GateRestClient:
    return GateRestClient('https://api.gateio.ws/api/v4', KEY, SECRET, timeout=5, request_expiry_seconds=10,
                          limiter=None, session=session)


def test_probe_expired_exptime_is_narrow(clock):
    gate = FakeGate()
    client = _client(gate)
    params = [('currency_pair', PAIR), ('account', 'spot')]
    now_ms = int(clock[0] * 1000)
    with pytest.raises(NotSent):
        client.probe_expired_exptime('1', params, expired_ms=now_ms + 5000)      # not in the past
    with pytest.raises(NotSent):
        client.probe_expired_exptime('t-abc', params, expired_ms=now_ms - 60_000)  # numeric ids only
    with pytest.raises(NotSent):
        client.probe_expired_exptime('1', [('account', 'spot')], expired_ms=now_ms - 60_000)
    with pytest.raises(NotSent):       # the override cannot reach any other request
        client._request('POST', '/spot/orders', None, {'text': 't-x'}, signed=True, bucket=None, emergency=None,
                        expire=None, cost=1, exptime_override_ms=now_ms - 60_000)
    assert gate.calls == []
    with pytest.raises(Rejected) as info:
        client.probe_expired_exptime('1', params, expired_ms=now_ms - 60_000)
    assert info.value.label == 'FAKE_REQUEST_EXPIRED'
    assert gate.calls[-1]['headers'][EXPTIME_HEADER] == str(now_ms - 60_000)
    with pytest.raises(Rejected) as info:   # ordinary cancels keep now + request_expiry
        client.private('DELETE', '/spot/orders/1', params)
    assert info.value.label == 'ORDER_NOT_FOUND'
    assert gate.calls[-1]['headers'][EXPTIME_HEADER] == str(now_ms + 10_000)


# ============================================================================ pure helpers

def meta(**changes):
    raw = dict(GATE_PAIR)
    raw.update(changes)
    return parse_pair_meta(raw)


def test_price_is_floored_to_tick_and_at_most_index_minus_10():
    m = meta()
    assert smoke_price(CENTER, Decimal(10), m) == EXPECTED_PRICE
    for center in ('0.1356', '0.135679', '1.23456789', '0.0000731', '7'):
        for precision in (2, 4, 5, 8):
            mm = meta(precision=precision)
            c = Decimal(center)
            price = smoke_price(c, Decimal(10), mm)
            assert price <= c * Decimal('0.9')
            assert price + mm.tick > c * Decimal('0.9')    # the largest tick not above
            assert price == price.quantize(mm.tick) or price == 0
    assert smoke_price(CENTER, Decimal(25), m) == Decimal('0.1017')


def test_quantity_meets_minimums_with_margin_rounded_up_to_lot():
    m = meta()
    qty = smoke_quantity(m, EXPECTED_PRICE)
    assert qty == EXPECTED_QTY
    assert qty * EXPECTED_PRICE >= Decimal(3) * Decimal('1.02') and qty >= Decimal('1.02')
    assert (qty - m.lot) * EXPECTED_PRICE < Decimal(3) * Decimal('1.02')      # minimal
    # base minimum dominates
    assert smoke_quantity(meta(min_base_amount='100', min_quote_amount='1'), Decimal('0.1')) == Decimal('102.0')
    # integer lots round up
    assert smoke_quantity(meta(amount_precision=0), EXPECTED_PRICE) == Decimal(26)
    # unknown minimums: 5 USDT
    none = meta(min_base_amount=None, min_quote_amount=None)
    qty = smoke_quantity(none, EXPECTED_PRICE)
    assert qty * EXPECTED_PRICE >= FALLBACK_QUOTE_MIN and (qty - none.lot) * EXPECTED_PRICE < FALLBACK_QUOTE_MIN
    zero = meta(min_quote_amount='0')
    assert smoke_quantity(zero, EXPECTED_PRICE) == qty


def test_price_guard():
    book = Book(bids=((Decimal('0.1350'), Decimal(1)),), asks=((Decimal('0.1360'), Decimal(1)),), received=0.0,
                event_ms=0)
    assert price_guard(EXPECTED_PRICE, CENTER, CENTER, book) is None
    assert price_guard(Decimal('0.12205'), CENTER, CENTER, book) == 'PRICE_ABOVE_INDEX_MINUS_10_PCT'
    assert price_guard(Decimal('0.12'), Decimal('0.2'), Decimal('0.2'), book) is None
    assert price_guard(Decimal('0.1355'), Decimal('0.2'), Decimal('0.2'), book) == 'PRICE_NOT_BELOW_BEST_BID'
    assert price_guard(Decimal('0.1365'), Decimal('0.2'), Decimal('0.2'), book) == 'PRICE_NOT_BELOW_BEST_ASK'
    assert price_guard(Decimal(0), CENTER, CENTER, book) == 'PRICE_NOT_POSITIVE'
    # the index bounds the price even when the centre (index * (1 + basis/100)) is higher
    assert price_guard(Decimal('0.1300'), Decimal('0.1559'), CENTER, book) == 'PRICE_ABOVE_INDEX_MINUS_10_PCT'
    assert price_guard(EXPECTED_PRICE, Decimal('0.1559'), CENTER, book) is None
    assert price_guard(EXPECTED_PRICE, Decimal('0.1300'), CENTER, book) == 'PRICE_ABOVE_CENTER_MINUS_10_PCT'
    assert price_guard(EXPECTED_PRICE, CENTER, None, book) == 'PRICE_ABOVE_INDEX_MINUS_10_PCT'
    # "far from the market": at least MIN_BID_GAP_PCT below the best bid, not just below it
    near = Book(bids=((Decimal('0.1234'), Decimal(1)),), asks=((Decimal('0.1240'), Decimal(1)),), received=0.0,
                event_ms=0)
    assert price_guard(EXPECTED_PRICE, CENTER, CENTER, near) == 'PRICE_TOO_CLOSE_TO_BEST_BID'
    limit = Decimal('0.1234') * (1 - MIN_BID_GAP_PCT / 100)
    assert price_guard(limit, CENTER, CENTER, near) is None
    assert price_guard(limit + Decimal('0.00001'), CENTER, CENTER, near) == 'PRICE_TOO_CLOSE_TO_BEST_BID'


def test_price_base_is_the_lower_of_index_and_center():
    assert price_base(Decimal('0.1559'), CENTER) == ('index', CENTER)       # positive basis offset
    assert price_base(Decimal('0.1200'), CENTER) == ('center', Decimal('0.1200'))
    assert price_base(CENTER, CENTER) == ('index', CENTER)
    for bad in (None, Decimal(0), Decimal('-1'), Decimal('NaN')):
        with pytest.raises(ValueError):
            price_base(CENTER, bad)
    m = meta()
    for basis in ('-20', '-1', '0', '1', '15', '49'):
        center = CENTER * (1 + Decimal(basis) / 100)
        _, base = price_base(center, CENTER)
        price = smoke_price(base, Decimal(10), m)
        assert price <= CENTER * Decimal('0.9') and price <= center * Decimal('0.9')


def test_option_refusals():
    assert option_refusal(SmokeOptions()) is None
    assert option_refusal(SmokeOptions(edge_pct=Decimal('9.9')))[0] == 'SMOKE_EDGE_TOO_SMALL'
    assert option_refusal(SmokeOptions(edge_pct=Decimal(100)))[0] == 'SMOKE_EDGE_INVALID'
    assert option_refusal(SmokeOptions(max_notional=Decimal(0)))[0] == 'SMOKE_MAX_NOTIONAL_INVALID'
    assert option_refusal(SmokeOptions(max_notional=Decimal('100.01')))[0] == 'SMOKE_MAX_NOTIONAL_INVALID'
    assert option_refusal(SmokeOptions(index_seconds=float('nan')))[0] == 'SMOKE_INDEX_SECONDS_INVALID'


def test_sanitize_masks_user_ids_ips_secrets_and_signatures():
    value = {'user_id': 10406147, 'uid': '5', 'main_uid': 7, 'ip_whitelist': ['1.2.3.4', '5.6.7.8'],
             'order_id': '912', 'stp_id': 0, 'note': f'key {KEY} sig ' + 'ab' * 40, 'price': Decimal('0.1'),
             'nested': [{'userId': 3}]}
    out = sanitize(value, (KEY,))
    assert out['user_id'] == '***' and out['uid'] == '***' and out['main_uid'] == '***'
    assert out['ip_whitelist'] == ['***', '***'] and out['nested'] == [{'userId': '***'}]
    assert out['order_id'] == '912' and out['stp_id'] == 0 and out['price'] == '0.1'
    assert KEY not in out['note'] and '[REDACTED]' in out['note']


def test_recording_session_never_stores_request_headers(clock):
    gate = FakeGate()
    rec = RecordingSession(gate, secrets=(KEY, SECRET))
    client = _client(rec)
    client.private('GET', '/spot/accounts')
    record = rec.records[0]
    assert record['signed'] is True and record['http_status'] == 200 and record['endpoint'] == '/spot/accounts'
    text = json.dumps(record)
    assert KEY not in text and 'SIGN' not in text and 'Timestamp' not in text
    assert gate.calls[0]['headers']['KEY'] == KEY        # the real request still carried them


# ============================================================================ files

def test_live_config_example_validates_with_customer_tolerances(tmp_path):
    from gate_stonk.cli import Deps as _Deps

    example = ROOT / 'config.live.example.yaml'
    cfg = load_config(example, read_env=False)
    assert cfg.live and cfg.app['campaign_id'] == 'stonk_live' and cfg.app['state_dir'] == 'runtime_live'
    assert cfg.safety['dedicated_account_ack'] is True and cfg.telegram['enabled'] is True
    assert cfg.pair['max_inventory_usdt'] == Decimal(50)
    for side in ('buy', 'sell'):
        settings = cfg.side(side)
        assert settings['band_tolerance_pct'] == Decimal('0.5')
        assert settings['reprice_threshold_pct'] == Decimal('0.5')
        assert settings['max_daily_notional_usdt'] == Decimal(50)
        for level in cfg.levels(side):
            assert not level.has_iceberg
    # same keys as the paper example (only values differ)
    paper = yaml.safe_load((ROOT / 'config.example.yaml').read_text(encoding='utf-8'))
    live = yaml.safe_load(example.read_text(encoding='utf-8'))
    assert set(paper) == set(live)
    for section in paper:
        if isinstance(paper[section], dict):
            assert set(paper[section]) == set(live[section]), section
    out = Output()
    assert main(['validate', '--config', str(example)], deps=_Deps(out=out, err=out, logging=False)) == 0
    assert 'Режим live' in out.text


def test_wrapper_exposes_smoke_test():
    text = (ROOT / 'run_stonk.ps1').read_bytes()
    assert text.startswith(b'\xef\xbb\xbf')
    body = text[3:].decode('utf-8')
    assert '"smoke-test"' in body and '[switch]$DmsFire' in body and '[switch]$ProbeExptime' in body
    assert '$Command -eq "smoke-test" -and $Live' in body
    assert body.index('$env:PYTHONUTF8 = "1"') < body.index('& $Python')
