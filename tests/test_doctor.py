"""doctor: offline fake HTTP routes + fake FeedHub + fake account venue."""
from __future__ import annotations

import json
import tempfile
from decimal import Decimal

import pytest

from gate_stonk.cli import Deps, ProcessLocks, main
from gate_stonk.config import load_config
from gate_stonk.doctor import Doctor

from cli_fakes import FakeHub, FakeResponse, FakeSession, Output, advance, doctor_routes, fixture, write_config

MIDS = {'bybit_perp': Decimal('0.1356'), 'bitget_perp': Decimal('0.1357')}


@pytest.fixture(autouse=True)
def _private_tempdir(tmp_path, monkeypatch):
    """doctor --probe-write takes the per-key/pair lock in the temp directory: keep it per test."""
    monkeypatch.setattr(tempfile, 'gettempdir', lambda: str(tmp_path / 'tmp'))


def no_iceberg(data):
    for side in ('buy', 'sell'):
        for level in data['pair'][side]['levels']:
            level.pop('iceberg_display_pct', None)


class FakeAccountVenue:
    def __init__(self, *, problems=None, opens=None, gt=False, probe=None):
        self.problems = list(problems or [])
        self.opens = list(opens or [])
        self.gt = gt
        self.probe = probe or {'ok': True, 'outcome': 'acknowledged', 'label': None}
        self.calls: list[str] = []

    def account_check(self):
        self.calls.append('account_check')
        return {'key_mode': 1, 'unified_mode': 'classic', 'unified_mode_error': None, 'detected_account': 'spot',
                'configured_account': 'spot', 'account_ok': not self.problems, 'pair_whitelist_ok': True,
                'pair_whitelist': [], 'ip_whitelist': ['1.2.3.4'], 'tier': 0, 'problems': list(self.problems)}

    def open_orders(self):
        self.calls.append('open_orders')
        return list(self.opens)

    def probe_write_permission(self):
        self.calls.append('probe')
        return dict(self.probe)

    def balances(self):
        from gate_stonk.types import Balance
        self.calls.append('balances')
        return {'USDT': Balance(Decimal('300.12'), Decimal('50')), 'STONK': Balance(Decimal('0'), Decimal('0'))}

    def fee_info(self):
        self.calls.append('fee_info')
        return {'maker_fee_pct': Decimal('0.2'), 'taker_fee_pct': Decimal('0.2'), 'gt_deduction_enabled': self.gt,
                'debit_fee_error': None}

    def place(self, *a, **k):
        raise AssertionError('doctor must never place orders')

    def cancel_many(self, *a, **k):
        raise AssertionError('doctor must never cancel orders')

    def close(self):
        self.calls.append('close')


def doctor_deps(out, clock, session, hub=None, venue=None):
    hub = hub or FakeHub(clock, MIDS)
    return Deps(out=out, err=out, http=session, hub_factory=lambda cfg: hub,
                doctor_venue_factory=(lambda cfg, http: venue) if venue is not None else None,
                sleep=advance(clock), logging=False, signals=False)


def test_doctor_all_required_checks_pass_without_keys(tmp_path, clock):
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    session = FakeSession(doctor_routes())
    rc = main(['doctor', '--seconds', '2', '--config', str(path)], deps=doctor_deps(out, clock, session))
    text = out.text
    assert rc == 0, text
    assert '[ OK ] Пара STONK_USDT' in text and 'точность цены 5' in text and 'минимум 1 STONK' in text
    assert '[ OK ] Часы' in text and 'Bybit: расхождение' in text
    assert '[ OK ] Инструмент bybit_perp' in text and '[ OK ] Инструмент bitget_perp' in text
    assert '[ OK ] Котировки и индекс' in text and 'индекс валиден 100%' in text
    assert 'спот Gate STONK_USDT: bid 0.135, ask 0.136' in text and 'базис к индексу' in text
    assert '[ OK ] Размеры уровней и минимумы пары' in text and 'BUY b1: край 1% → цена' in text
    assert '[ВНИМ] Iceberg в live' in text                     # example levels b2/s2 are icebergs
    assert '[ПРОП] Аккаунт Gate (ключи)' in text and 'ключи GATE_API_KEY/GATE_API_SECRET не заданы' in text
    assert 'Итог: обязательные проверки пройдены' in text
    report = json.loads((tmp_path / 'state' / 'doctor.json').read_text(encoding='utf-8'))
    assert report['ok'] is True and report['trading_tested'] is False and report['live_ready'] is False
    assert report['checks']['quotes']['data']['center'] is not None
    assert not any(method != 'GET' for method, _ in session.calls)        # read-only


def test_doctor_failing_required_source_exits_1(tmp_path, clock):
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    session = FakeSession(doctor_routes(**{
        '/v5/market/instruments-info': FakeResponse(200, fixture('bybit', 'instruments_info_scaled.json'))}))
    rc = main(['doctor', '--seconds', '1', '--config', str(path)], deps=doctor_deps(out, clock, session))
    assert rc == 1
    assert '[СБОЙ] Инструмент bybit_perp (bybit_linear STONKUSDT) (обязательная)' in out.text
    assert 'НЕ пройдены обязательные проверки: instrument:bybit_perp' in out.text


def test_doctor_optional_source_transient_failure_is_a_warning(tmp_path, clock):
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    session = FakeSession(doctor_routes(**{'/api/v2/mix/market/contracts': FakeResponse(429, '')}))
    rc = main(['doctor', '--seconds', '1', '--config', str(path)], deps=doctor_deps(out, clock, session))
    assert rc == 0, out.text
    assert '[ВНИМ] Инструмент bitget_perp' in out.text and 'REFERENCE_LOOKUP_FAILED:HTTP_429' in out.text


def test_doctor_index_never_valid_fails(tmp_path, clock):
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    hub = FakeHub(clock, {'bybit_perp': None, 'bitget_perp': Decimal('0.1357')})
    rc = main(['doctor', '--seconds', '1', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), hub=hub))
    assert rc == 1
    assert 'индекс ни разу не был валиден: REQUIRED_SOURCE_INVALID:bybit_perp' in out.text
    assert 'bybit_perp: валиден 0%' in out.text and 'последняя ошибка MISSING_SOURCE:bybit_perp' in out.text
    assert '[ПРОП] Размеры уровней' in out.text


def test_doctor_network_errors_are_reported_per_check(tmp_path, clock):
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    session = FakeSession(doctor_routes(**{'/spot/time': ConnectionError('https://api.gateio.ws/secret-path'),
                                           '/v5/market/time': ConnectionError('boom')}))
    rc = main(['doctor', '--seconds', '1', '--config', str(path)], deps=doctor_deps(out, clock, session))
    assert rc == 1
    assert '[СБОЙ] Часы (обязательная)' in out.text and 'GATE_TRANSPORT_ERROR' in out.text
    assert 'secret-path' not in out.text
    assert '[ OK ] Котировки и индекс' in out.text           # later checks still ran


def test_doctor_pair_minimums_and_pins(tmp_path, clock):
    path = write_config(tmp_path, lambda d: d['market'].update(expected_precision=4), two_sources=True)
    pair = dict(doctor_routes()['/spot/currency_pairs/STONK_USDT'], min_quote_amount='25')
    out = Output()
    session = FakeSession(doctor_routes(**{'/spot/currency_pairs/STONK_USDT': pair}))
    rc = main(['doctor', '--seconds', '1', '--config', str(path)], deps=doctor_deps(out, clock, session))
    assert rc == 1
    assert 'PAIR_PRECISION_MISMATCH:5!=4' in out.text
    assert 'BUY b1:' in out.text and 'ORDER_SIZE_BELOW_MINIMUM' in out.text      # 20 USDT < 25


def test_doctor_with_keys_runs_account_checks(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    venue = FakeAccountVenue(gt=True)
    rc = main(['doctor', '--seconds', '1', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
    text = out.text
    assert rc == 0, text
    assert '[ OK ] Режим аккаунта и ключ' in text and 'определён аккаунт spot' in text
    assert '[ПРОП] Права ключа' in text and 'БЕЗ вывода' in text
    assert 'USDT: всего 300.12, свободно 250.12' in text
    assert '[ВНИМ] Комиссии' in text and 'GT' in text and 'НЕ пересчитываются' in text
    assert 'probe' not in venue.calls and venue.calls[-1] == 'close'
    assert 'key-ABCDEFGH' not in text and 'secret-ZYXWVUT' not in text


def test_doctor_account_mode_mismatch_fails(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    venue = FakeAccountVenue(problems=['ACCOUNT_MODE_MISMATCH:unified'])
    rc = main(['doctor', '--seconds', '1', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
    assert rc == 1 and 'ACCOUNT_MODE_MISMATCH:unified' in out.text


@pytest.mark.parametrize('opens,probe,expect_rc,expect_probe', [
    ([], {'ok': True, 'outcome': 'acknowledged', 'label': None}, 0, True),
    ([], {'ok': False, 'outcome': 'rejected', 'label': 'READ_ONLY'}, 1, True),
    ([object()], None, 1, False),
])
def test_doctor_probe_write(tmp_path, clock, monkeypatch, opens, probe, expect_rc, expect_probe):
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    venue = FakeAccountVenue(opens=[type('O', (), {'text': 't-stk1-b1-x01', 'order_id': '1'})() for _ in opens],
                             probe=probe)
    rc = main(['doctor', '--seconds', '1', '--probe-write', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
    assert rc == expect_rc, out.text
    assert ('probe' in venue.calls) is expect_probe
    if probe and probe.get('label') == 'READ_ONLY':
        assert 'ключ только для чтения' in out.text
    if opens:
        assert 'проба записи НЕ выполнена' in out.text


def test_doctor_gate_futures_index_self_reference_warning(tmp_path, clock):
    def mutate(data):
        data['index']['sources'].append({'id': 'gate_perp', 'venue': 'gate_futures_usdt', 'symbol': 'STONK_USDT',
                                         'quote': 'USDT', 'price': 'mark', 'weight': 0.1, 'required': False,
                                         'max_spread_pct': None})
    path = write_config(tmp_path, mutate, two_sources=True)
    routes = doctor_routes(**{
        '/futures/usdt/contracts/STONK_USDT': FakeResponse(200, fixture('gate_futures', 'rest_contract_ok.json')),
        '/futures/usdt/index_constituents/STONK_USDT': {
            'index': 'STONK_USDT', 'constituents': [{'exchange': 'Gate.com', 'symbols': ['STONK_USDT']},
                                                     {'exchange': 'Bybit', 'symbols': ['STONK_USDT']}]}})
    out = Output()
    hub = FakeHub(clock, dict(MIDS, gate_perp=Decimal('0.1356')))
    cfg = load_config(path, read_env=False)
    doctor = Doctor(cfg, seconds=1, http=FakeSession(routes), hub_factory=lambda c: hub, sleep=advance(clock),
                    out=out, write_report=False)
    report = doctor.run()
    check = report['checks']['gate_futures_index:gate_perp']
    assert check['status'] == 'warn' and check['data']['gate_spot_included'] is True
    assert 'САМОССЫЛКА' in out.text
    assert report['ok'] in (True, False)      # optional check: never decides the exit code by itself


def test_doctor_seconds_bounds(tmp_path):
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    assert main(['doctor', '--seconds', '0', '--config', str(path)], deps=Deps(out=out, err=out, logging=False)) == 2


def test_doctor_probe_write_skipped_when_open_orders_unreadable(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    path = write_config(tmp_path, two_sources=True)
    out = Output()
    venue = FakeAccountVenue()

    def broken():
        from gate_stonk.errors import ReadError
        raise ReadError('GATE_READ_FAILED:GET spot:HTTP_502')
    venue.open_orders = broken
    rc = main(['doctor', '--seconds', '1', '--probe-write', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
    assert rc == 1
    assert 'probe' not in venue.calls
    assert 'открытые заявки пары не прочитаны' in out.text


@pytest.mark.parametrize('same_state_dir', [True, False])
def test_doctor_probe_write_refused_while_a_bot_holds_the_locks(tmp_path, clock, monkeypatch, same_state_dir):
    """Regression: --probe-write sends countdown_cancel_all {timeout: 0} (disarms the pair's
    DMS for the whole account) and took no process lock, so it could disarm a running bot's
    DMS. It now runs under ProcessLocks(cfg, live=True): refused while a bot holds the
    state_dir lock or the per-key/pair lock (another folder, same key and pair)."""
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    (tmp_path / 'doc').mkdir()
    path = write_config(tmp_path / 'doc', two_sources=True)
    bot_path = path
    if not same_state_dir:
        (tmp_path / 'bot').mkdir()
        bot_path = write_config(tmp_path / 'bot', two_sources=True)
    venue = FakeAccountVenue()
    out = Output()
    with ProcessLocks(load_config(bot_path), live=True):        # the running bot
        rc = main(['doctor', '--seconds', '1', '--probe-write', '--config', str(path)],
                  deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
        assert rc == 2
        assert 'doctor --probe-write отклонён' in out.text and 'DMS' in out.text
        assert venue.calls == []                                 # nothing read, nothing probed
        out = Output()                                           # read-only doctor still allowed
        rc = main(['doctor', '--seconds', '1', '--config', str(path)],
                  deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
        assert rc == 0, out.text
        assert 'probe' not in venue.calls
    venue = FakeAccountVenue()
    out = Output()
    rc = main(['doctor', '--seconds', '1', '--probe-write', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
    assert rc == 0, out.text
    assert 'probe' in venue.calls                                # free locks: the probe runs


def test_doctor_probe_write_holds_the_locks_while_running(tmp_path, clock, monkeypatch):
    """A bot cannot start during the probe: the locks are held for the whole doctor run."""
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    path = write_config(tmp_path, two_sources=True)
    cfg = load_config(path)
    seen = []

    class LockCheckingVenue(FakeAccountVenue):
        def probe_write_permission(self):
            from gate_stonk.cli import LockBusy
            try:
                with ProcessLocks(cfg, live=True):
                    seen.append('free')
            except LockBusy:
                seen.append('busy')
            return super().probe_write_permission()

    venue = LockCheckingVenue()
    out = Output()
    rc = main(['doctor', '--seconds', '1', '--probe-write', '--config', str(path)],
              deps=doctor_deps(out, clock, FakeSession(doctor_routes()), venue=venue))
    assert rc == 0, out.text
    assert seen == ['busy']
