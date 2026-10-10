"""Offline fakes for the CLI / doctor / index-watch / fill-wakeup tests (no network, fake clock)."""
from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import urlsplit

import yaml

from gate_stonk.types import Book, FxSnapshot, SourceSnapshot
from gate_stonk.venue import parse_pair_meta

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / 'config.example.yaml'
FIXTURES = ROOT / 'tests' / 'fixtures'

GATE_PAIR = {
    'id': 'STONK_USDT', 'base': 'STONK', 'quote': 'USDT', 'fee': '0.2', 'min_base_amount': '1',
    'min_quote_amount': '3', 'max_base_amount': None, 'max_quote_amount': '1000000', 'amount_precision': 1,
    'precision': 5, 'trade_status': 'tradable', 'sell_start': 0, 'buy_start': 0,
}

TWO_SOURCES = [
    {'id': 'bybit_perp', 'venue': 'bybit_linear', 'symbol': 'STONKUSDT', 'quote': 'USDT', 'price': 'mid',
     'weight': 0.6, 'required': True, 'max_spread_pct': 0.5, 'heartbeat_seconds': 3},
    {'id': 'bitget_perp', 'venue': 'bitget_usdt_futures', 'symbol': 'STONKUSDT', 'quote': 'USDT', 'price': 'mid',
     'weight': 0.4, 'required': False, 'max_spread_pct': 1.0},
]


def pair_meta(**overrides: Any):
    raw = dict(GATE_PAIR)
    raw.update(overrides)
    return parse_pair_meta(raw)


def example_data() -> dict:
    return yaml.safe_load(EXAMPLE.read_text(encoding='utf-8'))


def write_config(tmp_path: Path, mutate: Callable[[dict], None] | None = None, *, two_sources: bool = False,
                 name: str = 'config.yaml') -> Path:
    data = example_data()
    data['app']['state_dir'] = str(tmp_path / 'state')
    if two_sources:
        data['index']['sources'] = json.loads(json.dumps(TWO_SOURCES))
    if mutate is not None:
        mutate(data)
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding='utf-8')
    return path


def make_live(data: dict) -> None:
    """Live config that passes every local gate once the env is set (see live_env)."""
    data['app']['mode'] = 'live'
    data['safety']['dedicated_account_ack'] = True
    data['telegram']['enabled'] = True
    data['execution']['iceberg_poc_verified'] = True


def live_env(monkeypatch, *, confirm: bool = True) -> None:
    monkeypatch.setenv('GATE_API_KEY', 'key-ABCDEFGH-123456')
    monkeypatch.setenv('GATE_API_SECRET', 'secret-ZYXWVUT-987654')
    monkeypatch.setenv('TELEGRAM_BOT_TOKEN', '12345:token-QWERTY')
    monkeypatch.setenv('TELEGRAM_CHAT_ID', '777')
    if confirm:
        monkeypatch.setenv('GATE_LIVE_CONFIRM', 'I_UNDERSTAND_SPOT_TRADING')


class Output:
    """Collects Deps.out / Deps.err lines."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def __call__(self, text: str) -> None:
        self.lines.append(str(text))

    @property
    def text(self) -> str:
        return '\n'.join(self.lines)


def advance(clock: list) -> Callable[[float], None]:
    def sleep(seconds: float) -> None:
        clock[0] += max(0.0, float(seconds))
    return sleep


# ----------------------------------------------------------------------------- HTTP

class FakeResponse:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        if isinstance(body, (bytes, bytearray)):
            self.content = bytes(body)
        elif isinstance(body, str):
            self.content = body.encode('utf-8')
        else:
            self.content = json.dumps(body).encode('utf-8')
        self.headers: dict = {}

    @property
    def text(self) -> str:
        return self.content.decode('utf-8')

    def json(self) -> Any:
        return json.loads(self.content)


class FakeSession:
    """requests.Session stand-in for both ``get`` (adapters) and ``request`` (GateRestClient).

    Routes: URL path suffix -> body | FakeResponse | exception | callable(url) -> any of these.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = dict(routes)
        self.calls: list[tuple[str, str]] = []

    def _answer(self, method: str, url: str) -> Any:
        self.calls.append((method, url))
        path = urlsplit(url).path
        for suffix in sorted(self.routes, key=len, reverse=True):
            if path.endswith(suffix):
                answer = self.routes[suffix]
                if callable(answer) and not isinstance(answer, FakeResponse):
                    answer = answer(url)
                if isinstance(answer, BaseException):
                    raise answer
                return answer if isinstance(answer, FakeResponse) else FakeResponse(200, answer)
        return FakeResponse(404, {'label': 'NOT_FOUND', 'message': 'no route'})

    def get(self, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> Any:
        return self._answer('GET', url)

    def request(self, method: str, url: str, data: Any = None, headers: Any = None, timeout: Any = None,
                allow_redirects: bool = True) -> Any:
        return self._answer(method, url)

    def close(self) -> None:
        return None


def fixture(venue: str, name: str) -> str:
    return (FIXTURES / venue / name).read_text(encoding='utf-8')


def doctor_routes(**overrides: Any) -> dict[str, Any]:
    routes: dict[str, Any] = {
        '/spot/currency_pairs/STONK_USDT': dict(GATE_PAIR),
        '/spot/time': lambda url: {'server_time': int(time.time() * 1000)},
        '/v5/market/instruments-info': FakeResponse(200, fixture('bybit', 'instruments_info_ok.json')),
        '/v5/market/time': lambda url: {'retCode': 0, 'retMsg': 'OK',
                                        'result': {'timeSecond': str(int(time.time())),
                                                   'timeNano': str(int(time.time() * 1e9))},
                                        'time': int(time.time() * 1000)},
        '/api/v2/mix/market/contracts': FakeResponse(200, fixture('bitget', 'rest_contracts_ok.json')),
    }
    routes.update(overrides)
    return routes


# ----------------------------------------------------------------------------- feeds

class FakeWake:
    """``hub.wake``: waiting advances the fake clock by the timeout."""

    def __init__(self, clock: list) -> None:
        self.clock = clock
        self._set = False
        self.waits = 0

    def wait(self, timeout: float | None = None) -> bool:
        self.waits += 1
        if not self._set and timeout:
            self.clock[0] += float(timeout)
        return self._set

    def set(self) -> None:
        self._set = True

    def clear(self) -> None:
        self._set = False

    def is_set(self) -> bool:
        return self._set


def make_book(mid: Decimal, spread_pct: Decimal = Decimal('0.1'), *, age: float = 0.0) -> Book:
    half = mid * spread_pct / Decimal(200)
    received = time.monotonic() - age
    return Book(bids=((mid - half, Decimal(1000)),), asks=((mid + half, Decimal(1000)),), received=received,
                event_ms=int((time.time() - age) * 1000), update_id=None)


class FakeHub:
    """Produces fresh books at the current fake clock.

    ``sources``: source id -> Decimal mid, or callable(elapsed_seconds) -> Decimal | 'stale' | None.
    ``spot``: (bid, ask) or callable(elapsed) -> (bid, ask) | None.
    """

    def __init__(self, clock: list, sources: dict[str, Any], *, spot: Any = (Decimal('0.1350'), Decimal('0.1360')),
                 spread_pct: Decimal = Decimal('0.1')) -> None:
        self.clock = clock
        self.sources = sources
        self.spot = spot
        self.spread_pct = spread_pct
        self.wake = FakeWake(clock)
        self.started = 0
        self.stopped = 0
        self.t0 = clock[0]

    def _elapsed(self) -> float:
        return self.clock[0] - self.t0

    def start(self) -> None:
        self.started += 1
        self.t0 = self.clock[0]

    def stop(self) -> None:
        self.stopped += 1

    def source_snapshot(self, source_id: str) -> SourceSnapshot:
        value = self.sources.get(source_id)
        if callable(value):
            value = value(self._elapsed())
        if value is None:
            return SourceSnapshot(source_id, None, None, 'DISCONNECTED', False)
        if value == 'stale':
            return SourceSnapshot(source_id, make_book(Decimal('0.1356'), self.spread_pct, age=10.0), None, None, True)
        return SourceSnapshot(source_id, make_book(Decimal(value), self.spread_pct), None, None, True)

    def fx_snapshot(self) -> FxSnapshot | None:
        return None

    def _spot(self) -> Any:
        spot = self.spot(self._elapsed()) if callable(self.spot) else self.spot
        return spot

    def spot_book(self) -> Book | None:
        spot = self._spot()
        if spot is None:
            return None
        bid, ask = spot
        return Book(bids=((Decimal(bid), Decimal(500)),), asks=((Decimal(ask), Decimal(500)),),
                    received=time.monotonic(), event_ms=int(time.time() * 1000), update_id=1)

    def spot_book_error(self) -> str | None:
        return None if self._spot() is not None else 'NO_DATA'

    def diagnostics(self) -> dict:
        return {sid: {'connected': True} for sid in self.sources}

    def trades_since(self, seq: int) -> list:
        return []

    def last_trade_seq(self) -> int:
        return 0


# ----------------------------------------------------------------------------- run fakes

class FakeEngine:
    """Engine stand-in recording the runner's calls. ``script``: step number -> exception."""

    def __init__(self, *, script: dict[int, BaseException] | None = None, next_step: float = 1.0) -> None:
        self.script = dict(script or {})
        self.next_step = next_step
        self.calls: list[str] = []
        self.preflights: list[str] = []
        self.samples = 0
        self.steps = 0
        self.step_times: list[float] = []
        self.stops: list[str] = []
        self.cleared: list[str] = []
        self.args: tuple = ()
        self.kwargs: dict = {}

    def factory(self, *args: Any, **kwargs: Any) -> 'FakeEngine':
        self.args, self.kwargs = args, kwargs
        self.calls.append('created')
        return self

    def startup(self) -> None:
        self.calls.append('startup')

    def preflight(self, origin: str = 'loop') -> str | None:
        self.preflights.append(origin)
        return None

    def sample_index(self) -> None:
        self.samples += 1

    def step(self) -> str:
        self.steps += 1
        self.step_times.append(time.monotonic())
        exc = self.script.get(self.steps)
        if exc is not None:
            raise exc
        return 'RUNNING'

    def next_step_seconds(self) -> float:
        return self.next_step

    def next_reconciliation_deadline(self) -> float | None:
        return None

    def request_stop(self, reason: str) -> None:
        self.stops.append(reason)

    def clear_stop(self, reason: str) -> None:
        self.cleared.append(reason)

    def shutdown(self) -> None:
        self.calls.append('shutdown')

    def status(self) -> dict:
        return {'state': 'RUNNING', 'steps': self.steps, 'center': Decimal('0.13561'), 'stops': list(self.stops)}


class FakeNotifier:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []
        self.started = 0
        self.flushed: list[float] = []
        self.closed = 0

    def event(self, key: str, text: str, now_ms: int | None = None) -> bool:
        self.events.append((key, text))
        return True

    def start(self) -> None:
        self.started += 1

    def flush(self, timeout: float = 15.0) -> int:
        self.flushed.append(timeout)
        return 0

    def close(self) -> None:
        self.closed += 1

    def healthy(self) -> bool:
        return True


class FakeLiveVenue:
    """GateVenue stand-in for run / cancel-only (no network)."""

    live = True

    def __init__(self, *, problems: list[str] | None = None, skews_ms: list[int] | None = None,
                 opens: list | None = None, cancel_effect: bool = True) -> None:
        self.meta = None
        self.problems = list(problems or [])
        self.skews = list(skews_ms or [])
        self.opens = list(opens or [])
        self.cancel_effect = cancel_effect
        self.calls: list[str] = []
        self.cancel_calls: list[list[tuple]] = []
        self.cancel_all_calls = 0
        self.closed = 0

    def pair_meta(self):
        self.calls.append('pair_meta')
        self.meta = pair_meta()
        return self.meta

    def account_check(self) -> dict:
        self.calls.append('account_check')
        return {'problems': list(self.problems), 'key_mode': 1, 'detected_account': 'spot'}

    def server_time_ms(self) -> int:
        self.calls.append('server_time')
        skew = self.skews.pop(0) if self.skews else 0
        return int(time.time() * 1000) + skew

    def open_orders(self) -> list:
        self.calls.append('open_orders')
        return list(self.opens)

    def cancel_many(self, reqs) -> dict:
        self.cancel_calls.append([(r.text, r.order_id) for r in reqs])
        ids = {str(r.order_id) for r in reqs if r.order_id is not None}
        texts = {r.text for r in reqs if r.order_id is None}
        if self.cancel_effect:
            self.opens = [o for o in self.opens if str(o.order_id) not in ids and o.text not in texts]
        return {r.text: 'sent' if self.cancel_effect else 'unknown' for r in reqs}

    def cancel_all_pair(self) -> None:
        self.cancel_all_calls += 1
        self.opens = []

    def countdown(self, timeout_seconds: int):
        raise AssertionError('cancel-only / tests must never touch the DMS')

    def close(self) -> None:
        self.closed += 1


def order(order_id: str, text: str) -> SimpleNamespace:
    return SimpleNamespace(order_id=order_id, text=text)


class FakeFill:
    def __init__(self, kinds: list[str | None] | None = None) -> None:
        self.kinds = list(kinds or [])
        self.started = 0
        self.stopped = 0

    def start(self) -> None:
        self.started += 1

    def stop(self) -> None:
        self.stopped += 1

    def consume(self) -> str | None:
        return self.kinds.pop(0) if self.kinds else None

    def status(self) -> dict:
        return {'enabled': True, 'connected': True}


def forbid(name: str) -> Callable[..., Any]:
    def factory(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f'{name} must not be called')
    return factory
