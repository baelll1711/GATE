"""Offline helpers for the venue adapter tests (no network: conftest blocks sockets)."""
from __future__ import annotations

import gzip
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from gate_stonk.config import SourceConfig

FIXTURES = Path(__file__).resolve().parent / 'fixtures'

NOW = 1000.0                    # monotonic seconds passed to on_text / rest_snapshot
WALL_MS = 1_791_640_001_000     # wall clock ms passed to on_text / rest_snapshot


def fixture_text(venue: str, name: str) -> str:
    return (FIXTURES / venue / name).read_text(encoding='utf-8').strip()


def fixture_json(venue: str, name: str) -> Any:
    return json.loads(fixture_text(venue, name))


def gz(text: str) -> bytes:
    return gzip.compress(text.encode('utf-8'))


def source(venue: str, symbol: str, *, source_id: str = 'src', quote: str = 'USDT', price: str = 'mid',
           url: str | None = None, market_index: int | None = None) -> SourceConfig:
    return SourceConfig(id=source_id, venue=venue, symbol=symbol, quote=quote, price=price, weight=Decimal('0.2'),
                        required=False, max_spread_pct=Decimal('1') if price in ('mid', 'microprice') else None,
                        max_age_seconds=3.0, heartbeat_seconds=0.0, rest_refresh_after_seconds=1.5,
                        enabled=True, url=url, market_index=market_index)


class FakeResponse:
    """requests.Response stand-in: raw ``text`` plus ``json()`` (floats, as requests does)."""

    def __init__(self, status_code: int = 200, text: str | None = None):
        self.status_code = status_code
        self.text = text

    def json(self) -> Any:
        if self.text is None:
            raise ValueError('no body')
        return json.loads(self.text)


class JsonOnlyResponse:
    """A response object exposing only ``json()`` (no ``text`` attribute)."""

    def __init__(self, status_code: int, obj: Any):
        self.status_code = status_code
        self._obj = obj

    def json(self) -> Any:
        return self._obj


def reply(venue: str, name: str, status: int = 200) -> FakeResponse:
    return FakeResponse(status, fixture_text(venue, name))


class FakeHttp:
    """``requests.Session``-like: routes by URL path suffix, records every call."""

    def __init__(self, routes: dict[str, Any]):
        self.routes = routes
        self.calls: list[dict] = []

    def get(self, url: str, params: dict | None = None, headers: dict | None = None,
            timeout: float | None = None) -> Any:
        self.calls.append({'url': url, 'params': dict(params or {}), 'headers': dict(headers or {}),
                           'timeout': timeout})
        for suffix in sorted(self.routes, key=len, reverse=True):
            if url.endswith(suffix):
                answer = self.routes[suffix]
                if isinstance(answer, BaseException):
                    raise answer
                return answer
        raise AssertionError(f'unexpected URL {url}')

    def call(self, suffix: str) -> dict:
        matches = [call for call in self.calls if call['url'].endswith(suffix)]
        assert matches, f'no call to {suffix}'
        return matches[-1]
