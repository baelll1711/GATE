"""Offline test harness: no network, no live credentials, controllable clock."""
from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'tests' / 'fixtures'
SENSITIVE_PREFIXES = ('GATE_', 'TELEGRAM_')


class NetworkBlocked(RuntimeError):
    pass


def _blocked(*args, **kwargs):
    raise NetworkBlocked('network access is blocked in tests')


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    for name in list(os.environ):
        if name.startswith(SENSITIVE_PREFIXES):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(socket.socket, 'connect', _blocked)
    monkeypatch.setattr(socket.socket, 'connect_ex', _blocked)
    monkeypatch.setattr(socket, 'create_connection', _blocked)
    monkeypatch.setattr(socket, 'getaddrinfo', _blocked)
    yield


@pytest.fixture
def clock(monkeypatch):
    """Controllable wall/monotonic clock: ``clock[0]`` is seconds since epoch."""
    current = [1_800_000_000.0]
    monkeypatch.setattr('time.time', lambda: current[0])
    monkeypatch.setattr('time.monotonic', lambda: current[0])
    return current


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
