"""Live-only private Gate spot WebSocket used as a WAKE-UP HINT for reconciliation.

Subscribes ``spot.orders`` and ``spot.usertrades`` for the traded pair with Gate's
private-channel authentication (notes/api_gate_ws_futures.md §3.1)::

    SIGN = hex(HMAC_SHA512(secret, "channel=%s&event=%s&time=%d"))
    {"time": t, "id": n, "channel": c, "event": "subscribe", "payload": [pair],
     "auth": {"method": "api_key", "KEY": key, "SIGN": SIGN}}

Every own event (client ``text`` starting with ``t-<client_prefix>-``) sets ``wake``;
the run loop then starts the next REST reconciliation earlier. Quantities, prices and
balances from this stream are NEVER applied: REST stays the only source of truth
(reference rule "WS events are only wake-up hints, never ledger writes").

The socket runs in the generic ``feeds.ws.WsWorker`` (reconnect with backoff 1 -> 30 s,
``spot.ping`` every 10 s, idle limit, logs carry exception type names / reason codes
only). Subscription frames are rebuilt (fresh time and signature) on every connect.
The API key and secret never appear in logs, ``status()`` or exception messages.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
from collections.abc import Callable, Iterable
from decimal import Decimal
from typing import Any

from .errors import Unsafe

LOG = logging.getLogger(__name__)

PRIVATE_WS_URL = 'wss://api.gateio.ws/ws/v4/'
CHANNELS = ('spot.orders', 'spot.usertrades')
PING_SECONDS = 10.0
IDLE_SECONDS = 60.0
KEY = 'fill_wakeup'
FILL = 'fill'
EVENT = 'event'


def subscription_message(channel: str, event: str, ts: int) -> str:
    """The text Gate signs for a private channel request."""
    return 'channel=%s&event=%s&time=%d' % (channel, event, int(ts))


def sign_subscription(secret: str, channel: str, event: str, ts: int) -> str:
    """hex(HMAC-SHA512(secret, "channel=%s&event=%s&time=%d")), lowercase."""
    return hmac.new(secret.encode('utf-8'), subscription_message(channel, event, ts).encode('utf-8'),
                    hashlib.sha512).hexdigest()


def subscribe_request(key: str, secret: str, channel: str, payload: list[str], ts: int, request_id: int) -> dict:
    """One authenticated subscribe request (a dict; the caller serializes it)."""
    return {
        'time': int(ts), 'id': int(request_id), 'channel': channel, 'event': 'subscribe', 'payload': list(payload),
        'auth': {'method': 'api_key', 'KEY': key, 'SIGN': sign_subscription(secret, channel, 'subscribe', ts)},
    }


def _positive(value: Any) -> bool:
    try:
        return Decimal(str(value)) > 0
    except Exception:
        return False


class _PrivateAdapter:
    """VenueAdapter-shaped wrapper so ``WsWorker`` can own the private socket."""

    venue = 'gate_spot_private'
    key = KEY
    ping_interval = PING_SECONDS
    idle_timeout = IDLE_SECONDS

    def __init__(self, owner: 'FillWakeup') -> None:
        self._owner = owner

    def ws_url(self) -> str:
        return self._owner.url

    def subscribe_frames(self) -> list[str]:
        return self._owner.subscribe_frames()

    def ping_frame(self, wall_ms: int) -> str | None:
        return json.dumps({'time': int(wall_ms) // 1000, 'channel': 'spot.ping'}, separators=(',', ':'))

    def decode(self, raw: bytes | str) -> str:
        if isinstance(raw, (bytes, bytearray)):
            try:
                return bytes(raw).decode('utf-8')
            except UnicodeDecodeError:
                return ''
        return raw if isinstance(raw, str) else ''

    def on_reset(self) -> None:
        return None

    def on_text(self, text: str, now: float, wall_ms: int) -> list:
        self._owner.handle_text(text)
        return []          # nothing is ever published to a feed hub


class _Sink:
    """FeedSink for the worker: tracks the connection state only."""

    def __init__(self, owner: 'FillWakeup') -> None:
        self._owner = owner

    def publish(self, update: Any) -> None:
        return None

    def invalidate(self, key: str, reason: str) -> None:
        self._owner._state = reason

    def set_connected(self, key: str, connected: bool) -> None:
        self._owner._connected = bool(connected)

    def connection_reset(self, keys: Iterable[str]) -> None:
        return None

    def should_reconnect(self, stream: str, now: float, keys: Iterable[str]) -> bool:
        return False


class FillWakeup:
    """Private order/trade stream -> ``wake`` event (live only; never a data source)."""

    def __init__(self, cfg: Any, *, connect: Callable[..., Any] | None = None,
                 on_wake: Callable[[], None] | None = None, url: str | None = None) -> None:
        secrets = cfg.secrets
        if not (secrets.gate_key and secrets.gate_secret):
            raise Unsafe('FILL_WAKEUP_REQUIRES_KEYS')
        self._key = secrets.gate_key
        self._secret = secrets.gate_secret
        self.pair = str(cfg.market['currency_pair'])
        self.prefix = f"t-{cfg.app['client_prefix']}-"
        self.url = url or PRIVATE_WS_URL
        self.timeout = float(cfg.app['request_timeout_seconds'])
        self._connect = connect
        self.on_wake = on_wake
        self.wake = threading.Event()
        self._lock = threading.Lock()
        self._pending: str | None = None      # strongest pending kind: 'fill' > 'event'
        self._connected = False
        self._state: str | None = None
        self._worker: Any = None
        self.counters = {'frames': 0, 'own_events': 0, 'own_fills': 0, 'foreign_or_other': 0,
                         'subscribe_errors': 0, 'invalid_frames': 0}
        self.last_event_ms: int | None = None
        self.last_error: str | None = None

    def __repr__(self) -> str:   # never leak credentials
        return f'FillWakeup(pair={self.pair!r}, connected={self._connected})'

    # ------------------------------------------------------------------ lifecycle
    def make_worker(self) -> Any:
        """Create (not start) the socket worker; ``start()`` runs it in a daemon thread."""
        from .feeds.ws import WsWorker

        self._worker = WsWorker(_PrivateAdapter(self), _Sink(self), timeout=self.timeout, connect=self._connect)
        return self._worker

    def start(self) -> None:
        if self._worker is not None:
            return
        self.make_worker().start()

    def stop(self) -> None:
        worker, self._worker = self._worker, None
        if worker is not None:
            try:
                worker.stop()
            except Exception as exc:
                LOG.warning('fill wakeup stop failed: %s', type(exc).__name__)
        self._connected = False

    @property
    def worker(self) -> Any:
        return self._worker

    # ------------------------------------------------------------------ protocol
    def subscribe_frames(self, ts: int | None = None) -> list[str]:
        ts = int(time.time()) if ts is None else int(ts)
        return [json.dumps(subscribe_request(self._key, self._secret, channel, [self.pair], ts, index),
                           separators=(',', ':'))
                for index, channel in enumerate(CHANNELS, start=1)]

    def handle_text(self, text: str) -> str | None:
        """Process one frame; returns 'fill' / 'event' when it woke the loop, else None.

        Raises ``Unsafe`` for a refused subscription (the worker reconnects with backoff).
        """
        self.counters['frames'] += 1
        try:
            msg = json.loads(text)
        except (TypeError, ValueError):
            self.counters['invalid_frames'] += 1
            return None
        if not isinstance(msg, dict):
            self.counters['invalid_frames'] += 1
            return None
        channel, event = msg.get('channel'), msg.get('event')
        if channel not in CHANNELS:
            return None            # spot.pong and others
        error = msg.get('error')
        if event == 'subscribe':
            if error:
                self.counters['subscribe_errors'] += 1
                code = error.get('code') if isinstance(error, dict) else None
                code = code if isinstance(code, int) and not isinstance(code, bool) else 'UNKNOWN'
                self.last_error = f'FILL_WAKEUP_SUBSCRIBE_FAILED:{code}'
                raise Unsafe(self.last_error)
            return None
        if event != 'update':
            return None
        result = msg.get('result')
        rows = result if isinstance(result, list) else [result] if isinstance(result, dict) else []
        kind: str | None = None
        for row in rows:
            row_kind = self._row_kind(channel, row)
            if row_kind is None:
                self.counters['foreign_or_other'] += 1
                continue
            if row_kind == FILL or kind is None:
                kind = row_kind
        if kind is not None:
            self._signal(kind)
        return kind

    def _row_kind(self, channel: str, row: Any) -> str | None:
        if not isinstance(row, dict):
            return None
        text = row.get('text')
        if not isinstance(text, str) or not text.startswith(self.prefix):
            return None
        pair = row.get('currency_pair')
        if pair is not None and pair != self.pair:
            return None
        if channel == 'spot.usertrades':
            return FILL
        if _positive(row.get('filled_amount')) or row.get('finish_as') == 'filled':
            return FILL
        return EVENT

    def _signal(self, kind: str) -> None:
        with self._lock:
            if kind == FILL or self._pending is None:
                self._pending = kind
            self.counters['own_events'] += 1
            if kind == FILL:
                self.counters['own_fills'] += 1
            self.last_event_ms = int(time.time() * 1000)
        self.wake.set()
        if self.on_wake is not None:
            try:
                self.on_wake()
            except Exception as exc:
                LOG.warning('fill wakeup callback failed: %s', type(exc).__name__)

    def consume(self) -> str | None:
        """'fill' / 'event' if an own event arrived since the last call (and reset), else None."""
        with self._lock:
            kind, self._pending = self._pending, None
            self.wake.clear()
        return kind

    def status(self) -> dict:
        worker = self._worker
        stream = None
        if worker is not None:
            try:
                stream = worker.status()
            except Exception as exc:
                stream = {'diagnostic_error': type(exc).__name__}
        return {'enabled': True, 'connected': self._connected, 'state': self._state,
                'channels': list(CHANNELS), 'counters': dict(self.counters),
                'last_event_ms': self.last_event_ms, 'last_error': self.last_error, 'stream': stream,
                'note': 'wake-up only; REST reconciliation is authoritative'}
