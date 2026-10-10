"""Generic WebSocket worker: one daemon thread per venue adapter (DESIGN §16).

Port of the reference ``Feeds._worker`` loop (12-chagpt ``spot_portfolio/feeds.py``)
with the venue specifics moved into adapters:

* connect timeout = ``request_timeout_seconds``; recv timeout 1 s;
* reconnect backoff 1 -> 2 -> 4 ... <= 30 s, reset to 1 s after a connection
  cycle that lived more than 30 s; the backoff wait is interruptible by stop;
* application ping every ``adapter.ping_interval`` seconds (``adapter.ping_frame``),
  or a websocket control ping every 15 s when the adapter has no app ping;
* idle limit ``adapter.idle_timeout`` (no frame of any kind) -> ``WEBSOCKET_IDLE``;
* every (re)connect: keys invalidated ``CONNECTING``, ``adapter.on_reset()``,
  health streaks reset; every disconnect: keys invalidated ``DISCONNECTED``;
* ``Reply`` items from the adapter are sent back on the same socket;
* ``Update(kind='error')`` invalidates only that key (the socket stays up);
* an ``Unsafe`` raised by the adapter (socket-fatal, e.g. subscription rejected)
  or any other exception reconnects;
* a key that keeps receiving stale data reconnects through the bounded
  ``QuoteHealth.should_reconnect`` policy (``STALE_STREAM_RECONNECT``);
* logs never contain payloads, URLs or exception messages: only exception type
  names (and our own UPPER_SNAKE reason codes).
"""
from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any, Protocol

import websocket

from ..errors import Unsafe
from .base import Reply, Update, VenueAdapter

LOG = logging.getLogger(__name__)

RECV_TIMEOUT_SECONDS = 1.0
CONTROL_PING_SECONDS = 15.0
DEFAULT_IDLE_TIMEOUT_SECONDS = 60.0
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 30.0
BACKOFF_RESET_AFTER_SECONDS = 30.0
STOP_JOIN_SECONDS = 2.0
DATA_KINDS = ('book', 'ticker', 'trades', 'error')
_CODE = re.compile(r'^[A-Z][A-Z0-9_]*(:[A-Za-z0-9_.\-]+)*$')


class FeedSink(Protocol):
    """What a worker needs from the hub (FeedHub implements it)."""

    def publish(self, update: Update) -> None: ...          # raises Unsafe when the update was rejected

    def invalidate(self, key: str, reason: str) -> None: ...

    def set_connected(self, key: str, connected: bool) -> None: ...

    def connection_reset(self, keys: Iterable[str]) -> None: ...

    def should_reconnect(self, stream: str, now: float, keys: Iterable[str]) -> bool: ...


def safe_reason(exc: BaseException) -> str:
    """Loggable failure name: our own reason code for Unsafe, else the type name only."""
    if isinstance(exc, Unsafe):
        text = str(exc)
        if len(text) <= 120 and _CODE.match(text):
            return text
    return type(exc).__name__


def adapter_keys(adapter: Any) -> tuple[str, ...]:
    keys = getattr(adapter, 'keys', None)
    if keys:
        return tuple(keys)
    return (adapter.key,)


class WsWorker:
    """Owns one adapter's socket in a daemon thread. ``run()`` is the thread body
    and may be called directly (tests drive it synchronously with fake sockets)."""

    def __init__(self, adapter: VenueAdapter, sink: FeedSink, *, timeout: float,
                 connect: Callable[..., Any] | None = None,
                 wait: Callable[[float], bool] | None = None) -> None:
        timeout = float(timeout)
        if not timeout > 0:
            raise ValueError('timeout must be positive')
        self.adapter = adapter
        self.sink = sink
        self.timeout = timeout
        self.name = str(adapter.key)
        self.keys = adapter_keys(adapter)
        self._connect = connect
        self._stop = threading.Event()
        self._wait = wait if wait is not None else self._stop.wait
        self._lock = threading.Lock()
        self._conn: Any = None
        self._thread: threading.Thread | None = None
        self.connects = 0          # successful connections
        self.attempts = 0          # connection attempts
        self.disconnects = 0
        self.last_error: str | None = None
        self.backoff = BACKOFF_INITIAL_SECONDS

    # ---------------------------------------------------------------- control
    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self.run, name=f'ws-{self.name}', daemon=True)
        self._thread.start()

    def request_stop(self) -> None:
        """Ask the loop to exit and unblock a pending recv by closing the socket."""
        self._stop.set()
        with self._lock:
            conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def stop(self, timeout: float = STOP_JOIN_SECONDS) -> None:
        self.request_stop()
        self.join(timeout)

    def join(self, timeout: float | None = None) -> None:
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict:
        return {'alive': self.is_alive(), 'attempts': self.attempts, 'connects': self.connects,
                'disconnects': self.disconnects, 'last_error': self.last_error,
                'backoff_seconds': self.backoff}

    # ------------------------------------------------------------------ loop
    def run(self) -> None:
        backoff = BACKOFF_INITIAL_SECONDS
        while not self._stop.is_set():
            started = time.monotonic()
            self._cycle()
            if self._stop.is_set():
                break
            if time.monotonic() - started > BACKOFF_RESET_AFTER_SECONDS:
                backoff = BACKOFF_INITIAL_SECONDS
            self.backoff = backoff
            if self._wait(backoff):
                break
            backoff = min(BACKOFF_MAX_SECONDS, backoff * 2)

    def _cycle(self) -> None:
        """One connection attempt and its receive loop; never raises."""
        conn = None
        self._invalidate_all('CONNECTING')
        self._safely(self.sink.connection_reset, self.keys)
        self.attempts += 1
        try:
            self.adapter.on_reset()
            prepare = getattr(self.adapter, 'prepare', None)
            if callable(prepare):
                prepare()          # e.g. Lighter resolves its market index over REST before subscribing
            conn = self._open()
            with self._lock:
                self._conn = conn
            if self._stop.is_set():
                return
            conn.settimeout(RECV_TIMEOUT_SECONDS)
            for frame in self.adapter.subscribe_frames():
                conn.send(frame)
            self.connects += 1
            for key in self.keys:
                self._safely(self.sink.set_connected, key, True)
            self._receive(conn)
        except Exception as exc:
            self.last_error = safe_reason(exc)
            if not self._stop.is_set():
                LOG.warning('%s feed disconnected: %s', self.name, self.last_error)
        finally:
            with self._lock:
                self._conn = None
            if conn is not None:
                self.disconnects += 1
                try:
                    conn.close()
                except Exception:
                    pass
            for key in self.keys:
                self._safely(self.sink.set_connected, key, False)
            self._invalidate_all('DISCONNECTED')

    def _open(self) -> Any:
        connect = self._connect if self._connect is not None else websocket.create_connection
        options: dict[str, Any] = {}
        headers = getattr(self.adapter, 'ws_headers', None)
        if callable(headers):
            extra = headers()
            if extra:
                options['header'] = extra
        return connect(self.adapter.ws_url(), timeout=self.timeout, **options)

    def _receive(self, conn: Any) -> None:
        adapter = self.adapter
        app_ping = float(getattr(adapter, 'ping_interval', 0) or 0)
        ping_every = app_ping if app_ping > 0 else CONTROL_PING_SECONDS
        idle_limit = float(getattr(adapter, 'idle_timeout', 0) or DEFAULT_IDLE_TIMEOUT_SECONDS)
        last_ping = last_message = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if now - last_ping >= ping_every:
                frame = adapter.ping_frame(int(time.time() * 1000)) if app_ping > 0 else None
                if frame is not None:
                    conn.send(frame)
                else:
                    conn.ping()
                last_ping = now
            if now - last_message > idle_limit:
                raise Unsafe('WEBSOCKET_IDLE')
            try:
                raw = conn.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if self._stop.is_set():
                return
            if raw is None or len(raw) == 0:
                raise Unsafe('WEBSOCKET_CLOSED')
            last_message = time.monotonic()
            text = adapter.decode(raw)
            now, wall_ms = time.monotonic(), int(time.time() * 1000)
            items = adapter.on_text(text, now, wall_ms)   # Unsafe here is socket-fatal -> reconnect
            for item in items or ():
                if isinstance(item, Reply):
                    conn.send(item.text)
                    continue
                if not isinstance(item, Update):
                    raise Unsafe('INVALID_ADAPTER_OUTPUT')
                if item.kind not in DATA_KINDS:
                    continue      # 'subscribed' / 'ignore'
                rejected = False
                try:
                    self.sink.publish(item)
                except Unsafe:
                    # The hub already invalidated exactly the affected key and
                    # recorded bounded diagnostics. Other keys stay usable.
                    rejected = True
                if (rejected or item.kind == 'error') and self.sink.should_reconnect(
                        self.name, time.monotonic(), self.keys):
                    # Receiving many old messages is not a healthy connection.
                    raise Unsafe('STALE_STREAM_RECONNECT')

    # --------------------------------------------------------------- helpers
    def _invalidate_all(self, reason: str) -> None:
        for key in self.keys:
            self._safely(self.sink.invalidate, key, reason)

    @staticmethod
    def _safely(fn: Callable[..., Any], *args: Any) -> None:
        try:
            fn(*args)
        except Exception as exc:
            LOG.warning('feed sink call failed: %s', type(exc).__name__)
