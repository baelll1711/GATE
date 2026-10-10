"""Contract between the generic WebSocket worker / FeedHub and venue adapters (DESIGN §16).

Adapters translate one venue's wire protocol into ``Update`` objects; the worker
owns sockets, threads, reconnects and pings; the hub owns snapshots and
validation. Adapters never touch sockets or threads themselves.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ..types import Book, Ticker

GATE_SPOT_KEY = 'gate_spot'
FX_KEY = 'fx'


@dataclass(frozen=True)
class Reply:
    """A text frame the worker must send back (e.g. 'Pong', '{"type":"pong"}')."""

    text: str


@dataclass(frozen=True)
class Update:
    kind: str                          # 'book' | 'ticker' | 'trades' | 'error' | 'subscribed' | 'ignore'
    key: str                           # source id, GATE_SPOT_KEY or FX_KEY
    book: Book | None = None
    ticker: Ticker | None = None
    trades: tuple[dict, ...] = ()      # {id, price, amount, side, create_time_ms} (Decimal/str/int values)
    error: str | None = None           # kind == 'error': invalidate this key with the code
    source: str = 'ws'                 # 'ws' | 'rest'


class VenueAdapter(Protocol):
    venue: str
    key: str
    ping_interval: float               # seconds between app-level pings; 0 -> websocket control ping only
    idle_timeout: float                # no frame of any kind for this long -> reconnect

    def ws_url(self) -> str: ...

    def subscribe_frames(self) -> list[str]: ...

    def ping_frame(self, wall_ms: int) -> str | None: ...

    def decode(self, raw: bytes | str) -> str: ...

    def on_text(self, text: str, now: float, wall_ms: int) -> list[Update | Reply]: ...

    def on_reset(self) -> None: ...

    def rest_snapshot(self, http: Any, timeout: float, now: float, wall_ms: int) -> list[Update]: ...

    def instrument_info(self, http: Any, timeout: float) -> dict: ...
