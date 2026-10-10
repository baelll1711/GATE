"""Per-endpoint-group REST rate limiter (DESIGN §10, notes/api_gate_rest.md §12).

Mechanism ported from the reference RestBudget (zec_spot/ratelimit.py), numbers
replaced by Gate's documented per-group limits:

* one sliding window per bucket (``limit`` requests per ``window`` seconds);
* ``admit(bucket)`` is called BEFORE a request is signed or sent. An ordinary
  (non-emergency) request that does not fit raises ``BudgetWait`` and nothing is
  sent; an emergency request (cancel, DMS) is NEVER refused, only recorded (and
  counted as an overrun when it exceeds the limit);
* ``rate_limited(bucket)`` after an HTTP 429 / ``TOO_MANY_REQUESTS`` blocks
  ordinary admits of that bucket for 15 s; emergency admits ignore the block;
* bucket names may be a template plus an endpoint group (``private_read:/spot/orders``):
  each endpoint group gets its own window with the template's limits, as Gate
  counts "other private" and public requests per endpoint.

Gate limits (conservative; notes §12): place + amend share 10 r/s per UID+market
and a low fill ratio can cut them to 10 r / 10 s, hence the ``order`` bucket is
``safety.max_order_requests_per_10s`` per 10 s (a batch create is one request);
cancel 200 r/s; other private 200 r / 10 s per endpoint; public 200 r / 10 s per
endpoint (IP). Every limit except ``order`` is multiplied by
``safety.rate_limit_utilization`` (default 0.5).
"""
from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from decimal import Decimal
from typing import Any, Mapping

from .dec import D
from .errors import BudgetWait

LOG = logging.getLogger(__name__)

RATE_LIMITED_BLOCK_SECONDS = 15.0

# Gate documented limits (requests, window seconds) before utilization.
GATE_CANCEL_LIMIT = (200, 1.0)
GATE_DMS_LIMIT = (200, 10.0)          # "other private" 200 r / 10 s
GATE_PRIVATE_READ_LIMIT = (200, 10.0)
GATE_PUBLIC_LIMIT = (200, 10.0)
ORDER_WINDOW_SECONDS = 10.0

ORDER = 'order'
CANCEL = 'cancel'
DMS = 'dms'
PRIVATE_READ = 'private_read'
PUBLIC = 'public'
# Buckets whose requests protect the account (cancel, dead man's switch): never refused.
EMERGENCY_BUCKETS = frozenset({CANCEL, DMS})


def _scaled(limit: int, utilization: Decimal) -> int:
    return max(1, int(math.floor(D(limit) * utilization)))


def default_buckets(max_order_requests_per_10s: int,
                    utilization: Decimal | float | str = Decimal('0.5')) -> dict[str, tuple[int, float]]:
    """Bucket table from the two safety settings (validated here as well, fail closed)."""
    if (isinstance(max_order_requests_per_10s, bool) or not isinstance(max_order_requests_per_10s, int)
            or max_order_requests_per_10s < 1):
        raise ValueError('max_order_requests_per_10s must be a positive integer')
    util = D(utilization)
    if not Decimal(0) < util <= Decimal(1):
        raise ValueError('rate_limit_utilization must be in (0, 1]')
    return {
        ORDER: (max_order_requests_per_10s, ORDER_WINDOW_SECONDS),
        CANCEL: (_scaled(GATE_CANCEL_LIMIT[0], util), GATE_CANCEL_LIMIT[1]),
        DMS: (_scaled(GATE_DMS_LIMIT[0], util), GATE_DMS_LIMIT[1]),
        PRIVATE_READ: (_scaled(GATE_PRIVATE_READ_LIMIT[0], util), GATE_PRIVATE_READ_LIMIT[1]),
        PUBLIC: (_scaled(GATE_PUBLIC_LIMIT[0], util), GATE_PUBLIC_LIMIT[1]),
    }


class _Window:
    __slots__ = ('limit', 'window', 'events', 'total', 'admitted', 'refused', 'emergency_admits',
                 'emergency_over_limit', 'charged_total', 'peak', 'last_headers')

    def __init__(self, limit: int, window: float):
        self.limit = limit
        self.window = window
        self.events: deque[tuple[float, int]] = deque()
        self.total = 0
        self.admitted = 0
        self.refused = 0
        self.emergency_admits = 0
        self.emergency_over_limit = 0
        self.charged_total = 0
        self.peak = 0
        self.last_headers: dict[str, Any] | None = None

    def prune(self, now: float) -> None:
        while self.events and now - self.events[0][0] >= self.window:
            self.total -= self.events.popleft()[1]

    def until_fits(self, cost: int, now: float) -> float:
        excess = self.total + cost - self.limit
        if excess <= 0 or not self.events:
            return 0.0
        freed = 0
        for at, weight in self.events:
            freed += weight
            if freed >= excess:
                return max(0.0, at + self.window - now)
        return self.window

    def record(self, cost: int, now: float) -> None:
        self.events.append((now, cost))
        self.total += cost
        self.charged_total += cost
        self.peak = max(self.peak, self.total)


class RateLimiter:
    """Sliding-window limiter with one window per bucket (and per endpoint group)."""

    def __init__(self, buckets: Mapping[str, tuple[int, float]], *,
                 block_seconds: float = RATE_LIMITED_BLOCK_SECONDS):
        if not isinstance(buckets, Mapping) or not buckets:
            raise ValueError('RateLimiter needs at least one bucket')
        templates: dict[str, tuple[int, float]] = {}
        for name, spec in buckets.items():
            if not isinstance(name, str) or not name or ':' in name:
                raise ValueError('Bucket names must be non-empty and contain no ":"')
            try:
                limit, window = spec
            except (TypeError, ValueError):
                raise ValueError(f'Bucket {name}: expected (limit, window_seconds)') from None
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                raise ValueError(f'Bucket {name}: limit must be a positive integer')
            if isinstance(window, bool) or not isinstance(window, (int, float)) or not 0 < float(window) < 3600:
                raise ValueError(f'Bucket {name}: window must be in (0, 3600) seconds')
            templates[name] = (limit, float(window))
        if isinstance(block_seconds, bool) or not 0 < float(block_seconds) <= 600:
            raise ValueError('block_seconds must be in (0, 600]')
        self._templates = templates
        self._windows: dict[str, _Window] = {}
        self._blocked: dict[str, float] = {}
        self._block_seconds = float(block_seconds)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ factories
    @classmethod
    def from_settings(cls, max_order_requests_per_10s: int,
                      utilization: Decimal | float | str = Decimal('0.5')) -> 'RateLimiter':
        return cls(default_buckets(max_order_requests_per_10s, utilization))

    @classmethod
    def from_config(cls, cfg: Any) -> 'RateLimiter':
        safety = cfg.safety
        orders = safety.get('max_order_requests_per_10s', 8)
        if isinstance(orders, Decimal) and orders == orders.to_integral_value():
            orders = int(orders)
        return cls.from_settings(orders, safety.get('rate_limit_utilization', Decimal('0.5')))

    # ------------------------------------------------------------------ internals
    def _resolve(self, bucket: str) -> tuple[str, str]:
        if not isinstance(bucket, str) or not bucket:
            raise ValueError('Bucket name required')
        template = bucket.split(':', 1)[0]
        if template not in self._templates:
            raise ValueError(f'Unknown rate-limit bucket: {bucket}')
        return bucket, template

    def _window(self, full: str, template: str) -> _Window:
        window = self._windows.get(full)
        if window is None:
            limit, seconds = self._templates[template]
            window = self._windows[full] = _Window(limit, seconds)
        return window

    def _blocked_for(self, full: str, template: str, now: float) -> float:
        until = max(self._blocked.get(full, 0.0), self._blocked.get(template, 0.0))
        return max(0.0, until - now)

    @staticmethod
    def _cost(cost: int) -> int:
        if isinstance(cost, bool) or not isinstance(cost, int) or cost < 1:
            raise ValueError('Request cost must be a positive integer')
        return cost

    # ------------------------------------------------------------------ API
    def wait(self, bucket: str, cost: int = 1, *, emergency: bool = False) -> float:
        """Seconds until ``cost`` more requests may start in ``bucket`` (0.0 = now). Records nothing."""
        cost = self._cost(cost)
        full, template = self._resolve(bucket)
        with self._lock:
            now = time.monotonic()
            window = self._window(full, template)
            window.prune(now)
            if emergency:
                return 0.0
            if cost > window.limit:
                raise ValueError(f'Request cost {cost} exceeds the {full} limit {window.limit}')
            return max(self._blocked_for(full, template, now), window.until_fits(cost, now))

    def admit(self, bucket: str, cost: int = 1, *, emergency: bool = False) -> None:
        """Admit ONE physical request before it is signed/sent, or raise BudgetWait (nothing sent).

        Emergency requests are always admitted and recorded; exceeding the limit only
        increments ``emergency_over_limit`` (diagnostics) and logs a warning.
        """
        cost = self._cost(cost)
        full, template = self._resolve(bucket)
        with self._lock:
            now = time.monotonic()
            window = self._window(full, template)
            window.prune(now)
            if emergency:
                window.emergency_admits += 1
                if window.total + cost > window.limit:
                    window.emergency_over_limit += 1
                    over = True
                else:
                    over = False
                window.record(cost, now)
                window.admitted += 1
            else:
                if cost > window.limit:
                    raise ValueError(f'Request cost {cost} exceeds the {full} limit {window.limit}')
                wait = max(self._blocked_for(full, template, now), window.until_fits(cost, now))
                if wait > 0:
                    window.refused += 1
                    raise BudgetWait(wait, full)
                window.record(cost, now)
                window.admitted += 1
                over = False
        if over:
            try:
                LOG.warning('RATE_LIMIT_EMERGENCY_OVERRUN bucket=%s', full)
            except Exception:
                pass

    def rate_limited(self, bucket: str, seconds: float | None = None) -> None:
        """After HTTP 429 / TOO_MANY_REQUESTS: block ordinary admits of ``bucket`` for a while.

        A template name (``private_read``) blocks every endpoint group of that template.
        """
        full, _template = self._resolve(bucket)
        seconds = self._block_seconds if seconds is None else float(seconds)
        if not 0 < seconds <= 3600:
            raise ValueError('Block seconds must be in (0, 3600]')
        with self._lock:
            until = time.monotonic() + seconds
            self._blocked[full] = max(self._blocked.get(full, 0.0), until)

    def blocked_for(self, bucket: str) -> float:
        full, template = self._resolve(bucket)
        with self._lock:
            return self._blocked_for(full, template, time.monotonic())

    def used(self, bucket: str) -> int:
        full, template = self._resolve(bucket)
        with self._lock:
            window = self._window(full, template)
            window.prune(time.monotonic())
            return window.total

    def limit(self, bucket: str) -> int:
        _full, template = self._resolve(bucket)
        return self._templates[template][0]

    def record_headers(self, bucket: str, info: Mapping[str, Any]) -> None:
        """Remember the last rate-limit headers Gate sent for this bucket (diagnostics only)."""
        full, template = self._resolve(bucket)
        clean = {str(k): (v if v is None or isinstance(v, (int, str)) else str(v)) for k, v in dict(info).items()}
        with self._lock:
            self._window(full, template).last_headers = clean

    def status(self) -> dict:
        """JSON-safe snapshot of every bucket in use."""
        with self._lock:
            now = time.monotonic()
            result: dict[str, Any] = {}
            names = set(self._windows) | set(self._templates)
            for name in sorted(names):
                template = name.split(':', 1)[0]
                window = self._window(name, template)
                window.prune(now)
                result[name] = {
                    'limit': window.limit,
                    'window_seconds': window.window,
                    'used': window.total,
                    'peak': window.peak,
                    'admitted': window.admitted,
                    'refused': window.refused,
                    'charged_total': window.charged_total,
                    'emergency_admits': window.emergency_admits,
                    'emergency_over_limit': window.emergency_over_limit,
                    'blocked_seconds': round(self._blocked_for(name, template, now), 3),
                    'last_headers': dict(window.last_headers) if window.last_headers else None,
                }
            return result
