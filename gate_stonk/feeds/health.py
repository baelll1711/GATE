"""Bounded quote-fault diagnostics and the stale-stream reconnect policy.

Port of the reference ``feed_health.py`` (12-chagpt). No quote is accepted here:
event/receipt-age validation stays mandatory in the hub and the index builder.
This module only

* aggregates ingestion faults per feed key into rate-limited log records
  (``QUOTE_FAULT`` at most once per 5 s per key, ``QUOTE_RECOVERED`` once),
* keeps lifetime counters and wall-clock step detection per key,
* decides when a stream that keeps delivering stale data must be reconnected
  (streak >= 3 and stale for >= 5 s, cooldown 30 s, <= 3 reconnects per 300 s).

Differences from the reference (deliberate):

* rows are keyed by the hub feed key (``bybit_perp``, ``gate_spot``, ``fx``)
  instead of ``source:symbol``; ``symbol`` is kept as an informational field;
* the stale reconnect policy applies to every stream, not only Binance;
* ``connection_reset`` / ``should_reconnect`` take the stream's keys explicitly.
"""
from __future__ import annotations

import copy
import json
import logging
import threading
from collections import deque
from collections.abc import Callable, Iterable

LOG = logging.getLogger(__name__)

STALE_RECONNECT_SECONDS = 5.0
RECONNECT_COOLDOWN_SECONDS = 30.0
RECONNECT_WINDOW_SECONDS = 300.0
MAX_RECONNECTS_PER_WINDOW = 3
LOG_INTERVAL_SECONDS = 5.0
STALE_RECONNECT_STREAK = 3
# Only a stream that keeps delivering OLD data is "stale but chatty"; a silent
# stream is handled by the worker's idle timeout instead.
STALE_RECONNECT_REASONS = frozenset({'STALE_EVENT_OR_CLOCK_SKEW'})
_PRIVATE_FIELDS = ('next_log', 'stale_since')


def _keys(keys: str | Iterable[str]) -> frozenset[str]:
    if isinstance(keys, str):
        return frozenset((keys,))
    return frozenset(keys)


class QuoteHealth:
    """Thread-safe fault aggregation; never changes quote validity."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.rows: dict[str, dict] = {}
        self.clock_samples: dict[str, tuple[float, int]] = {}
        self.reconnects: dict[str, deque] = {}
        self.markets: dict[str, dict] = {}

    # ------------------------------------------------------------------ quotes
    def observe(self, key: str, reason: str | None, now: float, wall_ms: int,
                event_ms: int | None, max_age: float, *, symbol: str | None = None) -> None:
        """Record one ingestion result for ``key`` (``reason`` None = accepted)."""
        with self.lock:
            row = self.rows.setdefault(key, {'invalid_total': 0, 'suppressed': 0})
            previous = self.clock_samples.get(key)
            jump = None if previous is None else round(wall_ms - previous[1] - (now - previous[0]) * 1000, 3)
            self.clock_samples[key] = (now, wall_ms)
            row.update(source=key, symbol=symbol, received_ms=wall_ms, event_ms=event_ms,
                       event_age_ms=None if event_ms is None else wall_ms - event_ms,
                       wall_clock_step_ms=jump, max_age_seconds=max_age)
            row['max_abs_wall_clock_step_ms'] = max(row.get('max_abs_wall_clock_step_ms', 0), abs(jump or 0))
            if event_ms is not None:
                age = wall_ms - event_ms
                row['max_event_age_ms'] = max(row.get('max_event_age_ms', age), age)
            if reason is None:
                if row.get('streak_count', 0):
                    self._log('QUOTE_RECOVERED', row)
                row.update(error=None, streak_count=0, stale_since=None, suppressed=0)
                return
            if row.get('error') != reason or not row.get('streak_count'):
                row.update(streak_count=0, stale_since=now, next_log=now)
            row['error'] = reason
            row['streak_count'] += 1
            row['invalid_total'] += 1
            if now >= row['next_log']:
                self._log('QUOTE_FAULT', row)
                row.update(next_log=now + LOG_INTERVAL_SECONDS, suppressed=0)
            else:
                row['suppressed'] += 1

    def should_reconnect(self, stream: str, now: float, keys: str | Iterable[str] | None = None) -> bool:
        """Bounded reconnect of a stream whose keys keep receiving stale data.

        ``stream`` names the retry budget (one per worker); ``keys`` are the feed
        keys that worker publishes (default: ``(stream,)``).
        """
        wanted = _keys(stream if keys is None else keys)
        with self.lock:
            stale = any(key in wanted and row.get('error') in STALE_RECONNECT_REASONS
                        and row.get('streak_count', 0) >= STALE_RECONNECT_STREAK
                        and row.get('stale_since') is not None
                        and now - row['stale_since'] >= STALE_RECONNECT_SECONDS
                        for key, row in self.rows.items())
            if not stale:
                return False
            attempts = self.reconnects.setdefault(stream, deque())
            while attempts and now - attempts[0] >= RECONNECT_WINDOW_SECONDS:
                attempts.popleft()
            if len(attempts) >= MAX_RECONNECTS_PER_WINDOW or (attempts and now - attempts[-1] < RECONNECT_COOLDOWN_SECONDS):
                return False
            attempts.append(now)
            return True

    def connection_reset(self, keys: str | Iterable[str]) -> None:
        """A stream (re)connected: restart streaks, keep lifetime counters and budget."""
        wanted = _keys(keys)
        with self.lock:
            for key, row in self.rows.items():
                if key in wanted:
                    row.update(streak_count=0, stale_since=None)

    def snapshot(self) -> dict:
        """Deep copy of the per-key rows (JSON-safe values only)."""
        with self.lock:
            return {key: {k: v for k, v in row.items() if k not in _PRIVATE_FIELDS}
                    for key, row in copy.deepcopy(self.rows).items()}

    # ----------------------------------------------------------------- markets
    def observe_market(self, symbol: str, reason: str | None, snapshot: Callable[[], dict], now: float) -> None:
        """Bounded logs for a market-level validation (index) result.

        Counts are validation attempts, not unique bad exchange messages. The
        evidence ``snapshot`` is lazy so a healthy fast path does no work.
        """
        with self.lock:
            row = self.markets.get(symbol)
            if reason is None and (row is None or row.get('error') is None):
                return
            if row is None:
                row = self.markets[symbol] = {'symbol': symbol, 'invalid_checks': 0, 'suppressed': 0}
            changed = row.get('error') != reason
            previous_error = row.get('error')
            row['error'] = reason
            if reason is not None:
                row.pop('recovered_from', None)
                row['invalid_checks'] += 1
            if changed or now >= row.get('next_log', 0):
                try:
                    row.update(snapshot())
                except Exception as exc:  # evidence is optional; never change the verdict
                    row['evidence_error'] = type(exc).__name__
                if reason is None:
                    row['recovered_from'] = previous_error
                self._log('MARKET_SOURCE_RECOVERED' if reason is None else 'MARKET_SOURCE_FAULT', row)
                row.update(next_log=now + LOG_INTERVAL_SECONDS, suppressed=0)
            else:
                row['suppressed'] += 1

    def market_snapshot(self) -> dict:
        with self.lock:
            return {key: {k: v for k, v in row.items() if k not in _PRIVATE_FIELDS}
                    for key, row in copy.deepcopy(self.markets).items()}

    # ----------------------------------------------------------------- logging
    @staticmethod
    def _log(event: str, row: dict) -> None:
        # Never interpolate raw websocket payloads or account/config data.
        try:
            LOG.warning('%s %s', event, json.dumps({k: v for k, v in row.items() if k not in _PRIVATE_FIELDS},
                                                   separators=(',', ':'), default=str))
        except Exception:
            pass  # Broken diagnostic output cannot change quote validity.
