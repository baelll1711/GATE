"""Execution-time index marks from locally observed index samples.

A fill reaches the bot over REST seconds after it happened, so the index known at
reconciliation is not the index at execution and is never used as an execution mark.
The engine records every index it computes (each loop tick while orders exist and
every pricing decision) both in memory (``IndexTrail``) and in SQLite (``index_obs``).

A fill at exchange time T gets a mark only when an observation at or before T and an
observation at or after T are both within ``MARK_BRACKET_MS`` of T; the closer one is
used (a tie goes to the earlier one). Otherwise the fill has no mark: an unknown
execution index is reported as unknown, not replaced by a later quote. Markouts use the
same rule at T + 5, 30 and 60 s. Accuracy is also bounded by the offset between the
computer clock and exchange time, so the computer clock should be synchronised.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from decimal import Decimal
from typing import Callable, Iterable, Sequence

MARK_BRACKET_MS = 1000
MARKOUT_SECONDS = (5, 30, 60)
MARK_HORIZONS = (0,) + MARKOUT_SECONDS
TRAIL_RETENTION_MS = 15 * 60 * 1000

Mark = tuple[Decimal, int]            # (index, observation ms)
MarkRow = tuple[str, int, Decimal, int]   # (fill_key, horizon_s, index, sample_ms)


def _ms(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError('Invalid millisecond timestamp')
    return value


def _index(value) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise ValueError('Invalid index value')
    return value


class IndexTrail:
    """Runs of equal consecutive observations no more than ``bracket_ms`` apart:
    ``[first_ms, last_ms, index]`` (one pair, one series)."""

    def __init__(self, bracket_ms: int = MARK_BRACKET_MS, retention_ms: int = TRAIL_RETENTION_MS):
        if _ms(bracket_ms) < 0 or _ms(retention_ms) <= 0:
            raise ValueError('Invalid trail settings')
        self.bracket_ms, self.retention_ms = bracket_ms, retention_ms
        self._first: list[int] = []
        self._last: list[int] = []
        self._value: list[Decimal] = []

    def __len__(self) -> int:
        return len(self._first)

    def observe(self, ms: int, index: Decimal) -> None:
        ms, index = _ms(ms), _index(index)
        first, last, value = self._first, self._last, self._value
        if last and ms < last[-1]:
            return  # the computer clock stepped back; keep the trail ordered
        if value and value[-1] == index and ms - last[-1] <= self.bracket_ms:
            last[-1] = ms
        else:
            first.append(ms)
            last.append(ms)
            value.append(index)
        cutoff = ms - self.retention_ms
        drop = 0
        while drop < len(last) - 1 and last[drop] < cutoff:
            drop += 1
        if drop:
            del first[:drop], last[:drop], value[:drop]

    def latest_ms(self) -> int | None:
        return self._last[-1] if self._last else None

    def at(self, ms: int) -> Mark | None:
        """(index, observation ms) bracketing ``ms``, or None when not bracketed."""
        ms = _ms(ms)
        first, last, value = self._first, self._last, self._value
        if not first:
            return None
        i = bisect_right(first, ms) - 1
        if i >= 0 and ms <= last[i]:
            return value[i], ms  # equal observations on both sides, gaps <= bracket
        if i < 0 or i + 1 >= len(first):
            return None
        before, after = ms - last[i], first[i + 1] - ms
        if before > self.bracket_ms or after > self.bracket_ms:
            return None
        return (value[i], last[i]) if before <= after else (value[i + 1], first[i + 1])


def mark_from_samples(samples: Sequence[tuple[int, Decimal]], ms: int,
                      bracket_ms: int = MARK_BRACKET_MS) -> Mark | None:
    """Same bracket rule over raw persisted samples ``[(ts_ms, index), ...]`` sorted by time
    (e.g. valid rows of ``index_obs``); restart-safe and usable offline by the report."""
    ms = _ms(ms)
    if not samples:
        return None
    times = [s[0] for s in samples]
    after_i = bisect_left(times, ms)            # first sample with ts >= ms
    before_i = bisect_right(times, ms) - 1      # last sample with ts <= ms
    if before_i < 0 or after_i >= len(samples):
        return None
    before_ms, before_value = samples[before_i]
    after_ms, after_value = samples[after_i]
    before, after = ms - before_ms, after_ms - ms
    if before > bracket_ms or after > bracket_ms:
        return None
    return (before_value, before_ms) if before <= after else (after_value, after_ms)


def resolve_marks(lookup: Callable[[int], Mark | None], fill_key: str, time_ms: int, now_ms: int,
                  pending: Iterable[int] = MARK_HORIZONS, *, bracket_ms: int = MARK_BRACKET_MS,
                  give_up_after_ms: int = TRAIL_RETENTION_MS) -> tuple[list[MarkRow], tuple[int, ...]]:
    """Resolve the pending horizons (seconds) of one fill.

    For each horizon ``h``: target = T + h s. A bracketed observation gives a row
    ``(fill_key, h, index, sample_ms)``; once ``now >= target + bracket`` without one the
    horizon is given up (no mark). Everything is dropped after ``give_up_after_ms``.
    Returns ``(rows, still_pending)``.
    """
    time_ms, now_ms = _ms(time_ms), _ms(now_ms)
    rows: list[MarkRow] = []
    remaining: list[int] = []
    expired = now_ms - time_ms > give_up_after_ms
    for horizon in pending:
        target = time_ms + int(horizon) * 1000
        found = lookup(target)
        if found is not None:
            rows.append((fill_key, int(horizon), found[0], int(found[1])))
        elif not expired and now_ms < target + bracket_ms:
            remaining.append(int(horizon))
    return rows, tuple(remaining)
