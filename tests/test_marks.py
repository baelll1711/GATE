"""Execution-time index marks: the ±1 s bracket rule (reference marks.py)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from gate_stonk.marks import (MARK_BRACKET_MS, MARK_HORIZONS, MARKOUT_SECONDS, TRAIL_RETENTION_MS, IndexTrail,
                              mark_from_samples, resolve_marks)

D = Decimal


def test_constants_match_reference():
    assert MARK_BRACKET_MS == 1000
    assert MARKOUT_SECONDS == (5, 30, 60)
    assert MARK_HORIZONS == (0, 5, 30, 60)
    assert TRAIL_RETENTION_MS == 15 * 60 * 1000


def test_index_trail_brackets_and_closest_sample():
    # Pinned reference example (test_v030_features.test_index_trail_brackets_and_closest_sample).
    t = IndexTrail()
    t.observe(1000, D(100))
    t.observe(1800, D(100))
    t.observe(2500, D(101))
    assert t.at(1500) == (D(100), 1500)       # inside a run of equal samples
    assert t.at(2000) == (D(100), 1800)       # closer to the earlier sample
    assert t.at(2400) == (D(101), 2500)       # closer to the later sample
    assert t.at(2600) is None                 # nothing observed after yet
    assert t.at(900) is None                  # nothing observed before
    t.observe(5000, D(101))                   # gap > 1 s starts a new run
    assert t.at(3700) is None
    t.observe(4000, D(99))                    # clock stepped back: ignored
    assert t.at(5000) == (D(101), 5000)
    assert t.latest_ms() == 5000


def test_bracket_is_inclusive_at_exactly_one_second_and_tie_goes_to_earlier():
    t = IndexTrail()
    t.observe(10_000, D('1.0'))
    t.observe(12_000, D('2.0'))
    assert t.at(11_000) == (D('1.0'), 10_000)   # both exactly 1000 ms away: tie -> earlier
    assert t.at(10_999) is None                 # after-side 1001 ms away
    assert t.at(11_001) is None                 # before-side 1001 ms away
    t3 = IndexTrail()
    t3.observe(10_000, D('1.0'))
    t3.observe(11_500, D('2.0'))
    assert t3.at(10_700) == (D('1.0'), 10_000)  # 700 vs 800 ms: closer wins
    assert t3.at(10_800) == (D('2.0'), 11_500)  # 800 vs 700 ms
    t2 = IndexTrail()
    t2.observe(10_000, D('1.0'))
    t2.observe(12_001, D('2.0'))
    assert t2.at(11_000) is None                # after-side 1001 ms away: no mark
    assert t2.at(11_001) is None                # before-side 1001 ms away: no mark


def test_equal_values_further_apart_than_bracket_do_not_form_a_run():
    t = IndexTrail()
    t.observe(0, D(5))
    t.observe(1500, D(5))
    assert len(t) == 2
    assert t.at(750) == (D(5), 0)               # bracketed by both: closer (tie -> earlier)
    t.observe(2500, D(5))                       # within 1 s of 1500: extends the run
    assert len(t) == 2 and t.at(2000) == (D(5), 2000)


def test_retention_drops_old_runs_but_keeps_newest():
    t = IndexTrail(retention_ms=10_000)
    t.observe(0, D(1))
    t.observe(5_000, D(2))
    t.observe(20_000, D(3))
    assert len(t) == 1 and t.at(0) is None and t.at(20_000) == (D(3), 20_000)


def test_invalid_observations_are_refused():
    t = IndexTrail()
    for ms, index in ((True, D(1)), (1.5, D(1)), (1, D(0)), (1, D('-1')), (1, D('NaN')), (1, 1.0)):
        with pytest.raises(ValueError):
            t.observe(ms, index)
    assert len(t) == 0 and t.at(1) is None


def test_mark_from_persisted_samples_uses_the_same_rule():
    samples = [(1000, D(100)), (1800, D(100)), (2500, D(101)), (5000, D(101))]
    assert mark_from_samples(samples, 1800) == (D(100), 1800)       # exact sample
    assert mark_from_samples(samples, 2000) == (D(100), 1800)
    assert mark_from_samples(samples, 2400) == (D(101), 2500)
    assert mark_from_samples(samples, 900) is None
    assert mark_from_samples(samples, 3700) is None                 # 1200 ms before / 1300 after
    assert mark_from_samples(samples, 5001) is None                 # nothing after
    assert mark_from_samples([], 1000) is None
    # Equal distance: the earlier observation wins.
    assert mark_from_samples([(0, D(1)), (2000, D(2))], 1000) == (D(1), 0)


def test_resolve_marks_rows_pending_and_give_up():
    t = IndexTrail()
    t.observe(10_000, D(50))
    t.observe(10_400, D(51))
    t.observe(15_300, D(52))
    # At now = 15_300: horizon 0 bracketed (10_000 / 10_400 around T=10_200 -> closer earlier tie),
    # horizon 5 (target 15_200) has before 10_400 (4.8 s away) -> no mark yet but not expired
    # until now >= 16_200; 30 and 60 still pending.
    rows, pending = resolve_marks(t.at, 'k', 10_200, 15_300)
    assert rows == [('k', 0, D(50), 10_000)]
    assert pending == (5, 30, 60)
    # After the bracket window passed without an earlier sample close enough -> give up on 5 s.
    rows, pending = resolve_marks(t.at, 'k', 10_200, 16_200, pending)
    assert rows == [] and pending == (30, 60)
    # Everything expires after the retention horizon.
    rows, pending = resolve_marks(t.at, 'k', 10_200, 10_200 + TRAIL_RETENTION_MS + 1, pending)
    assert rows == [] and pending == ()
