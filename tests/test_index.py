"""IndexBuilder (DESIGN §4): validation order, FX, skew, outliers, rejoin hysteresis, aggregation.

All market data here is SYNTHETIC: Book/Ticker/SourceSnapshot/FxSnapshot objects are built directly
from gate_stonk.types (schema: DESIGN §2 and §4); no venue wire payloads are involved. Settings
mirror config.example.yaml (index section). Pinned reference behaviours are ported from the
12-chagpt conspectus (notes/ref_feeds.md §5-6, §18; notes/ref_pricing.md §3) with HL specifics
replaced by Gate/STONK sources.
"""
from __future__ import annotations

import json
import logging
from dataclasses import replace
from decimal import Decimal

import pytest

from gate_stonk import index as index_module
from gate_stonk.config import FxSettings, IndexSettings, SourceConfig
from gate_stonk.dec import D
from gate_stonk.errors import ConfigError
from gate_stonk.index import (IndexBuilder, effective_max_age, index_snapshot_to_dict, weighted_mean,
                              weighted_median)
from gate_stonk.types import Book, FxSnapshot, SourceSnapshot, Ticker

NOW = 1_800_000_000.0          # monotonic == wall seconds in these tests
WALL = 1_800_000_000_000       # wall ms


def ms(now: float) -> int:
    return int(round(now * 1000))


# --------------------------------------------------------------------------------------------
# Synthetic builders
# --------------------------------------------------------------------------------------------

def book(mid='100', *, now=NOW, age=0.0, lag=None, half='0.01', bid_size='2', ask_size='3',
         update_id=None) -> Book:
    """Two-level book around ``mid``: received ``age`` s ago, venue event ``lag`` s ago (default = age)."""
    mid, half = D(mid), D(half)
    lag = age if lag is None else lag
    return Book(bids=((mid - half, D(bid_size)), (mid - half - D('0.01'), D(5))),
                asks=((mid + half, D(ask_size)), (mid + half + D('0.01'), D(5))),
                received=now - age, event_ms=ms(now) - int(round(lag * 1000)), update_id=update_id)


def ticker(*, mark=None, last=None, now=NOW, age=0.0, lag=None) -> Ticker:
    lag = age if lag is None else lag
    return Ticker(mark=None if mark is None else D(mark), last=None if last is None else D(last),
                  received=now - age, event_ms=ms(now) - int(round(lag * 1000)))


def snap(source_id, b=None, t=None, *, error=None, connected=True) -> SourceSnapshot:
    return SourceSnapshot(source_id=source_id, book=b, ticker=t, error=error, connected=connected)


def source(sid, *, venue='bitget_usdt_futures', symbol='STONKUSDT', quote='USDT', price='mid', weight='1',
           required=False, max_spread_pct='1.0', max_age=3.0, heartbeat=0.0, enabled=True) -> SourceConfig:
    return SourceConfig(id=sid, venue=venue, symbol=symbol, quote=quote, price=price, weight=D(weight),
                        required=required, max_spread_pct=None if max_spread_pct is None else D(max_spread_pct),
                        max_age_seconds=max_age, heartbeat_seconds=heartbeat, enabled=enabled)


def fx_settings(mode='fixed', rate='1', depeg='0.5', max_age=3.0, spread='0.2') -> FxSettings:
    return FxSettings(mode=mode, usdc_to_usdt=D(rate), max_depeg_pct=D(depeg), max_age_seconds=max_age,
                      max_spread_pct=D(spread))


def settings(sources, *, method='weighted_median', min_sources=1, skew=2.0, deviation='1.0', basis='0',
             rejoin=3, fx=None) -> IndexSettings:
    return IndexSettings(method=method, min_sources=min_sources, max_age_seconds=3.0,
                         max_timestamp_skew_seconds=skew, max_source_deviation_pct=D(deviation),
                         basis_offset_pct=D(basis), rejoin_snapshots=rejoin, observation_interval_seconds=0.2,
                         fx=fx or fx_settings(), sources=tuple(sources))


def example_sources() -> list[SourceConfig]:
    """The five sources of config.example.yaml."""
    return [
        source('bybit_perp', venue='bybit_linear', weight='0.4', required=True, max_spread_pct='0.5',
               heartbeat=3.0),
        source('bitget_perp', venue='bitget_usdt_futures', weight='0.25'),
        source('bingx_perp', venue='bingx_swap', symbol='STONK-USDT', weight='0.15'),
        source('gate_perp', venue='gate_futures_usdt', symbol='STONK_USDT', weight='0.1'),
        source('lighter_perp', venue='lighter', symbol='STONK', quote='USDC', weight='0.1'),
    ]


IDS = ('bybit_perp', 'bitget_perp', 'bingx_perp', 'gate_perp', 'lighter_perp')


def example_settings(**kwargs) -> IndexSettings:
    kwargs.setdefault('min_sources', 2)
    return settings(example_sources(), **kwargs)


def all_books(prices=None, **kwargs) -> dict[str, SourceSnapshot]:
    prices = prices or {}
    return {sid: snap(sid, book(prices.get(sid, '100'), **kwargs)) for sid in IDS}


def compute(builder, snaps, fx=None, now=NOW):
    return builder.compute(snaps, fx, now, ms(now))


def comp(result, sid):
    return next(c for c in result.components if c.source_id == sid)


def reasons(result) -> dict[str, str | None]:
    return {c.source_id: c.reason for c in result.components}


# --------------------------------------------------------------------------------------------
# Aggregation: weights, weighted median (incl. exact-half tie), weighted mean
# --------------------------------------------------------------------------------------------

def test_all_example_sources_valid_give_ok_index_and_center():
    result = compute(IndexBuilder(example_settings()), all_books())
    assert result.ok and result.reason is None
    assert result.index == D(100) and result.center == D(100)
    assert result.n_included == 5
    assert result.method == 'weighted_median' and result.at_ms == WALL
    assert all(c.included and c.reason is None for c in result.components)


def test_weighted_median_uses_configured_weights():
    prices = {'bybit_perp': '100.0', 'bitget_perp': '100.2', 'bingx_perp': '100.4',
              'gate_perp': '100.6', 'lighter_perp': '100.8'}
    result = compute(IndexBuilder(example_settings()), all_books(prices))
    # sorted: 100.0 (0.4) -> cum 0.4; 100.2 (0.25) -> cum 0.65 >= 0.5
    assert result.ok and result.index == D('100.2')


def test_weighted_mean_uses_configured_weights():
    prices = {'bybit_perp': '100.0', 'bitget_perp': '100.2', 'bingx_perp': '100.4',
              'gate_perp': '100.6', 'lighter_perp': '100.8'}
    result = compute(IndexBuilder(example_settings(method='weighted_mean')), all_books(prices))
    expected = (D('0.4') * D('100') + D('0.25') * D('100.2') + D('0.15') * D('100.4')
                + D('0.1') * D('100.6') + D('0.1') * D('100.8'))
    assert result.ok and result.method == 'weighted_mean'
    assert result.index == expected == D('100.25')


def test_weights_are_renormalised_over_included_sources():
    prices = {'bybit_perp': '100.0', 'bitget_perp': '100.2', 'bingx_perp': '100.4',
              'gate_perp': '100.6', 'lighter_perp': '100.8'}
    snaps = all_books(prices)
    del snaps['bitget_perp']
    del snaps['bingx_perp']
    builder = IndexBuilder(example_settings(method='weighted_mean'))
    result = compute(builder, snaps)
    assert result.ok and result.n_included == 3
    total = D('0.4') + D('0.1') + D('0.1')
    assert result.index == (D('0.4') * 100 + D('0.1') * D('100.6') + D('0.1') * D('100.8')) / total
    # weighted median: Bybit holds 0.4/0.6 > 50% of the included weight, so it IS the index.
    median = compute(IndexBuilder(example_settings()), snaps)
    assert median.ok and median.index == D('100.0')
    # The component keeps the configured (not normalised) weight.
    assert comp(result, 'bybit_perp').weight == D('0.4')


def test_weighted_median_exact_half_tie_takes_mean_of_neighbours():
    srcs = [source('a', weight='0.3'), source('b', weight='0.2'), source('c', weight='0.3'),
            source('d', weight='0.2')]
    snaps = {'a': snap('a', book('100')), 'b': snap('b', book('100.2')),
             'c': snap('c', book('100.4')), 'd': snap('d', book('100.6'))}
    result = compute(IndexBuilder(settings(srcs)), snaps)
    # cum after b = 0.5 exactly -> mean(100.2, 100.4)
    assert result.ok and result.index == D('100.3')


def test_weighted_median_two_equal_weights_is_their_mean():
    srcs = [source('a'), source('b')]
    result = compute(IndexBuilder(settings(srcs)), {'a': snap('a', book('100')), 'b': snap('b', book('100.4'))})
    assert result.index == D('100.2')


def test_source_with_more_than_half_of_weight_is_the_index():
    srcs = [source('big', weight='0.6'), source('s1', weight='0.1'), source('s2', weight='0.1'),
            source('s3', weight='0.1'), source('s4', weight='0.1')]
    snaps = {sid: snap(sid, book('100')) for sid in ('s1', 's2', 's3', 's4')}
    snaps['big'] = snap('big', book('100.5'))
    result = compute(IndexBuilder(settings(srcs)), snaps)
    assert result.ok and result.n_included == 5
    assert result.index == D('100.5')
    # ... while the weighted mean is pulled only by its weight.
    mean = compute(IndexBuilder(settings(srcs, method='weighted_mean')), snaps)
    assert mean.index == D('100.3')


def test_reference_pinned_mean_not_median_example():
    # Reference: sources 100, 100, 100.6 -> arithmetic mean 100.2 (equal weights).
    srcs = [source('a'), source('b'), source('c')]
    snaps = {'a': snap('a', book('100')), 'b': snap('b', book('100')), 'c': snap('c', book('100.6'))}
    assert compute(IndexBuilder(settings(srcs, method='weighted_mean')), snaps).index == D('100.2')
    assert compute(IndexBuilder(settings(srcs)), snaps).index == D('100')


def test_weighted_median_function_exact_rules():
    # 1/6 + 2/6 == 1/2 exactly: compared as 2*cum vs total, so the tie is detected despite 1/3 weights.
    assert weighted_median([('a', D(10), D(1)), ('b', D(20), D(2)), ('c', D(30), D(3))]) == D(25)
    assert weighted_median([('a', D(1), D(1)), ('b', D(2), D(1)), ('c', D(3), D(1))]) == D(2)
    assert weighted_median([('only', D('7.5'), D('0.1'))]) == D('7.5')
    # input order does not matter; equal prices are ordered by source_id deterministically
    assert weighted_median([('z', D(101), D(1)), ('b', D(100), D(1)), ('a', D(100), D(1))]) == D(100)
    # a strict majority at the top end wins
    assert weighted_median([('a', D(100), D(1)), ('b', D(105), D(3))]) == D(105)


def test_weighted_mean_function():
    assert weighted_mean([('a', D(100), D(1)), ('b', D(103), D(2))]) == D(102)
    assert weighted_mean([('a', D('99.5'), D('0.25'))]) == D('99.5')


# --------------------------------------------------------------------------------------------
# Per-source exclusion: age, event time, spread, structure, missing data
# --------------------------------------------------------------------------------------------

def test_stale_receipt_excludes_source_with_stale_quote():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', age=3.1))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert result.ok and result.n_included == 4
    assert comp(result, 'bitget_perp').reason == 'STALE_QUOTE'
    assert comp(result, 'bitget_perp').received_age_s == pytest.approx(3.1)


def test_future_receipt_is_stale_quote_not_clamped():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', age=-0.01, lag=0))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert comp(result, 'bitget_perp').reason == 'STALE_QUOTE'
    assert comp(result, 'bitget_perp').received_age_s < 0


@pytest.mark.parametrize('lag', [3.5, -2.5])
def test_event_time_outside_window_is_stale_event_or_clock_skew(lag):
    snaps = all_books()
    snaps['bingx_perp'] = snap('bingx_perp', book('100', age=0.0, lag=lag))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert result.ok
    assert comp(result, 'bingx_perp').reason == 'STALE_EVENT_OR_CLOCK_SKEW'


def test_venue_clock_up_to_two_seconds_ahead_is_accepted():
    snaps = all_books()
    snaps['bingx_perp'] = snap('bingx_perp', book('100', age=0.0, lag=-1.5))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert result.ok and result.n_included == 5


def test_wide_spread_excludes_source_and_keeps_diagnostics():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', half='0.6'))   # 1.2% > 1.0%
    result = compute(IndexBuilder(example_settings()), snaps)
    c = comp(result, 'bitget_perp')
    assert result.ok and c.reason == 'WIDE_SOURCE_SPREAD' and not c.included
    assert c.spread_pct == D('1.2') and c.bid == D('99.4') and c.ask == D('100.6')


def test_spread_exactly_at_limit_is_accepted():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', half='0.5'))   # exactly 1.0%
    assert comp(compute(IndexBuilder(example_settings()), snaps), 'bitget_perp').included


def test_gate_futures_with_fifty_percent_spread_drops_out_while_others_form_index():
    snaps = all_books()
    snaps['gate_perp'] = snap('gate_perp', book('100', half='25'))       # bid 75 / ask 125 = 50%
    result = compute(IndexBuilder(example_settings()), snaps)
    gate = comp(result, 'gate_perp')
    assert result.ok and result.index == D(100) and result.n_included == 4
    assert gate.reason == 'WIDE_SOURCE_SPREAD' and not gate.included
    assert gate.spread_pct == D(50) and gate.bid == D(75) and gate.ask == D(125)


@pytest.mark.parametrize('bad, code', [
    (Book(bids=(), asks=((D('100.01'), D(1)),), received=NOW, event_ms=WALL), 'EMPTY_BOOK'),
    (Book(bids=((D('100.02'), D(1)),), asks=((D('100.01'), D(1)),), received=NOW, event_ms=WALL),
     'CROSSED_BOOK'),
    (Book(bids=((D('99.99'), D(0)),), asks=((D('100.01'), D(1)),), received=NOW, event_ms=WALL),
     'INVALID_BOOK_LEVEL'),
    (Book(bids=((D('99.98'), D(1)), (D('99.99'), D(1))), asks=((D('100.01'), D(1)),), received=NOW,
          event_ms=WALL), 'UNSORTED_OR_DUPLICATE_BOOK_LEVEL'),
])
def test_malformed_book_codes_pass_through(bad, code):
    snaps = all_books()
    snaps['bingx_perp'] = snap('bingx_perp', bad)
    c = comp(compute(IndexBuilder(example_settings()), snaps), 'bingx_perp')
    assert c.reason == code and not c.included


def test_crossed_book_has_no_diagnostic_prices():
    snaps = all_books()
    snaps['bingx_perp'] = snap('bingx_perp', Book(bids=((D('100.02'), D(1)),), asks=((D('100.01'), D(1)),),
                                                  received=NOW, event_ms=WALL))
    c = comp(compute(IndexBuilder(example_settings()), snaps), 'bingx_perp')
    assert c.bid is None and c.ask is None and c.native_price is None and c.usdt_price is None


def test_missing_snapshot_and_stored_error_codes():
    snaps = all_books()
    del snaps['bitget_perp']
    snaps['bingx_perp'] = snap('bingx_perp', None, error='DISCONNECTED', connected=False)
    snaps['gate_perp'] = snap('gate_perp', None)
    result = compute(IndexBuilder(example_settings()), snaps)
    assert result.ok and result.n_included == 2
    assert reasons(result)['bitget_perp'] == 'MISSING_SOURCE:bitget_perp'
    assert reasons(result)['bingx_perp'] == 'MISSING_SOURCE:bingx_perp:DISCONNECTED'
    assert reasons(result)['gate_perp'] == 'MISSING_SOURCE:gate_perp'


def test_snapshot_for_another_source_is_rejected():
    snaps = all_books()
    snaps['bingx_perp'] = snap('bitget_perp', book('100'))
    assert reasons(compute(IndexBuilder(example_settings()), snaps))['bingx_perp'] == \
        'MISSING_SOURCE:bingx_perp:SOURCE_ID_MISMATCH'


def test_disconnected_flag_alone_does_not_exclude_fresh_rest_data():
    # `connected` reports the transport only; a fresh REST-refreshed book is valid data.
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100'), connected=False)
    assert comp(compute(IndexBuilder(example_settings()), snaps), 'bitget_perp').included


def test_invalid_timestamp_type_excludes_source():
    snaps = all_books()
    good = book('100')
    snaps['bingx_perp'] = snap('bingx_perp', replace(good, event_ms=float(good.event_ms)))
    assert reasons(compute(IndexBuilder(example_settings()), snaps))['bingx_perp'] == 'INVALID_SOURCE_TIMESTAMP'


# --------------------------------------------------------------------------------------------
# Quiet Bybit heartbeat and cross-source timestamp skew
# --------------------------------------------------------------------------------------------

def test_effective_max_age_rule():
    assert effective_max_age(3.0, 3.0) == 4.0
    assert effective_max_age(5.0, 3.0) == 5.0
    assert effective_max_age(3.0, 0.0) == 3.0


def test_quiet_bybit_book_3_5_seconds_old_is_valid_with_heartbeat():
    snaps = all_books()
    snaps['bybit_perp'] = snap('bybit_perp', book('100', age=3.5))
    result = compute(IndexBuilder(example_settings()), snaps)
    bybit = comp(result, 'bybit_perp')
    assert result.ok and bybit.included
    assert bybit.event_ms == WALL - 3500
    assert bybit.effective_ms == WALL - 500          # event + 3 s heartbeat, below newest


def test_quiet_bybit_book_4_1_seconds_old_is_stale_and_blocks_as_required():
    snaps = all_books()
    snaps['bybit_perp'] = snap('bybit_perp', book('100', age=4.1))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert not result.ok and result.index is None and result.center is None
    assert comp(result, 'bybit_perp').reason == 'STALE_QUOTE'
    assert result.reason == 'REQUIRED_SOURCE_INVALID:bybit_perp:STALE_QUOTE'


def test_non_heartbeat_source_3_5_seconds_old_is_stale():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', age=3.5))
    assert reasons(compute(IndexBuilder(example_settings()), snaps))['bitget_perp'] == 'STALE_QUOTE'


def test_bybit_2_5_seconds_behind_is_not_skewed():
    snaps = all_books()
    snaps['bybit_perp'] = snap('bybit_perp', book('100', age=0.0, lag=2.5))
    result = compute(IndexBuilder(example_settings()), snaps)
    bybit = comp(result, 'bybit_perp')
    assert result.ok and result.n_included == 5
    assert bybit.event_ms == WALL - 2500 and bybit.effective_ms == WALL   # capped at the newest


def test_non_heartbeat_source_2_5_seconds_behind_is_skew_excluded():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', age=0.0, lag=2.5))
    result = compute(IndexBuilder(example_settings()), snaps)
    bitget = comp(result, 'bitget_perp')
    assert result.ok and result.n_included == 4
    assert bitget.reason == 'SOURCE_TIMESTAMP_SKEW'
    assert bitget.effective_ms == WALL - 2500


def test_skew_exactly_at_limit_is_accepted():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', book('100', age=0.0, lag=2.0))
    assert comp(compute(IndexBuilder(example_settings()), snaps), 'bitget_perp').included


def test_skew_is_measured_among_survivors_only():
    # A stale (excluded) source does not define "newest" for the others.
    snaps = all_books(lag=1.0)
    # Venue clock 1.9 s ahead (would make every other source 2.9 s "behind"), but its receipt is stale.
    snaps['bitget_perp'] = snap('bitget_perp', book('100', age=3.2, lag=-1.9))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert reasons(result)['bitget_perp'] == 'STALE_QUOTE'
    assert result.ok and result.n_included == 4


# --------------------------------------------------------------------------------------------
# Outlier test (unweighted median)
# --------------------------------------------------------------------------------------------

def test_outlier_source_is_excluded_and_others_form_index():
    snaps = all_books({'bingx_perp': '102'})
    result = compute(IndexBuilder(example_settings()), snaps)
    assert result.ok and result.index == D(100) and result.n_included == 4
    assert comp(result, 'bingx_perp').reason == 'SOURCE_OUTLIER'
    assert comp(result, 'bingx_perp').usdt_price == D(102)


def test_deviation_exactly_at_limit_is_accepted():
    srcs = [source('a'), source('b'), source('c')]
    snaps = {'a': snap('a', book('100')), 'b': snap('b', book('100')), 'c': snap('c', book('101'))}
    result = compute(IndexBuilder(settings(srcs)), snaps)
    assert result.n_included == 3


def test_two_source_disagreement_blocks_when_one_is_required():
    srcs = [source('bybit_perp', venue='bybit_linear', required=True, heartbeat=3.0), source('bitget_perp')]
    snaps = {'bybit_perp': snap('bybit_perp', book('100')), 'bitget_perp': snap('bitget_perp', book('103'))}
    result = compute(IndexBuilder(settings(srcs)), snaps)
    assert reasons(result) == {'bybit_perp': 'SOURCE_OUTLIER', 'bitget_perp': 'SOURCE_OUTLIER'}
    assert not result.ok and result.index is None
    assert result.reason == 'REQUIRED_SOURCE_INVALID:bybit_perp:SOURCE_OUTLIER'


def test_two_optional_sources_disagreeing_block_by_insufficient_sources():
    srcs = [source('a'), source('b')]
    result = compute(IndexBuilder(settings(srcs)), {'a': snap('a', book('100')), 'b': snap('b', book('103'))})
    assert not result.ok and result.reason == 'INSUFFICIENT_SOURCES:0/1' and result.n_included == 0


def test_outlier_median_is_unweighted():
    # The heavy source is the minority by count: the plain median is 101.5, so it is the outlier.
    srcs = [source('heavy', weight='0.9'), source('b', weight='0.05'), source('c', weight='0.05')]
    snaps = {'heavy': snap('heavy', book('100')), 'b': snap('b', book('101.5')), 'c': snap('c', book('101.6'))}
    result = compute(IndexBuilder(settings(srcs)), snaps)
    assert reasons(result) == {'heavy': 'SOURCE_OUTLIER', 'b': None, 'c': None}
    assert result.ok and result.index == D('101.55')     # equal remaining weights: exact-half tie -> mean


def test_outlier_check_uses_usdt_normalised_prices():
    # Lighter quotes USDC 100 with FX 1.004 -> 100.4 USDT; vs 99 USDT others it must be compared in USDT.
    srcs = [source('a'), source('b'), source('l', venue='lighter', quote='USDC')]
    snaps = {'a': snap('a', book('101.4')), 'b': snap('b', book('101.4')), 'l': snap('l', book('100'))}
    result = compute(IndexBuilder(settings(srcs, fx=fx_settings(rate='1.004'))), snaps)
    assert comp(result, 'l').usdt_price == D('100.4')
    assert comp(result, 'l').included                    # 100.4 vs 101.4: 0.986% <= 1%
    snaps['l'] = snap('l', book('99.9'))                  # 100.2996 USDT: 1.085% -> outlier
    assert comp(compute(IndexBuilder(settings(srcs, fx=fx_settings(rate='1.004'))), snaps), 'l').reason == \
        'SOURCE_OUTLIER'


# --------------------------------------------------------------------------------------------
# Required / optional and min_sources
# --------------------------------------------------------------------------------------------

def test_optional_source_failure_does_not_block():
    snaps = all_books()
    snaps['bitget_perp'] = snap('bitget_perp', None, error='CONNECTING')
    result = compute(IndexBuilder(example_settings()), snaps)
    assert result.ok and result.n_included == 4


def test_required_source_failure_blocks_with_its_reason():
    snaps = all_books()
    snaps['bybit_perp'] = snap('bybit_perp', None, error='DISCONNECTED', connected=False)
    result = compute(IndexBuilder(example_settings()), snaps)
    assert not result.ok and result.index is None and result.center is None
    assert result.reason == 'REQUIRED_SOURCE_INVALID:bybit_perp:MISSING_SOURCE:bybit_perp:DISCONNECTED'
    assert result.n_included == 4                        # others are still reported as included


def test_first_failing_required_source_in_config_order_is_reported():
    srcs = [source('a', required=True), source('b', required=True), source('c')]
    result = compute(IndexBuilder(settings(srcs)), {'c': snap('c', book('100'))})
    assert result.reason == 'REQUIRED_SOURCE_INVALID:a:MISSING_SOURCE:a'


def test_min_sources_blocks_with_insufficient_sources():
    snaps = {'bybit_perp': snap('bybit_perp', book('100'))}
    result = compute(IndexBuilder(example_settings(min_sources=2)), snaps)
    assert not result.ok and result.reason == 'INSUFFICIENT_SOURCES:1/2' and result.n_included == 1
    snaps['bingx_perp'] = snap('bingx_perp', book('100'))
    assert compute(IndexBuilder(example_settings(min_sources=2)), snaps).ok


def test_required_check_precedes_min_sources():
    result = compute(IndexBuilder(example_settings(min_sources=3)), {})
    assert result.reason == 'REQUIRED_SOURCE_INVALID:bybit_perp:MISSING_SOURCE:bybit_perp'


# --------------------------------------------------------------------------------------------
# FX: USDC -> USDT (multiply), depeg guard, missing FX excludes only USDC sources
# --------------------------------------------------------------------------------------------

def fx_book(mid='1.003', *, now=NOW, age=0.0, half='0.0001'):
    return book(mid, now=now, age=age, half=half, bid_size='1000', ask_size='1000')


def live_fx(rate='1.003', *, mode='binance_spot', error=None, age=0.0, with_book=True):
    return FxSnapshot(rate=None if rate is None else D(rate), mode=mode,
                      book=fx_book(rate or '1', age=age) if with_book else None, error=error)


def test_fixed_fx_multiplies_usdc_price():
    srcs = [source('a', weight='1'), source('l', venue='lighter', quote='USDC', weight='1')]
    snaps = {'a': snap('a', book('100.4')), 'l': snap('l', book('100'))}
    result = compute(IndexBuilder(settings(srcs, method='weighted_mean', fx=fx_settings(rate='1.004'))), snaps)
    lighter = comp(result, 'l')
    assert lighter.native_price == D(100) and lighter.usdt_price == D('100.4')
    assert result.fx_rate == D('1.004') and result.fx_reason is None
    assert result.ok and result.index == D('100.4')


def test_reference_pinned_fixed_fx_example():
    # Reference: fixed rate 100/100.5 with sources at 100.5 -> index 100 (within 1e-24); ours multiplies.
    rate = D(100) / D('100.5')
    srcs = [source(sid, venue='lighter', quote='USDC') for sid in ('x', 'y', 'z')]
    snaps = {sid: snap(sid, book('100.5')) for sid in ('x', 'y', 'z')}
    result = compute(IndexBuilder(settings(srcs, method='weighted_mean', fx=fx_settings(rate=rate))), snaps)
    assert result.ok and abs(result.index - 100) < D('1e-24')


def test_fixed_fx_ignores_live_snapshot_argument():
    srcs = [source('l', venue='lighter', quote='USDC')]
    b = IndexBuilder(settings(srcs, fx=fx_settings(rate='1.002')))
    assert compute(b, {'l': snap('l', book('100'))}, fx=None).index == D('100.2')
    assert compute(b, {'l': snap('l', book('100'))}, fx=live_fx('1.4')).index == D('100.2')


def test_fixed_fx_depeg_excludes_only_usdc_sources():
    result = compute(IndexBuilder(example_settings(fx=fx_settings(rate='1.006'))), all_books())
    assert result.ok and result.n_included == 4 and result.index == D(100)
    assert comp(result, 'lighter_perp').reason == 'FX_DEPEG'
    assert comp(result, 'lighter_perp').usdt_price is None
    assert result.fx_rate == D('1.006') and result.fx_reason == 'FX_DEPEG'


def test_fixed_fx_depeg_boundary_is_inclusive():
    result = compute(IndexBuilder(example_settings(fx=fx_settings(rate='0.995'))), all_books())
    assert comp(result, 'lighter_perp').included and result.fx_reason is None


def test_live_fx_multiplies_usdc_price():
    srcs = [source('l', venue='lighter', quote='USDC')]
    b = IndexBuilder(settings(srcs, fx=fx_settings(mode='binance_spot')))
    result = compute(b, {'l': snap('l', book('100'))}, fx=live_fx('1.003'))
    assert result.ok and result.index == D('100.3') and result.fx_rate == D('1.003')


def test_live_fx_depeg_excludes_usdc_sources():
    b = IndexBuilder(example_settings(fx=fx_settings(mode='binance_spot')))
    result = compute(b, all_books(), fx=live_fx('1.02', with_book=False))
    assert result.ok and result.n_included == 4
    assert comp(result, 'lighter_perp').reason == 'FX_DEPEG' and result.fx_reason == 'FX_DEPEG'


@pytest.mark.parametrize('fx, code', [
    (None, 'FX_UNAVAILABLE:MISSING_FX'),
    (live_fx(None, error='DISCONNECTED', with_book=False), 'FX_UNAVAILABLE:DISCONNECTED'),
    (live_fx('1.001', error='STALE_QUOTE'), 'FX_UNAVAILABLE:STALE_QUOTE'),
    (live_fx(None, with_book=False), 'FX_UNAVAILABLE:MISSING_RATE'),
    (live_fx('1.001', mode='gate_spot'), 'FX_UNAVAILABLE:FX_MODE_MISMATCH'),
    (live_fx('1.001', age=3.5), 'FX_UNAVAILABLE:STALE_QUOTE'),
    (FxSnapshot(rate=D('-1'), mode='binance_spot', book=None, error=None), 'FX_UNAVAILABLE:INVALID_RATE'),
])
def test_missing_or_bad_live_fx_excludes_only_usdc_sources(fx, code):
    b = IndexBuilder(example_settings(fx=fx_settings(mode='binance_spot')))
    result = compute(b, all_books(), fx=fx)
    assert result.ok and result.n_included == 4 and result.index == D(100)
    assert comp(result, 'lighter_perp').reason == code
    assert result.fx_reason == code
    assert all(c.included for c in result.components if c.quote == 'USDT')


def test_wide_fx_book_is_unavailable():
    b = IndexBuilder(example_settings(fx=fx_settings(mode='gate_spot')))
    wide = FxSnapshot(rate=D('1.001'), mode='gate_spot', book=fx_book('1.001', half='0.003'), error=None)
    assert comp(compute(b, all_books(), fx=wide), 'lighter_perp').reason == 'FX_UNAVAILABLE:WIDE_SOURCE_SPREAD'


def test_fx_failure_does_not_override_earlier_source_reason():
    snaps = all_books()
    snaps['lighter_perp'] = snap('lighter_perp', book('100', age=3.5))
    b = IndexBuilder(example_settings(fx=fx_settings(mode='binance_spot')))
    assert reasons(compute(b, snaps, fx=None))['lighter_perp'] == 'STALE_QUOTE'


def test_required_usdc_source_without_fx_blocks():
    srcs = [source('l', venue='lighter', quote='USDC', required=True), source('a')]
    b = IndexBuilder(settings(srcs, fx=fx_settings(mode='binance_spot')))
    result = compute(b, {'l': snap('l', book('100')), 'a': snap('a', book('100'))}, fx=None)
    assert result.reason == 'REQUIRED_SOURCE_INVALID:l:FX_UNAVAILABLE:MISSING_FX'


def test_no_usdc_source_needs_no_fx():
    srcs = [s for s in example_sources() if s.quote == 'USDT']
    b = IndexBuilder(settings(srcs, min_sources=2, fx=fx_settings(mode='binance_spot')))
    result = compute(b, all_books(), fx=None)
    assert result.ok and result.n_included == 4
    assert result.fx_rate is None and result.fx_reason is None


# --------------------------------------------------------------------------------------------
# Price kinds: mid / microprice / mark / last
# --------------------------------------------------------------------------------------------

def test_mid_and_microprice():
    b = Book(bids=((D('99.9'), D(3)),), asks=((D('100.1'), D(1)),), received=NOW, event_ms=WALL)
    mid = compute(IndexBuilder(settings([source('a', price='mid')])), {'a': snap('a', b)})
    micro = compute(IndexBuilder(settings([source('a', price='microprice')])), {'a': snap('a', b)})
    assert mid.index == D(100)
    # (bid*ask_size + ask*bid_size) / (bid_size + ask_size) = (99.9*1 + 100.1*3) / 4
    assert micro.index == D('100.05') and comp(micro, 'a').native_price == D('100.05')


@pytest.mark.parametrize('kind, expected', [('mark', '100.3'), ('last', '100.1')])
def test_mark_and_last_from_ticker_without_book(kind, expected):
    src = source('a', price=kind, max_spread_pct=None)
    result = compute(IndexBuilder(settings([src])), {'a': snap('a', None, ticker(mark='100.3', last='100.1'))})
    assert result.ok and result.index == D(expected)
    assert comp(result, 'a').price_kind == kind


def test_mark_with_null_spread_limit_ignores_wide_book():
    src = source('gate_perp', venue='gate_futures_usdt', price='mark', max_spread_pct=None)
    snaps = {'gate_perp': snap('gate_perp', book('100', half='25'), ticker(mark='100.2'))}
    result = compute(IndexBuilder(settings([src])), snaps)
    c = comp(result, 'gate_perp')
    assert result.ok and result.index == D('100.2') and c.included
    assert c.spread_pct == D(50)                         # still reported for index-watch


def test_mark_with_spread_limit_checks_book_spread():
    src = source('gate_perp', venue='gate_futures_usdt', price='mark', max_spread_pct='1.0')
    wide = {'gate_perp': snap('gate_perp', book('100', half='25'), ticker(mark='100.2'))}
    assert reasons(compute(IndexBuilder(settings([src])), wide))['gate_perp'] == 'WIDE_SOURCE_SPREAD'
    no_book = {'gate_perp': snap('gate_perp', None, ticker(mark='100.2'))}
    assert reasons(compute(IndexBuilder(settings([src])), no_book))['gate_perp'] == 'MISSING_SOURCE:gate_perp'
    stale_book = {'gate_perp': snap('gate_perp', book('100', age=3.5), ticker(mark='100.2'))}
    assert reasons(compute(IndexBuilder(settings([src])), stale_book))['gate_perp'] == 'STALE_QUOTE'


@pytest.mark.parametrize('kind', ['mark', 'last'])
def test_missing_ticker_field_is_missing_price(kind):
    src = source('a', price=kind, max_spread_pct=None)
    t = ticker(mark=None, last='100') if kind == 'mark' else ticker(mark='100', last=None)
    result = compute(IndexBuilder(settings([src])), {'a': snap('a', None, t)})
    assert reasons(result)['a'] == f'MISSING_PRICE:{kind}'


def test_ticker_freshness_and_missing_ticker():
    src = source('a', price='mark', max_spread_pct=None)
    b = IndexBuilder(settings([src]))
    assert reasons(compute(b, {'a': snap('a', None, ticker(mark='100', age=3.1))}))['a'] == 'STALE_QUOTE'
    assert reasons(compute(b, {'a': snap('a', None, ticker(mark='100', lag=3.5))}))['a'] == \
        'STALE_EVENT_OR_CLOCK_SKEW'
    assert reasons(compute(b, {'a': snap('a', book('100'), None, error='PARSE_ERROR')}))['a'] == \
        'MISSING_SOURCE:a:PARSE_ERROR'


def test_nonpositive_ticker_price_is_invalid():
    src = source('a', price='mark', max_spread_pct=None)
    result = compute(IndexBuilder(settings([src])), {'a': snap('a', None, ticker(mark='0'))})
    assert reasons(result)['a'] == 'INVALID_SOURCE_PRICE'


def test_ticker_source_uses_ticker_times():
    src = source('a', price='last', max_spread_pct='1.0')
    result = compute(IndexBuilder(settings([src])), {'a': snap('a', book('100', age=0.2), ticker(last='100',
                                                                                             age=1.0))})
    c = comp(result, 'a')
    assert c.included and c.event_ms == WALL - 1000 and c.received_age_s == pytest.approx(1.0)


def test_heartbeat_window_applies_to_ticker_sources_too():
    src = source('a', venue='bybit_linear', price='mark', max_spread_pct=None, heartbeat=3.0)
    assert compute(IndexBuilder(settings([src])), {'a': snap('a', None, ticker(mark='100', age=3.5))}).ok


# --------------------------------------------------------------------------------------------
# Rejoin hysteresis
# --------------------------------------------------------------------------------------------

def two_source_builder(rejoin=3):
    srcs = [source('bybit_perp', venue='bybit_linear', required=True, max_spread_pct='0.5', heartbeat=3.0),
            source('bitget_perp')]
    return IndexBuilder(settings(srcs, rejoin=rejoin))


def pair(now, bitget_book=None, bybit_book=None):
    snaps = {'bybit_perp': snap('bybit_perp', bybit_book or book('100', now=now))}
    if bitget_book is not None:
        snaps['bitget_perp'] = snap('bitget_perp', bitget_book)
    return snaps


def test_sources_are_eligible_at_first_sight():
    b = two_source_builder()
    result = compute(b, pair(NOW, book('100')))
    assert result.n_included == 2 and reasons(result) == {'bybit_perp': None, 'bitget_perp': None}


def test_rejoin_needs_distinct_validated_snapshots_not_reads():
    b = two_source_builder()
    now = NOW
    assert compute(b, pair(now, book('100', now=now)), now=now).n_included == 2
    now += 0.1
    assert reasons(compute(b, pair(now), now=now))['bitget_perp'] == 'MISSING_SOURCE:bitget_perp'
    now += 0.1
    first = book('100', now=now)
    assert reasons(compute(b, pair(now, first), now=now))['bitget_perp'] == 'REJOIN_PENDING:1/3'
    for _ in range(5):                                   # re-reading the same snapshot does not count
        now += 0.05
        result = compute(b, pair(now, first), now=now)
        assert reasons(result)['bitget_perp'] == 'REJOIN_PENDING:1/3'
        assert result.ok and result.n_included == 1      # the optional source is just not included
    now += 0.1
    assert reasons(compute(b, pair(now, book('100', now=now)), now=now))['bitget_perp'] == 'REJOIN_PENDING:2/3'
    state = b.status()['sources']['bitget_perp']
    assert state['rejoin_pending'] is True and state['rejoin_count'] == 2 and state['rejoin_required'] == 3
    now += 0.1
    result = compute(b, pair(now, book('100', now=now)), now=now)
    assert comp(result, 'bitget_perp').included and result.n_included == 2
    assert b.status()['sources']['bitget_perp']['rejoin_pending'] is False
    # Once included, re-reading the same snapshot keeps it included.
    assert comp(compute(b, pair(now, book('100', now=now)), now=now), 'bitget_perp').included


def test_rejoin_counter_resets_on_any_failure():
    b = two_source_builder()
    now = NOW
    compute(b, pair(now, book('100', now=now)), now=now)                   # included once
    now += 0.1
    compute(b, pair(now, book('100', now=now, age=3.5)), now=now)         # STALE_QUOTE
    for expected in ('1/3', '2/3'):
        now += 0.1
        assert reasons(compute(b, pair(now, book('100', now=now)), now=now))['bitget_perp'] == \
            f'REJOIN_PENDING:{expected}'
    now += 0.1
    outlier = compute(b, pair(now, book('103', now=now)), now=now)
    assert reasons(outlier)['bitget_perp'] == 'SOURCE_OUTLIER'
    assert b.status()['sources']['bitget_perp']['rejoin_count'] == 0
    now += 0.1
    assert reasons(compute(b, pair(now, book('100', now=now)), now=now))['bitget_perp'] == 'REJOIN_PENDING:1/3'


def test_source_never_included_joins_on_first_valid_snapshot():
    # Hysteresis protects against a flapping source only: a source still CONNECTING at start joins at once.
    b = two_source_builder()
    compute(b, pair(NOW))                                # bitget still CONNECTING / absent
    assert reasons(compute(b, pair(NOW + 0.1, book('100', now=NOW + 0.1)), now=NOW + 0.1))['bitget_perp'] is None


def test_source_failing_after_being_included_must_rejoin():
    b = two_source_builder()
    compute(b, pair(NOW, book('100', now=NOW)))          # included once
    compute(b, pair(NOW + 0.1), now=NOW + 0.1)           # bitget disappears
    assert reasons(compute(b, pair(NOW + 0.2, book('100', now=NOW + 0.2)), now=NOW + 0.2))['bitget_perp'] == \
        'REJOIN_PENDING:1/3'


def test_required_source_rejoining_blocks_the_index():
    b = two_source_builder()
    compute(b, pair(NOW - 1, book('100', now=NOW - 1), bybit_book=book('100', now=NOW - 1)), now=NOW - 1)
    now = NOW
    compute(b, pair(now, book('100', now=now), bybit_book=book('100', now=now, age=4.1)), now=now)
    now += 0.1
    result = compute(b, pair(now, book('100', now=now), bybit_book=book('100', now=now)), now=now)
    assert not result.ok and result.reason == 'REQUIRED_SOURCE_INVALID:bybit_perp:REJOIN_PENDING:1/3'


def test_quiet_bybit_repeats_with_same_event_time_count_as_distinct_snapshots():
    # orderbook.1 re-sends an unchanged snapshot (same u, possibly same ts): a new receipt is a new snapshot.
    b = two_source_builder()
    compute(b, pair(NOW - 1, book('100', now=NOW - 1), bybit_book=book('100', now=NOW - 1)), now=NOW - 1)
    now = NOW
    compute(b, pair(now, book('100', now=now), bybit_book=book('100', now=now, age=4.1)), now=now)
    event_ms = ms(NOW)
    results = []
    for _ in range(3):
        now += 0.5
        repeat = replace(book('100', now=now), event_ms=event_ms)
        results.append(compute(b, pair(now, book('100', now=now), bybit_book=repeat), now=now))
    assert [r.reason for r in results[:2]] == ['REQUIRED_SOURCE_INVALID:bybit_perp:REJOIN_PENDING:1/3',
                                               'REQUIRED_SOURCE_INVALID:bybit_perp:REJOIN_PENDING:2/3']
    assert results[2].ok and results[2].n_included == 2


def test_rejoin_snapshots_one_or_zero_means_immediate_rejoin():
    for rejoin in (0, 1):
        b = two_source_builder(rejoin=rejoin)
        compute(b, pair(NOW))
        assert comp(compute(b, pair(NOW + 0.1, book('100', now=NOW + 0.1)), now=NOW + 0.1), 'bitget_perp').included


def test_rejoining_source_still_votes_in_the_outlier_median():
    # DESIGN §4 order: the outlier test (step 4) precedes rejoin (step 5), so a validated but not yet
    # re-admitted source is part of the plain median; it cannot enter the weighted aggregate itself.
    srcs = [source('a'), source('b'), source('p')]
    b = IndexBuilder(settings(srcs, method='weighted_mean'))
    compute(b, {'a': snap('a', book('100')), 'b': snap('b', book('100.2')), 'p': snap('p', book('100.1'))})
    compute(b, {'a': snap('a', book('100', now=NOW + 0.05)), 'b': snap('b', book('101.2', now=NOW + 0.05))},
            now=NOW + 0.05)                                                            # p missing -> pending
    now = NOW + 0.1
    result = compute(b, {'a': snap('a', book('100', now=now)), 'b': snap('b', book('101.2', now=now)),
                         'p': snap('p', book('101.3', now=now))}, now=now)
    # median(100, 101.2, 101.3) = 101.2 -> a deviates 1.19% and is excluded; p is pending.
    assert reasons(result) == {'a': 'SOURCE_OUTLIER', 'b': None, 'p': 'REJOIN_PENDING:1/3'}
    assert result.ok and result.index == D('101.2') and result.n_included == 1


# --------------------------------------------------------------------------------------------
# Center, NONPOSITIVE_INDEX, components, status JSON-safety, purity
# --------------------------------------------------------------------------------------------

@pytest.mark.parametrize('basis, center', [('0.5', '100.5'), ('-0.3', '99.7'), ('0', '100')])
def test_basis_offset_moves_center_not_index(basis, center):
    result = compute(IndexBuilder(example_settings(basis=basis)), all_books())
    assert result.ok and result.index == D(100) and result.center == D(center)


def test_nonpositive_center_blocks():
    result = compute(IndexBuilder(example_settings(basis='-100')), all_books())
    assert not result.ok and result.reason == 'NONPOSITIVE_INDEX'
    assert result.index is None and result.center is None


def test_components_contain_every_source_in_config_order_with_reasons():
    snaps = all_books({'bingx_perp': '102'})
    del snaps['bitget_perp']
    snaps['gate_perp'] = snap('gate_perp', book('100', half='25'))
    result = compute(IndexBuilder(example_settings()), snaps)
    assert [c.source_id for c in result.components] == list(IDS)
    assert reasons(result) == {'bybit_perp': None, 'bitget_perp': 'MISSING_SOURCE:bitget_perp',
                               'bingx_perp': 'SOURCE_OUTLIER', 'gate_perp': 'WIDE_SOURCE_SPREAD',
                               'lighter_perp': None}
    assert [c.included for c in result.components] == [True, False, False, False, True]
    assert result.n_included == 2 and result.ok
    bybit = comp(result, 'bybit_perp')
    assert (bybit.venue, bybit.symbol, bybit.quote, bybit.price_kind, bybit.weight, bybit.required) == \
        ('bybit_linear', 'STONKUSDT', 'USDT', 'mid', D('0.4'), True)
    assert bybit.native_price == D(100) and bybit.usdt_price == D(100)
    assert bybit.bid == D('99.99') and bybit.ask == D('100.01') and bybit.event_ms == WALL
    missing = comp(result, 'bitget_perp')
    assert missing.native_price is None and missing.event_ms is None and missing.effective_ms is None


def test_disabled_source_is_not_a_component():
    srcs = example_sources()
    srcs[1] = replace(srcs[1], enabled=False)
    result = compute(IndexBuilder(settings(srcs, min_sources=2)), all_books())
    assert 'bitget_perp' not in [c.source_id for c in result.components] and result.n_included == 4


def test_status_is_json_safe_and_reports_rejoin_state():
    b = two_source_builder()
    initial = b.status()
    json.dumps(initial, allow_nan=False)
    assert initial['computes'] == 0 and initial['sources']['bitget_perp']['included'] is None
    compute(b, pair(NOW - 1, book('100', now=NOW - 1)), now=NOW - 1)
    compute(b, pair(NOW, book('100', age=3.5)))
    compute(b, pair(NOW + 0.1, book('100', now=NOW + 0.1)), now=NOW + 0.1)
    status = b.status()
    text = json.dumps(status, allow_nan=False)
    assert 'Decimal' not in text
    assert status['computes'] == 3 and status['last_ok'] is True and status['last_at_ms'] == ms(NOW + 0.1)
    row = status['sources']['bitget_perp']
    assert row['included'] is False and row['reason'] == 'REJOIN_PENDING:1/3'
    assert row['rejoin_pending'] is True and row['rejoin_count'] == 1
    assert row['last_fingerprint'] == {'event_ms': ms(NOW + 0.1), 'received': NOW + 0.1}
    assert status['sources']['bybit_perp']['included'] is True


def test_index_snapshot_to_dict_is_json_safe():
    snaps = all_books()
    del snaps['bitget_perp']
    result = compute(IndexBuilder(example_settings()), snaps)
    data = index_snapshot_to_dict(result)
    json.dumps(data, allow_nan=False)
    assert D(data['index']) == 100 and data['ok'] is True and len(data['components']) == 5
    assert D(data['components'][0]['weight']) == D('0.4')
    assert data['components'][1]['reason'] == 'MISSING_SOURCE:bitget_perp'


def test_compute_is_pure_of_clocks(monkeypatch):
    def boom():
        raise AssertionError('IndexBuilder must not read clocks')
    monkeypatch.setattr('time.time', boom)
    monkeypatch.setattr('time.monotonic', boom)
    assert compute(IndexBuilder(example_settings()), all_books()).ok


def test_broken_logger_never_changes_the_result(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError('logger down')
    monkeypatch.setattr(index_module.LOG, 'warning', boom)
    monkeypatch.setattr(index_module.LOG, 'info', boom)
    b = two_source_builder()
    compute(b, pair(NOW, book('100')))
    result = compute(b, pair(NOW + 0.1), now=NOW + 0.1)
    assert result.ok and reasons(result)['bitget_perp'] == 'MISSING_SOURCE:bitget_perp'


def test_transitions_are_logged_once(caplog):
    b = two_source_builder()
    caplog.set_level(logging.INFO, logger='gate_stonk.index')
    compute(b, pair(NOW, book('100')))
    for i in range(1, 4):
        compute(b, pair(NOW + i * 0.1), now=NOW + i * 0.1)
    excluded = [r for r in caplog.records if 'bitget_perp' in r.getMessage() and r.levelno == logging.WARNING]
    assert len(excluded) == 1


def test_inputs_are_not_mutated():
    snaps = all_books()
    before = dict(snaps)
    compute(IndexBuilder(example_settings()), snaps)
    assert snaps == before


# --------------------------------------------------------------------------------------------
# Settings validation (fail closed at construction)
# --------------------------------------------------------------------------------------------

@pytest.mark.parametrize('mutate, key', [
    (lambda s: replace(s, method='mean'), 'index.method'),
    (lambda s: replace(s, min_sources=0), 'index.min_sources'),
    (lambda s: replace(s, rejoin_snapshots=-1), 'index.rejoin_snapshots'),
    (lambda s: replace(s, max_timestamp_skew_seconds=0), 'index.max_timestamp_skew_seconds'),
    (lambda s: replace(s, max_source_deviation_pct=D(0)), 'index.max_source_deviation_pct'),
    (lambda s: replace(s, fx=replace(s.fx, mode='oracle')), 'index.fx.mode'),
    (lambda s: replace(s, fx=None), 'index.fx'),
    (lambda s: replace(s, sources=(replace(s.sources[0], max_spread_pct=None),) + s.sources[1:]),
     'index.sources[0].max_spread_pct'),
    (lambda s: replace(s, sources=s.sources + (s.sources[0],)), 'index.sources[5].id'),
    (lambda s: replace(s, sources=(replace(s.sources[0], weight=D(0)),) + s.sources[1:]),
     'index.sources[0].weight'),
    (lambda s: replace(s, sources=(replace(s.sources[0], enabled=False),) + s.sources[1:]),
     'index.sources[0].required'),
    (lambda s: replace(s, sources=(replace(s.sources[0], quote='USD'),) + s.sources[1:]),
     'index.sources[0].quote'),
    (lambda s: replace(s, sources=(replace(s.sources[0], price='vwap'),) + s.sources[1:]),
     'index.sources[0].price'),
    (lambda s: replace(s, sources=(replace(s.sources[0], max_age_seconds=0),) + s.sources[1:]),
     'index.sources[0].max_age_seconds'),
])
def test_invalid_settings_raise_config_error(mutate, key):
    with pytest.raises(ConfigError, match=key.replace('[', r'\[').replace(']', r'\]').replace('.', r'\.')):
        IndexBuilder(mutate(example_settings()))


def test_mark_and_last_may_have_null_spread_limit():
    srcs = [source('a', price='mark', max_spread_pct=None), source('b', price='last', max_spread_pct=None)]
    IndexBuilder(settings(srcs))


def test_plain_int_weight_and_decimal_seconds_are_normalised():
    src = replace(source('a'), weight=2, max_age_seconds=Decimal('3'), heartbeat_seconds=Decimal('0'))
    b = IndexBuilder(settings([src]))
    result = compute(b, {'a': snap('a', book('100'))})
    assert result.ok and comp(result, 'a').weight == D(2) and isinstance(comp(result, 'a').weight, Decimal)
    assert reasons(compute(b, {'a': snap('a', book('100', age=3.1))}))['a'] == 'STALE_QUOTE'


@pytest.mark.parametrize('weight', [True, float('nan'), 'abc'])
def test_garbage_weight_is_config_error(weight):
    with pytest.raises(ConfigError, match=r'index\.sources\[0\]\.weight'):
        IndexBuilder(settings([replace(source('a'), weight=weight)]))
