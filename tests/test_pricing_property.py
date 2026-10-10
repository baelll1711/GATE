"""Seeded randomized property test for ladder pricing (notes/ref_pricing.md §14, adapted to Gate).

>= 5000 random valid books / centers / own orders / level configurations (plus a few
percent of deliberately malformed inputs). For every non-raising decision we check:

1. grid and bounds: price, lower, upper on the price grid; lower <= price <= upper
   (PROFIT_FLOOR_QUOTE: price == ceil(floor));
2. rounding direction: exact BUY <= C*(1-e/100) < price + tick, SELL >= C*(1+e/100) >
   price - tick; range lower >= raw lower (within a tick), upper <= raw upper (within a tick);
3. maker side: a placeable BUY is < ask and < every own SELL, a placeable SELL is > bid and
   > every own BUY; a crossing level is never placeable; +TOUCH never leaves the band and a
   range level only crosses when no in-band maker tick exists; exact levels never move;
4. evidence: skipped == sum of strictly-better foreign notional; visible == sum of foreign;
   own subtraction capped per level; more own size never increases visible volume;
5. wait policy: a returned volume decision with ``insufficient_depth: wait`` has
   skipped >= skip and visible >= skip;
6. clamp monotonicity: requiring more volume in front never makes a clamp price more
   aggressive, and clamp is never more aggressive than nearest_index;
7. ladder: every placeable level has edge >= min_new_edge after rounding; non-collapsed
   decisions are strictly monotone; +LADDER_SHIFT moves only deeper, inside the band for
   range levels; LEVEL_COLLAPSED only when the move is impossible;
8. only documented Unsafe codes are raised (never ValueError/ZeroDivision/... from data).

Synthetic data: books are generated in memory from the schema of DESIGN §2 ``Book``.
"""
from __future__ import annotations

import random
import time
from decimal import Decimal

import pytest

from gate_stonk.config import LevelConfig
from gate_stonk.dec import ONE, ZERO, D, on_grid
from gate_stonk.errors import Unsafe
from gate_stonk.pricing import (
    LADDER_WAIT_CODES, UNSAFE_CODES, ceil_price, edge_pct, floor_price, ladder_targets, level_bounds,
    maker_limit, post_only_ok, price_after, price_before, select_level_price,
)
from gate_stonk.types import Book, PairMeta

SEED = 20261010
CASES = 5000
HUNDRED = Decimal(100)


def _meta(precision: int) -> PairMeta:
    return PairMeta('STONK_USDT', 'STONK', 'USDT', precision, 2, None, None, None, None, 'tradable')


def _dec(r: random.Random, lo: float, hi: float, places: int) -> Decimal:
    return Decimal(round(r.uniform(lo, hi), places)).quantize(Decimal(1).scaleb(-places))


def _book(r: random.Random, center_ticks: int, tick: Decimal) -> Book:
    basis = r.uniform(-0.06, 0.06)
    mid = max(4, int(center_ticks * (1 + basis)))
    spread = r.randint(1, max(1, center_ticks // r.choice([20, 100, 1000])))
    best_bid = max(1, mid - spread // 2 - 1)
    best_ask = best_bid + spread
    max_gap = max(1, center_ticks // r.choice([50, 200, 2000]))

    def side(start: int, sign: int) -> tuple[tuple[Decimal, Decimal], ...]:
        rows, n = [], start
        for _ in range(r.randint(1, 8)):
            if n <= 0:
                break
            qty = Decimal(r.randint(1, 50000)).scaleb(-r.choice([0, 1, 2]))
            rows.append((Decimal(n) * tick, qty))
            n += sign * r.randint(1, max_gap)
        return tuple(rows)

    return Book(side(best_bid, -1), side(best_ask, +1), 100.0, 100000)


def _levels(r: random.Random, side: str, min_new_edge: Decimal, side_notional: Decimal) -> list[LevelConfig]:
    edge = max(Decimal('-0.5'), min_new_edge + _dec(r, -0.4, 1.0, 2))
    result = []
    for k in range(r.randint(1, 4)):
        kind = r.random()
        common = dict(out_of_range=r.choice(['nearest_index', 'clamp']),
                      insufficient_depth='wait' if r.random() < 0.25 else 'clamp')
        if kind < 0.4:
            result.append(LevelConfig(f'{side[0]}{k}', side, 'exact', edge_pct=edge, **common))
        else:
            width = Decimal(0) if kind < 0.45 else _dec(r, 0.01, 3.0, 3)
            roll = r.random()
            if roll < 0.2:
                skip = ZERO
            elif roll < 0.9:
                skip = (side_notional * Decimal(r.uniform(0, 1.5))).quantize(Decimal('0.01'))
            else:
                skip = Decimal(r.randint(1, 10**9))
            result.append(LevelConfig(f'{side[0]}{k}', side, 'range', min_edge_pct=edge, max_edge_pct=edge + width,
                                      skip_notional_usdt=skip, **common))
        edge += _dec(r, 0, 1.0, 2) if r.random() < 0.85 else ZERO
    return result


def _own(r: random.Random, levels, tick: Decimal) -> dict[Decimal, Decimal]:
    own: dict[Decimal, Decimal] = {}
    for price, qty in levels:
        roll = r.random()
        if roll < 0.15:
            own[price] = qty * Decimal(r.choice(['0.3', '1', '2.5']))
        elif roll < 0.2:
            own[price] = qty
    if r.random() < 0.2:
        own[levels[0][0] + tick * r.randint(1, 5)] = Decimal(r.randint(1, 100))   # not in the book
    return own


def _foreign(levels, own):
    return [(p, p * (q - min(q, own.get(p, ZERO)))) for p, q in levels]


def _aggressive(side: str, a: Decimal, b: Decimal) -> bool:
    """True if price ``a`` is strictly more aggressive (closer to the touch) than ``b``."""
    return a > b if side == 'buy' else a < b


def _deeper_or_equal(side: str, a: Decimal, b: Decimal) -> bool:
    return a <= b if side == 'buy' else a >= b


def _replace_skip(level: LevelConfig, **kw) -> LevelConfig:
    fields = {f: getattr(level, f) for f in level.__dataclass_fields__}
    fields.update(kw)
    return LevelConfig(**fields)


class Stats:
    def __init__(self):
        self.reasons: dict[str, int] = {}
        self.decisions = 0
        self.placeable = 0
        self.raised = 0

    def add(self, key: str):
        self.reasons[key] = self.reasons.get(key, 0) + 1


def _call(fn, *args, **kwargs):
    """Run a pricing call; only documented Unsafe codes may escape."""
    try:
        return fn(*args, **kwargs), None
    except Unsafe as exc:
        assert str(exc) in UNSAFE_CODES, f'undocumented Unsafe code {exc!s}'
        return None, str(exc)


def _check_decision(d, *, side, level, center, meta, snap, own, floor, ceiling, mode, limit):
    tick = meta.tick
    depth = snap.bids if side == 'buy' else snap.asks
    # 1. grid and bounds
    for value in (d.price, d.lower, d.upper):
        assert value > 0 and on_grid(value, meta.precision), d
    assert d.lower <= d.price <= d.upper, d
    lb, ub = level_bounds(center, meta, level)
    fixed = level.mode == 'exact' or level.min_edge_pct == level.max_edge_pct
    # 2. rounding direction of the unadjusted band
    if fixed:
        e = level.edge_pct if level.mode == 'exact' else level.min_edge_pct
        raw = center * (ONE - e / HUNDRED) if side == 'buy' else center * (ONE + e / HUNDRED)
        assert lb == ub
        if side == 'buy':
            assert lb <= raw < lb + tick
        else:
            assert lb - tick < raw <= lb
    else:
        lo_e, hi_e = level.min_edge_pct, level.max_edge_pct
        raw_lower = center * (ONE - hi_e / HUNDRED) if side == 'buy' else center * (ONE + lo_e / HUNDRED)
        raw_upper = center * (ONE - lo_e / HUNDRED) if side == 'buy' else center * (ONE + hi_e / HUNDRED)
        assert raw_lower <= lb < raw_lower + tick
        assert raw_upper - tick < ub <= raw_upper
    quote = d.reason == 'PROFIT_FLOOR_QUOTE'
    if quote:
        assert d.price == d.lower == d.upper == ceil_price(floor, meta) and d.price > ub
    else:
        assert lb <= d.lower and d.upper <= ub
        if floor is not None:
            assert d.lower == max(lb, ceil_price(floor, meta))
        if ceiling is not None:
            assert d.upper == min(ub, floor_price(ceiling, meta))
    # 3. maker side / TOUCH / exact never moves
    if fixed and not quote:
        assert d.price == lb and '+TOUCH' not in d.reason
    if '+TOUCH' in d.reason:
        assert not _crossing(side, d.price, limit)
    if not fixed and not quote and _crossing(side, d.price, limit):
        if side == 'buy':
            assert price_before(limit, meta) < d.lower
        else:
            assert price_after(limit, meta) > d.upper
    # 4. evidence
    foreign = _foreign(depth, own)
    visible = sum((a for _, a in foreign), ZERO)
    excluded = sum((p * min(q, own.get(p, ZERO)) for p, q in depth), ZERO)
    assert d.visible_notional == visible
    assert d.own_excluded_notional == excluded
    assert visible + excluded == sum((p * q for p, q in depth), ZERO)
    assert excluded <= sum((p * own.get(p, ZERO) for p, _ in depth), ZERO)
    better = (lambda p: p > d.price) if side == 'buy' else (lambda p: p < d.price)
    assert d.skipped_notional == sum((a for p, a in foreign if better(p)), ZERO)
    skip = level.skip_notional_usdt
    assert d.requested_skip_notional == skip
    assert d.depth_sufficient == (visible >= skip) and d.skip_satisfied == (d.skipped_notional >= skip)
    assert d.volume_pricing_applied == (not fixed and skip > 0)
    last = depth[-1][0]
    assert d.ahead_volume_complete == (d.price >= last if side == 'buy' else d.price <= last)
    # 5. wait policy
    if level.insufficient_depth == 'wait' and d.volume_pricing_applied and not quote:
        assert d.skipped_notional >= skip and visible >= skip


def _crossing(side: str, price: Decimal, limit: Decimal) -> bool:
    return price >= limit if side == 'buy' else price <= limit


def _malformed(r: random.Random, snap: Book) -> tuple[Book, dict | None, Decimal | None]:
    kind = r.randrange(5)
    if kind == 0:   # crossed
        return Book(snap.bids, ((snap.bids[0][0], Decimal(1)),), 100.0, 100000), None, None
    if kind == 1:   # duplicate level
        return Book(snap.bids + (snap.bids[-1],), snap.asks, 100.0, 100000), None, None
    if kind == 2:   # NaN / zero quantity deep in the book
        bad = Decimal('NaN') if r.random() < 0.5 else ZERO
        return Book(snap.bids, snap.asks + ((snap.asks[-1][0] * 2, bad),), 100.0, 100000), None, None
    if kind == 3:   # negative own size
        return snap, {snap.bids[0][0]: Decimal(-1), snap.asks[0][0]: Decimal(-1)}, None
    return snap, None, r.choice([ZERO, Decimal(-1)])   # nonpositive center


def test_random_ladders_hold_the_pricing_invariants():
    r = random.Random(SEED)
    stats = Stats()
    started = time.monotonic()
    for case in range(CASES):
        precision = r.choice([2, 3, 4, 5, 6])
        meta = _meta(precision)
        tick = meta.tick
        center_ticks = r.randint(200, 200000)
        center = Decimal(center_ticks) * tick + Decimal(r.randint(0, 999)) * tick / 1000
        snap = _book(r, center_ticks, tick)
        min_new_edge = Decimal(r.choice(['0.5', '0.8', '1.0', '1.2']))
        own_buy = _own(r, snap.bids, tick)
        own_sell = _own(r, snap.asks, tick)
        own_sell_prices = list(own_sell) + [Decimal(r.randint(1, center_ticks * 2)) * tick
                                            for _ in range(r.choice([0, 0, 1, 2]))]
        own_buy_prices = list(own_buy) + [Decimal(r.randint(1, center_ticks * 2)) * tick
                                          for _ in range(r.choice([0, 0, 1, 2]))]
        bad_center = None
        if r.random() < 0.03:
            snap, bad_own, bad_center = _malformed(r, snap)
            if bad_own is not None:
                own_buy, own_sell = bad_own, bad_own
        center_used = center if bad_center is None else bad_center
        for side in ('buy', 'sell'):
            depth = snap.bids if side == 'buy' else snap.asks
            own = own_buy if side == 'buy' else own_sell
            opposite = own_sell_prices if side == 'buy' else own_buy_prices
            side_notional = sum((p * q for p, q in depth if q.is_finite()), ZERO)
            levels = _levels(r, side, min_new_edge, side_notional)
            floor = ceiling = None
            if side == 'sell' and r.random() < 0.3:
                floor = (center * Decimal(1 + r.uniform(-0.02, 0.06))).quantize(Decimal('1e-8'))
            if side == 'buy' and r.random() < 0.3:
                ceiling = (center * Decimal(1 - r.uniform(-0.02, 0.06))).quantize(Decimal('1e-8'))
            pf_mode = r.choice(['hold', 'quote'])
            try:
                limit = maker_limit(side, snap, opposite)
            except Unsafe as exc:   # pragma: no cover - books always have both sides here
                assert str(exc) in UNSAFE_CODES
                continue
            kwargs = dict(own_levels=own, floor=floor, ceiling=ceiling, maker_limit=limit,
                          profit_floor_mode=pf_mode)
            targets, code = _call(ladder_targets, center_used, snap, meta, levels, side,
                                  min_new_edge_pct=min_new_edge, **kwargs)
            assert code is None and len(targets) == len(levels), code
            reference = None
            for level, target in zip(levels, targets):
                assert (target.side, target.level) == (side, level.id)
                direct, direct_code = _call(select_level_price, center_used, snap, meta, level, **kwargs)
                if direct is None:
                    stats.raised += 1
                    stats.add(direct_code)
                    assert target.decision is None and not target.placeable and target.reason == direct_code
                    continue
                stats.decisions += 1
                _check_decision(direct, side=side, level=level, center=center, meta=meta, snap=snap, own=own,
                                floor=floor, ceiling=ceiling, mode=level.out_of_range, limit=limit)
                d = target.decision
                assert d is not None
                # 7. ladder: shift / collapse relative to the previous ladder position
                violates = reference is not None and not (direct.price < reference if side == 'buy'
                                                          else direct.price > reference)
                if not violates:
                    assert d == direct
                else:
                    moved = price_before(reference, meta) if side == 'buy' else price_after(reference, meta)
                    fixed = level.mode == 'exact' or level.min_edge_pct == level.max_edge_pct
                    allowed = moved > 0 and (fixed or direct.lower <= moved <= direct.upper)
                    if allowed:
                        assert d.price == moved and d.reason == direct.reason + '+LADDER_SHIFT'
                        assert _deeper_or_equal(side, d.price, direct.price) and d.price != direct.price
                        assert d.lower <= d.price <= d.upper and on_grid(d.price, precision)
                        if not fixed:
                            assert (d.lower, d.upper) == (direct.lower, direct.upper)
                        foreign = _foreign(depth, own)
                        better = (lambda p: p > d.price) if side == 'buy' else (lambda p: p < d.price)
                        assert d.skipped_notional == sum((a for p, a in foreign if better(p)), ZERO)
                        assert d.skip_satisfied == (d.skipped_notional >= d.requested_skip_notional)
                        if level.insufficient_depth == 'wait' and d.volume_pricing_applied \
                                and d.reason != 'PROFIT_FLOOR_QUOTE':
                            assert d.skipped_notional >= d.requested_skip_notional
                    else:
                        assert target.reason == 'LEVEL_COLLAPSED' and not target.placeable and d == direct
                collapsed = target.reason == 'LEVEL_COLLAPSED'
                if not collapsed:
                    if reference is not None:
                        assert (d.price < reference) if side == 'buy' else (d.price > reference)
                    reference = d.price
                stats.add(target.reason)
                too_close = edge_pct(side, direct.price, center) < min_new_edge
                crossing = _crossing(side, d.price, limit)
                if target.placeable:
                    stats.placeable += 1
                    assert target.reason == d.reason
                    assert edge_pct(side, d.price, center) >= min_new_edge
                    assert post_only_ok(side, d.price, snap, opposite)
                    assert not too_close and not crossing
                else:
                    assert target.reason in LADDER_WAIT_CODES
                    if target.reason == 'MIN_NEW_EDGE_BUFFER':
                        assert too_close
                    if target.reason == 'POST_ONLY_OR_SELF_CROSS_WAIT':
                        assert crossing and not too_close
                        assert not post_only_ok(side, d.price, snap, opposite)
                    if crossing or too_close:
                        pass
                    else:
                        assert collapsed
                # 6. clamp monotonicity (range levels using volume)
                if level.mode == 'range' and level.skip_notional_usdt > 0 and direct.reason != 'PROFIT_FLOOR_QUOTE':
                    other_mode = 'nearest_index' if level.out_of_range == 'clamp' else 'clamp'
                    other, _ = _call(select_level_price, center, snap, meta,
                                     _replace_skip(level, out_of_range=other_mode), **kwargs)
                    if other is not None:
                        clamp_px, nearest_px = ((direct.price, other.price) if level.out_of_range == 'clamp'
                                                else (other.price, direct.price))
                        assert not _aggressive(side, clamp_px, nearest_px), (clamp_px, nearest_px)
                    if level.out_of_range == 'clamp':
                        bigger = _replace_skip(level, skip_notional_usdt=level.skip_notional_usdt
                                               * Decimal(r.choice(['1.5', '3', '10'])))
                        more, _ = _call(select_level_price, center, snap, meta, bigger, **kwargs)
                        if more is not None:
                            assert not _aggressive(side, more.price, direct.price), (more.price, direct.price)
            # 4b. more own size never increases the visible volume
            if levels and case % 2 == 0 and bad_center is None:
                price, qty = depth[r.randrange(len(depth))]
                bigger_own = dict(own)
                bigger_own[price] = own.get(price, ZERO) + qty * Decimal('0.5')
                base, _ = _call(select_level_price, center, snap, meta, levels[0], **kwargs)
                more, _ = _call(select_level_price, center, snap, meta, levels[0],
                                **{**kwargs, 'own_levels': bigger_own})
                if base is not None and more is not None:
                    assert more.visible_notional <= base.visible_notional
                    assert more.own_excluded_notional >= base.own_excluded_notional
    elapsed = time.monotonic() - started
    # The generator must actually exercise the interesting paths.
    for reason in ('FIXED_EDGE', 'ZERO_SKIP', 'BOOK_VOLUME', 'MIN_NEW_EDGE_BUFFER', 'LEVEL_COLLAPSED',
                   'POST_ONLY_OR_SELF_CROSS_WAIT', 'INSUFFICIENT_VISIBLE_DEPTH', 'SKIP_NOTIONAL_NOT_REACHED',
                   'PROFIT_FLOOR_ABOVE_RANGE', 'PROFIT_CEILING_BELOW_RANGE', 'EMPTY_PRICE_RANGE',
                   'CROSSED_BOOK', 'INVALID_OWN_LEVEL', 'INVALID_PRICE_RANGE'):
        assert stats.reasons.get(reason, 0) > 0 or any(k.startswith(reason) for k in stats.reasons), reason
    assert any('+TOUCH' in k for k in stats.reasons)
    assert any('+LADDER_SHIFT' in k for k in stats.reasons)
    assert any(k.startswith('DEPTH_EXHAUSTED') for k in stats.reasons)
    assert any('CLAMP' in k for k in stats.reasons)
    assert stats.placeable > CASES // 2 and stats.decisions > CASES
    assert elapsed < 20, f'property test too slow: {elapsed:.1f}s'


@pytest.mark.parametrize('seed', [1, 2, 3])
def test_grid_helpers_on_random_prices(seed):
    r = random.Random(seed)
    for _ in range(2000):
        precision = r.randint(0, 8)
        meta = _meta(precision)
        p = Decimal(r.randint(1, 10**9)).scaleb(-r.randint(0, 10))
        f, c = floor_price(p, meta), ceil_price(p, meta)
        assert f <= p <= c and c - f in (ZERO, meta.tick)
        assert on_grid(f, precision) and on_grid(c, precision)
        before, after = price_before(p, meta), price_after(p, meta)
        assert after > p and on_grid(after, precision) and after - meta.tick <= p
        assert before < p and (before == 0 or (on_grid(before, precision) and before + meta.tick >= p))
