"""Pure ladder pricing on Gate's fixed decimal price grid (DESIGN §5).

Ported from the reference ``spot_portfolio/pricing.py`` (``price_bounds`` +
``select_price``) with three changes:

* the tick is constant: ``meta.tick = 10**-meta.precision`` (no Hyperliquid
  significant-figure rule, no tick change at powers of ten);
* the band comes from a ladder level (:class:`gate_stonk.config.LevelConfig`):
  ``exact`` = one FIXED_EDGE price rounded away from the center, ``range`` =
  the reference volume band (``skip_notional_usdt``, ``out_of_range``,
  ``insufficient_depth``);
* no index coefficient, no inventory skew and no entry buffer (the entry buffer
  is replaced by ``safety.min_new_edge_pct`` measured AFTER rounding in
  :func:`ladder_targets`).

Rounding always goes toward safety / away from the center: BUY prices and upper
bounds FLOOR, SELL prices and lower bounds CEIL, profit floor CEIL, profit
ceiling FLOOR. Every computed price must be > 0.

All functions are pure: no I/O, no clock, no logging. Data problems raise
``Unsafe(code)``; programming errors (wrong side, a profit floor on a BUY,
invalid tolerance input) raise ``ValueError``.

Unsafe codes raised by this module (documented set, see ``UNSAFE_CODES``):

``INVALID_PRICE_RANGE``           center not a positive finite number, missing or
                                  invalid edge, ``min_edge_pct > max_edge_pct``
``INVALID_LEVEL_MODE``            level mode is neither ``exact`` nor ``range``
``NONPOSITIVE_PRICE_RANGE``       a raw or rounded price would be <= 0
``EMPTY_PRICE_RANGE``             no tick inside a narrow range band
``MISSING_BOOK_DEPTH``            book missing or one side empty
``INVALID_BOOK_LEVEL``            malformed level (either side of the book)
``UNSORTED_OR_DUPLICATE_BOOK_LEVEL``
``CROSSED_BOOK``
``INVALID_OUT_OF_RANGE_MODE``     out_of_range not nearest_index|clamp
``INVALID_INSUFFICIENT_DEPTH_POLICY``  insufficient_depth not clamp|wait
``INVALID_PROFIT_FLOOR_MODE``     profit_floor_mode not hold|quote
``INVALID_PROFIT_FLOOR_INPUT``    nonpositive/non-finite floor, ceiling or inputs
                                  of the profit formulas
``PROFIT_FLOOR_ABOVE_RANGE``      SELL floor above the band (hold mode)
``PROFIT_CEILING_BELOW_RANGE``    BUY ceiling below the band
``INVALID_OWN_LEVEL``             own_levels entry not Decimal/finite/positive
``INVALID_SKIP_NOTIONAL``         negative or missing skip notional (range level)
``INVALID_MAKER_LIMIT``           maker limit not a positive finite number
``INSUFFICIENT_VISIBLE_DEPTH``    ``insufficient_depth: wait`` and visible < skip
``SKIP_NOTIONAL_NOT_REACHED``     ``insufficient_depth: wait`` and skipped < skip
``INVALID_RESTING_PRICE``         ``resting`` entry not a positive finite Decimal

Ladder wait/refusal reasons (``LevelTarget.reason`` when not placeable):
``MIN_NEW_EDGE_BUFFER``, ``LEVEL_COLLAPSED``, ``LEVEL_ORDER_WAIT``,
``POST_ONLY_OR_SELF_CROSS_WAIT`` (or one of the Unsafe codes above when the level
produced no decision).

Decision reasons: ``FIXED_EDGE``, ``ZERO_SKIP``, ``BOOK_VOLUME``,
``INSUFFICIENT_VISIBLE_DEPTH``, ``VOLUME_BELOW_RANGE``, ``VOLUME_ABOVE_RANGE``,
``NO_FOREIGN_DEPTH``, ``DEPTH_EXHAUSTED``, ``BOOK_VOLUME_CLAMP_LOWER``,
``BOOK_VOLUME_CLAMP_UPPER``, ``DEPTH_EXHAUSTED_CLAMP_LOWER``,
``DEPTH_EXHAUSTED_CLAMP_UPPER``, ``PROFIT_FLOOR_QUOTE``; suffixes
``+PROFIT_FLOOR``, ``+PROFIT_CEILING``, ``+TOUCH``, ``+LADDER_SHIFT`` (chained in
that order, e.g. ``INSUFFICIENT_VISIBLE_DEPTH+TOUCH+LADDER_SHIFT``).
"""
from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from .dec import HUNDRED, ONE, ZERO, D, ceil_to, floor_to
from .errors import Unsafe
from .types import SIDES, Book, LevelTarget, PairMeta, PriceDecision

if TYPE_CHECKING:  # attribute access only; keeps pricing importable on its own
    from .config import LevelConfig

UNSAFE_CODES = frozenset({
    'INVALID_PRICE_RANGE', 'INVALID_LEVEL_MODE', 'NONPOSITIVE_PRICE_RANGE', 'EMPTY_PRICE_RANGE',
    'MISSING_BOOK_DEPTH', 'INVALID_BOOK_LEVEL', 'UNSORTED_OR_DUPLICATE_BOOK_LEVEL', 'CROSSED_BOOK',
    'INVALID_OUT_OF_RANGE_MODE', 'INVALID_INSUFFICIENT_DEPTH_POLICY', 'INVALID_PROFIT_FLOOR_MODE',
    'INVALID_PROFIT_FLOOR_INPUT', 'PROFIT_FLOOR_ABOVE_RANGE', 'PROFIT_CEILING_BELOW_RANGE',
    'INVALID_OWN_LEVEL', 'INVALID_SKIP_NOTIONAL', 'INVALID_MAKER_LIMIT',
    'INSUFFICIENT_VISIBLE_DEPTH', 'SKIP_NOTIONAL_NOT_REACHED', 'INVALID_RESTING_PRICE',
})
LADDER_WAIT_CODES = frozenset({'MIN_NEW_EDGE_BUFFER', 'LEVEL_COLLAPSED', 'LEVEL_ORDER_WAIT',
                               'POST_ONLY_OR_SELF_CROSS_WAIT'})

OUT_OF_RANGE_MODES = ('nearest_index', 'clamp')
INSUFFICIENT_DEPTH_POLICIES = ('clamp', 'wait')
PROFIT_FLOOR_MODES = ('hold', 'quote')
LEVEL_MODES = ('exact', 'range')


# --------------------------------------------------------------------------- grid helpers
def floor_price(p: Decimal, meta: PairMeta) -> Decimal:
    """Largest tick <= p (BUY prices, upper bounds, profit ceiling)."""
    return floor_to(p, meta.precision)


def ceil_price(p: Decimal, meta: PairMeta) -> Decimal:
    """Smallest tick >= p (SELL prices, lower bounds, profit floor)."""
    return ceil_to(p, meta.precision)


def price_before(p: Decimal, meta: PairMeta) -> Decimal:
    """Largest tick STRICTLY below ``p``; ``ZERO`` means no positive tick exists."""
    p = D(p)
    rounded = floor_price(p, meta)
    result = rounded if rounded < p else rounded - meta.tick
    return result if result > 0 else ZERO


def price_after(p: Decimal, meta: PairMeta) -> Decimal:
    """Smallest tick STRICTLY above ``p`` (never at a level's tail)."""
    p = D(p)
    rounded = ceil_price(p, meta)
    return rounded if rounded > p else rounded + meta.tick


def _side(side: str) -> str:
    if side not in SIDES:
        raise ValueError('side must be buy or sell')
    return side


def _center(center) -> Decimal:
    try:
        value = D(center)
    except (ValueError, TypeError) as exc:
        raise Unsafe('INVALID_PRICE_RANGE') from exc
    if value <= 0:
        raise Unsafe('INVALID_PRICE_RANGE')
    return value


def _edge(value) -> Decimal:
    if value is None:
        raise Unsafe('INVALID_PRICE_RANGE')
    try:
        return D(value)
    except (ValueError, TypeError) as exc:
        raise Unsafe('INVALID_PRICE_RANGE') from exc


def _edges(level: LevelConfig) -> tuple[Decimal, Decimal]:
    """(minimum, maximum) edge percent of a level; equal for an exact level."""
    if level.mode == 'exact':
        edge = _edge(level.edge_pct)
        return edge, edge
    if level.mode == 'range':
        minimum, maximum = _edge(level.min_edge_pct), _edge(level.max_edge_pct)
        if minimum > maximum:
            raise Unsafe('INVALID_PRICE_RANGE')
        return minimum, maximum
    raise Unsafe('INVALID_LEVEL_MODE')


def _is_fixed(level: LevelConfig) -> bool:
    """FIXED_EDGE: an exact level, or (reference 0.3.2) a range with equal edges."""
    minimum, maximum = _edges(level)
    return minimum == maximum


# --------------------------------------------------------------------------- band
def level_bounds(center: Decimal, meta: PairMeta, level: LevelConfig) -> tuple[Decimal, Decimal]:
    """Rounded new-order band of a level (reference ``price_bounds``).

    exact: BUY ``floor(C*(1-e/100))`` / SELL ``ceil(C*(1+e/100))`` for both bounds
    (FIXED_EDGE, rounded away from the center). range: BUY
    ``[ceil(C*(1-max/100)), floor(C*(1-min/100))]``, SELL
    ``[ceil(C*(1+min/100)), floor(C*(1+max/100))]``. Edges are signed (a negative
    edge allows a BUY above / SELL below the center); the config validator keeps
    them above ``safety.min_new_edge_pct``. A band without a tick is never rounded
    outward: ``EMPTY_PRICE_RANGE``.
    """
    side = _side(level.side)
    center = _center(center)
    minimum, maximum = _edges(level)
    if side == 'buy':
        raw_lower, raw_upper = center * (ONE - maximum / HUNDRED), center * (ONE - minimum / HUNDRED)
    else:
        raw_lower, raw_upper = center * (ONE + minimum / HUNDRED), center * (ONE + maximum / HUNDRED)
    if raw_lower <= 0 or raw_upper <= 0:
        raise Unsafe('NONPOSITIVE_PRICE_RANGE')
    if minimum == maximum:
        price = ceil_price(raw_lower, meta) if side == 'sell' else floor_price(raw_upper, meta)
        if price <= 0:
            raise Unsafe('NONPOSITIVE_PRICE_RANGE')
        return price, price
    lower, upper = ceil_price(raw_lower, meta), floor_price(raw_upper, meta)
    if lower <= 0 or upper <= 0:
        raise Unsafe('NONPOSITIVE_PRICE_RANGE')
    if lower > upper:
        raise Unsafe('EMPTY_PRICE_RANGE')
    return lower, upper


def outside_tolerance(price: Decimal, lower: Decimal, upper: Decimal, tolerance_pct: Decimal) -> bool:
    """Tolerance is a percentage of each rounded price boundary, not edge points.

    The boundary itself is inside (inclusive bounds).
    """
    price, lower, upper, tolerance = D(price), D(lower), D(upper), D(tolerance_pct)
    if price <= 0 or lower <= 0 or lower > upper or not ZERO <= tolerance < HUNDRED:
        raise ValueError('Invalid band tolerance input')
    return price < lower * (ONE - tolerance / HUNDRED) or price > upper * (ONE + tolerance / HUNDRED)


def edge_pct(side: str, price: Decimal, center: Decimal) -> Decimal:
    """Signed distance from the center in percent: BUY (C-p)/C*100, SELL (p-C)/C*100."""
    side = _side(side)
    price, center = D(price), D(center)
    if center <= 0:
        raise ValueError('center must be positive')
    if side == 'buy':
        return (center - price) / center * HUNDRED
    return (price - center) / center * HUNDRED


# --------------------------------------------------------------------------- book helpers
def _validated_levels(book: Book | None, side: str) -> tuple[tuple[Decimal, Decimal], ...]:
    """Validate BOTH sides of the snapshot; return the side used for pricing.

    A malformed or missing depth is not equivalent to insufficient visible depth.
    """
    if book is None or not book.bids or not book.asks:
        raise Unsafe('MISSING_BOOK_DEPTH')
    for levels, ascending in ((book.bids, False), (book.asks, True)):
        previous = None
        for row in levels:
            if not isinstance(row, (tuple, list)) or len(row) != 2:
                raise Unsafe('INVALID_BOOK_LEVEL')
            price, qty = row
            if (not isinstance(price, Decimal) or not isinstance(qty, Decimal)
                    or not price.is_finite() or not qty.is_finite() or price <= 0 or qty <= 0):
                raise Unsafe('INVALID_BOOK_LEVEL')
            if previous is not None and ((price <= previous) if ascending else (price >= previous)):
                raise Unsafe('UNSORTED_OR_DUPLICATE_BOOK_LEVEL')
            previous = price
    if book.bids[0][0] >= book.asks[0][0]:
        raise Unsafe('CROSSED_BOOK')
    return tuple(book.bids) if side == 'buy' else tuple(book.asks)


def _validated_own(own_levels: Mapping[Decimal, Decimal] | None) -> Mapping[Decimal, Decimal]:
    own = own_levels or {}
    for price, qty in own.items():
        if (not isinstance(price, Decimal) or not isinstance(qty, Decimal)
                or not price.is_finite() or not qty.is_finite() or price <= 0 or qty < 0):
            raise Unsafe('INVALID_OWN_LEVEL')
    return own


def _foreign_levels(levels: Sequence[tuple[Decimal, Decimal]], own: Mapping[Decimal, Decimal]
                    ) -> tuple[list[tuple[Decimal, Decimal]], Decimal]:
    """Foreign notional per level and own excluded notional.

    Own volume is capped PER LEVEL and never carried to the next level.
    """
    foreign: list[tuple[Decimal, Decimal]] = []
    excluded = ZERO
    for price, qty in levels:
        removed = min(qty, own.get(price, ZERO))
        excluded += price * removed
        foreign.append((price, price * (qty - removed)))
    return foreign, excluded


def _evidence(foreign: Sequence[tuple[Decimal, Decimal]], levels: Sequence[tuple[Decimal, Decimal]],
              side: str, selected: Decimal) -> tuple[Decimal, bool]:
    """(skipped, ahead_complete) for a final price.

    Strictly better prices only: same-price queue position is unknown. If our
    price lies beyond the snapshot, the visible volume ahead is only a lower bound.
    """
    skipped = sum((amount for price, amount in foreign
                   if (price > selected if side == 'buy' else price < selected)), ZERO)
    ahead_complete = selected >= levels[-1][0] if side == 'buy' else selected <= levels[-1][0]
    return skipped, ahead_complete


def _positive_input(value, code: str) -> Decimal:
    try:
        result = D(value)
    except (ValueError, TypeError) as exc:
        raise Unsafe(code) from exc
    if result <= 0:
        raise Unsafe(code)
    return result


# --------------------------------------------------------------------------- selection
def select_level_price(center: Decimal, book: Book, meta: PairMeta, level: LevelConfig, *,
                       own_levels: Mapping[Decimal, Decimal] | None = None,
                       floor: Decimal | None = None,
                       ceiling: Decimal | None = None,
                       maker_limit: Decimal | None = None,
                       profit_floor_mode: str = 'hold') -> PriceDecision:
    """Follow visible foreign volume, then enforce the strict new-order price band.

    Port of the reference ``select_price`` for one ladder level:

    1. band from :func:`level_bounds`;
    2. book validation (both sides);
    3. modes: ``out_of_range`` nearest_index|clamp, ``insufficient_depth`` clamp|wait;
    4. ``floor`` (SELL only) raises the lower bound (``+PROFIT_FLOOR``); above the
       band ``profit_floor_mode: hold`` refuses (PROFIT_FLOOR_ABOVE_RANGE) and
       ``quote`` rests exactly at the floor (PROFIT_FLOOR_QUOTE);
    5. ``ceiling`` (BUY only) lowers the upper bound (``+PROFIT_CEILING``) or
       refuses (PROFIT_CEILING_BELOW_RANGE);
    6. ``own_levels`` maps price -> own VISIBLE qty at that price (for an iceberg the
       caller passes only the visible part). It is subtracted per level, capped at
       the level qty and never carried to the next level. Paper: pass nothing;
    7. candidate: PROFIT_FLOOR_QUOTE, FIXED_EDGE (exact level: book volume not
       used), ZERO_SKIP, then nearest_index / clamp walk over foreign volume (the
       clamp anchor ignores own volume so the bot cannot ratchet its own price);
    8. ``maker_limit`` (BUY: lowest of ask and own SELLs; SELL: highest of bid and own
       BUYs): a price at or through it moves one tick to the maker side if that tick
       is still inside [lower, upper] (``+TOUCH``); otherwise the crossing price is
       returned unchanged and the ladder marks it not placeable. FIXED_EDGE never
       moves (lower == upper);
    9. evidence: skipped (strictly better foreign notional), visible, own excluded;
    10. ``insufficient_depth: wait`` requires skip notional of foreign volume strictly
        ahead of the FINAL price (INSUFFICIENT_VISIBLE_DEPTH / SKIP_NOTIONAL_NOT_REACHED).

    The returned ``lower``/``upper`` are AFTER the profit floor/ceiling adjustment.
    """
    side = _side(level.side)
    lower, upper = level_bounds(center, meta, level)
    fixed = _is_fixed(level)
    levels = _validated_levels(book, side)
    mode = level.out_of_range or 'nearest_index'
    if mode not in OUT_OF_RANGE_MODES:
        raise Unsafe('INVALID_OUT_OF_RANGE_MODE')
    depth_policy = level.insufficient_depth
    if depth_policy not in INSUFFICIENT_DEPTH_POLICIES:
        raise Unsafe('INVALID_INSUFFICIENT_DEPTH_POLICY')
    if profit_floor_mode not in PROFIT_FLOOR_MODES:
        raise Unsafe('INVALID_PROFIT_FLOOR_MODE')

    floor_reason = None
    if floor is not None:
        if side != 'sell':
            raise ValueError('A profit floor applies only to SELL')
        floor_px = ceil_price(_positive_input(floor, 'INVALID_PROFIT_FLOOR_INPUT'), meta)
        if floor_px > upper:
            if profit_floor_mode != 'quote':
                raise Unsafe('PROFIT_FLOOR_ABOVE_RANGE')
            lower = upper = floor_px
            floor_reason = 'PROFIT_FLOOR_QUOTE'
        elif floor_px > lower:
            lower = floor_px
            floor_reason = 'PROFIT_FLOOR'
    ceiling_applied = False
    if ceiling is not None:
        if side != 'buy':
            raise ValueError('A profit ceiling applies only to BUY')
        ceiling_px = floor_price(_positive_input(ceiling, 'INVALID_PROFIT_FLOOR_INPUT'), meta)
        if ceiling_px < lower:
            raise Unsafe('PROFIT_CEILING_BELOW_RANGE')
        if ceiling_px < upper:
            upper, ceiling_applied = ceiling_px, True

    own = _validated_own(own_levels)
    foreign, excluded = _foreign_levels(levels, own)
    visible = sum((amount for _, amount in foreign), ZERO)
    raw_skip = level.skip_notional_usdt
    if raw_skip is None and fixed:
        raw_skip = ZERO
    try:
        skip = D(raw_skip)
    except (ValueError, TypeError) as exc:
        raise Unsafe('INVALID_SKIP_NOTIONAL') from exc
    if skip < 0:
        raise Unsafe('INVALID_SKIP_NOTIONAL')

    fallback = upper if side == 'buy' else lower       # nearest to the center: minimal advantage
    beyond = price_before if side == 'buy' else price_after
    candidate: Decimal | None = None
    if floor_reason == 'PROFIT_FLOOR_QUOTE':
        selected, reason = lower, floor_reason
    elif fixed:
        selected, reason = lower, 'FIXED_EDGE'          # exact level: book volume not used
    elif skip == 0:
        selected, reason = fallback, 'ZERO_SKIP'
    else:
        running = ZERO
        reached = None
        for price, amount in foreign:
            running += amount
            if running >= skip:
                reached = price
                break
        if mode == 'nearest_index':
            if reached is not None:
                candidate = beyond(reached, meta)
            if candidate is None:
                selected, reason = fallback, 'INSUFFICIENT_VISIBLE_DEPTH'
            elif candidate < lower:                      # ZERO from price_before is "below range"
                selected, reason = fallback, 'VOLUME_BELOW_RANGE'
            elif candidate > upper:
                selected, reason = fallback, 'VOLUME_ABOVE_RANGE'
            else:
                selected, reason = candidate, 'BOOK_VOLUME'
        else:
            depth_short = reached is None
            # Anchor on foreign volume only: the bot's own resting order may be the
            # deepest visible level, and anchoring on it would ratchet the price.
            anchor = reached if reached is not None else next(
                (price for price, amount in reversed(foreign) if amount > 0), None)
            prefix = 'DEPTH_EXHAUSTED' if depth_short else 'BOOK_VOLUME'
            if anchor is None:
                selected, reason = fallback, 'NO_FOREIGN_DEPTH'
            else:
                candidate = beyond(anchor, meta)
                if candidate < lower:
                    selected, reason = lower, prefix + '_CLAMP_LOWER'
                elif candidate > upper:
                    selected, reason = upper, prefix + '_CLAMP_UPPER'
                else:
                    selected, reason = candidate, prefix
    if floor_reason == 'PROFIT_FLOOR' and selected == lower and reason != 'BOOK_VOLUME':
        reason += '+PROFIT_FLOOR'
    if ceiling_applied and selected == upper and reason != 'BOOK_VOLUME':
        reason += '+PROFIT_CEILING'
    if maker_limit is not None:
        limit = _positive_input(maker_limit, 'INVALID_MAKER_LIMIT')
        if side == 'buy' and selected >= limit:
            moved = price_before(limit, meta)
            if moved > 0 and lower <= moved:
                selected, reason = moved, reason + '+TOUCH'
        elif side == 'sell' and selected <= limit:
            moved = price_after(limit, meta)
            if moved <= upper:
                selected, reason = moved, reason + '+TOUCH'
    if selected <= 0:
        raise Unsafe('NONPOSITIVE_PRICE_RANGE')
    skipped, ahead_complete = _evidence(foreign, levels, side, selected)
    uses_volume = not fixed and skip > 0
    # In strict mode the volume must actually stand ahead of the final maker
    # price, after own-order subtraction, band clamps and tick rounding.
    if depth_policy == 'wait' and uses_volume and floor_reason != 'PROFIT_FLOOR_QUOTE':
        if visible < skip:
            raise Unsafe('INSUFFICIENT_VISIBLE_DEPTH')
        if skipped < skip:
            raise Unsafe('SKIP_NOTIONAL_NOT_REACHED')
    return PriceDecision(selected, lower, upper, reason, candidate, skipped, visible, excluded,
                         skip, visible >= skip, skipped >= skip, ahead_complete, uses_volume)


# --------------------------------------------------------------------------- ladder
def _crosses(side: str, price: Decimal, limit: Decimal) -> bool:
    return price >= limit if side == 'buy' else price <= limit


def _validated_resting(resting: Mapping[str, Iterable[Decimal]] | None) -> dict[str, tuple[Decimal, ...]]:
    result: dict[str, tuple[Decimal, ...]] = {}
    for level_id, prices in (resting or {}).items():
        values = tuple(prices)
        for price in values:
            if not isinstance(price, Decimal) or not price.is_finite() or price <= 0:
                raise Unsafe('INVALID_RESTING_PRICE')
        if values:
            result[str(level_id)] = values
    return result


def ladder_targets(center: Decimal, book: Book, meta: PairMeta, levels: Sequence[LevelConfig], side: str, *,
                   own_levels: Mapping[Decimal, Decimal] | None = None,
                   floor: Decimal | None = None,
                   ceiling: Decimal | None = None,
                   maker_limit: Decimal | None = None,
                   profit_floor_mode: str = 'hold',
                   min_new_edge_pct: Decimal,
                   resting: Mapping[str, Iterable[Decimal]] | None = None) -> tuple[LevelTarget, ...]:
    """Targets for every level of one side, in config order (shallow -> deep).

    1. ``select_level_price`` per level; Unsafe -> no decision, reason = code.
    2. ``MIN_NEW_EDGE_BUFFER``: the level's own price is closer than
       ``min_new_edge_pct`` to the center AFTER rounding (decision kept).
    3. Strict monotonicity against the previous level that produced a ladder
       position (a decision that did not collapse) AND against every price a
       resting own order of a SHALLOWER level may rest at (``resting``: level id ->
       prices of that level's live same-side order): BUY ``p < min(p_prev, resting)``,
       SELL ``p > max(p_prev, resting)``. A violating level moves to the tick beyond
       that bound (``+LADDER_SHIFT``; evidence recomputed for the moved price). A
       range level may move only inside its own [lower, upper]; a FIXED_EDGE level
       may always move deeper (its reported band is widened by the move so a
       resting order at the shifted price stays inside the band). Otherwise
       ``LEVEL_COLLAPSED`` (not a ladder position: the next level is compared with
       ``p_prev``).
    4. ``LEVEL_ORDER_WAIT``: the price would not stay strictly shallower than a
       resting own order of a DEEPER level (BUY ``p <= max(resting deeper)``, SELL
       ``p >= min(resting deeper)``); the level waits until that order is repriced
       (decision kept).
    5. ``POST_ONLY_OR_SELF_CROSS_WAIT`` when the final price touches ``maker_limit``.

    Precedence of not-placeable reasons: Unsafe code > LEVEL_COLLAPSED >
    LEVEL_ORDER_WAIT > MIN_NEW_EDGE_BUFFER > POST_ONLY_OR_SELF_CROSS_WAIT. Placeable
    targets carry the decision reason. A level's own ``resting`` entry is ignored for
    that level (it is the order being replaced or kept).
    """
    side = _side(side)
    min_new_edge = D(min_new_edge_pct)
    resting_prices = _validated_resting(resting)
    level_ids = [level.id for level in levels]
    targets: list[LevelTarget] = []
    reference: Decimal | None = None
    foreign_cache: list = []

    def foreign_for_shift():
        if not foreign_cache:
            depth = _validated_levels(book, side)
            foreign, _ = _foreign_levels(depth, _validated_own(own_levels))
            foreign_cache.append((foreign, depth))
        return foreign_cache[0]

    for index, level in enumerate(levels):
        if level.side != side:
            raise ValueError(f'level {level.id} belongs to {level.side}, not {side}')
        try:
            decision = select_level_price(center, book, meta, level, own_levels=own_levels, floor=floor,
                                          ceiling=ceiling, maker_limit=maker_limit,
                                          profit_floor_mode=profit_floor_mode)
        except Unsafe as exc:
            targets.append(LevelTarget(side, level.id, None, str(exc), False))
            continue
        too_close = edge_pct(side, decision.price, center) < min_new_edge
        shallower = [p for other in level_ids[:index] if other != level.id for p in resting_prices.get(other, ())]
        deeper = [p for other in level_ids[index + 1:] if other != level.id for p in resting_prices.get(other, ())]
        bound = reference
        if shallower:
            nearest = min(shallower) if side == 'buy' else max(shallower)
            if bound is None:
                bound = nearest
            else:
                bound = min(bound, nearest) if side == 'buy' else max(bound, nearest)
        collapsed = False
        if bound is not None and not (decision.price < bound if side == 'buy' else decision.price > bound):
            moved = price_before(bound, meta) if side == 'buy' else price_after(bound, meta)
            allowed = moved > 0 and (_is_fixed(level) or decision.lower <= moved <= decision.upper)
            if allowed:
                foreign, depth = foreign_for_shift()
                skipped, ahead_complete = _evidence(foreign, depth, side, moved)
                decision = dataclasses.replace(
                    decision,
                    price=moved,
                    lower=min(decision.lower, moved),
                    upper=max(decision.upper, moved),
                    reason=decision.reason + '+LADDER_SHIFT',
                    skipped_notional=skipped,
                    skip_satisfied=skipped >= decision.requested_skip_notional,
                    ahead_volume_complete=ahead_complete,
                )
            else:
                collapsed = True
        if not collapsed:
            reference = decision.price
        waits = bool(deeper) and not (decision.price > max(deeper) if side == 'buy'
                                      else decision.price < min(deeper))
        if collapsed:
            reason, placeable = 'LEVEL_COLLAPSED', False
        elif waits:
            reason, placeable = 'LEVEL_ORDER_WAIT', False
        elif too_close:
            reason, placeable = 'MIN_NEW_EDGE_BUFFER', False
        elif maker_limit is not None and _crosses(side, decision.price, D(maker_limit)):
            reason, placeable = 'POST_ONLY_OR_SELF_CROSS_WAIT', False
        else:
            reason, placeable = decision.reason, True
        targets.append(LevelTarget(side, level.id, decision, reason, placeable))
    return tuple(targets)


# --------------------------------------------------------------------------- maker / self-cross
def maker_limit(side: str, book: Book, own_opposite_prices: Iterable[Decimal]) -> Decimal:
    """Nearest price a new order must not touch.

    BUY: ``min(book.ask, *own SELL prices)``; SELL: ``max(book.bid, *own BUY prices)``
    (all active own orders of the opposite side, every ladder level).
    """
    side = _side(side)
    if book is None or not book.bids or not book.asks:
        raise Unsafe('MISSING_BOOK_DEPTH')
    prices = [D(p) for p in own_opposite_prices]
    if side == 'buy':
        return min([book.asks[0][0], *prices])
    return max([book.bids[0][0], *prices])


def post_only_ok(side: str, price: Decimal, book: Book, own_opposite_prices: Iterable[Decimal]) -> bool:
    """BUY: price < ask and < every own SELL; SELL: price > bid and > every own BUY.

    A missing book is never OK (fail closed).
    """
    side = _side(side)
    if book is None or not book.bids or not book.asks:
        return False
    price = D(price)
    prices = [D(p) for p in own_opposite_prices]
    if side == 'buy':
        return price < book.asks[0][0] and all(price < p for p in prices)
    return price > book.bids[0][0] and all(price > p for p in prices)


# --------------------------------------------------------------------------- profit floor / ceiling
def _profit_inputs(price, min_profit_pct, fee_pct) -> tuple[Decimal, Decimal, Decimal]:
    try:
        price, profit, fee = D(price), D(min_profit_pct), D(fee_pct)
    except (ValueError, TypeError) as exc:
        raise Unsafe('INVALID_PROFIT_FLOOR_INPUT') from exc
    if price <= 0 or profit <= -HUNDRED or not ZERO <= fee < HUNDRED:
        raise Unsafe('INVALID_PROFIT_FLOOR_INPUT')
    return price, profit, fee


def profit_floor_price(avg_cost: Decimal, min_profit_pct: Decimal, fee_pct: Decimal) -> Decimal:
    """Lowest SELL price whose net proceeds exceed ``avg_cost`` by ``min_profit_pct``.

    ``fee_pct`` is the expected SELL fee (charged in USDT). ``avg_cost`` already
    includes the BUY fee because it divides paid USDT by the received net tokens.
    """
    avg_cost, profit, fee = _profit_inputs(avg_cost, min_profit_pct, fee_pct)
    return avg_cost * (ONE + profit / HUNDRED) / (ONE - fee / HUNDRED)


def coupled_sell_floor(max_own_buy: Decimal, min_profit_pct: Decimal, fee_pct: Decimal) -> Decimal:
    """SELL floor implied by the highest own resting BUY: ``B*(1+m/100)/(1-f/100)**2``."""
    buy, profit, fee = _profit_inputs(max_own_buy, min_profit_pct, fee_pct)
    return buy * (ONE + profit / HUNDRED) / (ONE - fee / HUNDRED) ** 2


def coupled_buy_ceiling(min_own_sell: Decimal, min_profit_pct: Decimal, fee_pct: Decimal) -> Decimal:
    """BUY ceiling implied by the lowest own resting SELL: ``S*(1-f/100)**2/(1+m/100)``."""
    sell, profit, fee = _profit_inputs(min_own_sell, min_profit_pct, fee_pct)
    return sell * (ONE - fee / HUNDRED) ** 2 / (ONE + profit / HUNDRED)


# --------------------------------------------------------------------------- resting-order guards
def _guard_pct(value, name: str) -> Decimal:
    value = D(value)
    if not ZERO <= value < HUNDRED:
        raise ValueError(f'{name} must be in [0, 100)')
    return value


def hard_guard_violation(side: str, price: Decimal, center: Decimal, hard_min_edge_pct: Decimal) -> bool:
    """True when a resting order's edge is strictly below ``hard_min_edge_pct``.

    BUY ``price > C*(1-h/100)``; SELL ``price < C*(1+h/100)`` (the boundary is OK).
    """
    side = _side(side)
    price, center = D(price), D(center)
    hard = _guard_pct(hard_min_edge_pct, 'hard_min_edge_pct')
    if center <= 0:
        raise ValueError('center must be positive')
    if side == 'buy':
        return price > center * (ONE - hard / HUNDRED)
    return price < center * (ONE + hard / HUNDRED)


def adverse_boundary(side: str, center: Decimal, meta: PairMeta, level: LevelConfig,
                     hard_min_edge_pct: Decimal) -> Decimal:
    """Band-tolerance boundary of a resting order, capped by the hard guard.

    BUY ``min(upper*(1+tol/100), C*(1-h/100))``; SELL ``max(lower*(1-tol/100),
    C*(1+h/100))`` with ``tol = level.band_tolerance_pct`` and the level's unadjusted
    band (no profit floor/ceiling). The tolerance never relaxes ``hard_min_edge``.
    An order is adverse iff BUY ``price > boundary`` / SELL ``price < boundary``.
    Raises the :func:`level_bounds` Unsafe codes (e.g. EMPTY_PRICE_RANGE) when the
    level has no band at this center; the caller decides (fail closed).
    """
    side = _side(side)
    if level.side != side:
        raise ValueError(f'level {level.id} belongs to {level.side}, not {side}')
    tolerance = _guard_pct(level.band_tolerance_pct, 'band_tolerance_pct')
    hard = _guard_pct(hard_min_edge_pct, 'hard_min_edge_pct')
    center = _center(center)
    lower, upper = level_bounds(center, meta, level)
    if side == 'buy':
        return min(upper * (ONE + tolerance / HUNDRED), center * (ONE - hard / HUNDRED))
    return max(lower * (ONE - tolerance / HUNDRED), center * (ONE + hard / HUNDRED))
