"""Read-only performance report per ladder slot (DESIGN §13; notes/ref_ledger_store_report.md §4-5).

Used to tune the ladder: how often each level (slot ``side:level``) fills, at what edge
to the index it was placed and filled, what the index did after the fills, how much of
the time the index was blocked, and what the bot's own round trips realized after fees.

Reading: ONE read transaction (one WAL snapshot) over meta, orders, fills, transfers,
marks and index samples, with ``PRAGMA query_only=ON`` (no write is possible from the
report). With a ``Store`` the store lock is held for the duration of the read and the
pragma is reset afterwards; with a path a separate connection is opened (works next to
a running bot).

Window: orders with ``created_ms`` and fills with ``time_ms`` in ``[now - hours, now]``
(``hours=None``: the whole campaign up to ``now``). Index blocked time and basis use the
index samples (``index_obs``) of the same window.

Metric definitions (edge: positive = favourable to us; reference ``_edge``):

* ``edge(side, p, R)`` = BUY ``(R - p) / R * 100``; SELL ``(p - R) / R * 100``.
* ``orders`` = window orders that reached the book: not ``rejected`` and not a post-only
  rejection (``finish_as == 'poc'`` with zero fill, Gate's "created then cancelled"
  form). Both kinds are counted separately (``orders_rejected``,
  ``orders_post_only_rejected``) and do not spoil the fill ratio.
* ``orders_with_fills`` = ``orders`` with ``filled_qty > 0`` (lifetime of the order);
  ``fill_ratio_pct = orders_with_fills / orders * 100`` (an ORDER COUNT ratio, not qty).
* ``filled_qty``, ``filled_notional_usdt`` (gross ``qty*price``), ``avg_fill_price`` =
  notional / qty (pre-fee VWAP; None for a group mixing BUY and SELL).
* Two references. INDEX ``I`` (``order.index_price`` at placement, the marks ``I(t)``):
  every ``*_index*`` edge and the markouts use it, so placement, at-fill and markout
  values are directly comparable (reference §7.1 reading: at-fill clearly worse than
  vs-placement = adverse selection). CENTRE ``C = I * (1 + basis_offset_pct / 100)``
  (``order.center_price``, the price the ladder was built from): the ``*_vs_center`` /
  ``*_center_*`` fields; they differ from the index ones by about the basis offset.
* ``avg_edge_at_placement_vs_index_pct`` / ``avg_edge_at_placement_vs_center_pct`` =
  unweighted mean of ``edge(side, placement price, order.index_price / center_price)``
  over ``orders``; the placement price is the price the order was created at (an amend
  changes ``order.price`` but not the placement index/centre, the first ``prior_prices``
  entry keeps the original price).
* ``avg_edge_of_fills_vs_placement_index_pct`` / ``avg_edge_of_fills_vs_placement_center_pct``
  = fill-qty-weighted ``edge(side, fill.price, order.index_price / center_price)``.
* Known limitation: an amended order keeps its CREATION index/centre (the store does not
  record the centre an amend was priced from), so fills of amended orders are measured
  against the creation reference in the two "vs placement" fill metrics. Amend
  repricing (``execution.reprice_via_amend``) is off by default.
* ``avg_edge_of_fills_vs_index_at_fill_pct`` = qty-weighted ``edge(side, fill.price,
  I(T))`` over fills that have a mark at execution time T (``fills_with_index_at_fill``).
* ``markout_{5,30,60}s_pct`` = qty-weighted ``edge(side, fill.price, I(T + h))`` — the
  reference definition, i.e. BUY ``(I - p) / I * 100`` and SELL ``(p - I) / I * 100``
  with the index ``I`` h seconds after execution in the DENOMINATOR (positive: the
  index then was still on our favourable side); counts in ``fills_with_markout_{h}s``.
* Marks ``I(t)``: ``fill_index_marks`` rows recorded by the engine (first value wins);
  a missing horizon is computed from the persisted valid ``index_obs`` samples with the
  same ±1 s bracket rule (``marks.mark_from_samples``); unknown stays unknown.
* ``median_rest_seconds``: closed ``orders``, first terminal observation - creation.
* ``placement_rules`` / ``cancel_reasons``: counts of ``orders.reason`` /
  ``orders.cancel_reason`` over ``orders``.
* ``fees``: per currency over window fills (``fee`` by ``fee_currency`` plus ``GT``
  = Σ ``gt_fee`` and ``POINT`` = Σ ``point_fee``); ``fees_usdt_equiv`` = quote fees +
  base fees × fill price (GT/POINT/other currencies are not converted).
* ``order_rebates``: the order-level ``rebated_fee`` Gate reports on finished orders
  (table ``order_rebates``, latest value per order). It is NOT part of the ledger replay
  (cost, realized); it is listed separately per currency for the campaign and for the
  window (an order belongs to the window by its ``created_ms``) and per slot, and
  ``realized_incl_quote_rebates_usdt`` = campaign ``realized_usdt`` + quote-currency
  rebates (a rebate lowers costs; other currencies are not converted).
* ``limitations`` (paper only): paper does not model the queue at a price level nor the
  priority / taker fee of an iceberg's hidden part (the whole order is treated as
  visible) and fills only on a trade strictly through the price, so its fill ratios and
  edges are not directly comparable with live; ``iceberg_slots`` = window slots with an
  iceberg order.
* Profit: the position is replayed from ALL fills and transfers (execution order, as
  the live ledger). ``realized_usdt`` (campaign) / ``realized_in_window_usdt`` per SELL
  slot = Σ realized of that slot's SELL fills (attribution of the shared average-cost
  inventory to the SELL level that sold it); BUY slots have None. Totals use the
  position. ``avg_sell_minus_avg_buy_pct = (sell VWAP - buy VWAP) / buy VWAP * 100``
  over window fills.
* Index: each window sample covers the time to the next sample (or to ``now``) capped
  at 1 s; longer gaps are "no data". ``blocked_index_time_pct = blocked / observed *
  100``; reasons by full code and by the code before the first ``:``.
* Basis: ``(spot_mid / index - 1) * 100`` per valid sample with spot bid/ask (sample
  weighted); mean, median, p5, p95 (nearest rank), min, max, count.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from bisect import bisect_left, bisect_right
from collections import Counter
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from .dec import HUNDRED, ZERO, D, fmt, usd
from .ledger import FillEffect, Position, realized_including_initial, replay_effects, signed, tid_number
from .marks import MARK_BRACKET_MS, MARK_HORIZONS, MARKOUT_SECONDS, mark_from_samples
from .store import Store, utc_text
from .types import SIDES

__all__ = ['build_report', 'format_report_ru', 'distribution', 'edge_pct']

HOUR_MS = 3_600_000
GAP_CAP_MS = 1000
POST_ONLY_FINISH = 'poc'
_DIGITS = re.compile(r'(\d+)')


# ----------------------------------------------------------------------------- helpers

def edge_pct(side: str, price: Decimal, reference: Decimal | None) -> Decimal | None:
    """BUY ``(R - p) / R * 100``; SELL ``(p - R) / R * 100``; None without a reference."""
    if reference is None or not reference:
        return None
    if side == 'buy':
        return (reference - price) / reference * HUNDRED
    return (price - reference) / reference * HUNDRED


def _weighted(pairs: Sequence[tuple[Decimal, Decimal]]) -> Decimal | None:
    qty = sum((q for _, q in pairs), ZERO)
    return sum((e * q for e, q in pairs), ZERO) / qty if qty else None


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


def _nearest_rank(ordered: Sequence[Decimal], q: Decimal) -> Decimal:
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def distribution(values: Iterable[Decimal]) -> dict:
    """``{count, mean, median, p5, p95, min, max}`` (nearest-rank percentiles)."""
    ordered = sorted(D(v) for v in values)
    if not ordered:
        return {'count': 0, 'mean': None, 'median': None, 'p5': None, 'p95': None, 'min': None, 'max': None}
    return {'count': len(ordered), 'mean': sum(ordered, ZERO) / len(ordered), 'median': _median(ordered),
            'p5': _nearest_rank(ordered, Decimal('0.05')), 'p95': _nearest_rank(ordered, Decimal('0.95')),
            'min': ordered[0], 'max': ordered[-1]}


def _post_only_reject(order: Mapping[str, Any]) -> bool:
    return order.get('finish_as') == POST_ONLY_FINISH and not order['filled_qty']


def _placement_price(order: Mapping[str, Any]) -> Decimal:
    """Price the order was created at (amends keep the old prices in ``prior_prices``)."""
    prior = order.get('prior_prices') or ()
    return prior[0][0] if prior else order['price']


def _slot_sort_key(slot: str) -> tuple:
    side, _, level = slot.partition(':')
    natural = tuple(int(p) if p.isdigit() else p for p in _DIGITS.split(level) if p)
    return (SIDES.index(side) if side in SIDES else len(SIDES), tuple((isinstance(p, str), p) for p in natural))


def _seconds(ms: int | Decimal) -> Decimal:
    return D(ms) / 1000


# ----------------------------------------------------------------------------- snapshot

@contextmanager
def _snapshot(source: Any) -> Iterator[sqlite3.Connection]:
    """One read transaction with ``query_only`` on a Store (under its lock) or a path."""
    if isinstance(source, Store) or (hasattr(source, 'read') and hasattr(source, 'db')):
        with source.read() as db:
            if db.in_transaction:
                raise RuntimeError('The report cannot run inside an open store transaction')
            db.execute('PRAGMA query_only=ON')
            try:
                db.execute('BEGIN')
                try:
                    yield db
                finally:
                    if db.in_transaction:
                        db.execute('ROLLBACK')
            finally:
                db.execute('PRAGMA query_only=OFF')
        return
    path = Path(source)
    if not path.is_file():
        raise ValueError(f'Database not found: {path}')
    db = sqlite3.connect(str(path.absolute()), timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        yield db
    finally:
        db.close()


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value is None else D(value)


def _read(db: sqlite3.Connection, obs_from: int | None, obs_to: int) -> dict:
    meta = {row['key']: json.loads(row['value']) for row in db.execute('SELECT key,value FROM meta')}
    orders = [Store._order(row) for row in db.execute('SELECT * FROM orders ORDER BY created_ms, rowid')]
    fills = [Store._fill(row) for row in db.execute('SELECT * FROM fills')]
    fills.sort(key=lambda f: (f['time_ms'], tid_number(f['trade_id']), f['fill_key']))
    transfers = [dict(row) for row in db.execute('SELECT id,time_ms,qty FROM ledger_transfers')]
    marks: dict[int, dict[str, Decimal]] = {h: {} for h in MARK_HORIZONS}
    for row in db.execute('SELECT fill_key,horizon_s,index_price FROM fill_index_marks'):
        if row['horizon_s'] in marks:
            marks[row['horizon_s']][row['fill_key']] = D(row['index_price'])
    clauses, params = ['ts_ms<=?'], [obs_to]
    if obs_from is not None:
        clauses.append('ts_ms>=?')
        params.append(obs_from)
    obs = [{'ts_ms': int(r['ts_ms']), 'index': _optional_decimal(r['idx']), 'center': _optional_decimal(r['center']),
            'ok': bool(r['ok']), 'reason': r['reason'], 'spot_bid': _optional_decimal(r['spot_bid']),
            'spot_ask': _optional_decimal(r['spot_ask'])}
           for r in db.execute('SELECT ts_ms,"index" AS idx,center,ok,reason,spot_bid,spot_ask FROM index_obs WHERE '
                               + ' AND '.join(clauses) + ' ORDER BY ts_ms, rowid', params)]
    external_fills = int(db.execute('SELECT COUNT(*) FROM external_fills').fetchone()[0])
    try:
        rebates = [{'order_id': r['order_id'], 'text': r['text'], 'rebated_fee': D(r['rebated_fee']),
                    'currency': r['rebated_fee_currency'], 'recorded_ms': int(r['recorded_ms'])}
                   for r in db.execute('SELECT order_id,text,rebated_fee,rebated_fee_currency,recorded_ms '
                                       'FROM order_rebates ORDER BY recorded_ms, order_id')]
    except sqlite3.OperationalError:        # database created before the table existed
        rebates = []
    return {'meta': meta, 'orders': orders, 'fills': fills, 'transfers': transfers, 'marks': marks, 'obs': obs,
            'external_fills': external_fills, 'rebates': rebates}


def _currencies(meta: Mapping[str, Any]) -> tuple[str, str, str]:
    stored = meta.get('ledger_currencies')
    if isinstance(stored, dict) and stored.get('base') and stored.get('quote'):
        return stored['pair'], stored['base'], stored['quote']
    pair = (meta.get('identity') or {}).get('currency_pair') or 'STONK_USDT'
    base, _, quote = pair.partition('_')
    return pair, base or 'STONK', quote or 'USDT'


# ----------------------------------------------------------------------------- metrics

class _Context:
    def __init__(self, base: str, quote: str, orders_by_text: Mapping[str, dict],
                 effects: Mapping[str, FillEffect], mark: Callable[[dict, int], Decimal | None],
                 initial_avg_price: Decimal | None):
        self.base, self.quote = base, quote
        self.orders_by_text = orders_by_text
        self.effects = effects
        self.mark = mark
        self.initial_avg_price = initial_avg_price


def _group_metrics(ctx: _Context, sides: set[str], orders: list[dict], fills: list[dict],
                   all_fills: list[dict]) -> dict:
    rejected = [o for o in orders if o['state'] == 'rejected']
    post_only = [o for o in orders if o['state'] != 'rejected' and _post_only_reject(o)]
    placed = [o for o in orders if o['state'] != 'rejected' and not _post_only_reject(o)]
    with_fills = [o for o in placed if o['filled_qty'] > 0]
    qty = sum((f['qty'] for f in fills), ZERO)
    notional = sum((f['notional'] for f in fills), ZERO)
    single_side = len({f['side'] for f in fills}) == 1
    placement_index = [e for e in (edge_pct(o['side'], _placement_price(o), o['index_price']) for o in placed)
                       if e is not None]
    placement_center = [e for e in (edge_pct(o['side'], _placement_price(o), o['center_price']) for o in placed)
                        if e is not None]
    vs_placement_index: list[tuple[Decimal, Decimal]] = []
    vs_placement_center: list[tuple[Decimal, Decimal]] = []
    marked: dict[int, list[tuple[Decimal, Decimal]]] = {h: [] for h in MARK_HORIZONS}
    fees: dict[str, Decimal] = {ctx.base: ZERO, ctx.quote: ZERO, 'GT': ZERO, 'POINT': ZERO}
    fees_equiv = ZERO
    for f in fills:
        order = ctx.orders_by_text.get(f['text'])
        if order is not None:
            for reference, bucket in ((order['index_price'], vs_placement_index),
                                      (order['center_price'], vs_placement_center)):
                edge = edge_pct(f['side'], f['price'], reference)
                if edge is not None:
                    bucket.append((edge, f['qty']))
        for horizon in MARK_HORIZONS:
            edge = edge_pct(f['side'], f['price'], ctx.mark(f, horizon))
            if edge is not None:
                marked[horizon].append((edge, f['qty']))
        fees[f['fee_currency']] = fees.get(f['fee_currency'], ZERO) + f['fee']
        fees['GT'] += f['gt_fee']
        fees['POINT'] += f['point_fee']
        if f['fee_currency'] == ctx.quote:
            fees_equiv += f['fee']
        elif f['fee_currency'] == ctx.base:
            fees_equiv += f['fee'] * f['price']
    rested = []
    for o in placed:
        terminal = o['terminal_since_ms'] or o['last_terminal_ms']
        if o['state'] == 'closed' and terminal:
            rested.append(_seconds(int(terminal) - int(o['created_ms'])))
    result: dict[str, Any] = {
        'orders': len(placed),
        'orders_rejected': len(rejected),
        'orders_post_only_rejected': len(post_only),
        'orders_with_fills': len(with_fills),
        'fill_ratio_pct': Decimal(len(with_fills)) / len(placed) * HUNDRED if placed else None,
        'fills': len(fills),
        'filled_qty': qty,
        'filled_notional_usdt': notional,
        'avg_fill_price': notional / qty if qty and single_side else None,
        # index reference (comparable with the at-fill marks and the markouts)
        'avg_edge_at_placement_vs_index_pct': (sum(placement_index, ZERO) / len(placement_index)
                                               if placement_index else None),
        'avg_edge_of_fills_vs_placement_index_pct': _weighted(vs_placement_index),
        'avg_edge_of_fills_vs_index_at_fill_pct': _weighted(marked[0]),
        'fills_with_index_at_fill': len(marked[0]),
    }
    for horizon in MARKOUT_SECONDS:
        result[f'markout_{horizon}s_pct'] = _weighted(marked[horizon])
        result[f'fills_with_markout_{horizon}s'] = len(marked[horizon])
    # centre reference (the price the ladder was built from; differs by the basis offset)
    result['avg_edge_at_placement_vs_center_pct'] = (sum(placement_center, ZERO) / len(placement_center)
                                                     if placement_center else None)
    result['avg_edge_of_fills_vs_placement_center_pct'] = _weighted(vs_placement_center)
    result['median_rest_seconds'] = _median(rested)
    result['placement_rules'] = dict(sorted(Counter(o['reason'] for o in placed).items()))
    result['cancel_reasons'] = dict(sorted(Counter(o['cancel_reason'] for o in placed if o['cancel_reason']).items()))
    result['fees'] = fees
    result['fees_usdt_equiv'] = fees_equiv
    if 'sell' in sides:
        window_sells = [f for f in fills if f['side'] == 'sell']
        all_sells = [f for f in all_fills if f['side'] == 'sell']
        result['realized_usdt'] = sum((ctx.effects[f['fill_key']].realized or ZERO for f in all_sells), ZERO)
        result['realized_in_window_usdt'] = sum((ctx.effects[f['fill_key']].realized or ZERO for f in window_sells),
                                                ZERO)
        unmatched = sum((ctx.effects[f['fill_key']].unmatched_qty for f in window_sells), ZERO)
        result['window_unmatched_sold_qty'] = unmatched
        if ctx.initial_avg_price is not None:
            result['realized_in_window_incl_initial_usdt'] = sum(
                (realized_including_initial(ctx.effects[f['fill_key']], ctx.initial_avg_price) or ZERO
                 for f in window_sells), ZERO)
        result['window_has_unknown_cost_sales'] = bool(unmatched > 0 and ctx.initial_avg_price is None)
    else:
        result['realized_usdt'] = None
        result['realized_in_window_usdt'] = None
    return result


def _index_stats(rows: Sequence[Mapping[str, Any]], until_ms: int, window_start: int | None) -> dict:
    observed = blocked = 0
    blocked_samples = 0
    reasons: dict[str, list[int]] = {}
    codes: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        following = rows[i + 1]['ts_ms'] if i + 1 < len(rows) else until_ms
        duration = max(0, min(int(following), until_ms) - int(row['ts_ms']))
        duration = min(duration, GAP_CAP_MS)
        observed += duration
        if not row['ok']:
            blocked += duration
            blocked_samples += 1
            reason = row['reason'] or 'UNKNOWN'
            for table, key in ((reasons, reason), (codes, reason.split(':', 1)[0])):
                entry = table.setdefault(key, [0, 0])
                entry[0] += duration
                entry[1] += 1

    def summary(table: dict[str, list[int]]) -> dict:
        return {key: {'seconds': _seconds(ms), 'samples': n,
                      'pct_of_observed': D(ms) / observed * HUNDRED if observed else None}
                for key, (ms, n) in sorted(table.items(), key=lambda item: (-item[1][0], item[0]))}

    span = until_ms - window_start if window_start is not None else 0
    return {'samples': len(rows), 'blocked_samples': blocked_samples,
            'observed_seconds': _seconds(observed), 'blocked_seconds': _seconds(blocked),
            'blocked_index_time_pct': D(blocked) / observed * HUNDRED if observed else None,
            'coverage_pct': D(observed) / span * HUNDRED if span > 0 else None,
            'gap_cap_ms': GAP_CAP_MS,
            'blocked_reasons': summary(reasons), 'blocked_reason_codes': summary(codes)}


def _basis(rows: Sequence[Mapping[str, Any]]) -> dict:
    values = []
    for row in rows:
        index, bid, ask = row['index'], row['spot_bid'], row['spot_ask']
        if not row['ok'] or index is None or bid is None or ask is None or index <= 0 or bid <= 0 or ask < bid:
            continue
        values.append(((bid + ask) / 2 / index - 1) * HUNDRED)
    stats = distribution(values)
    return {'definition': '(spot_mid / index - 1) * 100', 'count': stats['count'],
            **{f'{key}_pct': stats[key] for key in ('mean', 'median', 'p5', 'p95', 'min', 'max')}}


def _order_rebates(rows: Sequence[Mapping[str, Any]], orders_by_text: Mapping[str, dict],
                   in_window: Callable[[int], bool], quote: str, realized: Decimal) -> dict:
    """Order-level ``rebated_fee`` per currency: campaign, window, per window slot."""
    campaign: dict[str, Decimal] = {}
    window: dict[str, Decimal] = {}
    by_slot: dict[str, dict[str, Decimal]] = {}
    window_orders = 0
    for row in rows:
        amount, currency = row['rebated_fee'], row['currency'] or 'UNKNOWN'
        campaign[currency] = campaign.get(currency, ZERO) + amount
        order = orders_by_text.get(row['text']) if row['text'] else None
        if not in_window(int(order['created_ms']) if order is not None else row['recorded_ms']):
            continue
        window_orders += 1
        window[currency] = window.get(currency, ZERO) + amount
        slot = by_slot.setdefault(order['slot'] if order is not None else 'unknown', {})
        slot[currency] = slot.get(currency, ZERO) + amount
    return {'definition': 'order-level rebated_fee (Gate); not included in cost / realized of the ledger',
            'in_ledger': False, 'orders': len(rows), 'orders_in_window': window_orders,
            'campaign': dict(sorted(campaign.items())), 'window': dict(sorted(window.items())),
            'by_slot': {slot: dict(sorted(v.items())) for slot, v in sorted(by_slot.items(),
                                                                            key=lambda kv: _slot_sort_key(kv[0]))},
            'realized_incl_quote_rebates_usdt': realized + campaign.get(quote, ZERO)}


PAPER_LIMITATIONS = ('paper: the queue at a price level and the priority / taker fee of the hidden iceberg part '
                     'are not modelled (the whole order is treated as visible); a fill needs a trade strictly '
                     'through the price; fill ratios and edges are not directly comparable with live')


def _position_block(position: Position, initial_avg_price: Decimal | None) -> dict:
    return {'qty': position.qty, 'avg_cost': position.avg_cost, 'cost_usdt': position.cost,
            'realized_usdt': position.realized,
            'realized_incl_initial_usdt': position.realized_with(initial_avg_price),
            'bought_qty': position.bought_qty, 'bought_cost_usdt': position.bought_cost,
            'sold_qty': position.sold_qty, 'sold_proceeds_usdt': position.sold_proceeds,
            'unmatched_sold_qty': position.unmatched_qty, 'unmatched_proceeds_usdt': position.unmatched_proceeds,
            'fees_base': position.fees_base, 'fees_quote': position.fees_quote,
            'fees_other': dict(sorted(position.fees_other.items())), 'rebates_quote': position.rebates_quote,
            'transferred_out_qty': position.transferred_qty, 'transferred_out_cost_usdt': position.transferred_cost,
            'fills': position.fills}


# ----------------------------------------------------------------------------- report

def build_report(store: Any, *, hours: float | int | Decimal | None = None, now_ms: int | None = None,
                 initial_avg_price: Decimal | None = None, slots: Iterable[str] | None = None) -> dict:
    """Per-slot and total metrics (see module docstring) from ONE read snapshot.

    ``store`` is a ``Store`` or a path to its SQLite file. ``slots`` (``side:level``)
    are always listed, even without activity (e.g. the configured ladder)."""
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    since_ms: int | None = None
    if hours is not None:
        span = D(hours)
        if span <= 0:
            raise ValueError('hours must be positive')
        since_ms = now_ms - int(span * HOUR_MS)
    initial = None if initial_avg_price is None else D(initial_avg_price)
    obs_from = None if since_ms is None else since_ms - MARK_BRACKET_MS
    obs_to = now_ms + max(MARKOUT_SECONDS) * 1000 + MARK_BRACKET_MS
    with _snapshot(store) as db:
        data = _read(db, obs_from, obs_to)

    meta = data['meta']
    identity = meta.get('identity') or {}
    pair, base, quote = _currencies(meta)
    orders, fills = data['orders'], data['fills']
    position, effects = replay_effects(fills, base, quote, transfers=data['transfers']) if (
        fills or data['transfers']) else (Position(), {})

    def in_window(ms: int) -> bool:
        return (since_ms is None or ms >= since_ms) and ms <= now_ms

    window_orders = [o for o in orders if in_window(o['created_ms'])]
    window_fills = [f for f in fills if in_window(f['time_ms'])]

    # Execution-time marks of the window fills: engine-recorded first, else the same
    # ±1 s bracket rule over the persisted valid samples; unknown stays unknown.
    samples = [(row['ts_ms'], row['index']) for row in data['obs'] if row['ok'] and row['index'] is not None]
    sample_times = [ts for ts, _ in samples]

    def sample_mark(ms: int) -> Decimal | None:
        # Only samples within the bracket can qualify, so the slice gives the same answer
        # as the full list while keeping each lookup O(log n).
        lo = bisect_left(sample_times, ms - MARK_BRACKET_MS)
        hi = bisect_right(sample_times, ms + MARK_BRACKET_MS)
        found = mark_from_samples(samples[lo:hi], ms)
        return None if found is None else found[0]

    mark_sources: Counter = Counter()
    fill_marks: dict[str, dict[int, Decimal]] = {}
    for fill in window_fills:
        resolved: dict[int, Decimal] = {}
        for horizon in MARK_HORIZONS:
            stored = data['marks'][horizon].get(fill['fill_key'])
            if stored is not None:
                resolved[horizon] = stored
                mark_sources['stored'] += 1
                continue
            found = sample_mark(fill['time_ms'] + horizon * 1000)
            if found is not None:
                resolved[horizon] = found
                mark_sources['computed'] += 1
        fill_marks[fill['fill_key']] = resolved

    def mark(fill: dict, horizon: int) -> Decimal | None:
        return fill_marks.get(fill['fill_key'], {}).get(horizon)

    ctx = _Context(base, quote, {o['text']: o for o in orders}, effects, mark, initial)

    slot_names = set(slots or ())
    slot_names.update(f"{o['side']}:{o['level']}" for o in window_orders)
    slot_names.update(f"{f['side']}:{f['level']}" for f in window_fills)
    slot_report: dict[str, dict] = {}
    for slot in sorted(slot_names, key=_slot_sort_key):
        side, _, level = slot.partition(':')
        entry = {'side': side, 'level': level}
        entry.update(_group_metrics(
            ctx, {side},
            [o for o in window_orders if o['side'] == side and o['level'] == level],
            [f for f in window_fills if f['side'] == side and f['level'] == level],
            [f for f in fills if f['side'] == side and f['level'] == level]))
        slot_report[slot] = entry
    side_report = {side: _group_metrics(ctx, {side}, [o for o in window_orders if o['side'] == side],
                                        [f for f in window_fills if f['side'] == side],
                                        [f for f in fills if f['side'] == side])
                   for side in SIDES}
    totals = _group_metrics(ctx, set(SIDES), window_orders, window_fills, fills)
    buy_avg, sell_avg = side_report['buy']['avg_fill_price'], side_report['sell']['avg_fill_price']
    totals['realized_usdt'] = position.realized
    totals['realized_incl_initial_usdt'] = position.realized_with(initial)
    totals['avg_sell_minus_avg_buy_pct'] = (sell_avg - buy_avg) / buy_avg * HUNDRED if buy_avg and sell_avg else None
    totals['turnover_usdt'] = totals['filled_notional_usdt']

    window_obs = [row for row in data['obs'] if in_window(row['ts_ms'])]
    start_ms = meta.get('start_ms')
    window_start = since_ms if since_ms is not None else (
        int(start_ms) if start_ms is not None else (window_obs[0]['ts_ms'] if window_obs else None))
    external = meta.get('external_activity')
    orders_by_text = ctx.orders_by_text
    iceberg_slots = sorted({o['slot'] for o in window_orders if o.get('iceberg')}, key=_slot_sort_key)
    return {
        'generated_ms': now_ms,
        'window_hours': None if hours is None else D(hours),
        'since_ms': since_ms,
        'until_ms': now_ms,
        'campaign_id': identity.get('campaign_id'),
        'mode': identity.get('mode'),
        'currency_pair': pair,
        'base': base,
        'quote': quote,
        'start_ms': start_ms,
        'external_activity': external,
        'account_review_required': bool(external),
        'external_fills_excluded': data['external_fills'],
        'slots': slot_report,
        'sides': side_report,
        'totals': totals,
        'position': _position_block(position, initial),
        'order_rebates': _order_rebates(data['rebates'], orders_by_text, in_window, quote, position.realized),
        'limitations': PAPER_LIMITATIONS if identity.get('mode') == 'paper' else None,
        'iceberg_slots': iceberg_slots,
        'index': _index_stats(window_obs, now_ms, window_start),
        'basis': _basis(window_obs),
        'marks': {'bracket_ms': MARK_BRACKET_MS, 'horizons_s': list(MARK_HORIZONS),
                  'stored': mark_sources['stored'], 'computed': mark_sources['computed']},
    }


# ----------------------------------------------------------------------------- Russian text

def _p(value: Decimal | None, places: int = 3) -> str:
    if value is None:
        return 'н/д'
    text = fmt(value, places)
    return text if text.startswith('-') or text == '0' else '+' + text


def _n(value: Decimal | None, places: int = 2) -> str:
    return 'н/д' if value is None else fmt(value, places)


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    widths = [max(len(str(row[i])) for row in (header, *rows)) for i in range(len(header))]

    def line(row: Sequence[str]) -> str:
        cells = [str(cell).ljust(widths[i]) if i == 0 else str(cell).rjust(widths[i]) for i, cell in enumerate(row)]
        return ' | '.join(cells).rstrip()

    rule = '-+-'.join('-' * w for w in widths)
    return [line(header), rule, *(line(row) for row in rows)]


def _row(name: str, m: Mapping[str, Any]) -> list[str]:
    realized = m.get('realized_in_window_usdt')
    return [name, str(m['orders']), str(m['orders_with_fills']), _n(m['fill_ratio_pct'], 1), str(m['fills']),
            fmt(m['filled_qty']), usd(m['filled_notional_usdt']),
            fmt(m['avg_fill_price'], 8) if m['avg_fill_price'] is not None else 'н/д',
            _p(m['avg_edge_at_placement_vs_index_pct']), _p(m['avg_edge_of_fills_vs_placement_index_pct']),
            f"{_p(m['avg_edge_of_fills_vs_index_at_fill_pct'])} ({m['fills_with_index_at_fill']})",
            *(_p(m[f'markout_{h}s_pct']) for h in MARKOUT_SECONDS),
            _p(m['avg_edge_at_placement_vs_center_pct']), _p(m['avg_edge_of_fills_vs_placement_center_pct']),
            'н/д' if realized is None else signed(realized)]


def _amounts(mapping: Mapping[str, Decimal]) -> str:
    return '; '.join(f'{currency} {fmt(amount)}' for currency, amount in mapping.items()) or 'нет'


def _counts(mapping: Mapping[str, int]) -> str:
    return ', '.join(f'{key}×{n}' for key, n in mapping.items()) or 'нет'


def format_report_ru(report: Mapping[str, Any]) -> str:
    """Human summary (Russian) with a per-slot table."""
    base, quote = report['base'], report['quote']
    if report.get('window_hours') is not None:
        period = (f"за {fmt(report['window_hours'])} ч ({utc_text(report['since_ms'])} — "
                  f"{utc_text(report['until_ms'])})")
    else:
        period = f"за всю кампанию (по {utc_text(report['until_ms'])})"
    lines = [f"Отчёт {report.get('mode') or 'н/д'} '{report.get('campaign_id') or 'н/д'}' "
             f"({report['currency_pair']}) {period}."]
    if report.get('mode') == 'paper':
        lines.append('Ограничения paper: очередь на уровне цены и приоритет/taker-комиссия скрытой части iceberg '
                     'не моделируются (вся заявка считается видимой); исполнение только сделкой строго через цену. '
                     'Доля исполнений и края не сопоставимы с live напрямую.')
        if report.get('iceberg_slots'):
            lines.append(f"Iceberg-слоты в окне: {', '.join(report['iceberg_slots'])} — в paper их скрытая часть "
                         'исполняется как видимая.')
    if report.get('account_review_required'):
        lines.append('ВНИМАНИЕ: обнаружена посторонняя активность на аккаунте, позиция требует сверки. '
                     f"Ниже учтены только исполнения собственных заявок (посторонних исполнений отложено: "
                     f"{report.get('external_fills_excluded', 0)}).")
    header = ['Слот', 'Заявок', 'С исп.', 'Fill,%', 'Исп.', f'Объём {base}', f'Сумма {quote}', 'Ср. цена',
              'Выст./инд.,%', 'Исп./инд. выст.,%', 'Исп./индекс,% (n)',
              *(f'{h} с,%' for h in MARKOUT_SECONDS), 'Выст./центр,%', 'Исп./центр,%', f'Реал. за окно, {quote}']
    rows = [_row(slot, metrics) for slot, metrics in report['slots'].items()]
    rows += [_row(f'Итого {side.upper()}', report['sides'][side]) for side in SIDES]
    rows.append(_row('Всего', report['totals']))
    lines.extend(_table(header, rows))
    lines.append('Края в % (плюс — в нашу пользу): BUY (эталон − цена)/эталон×100, SELL (цена − эталон)/эталон×100. '
                 'Эталон — индекс I, как у отметок и маркаутов: «Выст./инд.» — цена выставления к индексу при '
                 'выставлении; «Исп./инд. выст.» — исполнения к тому же индексу; «Исп./индекс» — к индексу в момент '
                 'исполнения (±1 с, n — исполнений с отметкой); '
                 f"{'/'.join(str(h) for h in MARKOUT_SECONDS)} с — маркауты к индексу через столько секунд. "
                 '«Выст./центр», «Исп./центр» — то же к центру C = I×(1 + basis_offset_pct/100), от которого '
                 'строилась лестница (при ненулевом смещении отличаются от индексных на смещение). '
                 'Fill — доля заявок с исполнением (отклонённые и post-only отказы не учитываются).')
    totals, position = report['totals'], report['position']
    rejected = sum(m['orders_rejected'] for m in report['sides'].values())
    post_only = sum(m['orders_post_only_rejected'] for m in report['sides'].values())
    lines.append(f'Отклонено биржей: {rejected}; post-only отказов: {post_only}.')
    lines.append(f"Реализовано за окно {signed(totals['realized_in_window_usdt'])} {quote}; "
                 f"за всю кампанию {signed(totals['realized_usdt'])} {quote}; "
                 f"средняя продажа − средняя покупка {_p(totals['avg_sell_minus_avg_buy_pct'])}%; "
                 f"оборот {usd(totals['turnover_usdt'])} {quote}.")
    rebates = report.get('order_rebates') or {}
    if rebates.get('orders'):
        slots = '; '.join(f'{slot}: {_amounts(v)}' for slot, v in rebates['by_slot'].items())
        lines.append(f"Rebate по заявкам (rebated_fee, уменьшает затраты; в себестоимость и «Реализовано» выше "
                     f"не входит): за окно {_amounts(rebates['window'])}"
                     + (f' ({slots})' if slots else '')
                     + f"; за кампанию {_amounts(rebates['campaign'])}. Реализовано за кампанию с учётом rebate "
                       f"в {quote}: {signed(rebates['realized_incl_quote_rebates_usdt'])} {quote}.")
    else:
        lines.append('Rebate по заявкам (rebated_fee): нет.')
    if totals.get('window_has_unknown_cost_sales'):
        lines.append('В окне есть продажи из прежнего запаса с неизвестной себестоимостью (initial_avg_price не задан).')
    avg = position['avg_cost']
    lines.append(f"Позиция бота: {fmt(position['qty'])} {base}"
                 + (f" по средней {fmt(avg, 8)} (себестоимость {usd(position['cost_usdt'])} {quote})"
                    if avg is not None else '')
                 + (f"; продано из прежнего запаса {fmt(position['unmatched_sold_qty'])} {base}"
                    if position['unmatched_sold_qty'] else '') + '.')
    fees = '; '.join(f'{currency} {fmt(amount)}' for currency, amount in totals['fees'].items())
    lines.append(f"Комиссии за окно: {fees}; ≈ {usd(totals['fees_usdt_equiv'])} {quote} "
                 '(GT/POINT не пересчитываются; комиссии уже учтены в себестоимости и выручке).')
    index = report['index']
    reasons = ', '.join(f"{reason} — {fmt(item['seconds'], 1)} с ({_n(item['pct_of_observed'], 1)}%)"
                        for reason, item in list(index['blocked_reasons'].items())[:8]) or 'нет'
    lines.append(f"Индекс: наблюдалось {fmt(index['observed_seconds'], 1)} с "
                 f"(покрытие {_n(index['coverage_pct'], 1)}%), заблокирован {_n(index['blocked_index_time_pct'], 2)}% "
                 f"наблюдаемого времени. Причины блокировки: {reasons}.")
    basis = report['basis']
    if basis['count']:
        lines.append(f"Базис (спот mid / индекс − 1), %: среднее {_p(basis['mean_pct'], 4)}, "
                     f"медиана {_p(basis['median_pct'], 4)}, p5 {_p(basis['p5_pct'], 4)}, "
                     f"p95 {_p(basis['p95_pct'], 4)}; наблюдений {basis['count']}.")
    else:
        lines.append('Базис: нет наблюдений со спот-стаканом.')
    rules = '; '.join(f"{slot}: {_counts(m['placement_rules'])}" for slot, m in report['slots'].items()
                      if m['placement_rules'])
    lines.append(f"Правила выставления: {rules or 'нет заявок'}.")
    cancels = '; '.join(f"{slot}: {_counts(m['cancel_reasons'])}" for slot, m in report['slots'].items()
                        if m['cancel_reasons'])
    if cancels:
        lines.append(f'Причины отмен: {cancels}.')
    marks = report['marks']
    lines.append(f"Отметки индекса: из журнала {marks['stored']}, рассчитано по наблюдениям {marks['computed']} "
                 f"(окно ±{marks['bracket_ms']} мс).")
    return '\n'.join(lines)
