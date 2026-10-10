"""Seeded stress: random index moves, fills (with history lag), read errors, lost replies,
STOP toggles and feed outages, driven like the CLI loop. Invariants checked every tick:

* never two active orders per slot (store) and at most one own open order per slot (exchange);
* every own open order on the exchange is an ACTIVE local row (no release while open);
* sum of BUY reservations x fee factor <= USDT total on the exchange (true remaining);
* every recorded fill belongs to an own order;
* no order was sent closer than min_new_edge_pct to the index center;
* a released row is terminal on the exchange with fills == ledger, a 'rejected' row never
  existed on the exchange.
"""
from __future__ import annotations

import random
from collections import Counter
from decimal import ROUND_DOWN, ROUND_UP, Decimal

import pytest

from engine_fakes import QUOTE, make_rig
from gate_stonk.dec import D, ZERO
from gate_stonk.errors import BudgetWait, ReadInterrupted
from gate_stonk.pricing import edge_pct

TICK = D('0.0001')


def set_market(rig, center: Decimal) -> None:
    rig.hub.set_index(center)
    rig.hub.set_spot((center * D('0.995')).quantize(TICK, rounding=ROUND_DOWN),
                     (center * D('1.005')).quantize(TICK, rounding=ROUND_UP))


def check_invariants(rig, placed) -> None:
    venue, store = rig.venue, rig.store
    active = store.orders()
    slots = Counter(row['slot'] for row in active)
    assert max(slots.values(), default=0) <= 1
    open_slots = Counter(order.text.split('-')[2] for order in venue.own_open())
    assert max(open_slots.values(), default=0) <= 1, open_slots
    active_texts = {row['text'] for row in active}
    for order in venue.own_open():
        assert order.text in active_texts, ('released while open', order.text)
    # Reservations against the exchange truth (fills not yet recorded already left the cash).
    factor = 1 + D(rig.cfg.portfolio['fee_reserve_pct']) / 100
    need = ZERO
    for row in active:
        if row['side'] != 'buy':
            continue
        order = venue.by_text(row['text'])
        filled = (order.amount - order.left) if order is not None else row['filled_qty']
        price = max([row['price']] + ([row['amend_price']] if row['amend_state'] and row['amend_price'] else []))
        need += (row['qty'] - max(filled, row['filled_qty'])) * price * factor
    assert need <= venue.total(QUOTE) + D('0.000001'), (need, venue.total(QUOTE))
    for fill in store.fills():
        assert store.order(fill['text']) is not None
        assert venue.orders[fill['order_id']].own
    min_new = D(rig.cfg.safety['min_new_edge_pct'])
    for req, center in placed:
        assert edge_pct(req.side, req.price, center) >= min_new, (req, center)
    for row in store.orders(active_only=False):
        if row['state'] == 'closed':
            order = venue.orders.get(row['order_id'])
            assert order is not None and order.status != 'open'
            assert order.amount - order.left == store.fill_qty(row['text'])
        elif row['state'] == 'rejected':
            assert venue.by_text(row['text']) is None


@pytest.mark.parametrize('seed', [11, 23, 37])
def test_stress_invariants(tmp_path, clock, seed):
    rng = random.Random(seed)
    overrides = {'pair.buy.band_tolerance_pct': 0.5, 'pair.sell.band_tolerance_pct': 0.5,
                 'pair.buy.max_daily_notional_usdt': None, 'pair.max_inventory_usdt': 400}
    rig = make_rig(tmp_path, clock, usdt='300', base='1500', overrides=overrides)
    center = D('0.1359')
    set_market(rig, center)
    placed: list = []
    rig.venue.on_place = lambda req: placed.append((req, rig.hub.center))
    states: Counter = Counter()
    next_step = clock[0]
    stop_on = False
    outage = 0
    for _ in range(600):
        rig.advance(rng.choice((0.2, 0.2, 0.4, 1.0, 2.5, 5.0)))
        roll = rng.random()
        if roll < 0.25:
            center = (center * (1 + D(str(round(rng.gauss(0, 0.002), 6))))).quantize(D('0.000001'))
            center = min(max(center, D('0.1200')), D('0.1500'))
            set_market(rig, center)
        if rng.random() < 0.2 and rig.venue.own_open():
            order = rng.choice(rig.venue.own_open())
            qty = (order.left * D(str(rng.choice((0.1, 0.3, 0.5, 1.0))))).quantize(D('0.01'), rounding=ROUND_DOWN)
            if qty > 0:
                rig.venue.fill(order.order_id, qty, visible_delay_ms=rng.choice((0, 0, 0, 1500, 4000)),
                               in_window=rng.random() > 0.1)
        if rng.random() < 0.02:
            rig.venue.read_errors[rng.choice(('open_orders', 'my_trades', 'balances', 'get_order'))] = 1
        if rng.random() < 0.012:
            rig.venue.batch_plan.append(rng.choice(('lost_before', 'lost_after', 'not_sent')))
        if rng.random() < 0.02:
            rig.venue.place_plan.append(rng.choice(('lost_before', 'lost_after', 'poc', 'reject:POC_FILL_IMMEDIATELY')))
        if rng.random() < 0.02:
            rig.venue.cancel_plan.append(rng.choice(('lost_before', 'lost_after')))
        if rng.random() < 0.008:
            stop_on = not stop_on
            rig.stop_file('STOP_TRADING', present=stop_on)
        if outage:
            outage -= 1
            if not outage:
                rig.hub.errors.clear()
        elif rng.random() < 0.01:
            rig.hub.errors['bybit_perp'] = 'DISCONNECTED'
            outage = rng.randint(2, 8)
        try:
            rig.engine.preflight()
        except (BudgetWait, ReadInterrupted):
            pass
        rig.engine.sample_index()
        deadline = rig.engine.next_reconciliation_deadline()
        if deadline is not None:
            next_step = min(next_step, deadline)
        if clock[0] >= next_step:
            states[rig.engine.step()] += 1
            next_step = clock[0] + rig.engine.next_step_seconds()
        check_invariants(rig, placed)
    rig.status_json()
    assert placed, 'the stress run never quoted'
    assert states['RUNNING'] > 0
    assert rig.engine.counters['fills'] > 0
