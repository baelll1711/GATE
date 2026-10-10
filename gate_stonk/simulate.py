"""Offline simulation on a SYNTHETIC market (``python -m gate_stonk simulate``).

THE MARKET IS SYNTHETIC, NOT REAL DATA (human output: «синтетический рынок, не реальные
данные»). The container that builds this project has no network access to the exchanges,
so the stage-3 "one hour of paper trading" cannot use live quotes there. This module drives
the REAL trading stack through a generated market on a VIRTUAL clock:

* the real ``cli.Runner`` loop (tick ``min(0.2, loop_seconds)``, ``engine.preflight()`` and
  ``engine.sample_index()`` every tick, ``engine.step()`` when due, ``status.json`` every
  ``app.status_interval_seconds``, shutdown cancels) with the real ``Engine``,
  ``PaperVenue`` (trade_through fills), ``FeedHub`` (no threads, data injected with
  ``hub.publish``), ``IndexBuilder``, ``Store``, ``Notifier`` (Telegram off) and report;
* ``time.time`` / ``time.monotonic`` / ``time.time_ns`` are replaced by a virtual clock
  for the duration of the run (restored afterwards); the loop's ``hub.wake.wait(tick)``
  advances the clock and the synthetic market by one tick, so an hour runs in minutes
  and every run with the same seed is identical;
* outputs go to a dedicated directory (default ``<state_dir>/simulation_<seed>/``):
  ``paper.sqlite3`` of the simulation (never the trading database), ``status.json``,
  ``report.json`` and ``logs/``. The directory is marked with ``SIMULATION_DIR``; a
  directory without that marker (or the trading state directory itself) is refused.

Synthetic market (deterministic per seed; floats are used only inside the random
processes, everything published is ``Decimal`` on the venue grids):

* fair price: geometric random walk from 0.1359 (``volatility`` % per sqrt(hour), RMS over
  calm and active regimes) plus Poisson jumps of 1-3 %; on a jump the Gate spot book lags
  the fair price by a short random half-life and taker prints walk the book from the old
  to the new touch;
* index sources: per enabled config source an L1 book ``fair(t - lag) * (1 + premium)``
  (premium = slow random drift around a per-venue mean) with venue-like spreads (Bybit
  0.15 %, Lighter 0.59 %, Gate futures 50 % so it drops out, Bitget/BingX 0.2-0.4 %),
  tick rounding, sticky quotes and their own cadence (Bybit publishes on change and
  repeats an unchanged L1 every 3 s with the same update id), occasional dropouts of the
  optional sources (disconnect or silent/stale) and rare 1-2 s Bybit disconnects;
* Gate spot: ``fair * (1 + basis)`` with the basis drifting around -0.1 %, 20 levels per
  side with persistent gaps/sizes; public tape with Poisson taker trades (lognormal
  sizes, printed at the touch levels they consume) and occasional sweeps through 1-3 %
  of the book (prints at every consumed level), after which the book recovers;
* every request to the paper exchange takes a REST latency (default 20-80 ms) during
  which the market keeps moving (fills can race cancels, books can move under a create).

Invariants (counted in the summary, never enforced on the engine): every tick one
active order per slot in the store and on the paper exchange, a monotone ladder without
self-cross among working orders, and the money on the exchange (BUY reservations with
their true remaining x fee factor <= USDT total, SELL reservations <= base total); after
every ``engine.preflight`` no working order inside ``safety.hard_min_edge_pct`` of the
index the guard used; after the run every order's creation price at least
``safety.min_new_edge_pct`` from its placement centre and monotone within its creation pass.
"""
from __future__ import annotations

import argparse
import copy
import heapq
import json
import logging
import math
import random
import re
import sqlite3
import time
from collections import Counter, deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any, Callable, Iterator

from .cli import (EXIT_ERROR, EXIT_FAIL, EXIT_OK, Deps, LockBusy, ProcessLocks, Runner, configure_logging, db_path,
                  remove_logging, status_path)
from .config import Config, Secrets
from .dec import HUNDRED, ZERO, D, fmt
from .feeds.base import FX_KEY, GATE_SPOT_KEY, Update
from .feeds.hub import FeedHub
from .index import IndexBuilder
from .notify import Notifier
from .pricing import edge_pct
from .store import ACTIVE_STATES
from .types import SIDES, Book, PairMeta
from .venue import parse_pair_meta, parse_text
from .venue_paper import PaperVenue

LOG = logging.getLogger(__name__)

SYNTHETIC_RU = 'синтетический рынок, не реальные данные'
SYNTHETIC_BANNER = f'СИМУЛЯЦИЯ — {SYNTHETIC_RU}'
SAY_PREFIX = '[СИМУЛЯЦИЯ] '
MARKER_NAME = 'SIMULATION_DIR'
REPORT_NAME = 'report.json'
VIRTUAL_START_MS = 1_791_795_600_000          # 2026-10-12 09:00:00 UTC (virtual clock origin)
VIRTUAL_MONOTONIC_START = 100_000.0
MAX_MINUTES = 24 * 60
MAX_SEED = 2 ** 31 - 1
DEFAULT_VOLATILITY_PCT = 1.5
PROGRESS_EVERY_SECONDS = 600.0
EVENT_LOG_LIMIT = 400
EXAMPLES_PER_CODE = 5
FAIR_HISTORY_SECONDS = 10.0


class SimulationRefused(Exception):
    """The simulation cannot run with these arguments (Russian message)."""


# ============================================================================ virtual clock

class VirtualClock:
    """Integer-microsecond virtual time; ``installed()`` patches the ``time`` module."""

    PATCHED = ('time', 'monotonic', 'time_ns', 'monotonic_ns')

    def __init__(self, start_wall_ms: int = VIRTUAL_START_MS, start_monotonic: float = VIRTUAL_MONOTONIC_START):
        self._wall0_us = int(start_wall_ms) * 1000
        self._mono0_us = int(round(start_monotonic * 1_000_000))
        self.us = 0

    @property
    def elapsed(self) -> float:
        return self.us / 1_000_000

    def time(self) -> float:
        return (self._wall0_us + self.us) / 1_000_000

    def monotonic(self) -> float:
        return (self._mono0_us + self.us) / 1_000_000

    def time_ns(self) -> int:
        return (self._wall0_us + self.us) * 1000

    def monotonic_ns(self) -> int:
        return (self._mono0_us + self.us) * 1000

    def wall_ms(self) -> int:
        return (self._wall0_us + self.us) // 1000

    def set_us(self, us: int) -> None:
        if us < self.us:
            raise ValueError('virtual clock cannot go backwards')
        self.us = int(us)

    @contextmanager
    def installed(self) -> Iterator['VirtualClock']:
        saved = {name: getattr(time, name) for name in self.PATCHED}
        for name in self.PATCHED:
            setattr(time, name, getattr(self, name))
        try:
            yield self
        finally:
            for name, function in saved.items():
                setattr(time, name, function)


# ============================================================================ market model

@dataclass(frozen=True)
class SourceModel:
    """Behaviour of one synthetic index source (percent values are percents)."""

    spread_pct: float
    interval: tuple[float, float]              # seconds between frames (uniform)
    tick: str = '0.00001'
    on_change_only: bool = False               # publish only when the L1 changed (+ heartbeat)
    heartbeat: float = 0.0                     # unchanged L1 re-sent after this many seconds
    lag: float = 0.2                           # quotes follow the fair price this much later
    latency_ms: tuple[int, int] = (20, 80)     # event_ms = receipt - latency
    event_is_receipt: bool = False             # Lighter: event time = receipt time
    premium_pct: float = 0.0                   # mean premium vs the fair price
    premium_sd_pct: float = 0.03               # stationary sd of the premium drift
    requote_pct: float = 0.015                 # sticky quotes: re-centre beyond this move
    update_ids: bool = True
    dropouts_per_hour: float = 0.0
    dropout_seconds: tuple[float, float] = (3.0, 15.0)
    disconnect_share: float = 0.5              # share of dropouts that are disconnects (else silent)


SOURCE_MODELS: dict[str, SourceModel] = {
    'bybit_linear': SourceModel(spread_pct=0.15, interval=(0.1, 0.1), on_change_only=True, heartbeat=3.0, lag=0.05,
                                latency_ms=(15, 60), premium_sd_pct=0.02, requote_pct=0.012,
                                dropouts_per_hour=2.0, dropout_seconds=(1.0, 2.0), disconnect_share=1.0),
    'bitget_usdt_futures': SourceModel(spread_pct=0.30, interval=(0.1, 0.5), lag=0.2, latency_ms=(30, 90),
                                       premium_pct=0.02, dropouts_per_hour=1.5, dropout_seconds=(3.0, 20.0)),
    'bingx_swap': SourceModel(spread_pct=0.25, interval=(0.5, 0.5), lag=0.3, latency_ms=(40, 150),
                              premium_pct=-0.03, dropouts_per_hour=1.5, dropout_seconds=(3.0, 20.0)),
    'gate_futures_usdt': SourceModel(spread_pct=50.0, interval=(0.2, 1.0), lag=0.2, latency_ms=(20, 80),
                                     dropouts_per_hour=1.0),
    'lighter': SourceModel(spread_pct=0.59, interval=(0.2, 1.0), tick='0.000001', lag=0.4, event_is_receipt=True,
                           premium_pct=0.04, update_ids=False, dropouts_per_hour=3.0, dropout_seconds=(5.0, 40.0)),
}
GENERIC_SOURCE = SourceModel(spread_pct=0.30, interval=(0.2, 0.6), lag=0.3, dropouts_per_hour=1.0)


@dataclass(frozen=True)
class MarketParams:
    """Synthetic market knobs (model parameters, not money)."""

    start_price: str = '0.1359'
    volatility_pct_per_hour: float = DEFAULT_VOLATILITY_PCT    # RMS over calm and active regimes
    fair_step_seconds: float = 0.1
    calm_share: float = 0.3                     # share of time in calm regimes (quiet books, heartbeats)
    calm_volatility_factor: float = 0.15
    calm_seconds: float = 60.0                  # mean length of a calm regime
    jumps_per_hour: float = 3.0
    jump_pct: tuple[float, float] = (1.0, 3.0)
    jump_spot_half_life: tuple[float, float] = (0.05, 0.5)
    spot_basis_mean_pct: float = -0.10
    spot_basis_sd_pct: float = 0.05
    spot_basis_reversion_seconds: float = 300.0
    spot_spread_pct: tuple[float, float, float] = (0.06, 0.12, 0.30)   # min, mean, max
    spot_levels: int = 20
    spot_level_gap_pct: float = 0.12
    spot_level_size: float = 900.0
    spot_requote_pct: float = 0.02
    spot_publish_seconds: float = 0.1
    spot_heartbeat_seconds: float = 1.0
    trades_per_second: float = 0.3
    trade_size_median: float = 250.0
    trade_size_sigma: float = 1.2
    sweeps_per_hour: float = 6.0
    sweep_pct: tuple[float, float] = (1.0, 3.0)
    source_dropouts: bool = True
    rest_latency_ms: tuple[int, int] = (20, 80)                # one-way REST latency of every venue request
    forced_sweeps: tuple[tuple[float, str, float], ...] = ()   # (at seconds, taker side, depth %)
    forced_jumps: tuple[tuple[float, float], ...] = ()         # (at seconds, signed jump %)


@dataclass
class _Source:
    key: str
    venue: str
    model: SourceModel
    tick: Decimal
    premium: float
    mid: float | None = None
    spread: float = 0.0
    l1: tuple | None = None
    sizes: tuple[Decimal, Decimal] = (Decimal(500), Decimal(500))
    update_id: int = 0
    event_ms: int = 0
    last_publish_us: int | None = None
    down_until_us: int | None = None
    down_kind: str | None = None


def _rate_delay(rng: random.Random, per_hour: float) -> float | None:
    if per_hour <= 0:
        return None
    return rng.expovariate(per_hour / 3600.0)


def _price(value: float, tick: Decimal, rounding: str) -> Decimal:
    return Decimal(repr(float(value))).quantize(tick, rounding=rounding)


class SyntheticMarket:
    """Deterministic event-driven generator publishing into a ``FeedHub`` (see module doc)."""

    def __init__(self, cfg: Config, hub: FeedHub, clock: VirtualClock, meta: PairMeta, params: MarketParams,
                 seed: int):
        self.cfg, self.hub, self.clock, self.meta, self.params, self.seed = cfg, hub, clock, meta, params, seed
        self.rng = {name: random.Random(f'gate_stonk-sim:{seed}:{name}')
                    for name in ('fair', 'spot', 'trades', 'sweeps', 'jumps', 'sources', 'dropouts', 'fx',
                                 'latency', 'regime')}
        self.fair = float(D(params.start_price))
        self.calm = False
        self._regime_until_us = 0
        self._vol_factor = 1.0
        self.basis = params.spot_basis_mean_pct / 100
        self.spot_spread = params.spot_spread_pct[1] / 100
        self.spot_tick = meta.tick
        self.lot = meta.lot
        self._fair_history: deque[tuple[int, float]] = deque()
        self._shocks: list[tuple[int, str, float, float]] = []    # (t0_us, kind, amount, half_life_s)
        self._aggressor: tuple[str, int] | None = None              # (taker side, until_us)
        self._heap: list = []
        self._seq = 0
        self._trade_id = 50_000_000
        self.stats: Counter = Counter()
        self.events: list[dict] = []
        self.sources: dict[str, _Source] = {}
        for source in cfg.sources():
            model = SOURCE_MODELS.get(source.venue, GENERIC_SOURCE)
            self.sources[source.id] = _Source(source.id, source.venue, model, Decimal(model.tick),
                                              premium=model.premium_pct / 100)
        self.fx_enabled = FX_KEY in hub.specs
        self._fx_update_id = 0
        self._spot_struct = {side: self._new_structure(side) for side in ('bids', 'asks')}
        self._spot_mid: float | None = None
        self.spot_book: Book | None = None
        self._spot_update_id = 0
        self._spot_event_ms = 0
        self._spot_last_publish_us: int | None = None

    # ------------------------------------------------------------------ scheduling
    def _at(self, t_us: int, handler: Callable[..., None], *args: Any) -> None:
        heapq.heappush(self._heap, (max(int(t_us), self.clock.us), self._seq, handler, args))
        self._seq += 1

    def _after(self, seconds: float, handler: Callable[..., None], *args: Any) -> None:
        self._at(self.clock.us + max(1, int(round(seconds * 1_000_000))), handler, *args)

    def _note(self, kind: str, **fields: Any) -> None:
        self.stats[kind] += 1
        if len(self.events) < EVENT_LOG_LIMIT:
            self.events.append({'t_s': round(self.clock.elapsed, 3), 'kind': kind, **fields})

    def start(self) -> None:
        """Publish the first snapshots (feeds connected at t=0) and schedule everything."""
        p = self.params
        self._fair_history.append((self.clock.us, self.fair))
        for key, source in self.sources.items():
            self.hub.set_connected(key, True)
            self._source_frame(source, force=True)
            self._after(self.rng['sources'].uniform(*source.model.interval), self._on_source, key)
            if p.source_dropouts:
                delay = _rate_delay(self.rng['dropouts'], source.model.dropouts_per_hour)
                if delay is not None:
                    self._after(delay, self._on_dropout, key)
        self.hub.set_connected(GATE_SPOT_KEY, True)
        self._spot_update(force=True)
        self._after(p.spot_publish_seconds, self._on_spot)
        if self.fx_enabled:
            self.hub.set_connected(FX_KEY, True)
            self._on_fx()
        self._after(p.fair_step_seconds, self._on_fair)
        if p.trades_per_second > 0:
            self._after(self.rng['trades'].expovariate(p.trades_per_second), self._on_trade)
        for name, rate, handler in (('sweeps', p.sweeps_per_hour, self._on_sweep),
                                    ('jumps', p.jumps_per_hour, self._on_jump)):
            delay = _rate_delay(self.rng[name], rate)
            if delay is not None:
                self._after(delay, handler)
        for at, side, depth in p.forced_sweeps:
            self._at(int(at * 1_000_000), self._sweep, side, float(depth) / 100, True)
        for at, pct in p.forced_jumps:
            self._at(int(at * 1_000_000), self._jump, float(pct) / 100, True)

    def request_latency(self) -> None:
        """One REST request travelling to the exchange: the market keeps moving meanwhile."""
        low, high = self.params.rest_latency_ms
        if high <= 0:
            return
        delay_ms = self.rng['latency'].randint(max(0, int(low)), int(high))
        self.stats['rest_requests'] += 1
        if delay_ms:
            self.advance(delay_ms / 1000)

    def advance(self, seconds: float) -> None:
        """Process every market event up to ``now + seconds`` (the clock follows the events)."""
        target = self.clock.us + int(round(seconds * 1_000_000))
        while self._heap and self._heap[0][0] <= target:
            t_us, _seq, handler, args = heapq.heappop(self._heap)
            self.clock.set_us(t_us)
            handler(*args)
        self.clock.set_us(target)

    # ------------------------------------------------------------------ publishing
    def _publish(self, update: Update) -> None:
        try:
            self.hub.publish(update)
            self.stats[f'published:{update.key}:{update.kind}'] += 1
        except Exception as exc:        # a generator bug must be visible, never fatal
            self.stats['publish_rejected'] += 1
            self._note('publish_rejected', key=update.key, error=str(exc) or type(exc).__name__)

    def _shock(self, kind: str) -> float:
        now = self.clock.us
        total = 0.0
        keep = []
        for t0, name, amount, half_life in self._shocks:
            value = amount * 0.5 ** ((now - t0) / 1_000_000 / half_life)
            if abs(value) > 1e-7:
                keep.append((t0, name, amount, half_life))
            if name == kind:
                total += value
        self._shocks = keep
        return total

    def _add_shock(self, kind: str, amount: float, half_life: float) -> None:
        self._shocks.append((self.clock.us, kind, amount, max(0.01, half_life)))

    def _fair_at(self, lag: float) -> float:
        cutoff = self.clock.us - int(lag * 1_000_000)
        value = self._fair_history[0][1]
        for t_us, fair in reversed(self._fair_history):
            if t_us <= cutoff:
                return fair
            value = fair
        return value

    # ------------------------------------------------------------------ fair price
    def _switch_regime(self) -> None:
        """Volatility clustering: calm regimes (quiet books, Bybit heartbeat repeats) alternate
        with active ones; the configured volatility is the RMS over both."""
        p = self.params
        rng = self.rng['regime']
        share = min(0.95, max(0.0, p.calm_share))
        self.calm = not self.calm if self._regime_until_us else rng.random() < share
        if share <= 0:
            self.calm = False
        factor = p.calm_volatility_factor
        active = math.sqrt(max(0.0, 1 - share * factor * factor) / (1 - share))
        self._vol_factor = factor if self.calm else active
        mean = p.calm_seconds if self.calm else p.calm_seconds * (1 - share) / max(share, 1e-9)
        self._regime_until_us = self.clock.us + max(1, int(rng.expovariate(1 / max(mean, 1e-3)) * 1_000_000))
        self.stats['calm_regimes' if self.calm else 'active_regimes'] += 1

    def _on_fair(self) -> None:
        p = self.params
        rng = self.rng['fair']
        dt = p.fair_step_seconds
        if self.clock.us >= self._regime_until_us:
            self._switch_regime()
        sigma = p.volatility_pct_per_hour / 100 * self._vol_factor * math.sqrt(dt / 3600)
        self.fair *= math.exp(rng.gauss(0.0, sigma) - sigma * sigma / 2)
        tau = p.spot_basis_reversion_seconds
        mean, sd = p.spot_basis_mean_pct / 100, p.spot_basis_sd_pct / 100
        self.basis += -(self.basis - mean) * dt / tau + sd * math.sqrt(2 * dt / tau) * rng.gauss(0.0, 1.0)
        low, mid, high = (x / 100 for x in p.spot_spread_pct)
        self.spot_spread += -(self.spot_spread - mid) * dt / 60 + (mid / 3) * math.sqrt(2 * dt / 60) * rng.gauss(0, 1)
        self.spot_spread = min(high, max(low, self.spot_spread))
        for source in self.sources.values():
            m = source.model
            target, sd_p = m.premium_pct / 100, m.premium_sd_pct / 100
            source.premium += -(source.premium - target) * dt / 120 + sd_p * math.sqrt(2 * dt / 120) * rng.gauss(0, 1)
        self._fair_history.append((self.clock.us, self.fair))
        horizon = self.clock.us - int(FAIR_HISTORY_SECONDS * 1_000_000)
        while len(self._fair_history) > 2 and self._fair_history[1][0] <= horizon:
            self._fair_history.popleft()
        self._after(dt, self._on_fair)

    def _on_jump(self) -> None:
        rng = self.rng['jumps']
        size = rng.uniform(*self.params.jump_pct) / 100
        self._jump(size if rng.random() < 0.5 else -size, False)
        delay = _rate_delay(rng, self.params.jumps_per_hour)
        if delay is not None:
            self._after(delay, self._on_jump)

    def _jump(self, fraction: float, forced: bool) -> None:
        """All venues move by ``fraction``; the spot book catches up with a short half-life
        while takers print through the levels in between."""
        half_life = self.rng['jumps'].uniform(*self.params.jump_spot_half_life)
        before = self.fair
        self.fair *= 1 + fraction
        self._fair_history.append((self.clock.us, self.fair))
        self._add_shock('mid', 1 / (1 + fraction) - 1, half_life)
        self._aggressor = ('sell' if fraction < 0 else 'buy', self.clock.us + int((6 * half_life + 1.0) * 1_000_000))
        self._note('jump', pct=round(fraction * 100, 4), forced=forced, fair_before=round(before, 8),
                   fair_after=round(self.fair, 8), spot_half_life_s=round(half_life, 3))

    # ------------------------------------------------------------------ index sources
    def _on_source(self, key: str) -> None:
        source = self.sources[key]
        now = self.clock.us
        if source.down_until_us is not None:
            if now < source.down_until_us:
                self._at(source.down_until_us, self._on_source, key)
                return
            source.down_until_us = None
            source.down_kind = None
            self.hub.set_connected(key, True)
            self._note('source_restored', key=key)
            self._source_frame(source, force=True)
        else:
            self._source_frame(source, force=False)
        self._after(self.rng['sources'].uniform(*source.model.interval), self._on_source, key)

    def _source_frame(self, source: _Source, *, force: bool) -> None:
        m = source.model
        rng = self.rng['sources']
        target = self._fair_at(m.lag) * (1 + source.premium)
        if source.mid is None or force or abs(target / source.mid - 1) * 100 > m.requote_pct or rng.random() < 0.005:
            source.mid = target                       # sticky quotes: re-centred only on a real move
            source.spread = m.spread_pct / 100 * (1 + rng.uniform(-0.15, 0.15))
        bid = _price(source.mid * (1 - source.spread / 2), source.tick, ROUND_FLOOR)
        ask = _price(source.mid * (1 + source.spread / 2), source.tick, ROUND_CEILING)
        if ask <= bid:
            ask = bid + source.tick
        if bid <= 0:
            return
        if rng.random() < 0.02:
            source.sizes = (Decimal(int(rng.lognormvariate(math.log(800), 0.8)) + 1),
                            Decimal(int(rng.lognormvariate(math.log(800), 0.8)) + 1))
        l1 = (bid, ask, source.sizes)
        now = self.clock.us
        changed = l1 != source.l1
        if (m.on_change_only and not changed and not force and source.last_publish_us is not None
                and now - source.last_publish_us < int(m.heartbeat * 1_000_000)):
            return
        wall_ms = self.clock.wall_ms()
        event_ms = wall_ms if m.event_is_receipt else wall_ms - rng.randint(*m.latency_ms)
        source.event_ms = max(source.event_ms, event_ms)
        if changed and m.update_ids:
            source.update_id += 1
        book = Book(bids=((bid, source.sizes[0]),), asks=((ask, source.sizes[1]),), received=self.clock.monotonic(),
                    event_ms=source.event_ms, update_id=source.update_id if m.update_ids else None)
        source.l1 = l1
        source.last_publish_us = now
        if not changed:
            self.stats[f'heartbeat_repeat:{source.key}'] += 1
        self._publish(Update(kind='book', key=source.key, book=book))

    def _on_dropout(self, key: str) -> None:
        source = self.sources[key]
        rng = self.rng['dropouts']
        m = source.model
        seconds = rng.uniform(*m.dropout_seconds)
        disconnect = rng.random() < m.disconnect_share
        source.down_until_us = self.clock.us + int(seconds * 1_000_000)
        source.down_kind = 'disconnect' if disconnect else 'silent'
        if disconnect:
            self.hub.invalidate(key, 'DISCONNECTED')
            self.hub.set_connected(key, False)
        self._note('source_dropout', key=key, mode=source.down_kind, seconds=round(seconds, 3))
        delay = _rate_delay(rng, m.dropouts_per_hour)
        if delay is not None:
            self._at(source.down_until_us + int(delay * 1_000_000), self._on_dropout, key)

    def _on_fx(self) -> None:
        rng = self.rng['fx']
        mid = 1 + rng.gauss(0.0, 0.0001)
        tick = Decimal('0.0001')
        bid = _price(mid - 0.00005, tick, ROUND_FLOOR)
        ask = max(_price(mid + 0.00005, tick, ROUND_CEILING), bid + tick)
        self._fx_update_id += 1
        book = Book(bids=((bid, Decimal(100000)),), asks=((ask, Decimal(100000)),), received=self.clock.monotonic(),
                    event_ms=self.clock.wall_ms(), update_id=self._fx_update_id)
        self._publish(Update(kind='book', key=FX_KEY, book=book))
        self._after(rng.uniform(0.1, 0.5), self._on_fx)

    # ------------------------------------------------------------------ Gate spot book
    def _new_structure(self, side: str) -> list[list[float]]:
        rng = self.rng['spot']
        p = self.params
        rows = []
        for i in range(max(1, int(p.spot_levels))):
            gap = 0.0 if i == 0 else rng.lognormvariate(math.log(p.spot_level_gap_pct / 100), 0.5)
            size = rng.lognormvariate(math.log(p.spot_level_size), 0.9) * (1 + i * 0.08)
            rows.append([gap, size])
        return rows

    def _qty(self, value: float) -> Decimal:
        qty = Decimal(repr(float(value))).quantize(self.lot, rounding=ROUND_FLOOR)
        return qty if qty >= self.lot else self.lot

    def _levels(self, side: str, best: Decimal) -> tuple[tuple[Decimal, Decimal], ...]:
        rows = self._spot_struct[side]
        out: list[tuple[Decimal, Decimal]] = []
        price_f = float(best)
        previous: Decimal | None = None
        for i, (gap, size) in enumerate(rows):
            if side == 'bids':
                price_f *= 1 - gap
                price = _price(price_f, self.spot_tick, ROUND_FLOOR) if i else best
                if previous is not None and price >= previous:
                    price = previous - self.spot_tick
                if price <= 0:
                    break
            else:
                price_f *= 1 + gap
                price = _price(price_f, self.spot_tick, ROUND_CEILING) if i else best
                if previous is not None and price <= previous:
                    price = previous + self.spot_tick
            out.append((price, self._qty(size)))
            previous = price
        return tuple(out)

    def _on_spot(self) -> None:
        self._spot_update(force=False)
        self._after(self.params.spot_publish_seconds, self._on_spot)

    def _spot_update(self, *, force: bool) -> None:
        p = self.params
        rng = self.rng['spot']
        target = self.fair * (1 + self.basis) * (1 + self._shock('mid'))
        aggressive = self._aggressor is not None and self.clock.us < self._aggressor[1]
        if not aggressive:
            self._aggressor = None
        if (self._spot_mid is None or force or aggressive
                or abs(target / self._spot_mid - 1) * 100 > p.spot_requote_pct or rng.random() < 0.02):
            self._spot_mid = target
        for side in ('bids', 'asks'):
            for row in self._spot_struct[side]:
                if rng.random() < 0.01:
                    row[1] = rng.lognormvariate(math.log(p.spot_level_size), 0.9)
        half = self.spot_spread / 2
        bid = _price(self._spot_mid * (1 - half - max(0.0, self._shock('bid'))), self.spot_tick, ROUND_FLOOR)
        ask = _price(self._spot_mid * (1 + half + max(0.0, self._shock('ask'))), self.spot_tick, ROUND_CEILING)
        if ask <= bid:
            ask = bid + self.spot_tick
        book_bids, book_asks = self._levels('bids', bid), self._levels('asks', ask)
        if not book_bids or not book_asks:
            return
        previous = self.spot_book
        if aggressive and previous is not None:
            taker = self._aggressor[0]
            moved = book_bids[0][0] < previous.bid if taker == 'sell' else book_asks[0][0] > previous.ask
            if moved:
                limit = book_bids[0][0] if taker == 'sell' else book_asks[0][0]
                prints = self._walk(taker, previous, limit, inclusive=False)
                if prints:
                    self._print(taker, prints, 'jump_print')
        now = self.clock.us
        changed = (previous is None or previous.bids != book_bids or previous.asks != book_asks)
        due = (self._spot_last_publish_us is None
               or now - self._spot_last_publish_us >= int(p.spot_heartbeat_seconds * 1_000_000))
        if not (changed or due or force):
            return
        if changed:
            self._spot_update_id += 1
        self._spot_event_ms = max(self._spot_event_ms, self.clock.wall_ms() - rng.randint(5, 30))
        book = Book(bids=book_bids, asks=book_asks, received=self.clock.monotonic(), event_ms=self._spot_event_ms,
                    update_id=self._spot_update_id)
        self.spot_book = book
        self._spot_last_publish_us = now
        self._publish(Update(kind='book', key=GATE_SPOT_KEY, book=book))

    # ------------------------------------------------------------------ public tape
    def _next_trade_id(self) -> str:
        self._trade_id += 1
        return str(self._trade_id)

    def _print(self, taker: str, prints: list[tuple[Decimal, Decimal]], kind: str) -> None:
        wall_us = self.clock.time_ns() // 1000
        stamp = f'{wall_us // 1000}.{wall_us % 1000:03d}'
        rows = tuple({'id': self._next_trade_id(), 'price': price, 'amount': amount, 'side': taker,
                      'create_time_ms': stamp} for price, amount in prints)
        self.stats['prints'] += len(rows)
        self.stats[f'prints:{kind}'] += len(rows)
        self._publish(Update(kind='trades', key=GATE_SPOT_KEY, trades=rows))

    def _walk(self, taker: str, book: Book, limit: Decimal, *, inclusive: bool = True) -> list[tuple[Decimal, Decimal]]:
        """Prints of a taker consuming ``book`` from the touch to ``limit`` (beyond the
        visible levels: synthetic hidden liquidity every ``spot_level_gap_pct``)."""
        rng = self.rng['sweeps']
        levels = book.bids if taker == 'sell' else book.asks

        def beyond(price: Decimal) -> bool:
            if taker == 'sell':
                return price < limit if inclusive else price <= limit
            return price > limit if inclusive else price >= limit

        prints: list[tuple[Decimal, Decimal]] = []
        last: Decimal | None = None
        for price, size in levels:
            if beyond(price):
                return prints
            prints.append((price, size))
            last = price
        gap = self.params.spot_level_gap_pct / 100
        price_f = float(last if last is not None else (book.bid if taker == 'sell' else book.ask))
        for _ in range(200):
            price_f *= (1 - gap) if taker == 'sell' else (1 + gap)
            price = _price(price_f, self.spot_tick, ROUND_FLOOR if taker == 'sell' else ROUND_CEILING)
            if price <= 0 or beyond(price):
                break
            prints.append((price, self._qty(rng.lognormvariate(math.log(self.params.spot_level_size), 0.9))))
        return prints

    def _on_trade(self) -> None:
        p = self.params
        rng = self.rng['trades']
        book = self.spot_book
        if book is not None:
            taker = 'buy' if rng.random() < 0.5 else 'sell'
            remaining = Decimal(repr(rng.lognormvariate(math.log(p.trade_size_median), p.trade_size_sigma)))
            remaining = remaining.quantize(self.lot, rounding=ROUND_FLOOR)
            prints = []
            for price, size in (book.asks if taker == 'buy' else book.bids)[:5]:
                if remaining < self.lot:
                    break
                take = min(size, remaining)
                prints.append((price, take))
                remaining -= take
            if prints:
                self._print(taker, prints, 'trade')
                self.stats['taker_trades'] += 1
        self._after(rng.expovariate(p.trades_per_second), self._on_trade)

    def _on_sweep(self) -> None:
        rng = self.rng['sweeps']
        taker = 'buy' if rng.random() < 0.5 else 'sell'
        self._sweep(taker, rng.uniform(*self.params.sweep_pct) / 100, False)
        delay = _rate_delay(rng, self.params.sweeps_per_hour)
        if delay is not None:
            self._after(delay, self._on_sweep)

    def _sweep(self, taker: str, depth: float, forced: bool) -> None:
        """A large taker order on Gate spot only (the futures index does not move)."""
        book = self.spot_book
        if book is None or taker not in SIDES:
            return
        mid = float(book.mid)
        if taker == 'sell':
            limit = _price(mid * (1 - depth), self.spot_tick, ROUND_CEILING)
        else:
            limit = _price(mid * (1 + depth), self.spot_tick, ROUND_FLOOR)
        prints = self._walk(taker, book, limit)
        if not prints:
            return
        self._print(taker, prints, 'sweep')
        self._add_shock('bid' if taker == 'sell' else 'ask', depth * 0.6, 4.0)
        self._add_shock('mid', (-depth if taker == 'sell' else depth) * 0.25, 20.0)
        self._note('sweep', taker=taker, depth_pct=round(depth * 100, 4), forced=forced, prints=len(prints),
                   from_price=str(prints[0][0]), to_price=str(prints[-1][0]),
                   qty=str(sum((amount for _price_, amount in prints), ZERO)))
        self._spot_update(force=True)


# ============================================================================ simulation

def synthetic_pair_meta(cfg: Config) -> PairMeta:
    """Gate-like metadata for the pair (SYNTHETIC: pinned precisions from the config if set)."""
    market = cfg.market
    precision = market.get('expected_precision')
    amount_precision = market.get('expected_amount_precision')
    return parse_pair_meta({
        'id': market['currency_pair'], 'base': market['base'], 'quote': market['quote'], 'fee': '0.2',
        'min_base_amount': '1', 'min_quote_amount': '3', 'max_base_amount': None, 'max_quote_amount': '1000000',
        'precision': 5 if precision is None else int(precision),
        'amount_precision': 1 if amount_precision is None else int(amount_precision),
        'trade_status': 'tradable', 'synthetic': True,
    })


def default_simulation_dir(cfg: Config, seed: int) -> Path:
    return Path(cfg.state_dir) / f'simulation_{seed}'


def check_simulation_dir(cfg: Config, out_dir: Path) -> None:
    """Never write into the trading state directory or a foreign directory."""
    out_dir = Path(out_dir).resolve()
    trading = Path(cfg.state_dir).resolve()
    if out_dir == trading:
        raise SimulationRefused(f'--state-dir {out_dir} совпадает с app.state_dir торговли: симуляция пишет только '
                                'в отдельную папку (по умолчанию <state_dir>/simulation_<seed>).')
    if out_dir.exists() and not out_dir.is_dir():
        raise SimulationRefused(f'{out_dir} существует и не является папкой.')
    if out_dir.is_dir() and any(out_dir.iterdir()) and not (out_dir / MARKER_NAME).is_file():
        raise SimulationRefused(f'Папка {out_dir} не пуста и не помечена как папка симуляции ({MARKER_NAME}); '
                                'укажите пустую или прежнюю папку симуляции.')
    for name in ('live.sqlite3',):
        if (out_dir / name).exists():
            raise SimulationRefused(f'В папке {out_dir} есть {name}: это не папка симуляции.')


def simulation_config(cfg: Config, out_dir: Path, seed: int) -> Config:
    """Paper copy of the config writing only to ``out_dir`` (STOP files are looked up there too)."""
    data = copy.deepcopy(cfg.data)
    data['app']['mode'] = 'paper'
    data['app']['state_dir'] = str(Path(out_dir).resolve())
    data['app']['campaign_id'] = f'SIM_seed{seed}'
    data['telegram']['enabled'] = False
    data['safety']['require_telegram'] = False
    return Config(data, Path(out_dir).resolve(), secrets=Secrets())


def _clear_previous(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / MARKER_NAME).write_text(f'gate_stonk simulate: {SYNTHETIC_RU}\n', encoding='utf-8')
    stale = [out_dir / name for name in ('paper.sqlite3', 'paper.sqlite3-wal', 'paper.sqlite3-shm', 'status.json',
                                         REPORT_NAME)]
    stale += sorted((out_dir / 'logs').glob('gate_stonk_*.log'))
    for path in stale:
        if path.is_file():
            path.unlink()


class _LatencyPaperVenue(PaperVenue):
    """The real PaperVenue; every request reaches the simulated exchange after a REST
    latency during which the synthetic market keeps moving (``_tick`` is the point where
    each request is processed by the exchange: after the local checkpoint/validation)."""

    def __init__(self, cfg: Config, store: Any, hub: Any, meta: PairMeta, *, latency: Callable[[], None]) -> None:
        super().__init__(cfg, store, hub, meta)
        self._latency = latency

    def _tick(self, *, safe: bool = False) -> list[int]:
        self._latency()
        return super()._tick(safe=safe)


class _ProbeLogFilter(logging.Filter):
    """Silences the independent index probe (the engine's own builder still logs)."""

    def __init__(self) -> None:
        super().__init__()
        self.active = False

    def filter(self, record: logging.LogRecord) -> bool:
        return not self.active


class _SimNotifier(Notifier):
    """Telegram is off in the simulation: events are recorded as skipped, no thread."""

    def start(self) -> None:
        return None


class _VirtualWake:
    """``hub.wake`` replacement: waiting one loop tick advances the virtual market."""

    def __init__(self, sim: 'Simulation') -> None:
        self.sim = sim
        self._set = False

    def set(self) -> None:
        self._set = True

    def clear(self) -> None:
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def wait(self, timeout: float | None = None) -> bool:
        self.sim.on_tick(float(timeout or 0.0))
        return False


class SimRunner(Runner):
    """The real ``run`` loop; status.json is marked as synthetic."""

    def __init__(self, cfg: Config, deps: Deps, *, seconds: float, sim: 'Simulation') -> None:
        super().__init__(cfg, deps, live=False, seconds=seconds)
        self.sim = sim

    def process_status(self) -> dict:
        value = super().process_status()
        value['simulation'] = self.sim.status_block()
        return value

    def write_status(self, final: bool = False) -> None:
        from .status import write_status

        value = self.engine.status()
        if isinstance(value, dict):
            value = {'synthetic_market': True, 'warning': SYNTHETIC_BANNER, **value}
            value.setdefault('updated_ms', int(time.time() * 1000))
            value['process'] = self.process_status()
            if final:
                value['process']['exit_reason'] = self.exit_reason
                value['process']['stopped_ms'] = int(time.time() * 1000)
        write_status(status_path(self.cfg), value)


class Simulation:
    """One synthetic run: hub + market + the real runner, invariant checks per tick."""

    def __init__(self, cfg: Config, clock: VirtualClock, *, minutes: float, seed: int, params: MarketParams,
                 deps: Deps) -> None:
        self.cfg, self.clock, self.minutes, self.seed, self.params, self.deps = cfg, clock, minutes, seed, params, deps
        self.meta = synthetic_pair_meta(cfg)
        self.hub = FeedHub(cfg, adapters=None, start_threads=False)
        self.hub.wake = _VirtualWake(self)
        self.market = SyntheticMarket(cfg, self.hub, clock, self.meta, params, seed)
        self.probe = IndexBuilder(cfg.index_settings())
        self.ticks = 0
        self.violations: Counter = Counter()
        self.examples: dict[str, list[dict]] = {}
        self.checks = Counter()
        self.probe_seconds = 0.0
        self.probe_blocked_seconds = 0.0
        self.probe_reasons: Counter = Counter()
        self._probe_state: tuple[float, str | None] | None = None
        self._next_progress = PROGRESS_EVERY_SECONDS
        self._wall_started = time.perf_counter()
        self._fee_factor = 1 + D(cfg.portfolio['fee_reserve_pct']) / HUNDRED
        self._hard_min = D(cfg.safety['hard_min_edge_pct'])
        self._level_rank = {side: {level.id: i for i, level in enumerate(cfg.levels(side))} for side in SIDES}
        self.probe_filter = _ProbeLogFilter()
        runner_deps = Deps(
            hub_factory=self._hub_factory, meta_factory=lambda _cfg, _http: self.meta,
            paper_venue_factory=lambda c, store, hub, meta: _LatencyPaperVenue(
                c, store, hub, meta, latency=self.market.request_latency),
            engine_factory=self._engine_factory,
            notifier_factory=lambda c, store: _SimNotifier(c, store, start_thread=False),
            out=lambda text: deps.say(SAY_PREFIX + str(text)),
            err=lambda text: deps.error(SAY_PREFIX + str(text)),
            logging=False, signals=deps.signals)
        self.runner = SimRunner(cfg, runner_deps, seconds=minutes * 60, sim=self)

    def _hub_factory(self, _cfg: Config) -> FeedHub:
        self.market.start()
        return self.hub

    def run(self) -> int:
        index_log = logging.getLogger('gate_stonk.index')
        index_log.addFilter(self.probe_filter)
        try:
            return self.runner.run()
        finally:
            index_log.removeFilter(self.probe_filter)

    # ----- per tick -----
    def on_tick(self, seconds: float) -> None:
        self.ticks += 1
        try:
            self._check_invariants()
        except Exception as exc:      # a checker failure is itself a finding, never a crash
            self._violation('INVARIANT_CHECK_FAILED', error=type(exc).__name__)
        self._probe_index()
        self.market.advance(seconds if seconds > 0 else 0.2)
        if self.clock.elapsed >= self._next_progress:
            self._next_progress += PROGRESS_EVERY_SECONDS
            engine = self.runner.engine
            fills = engine.counters.get('fills', 0) if engine is not None else 0
            sent = engine.counters.get('orders_sent', 0) if engine is not None else 0
            self.deps.say(f'{SAY_PREFIX}{self.clock.elapsed / 60:.0f}/{self.minutes:g} мин виртуального времени '
                          f'({SYNTHETIC_RU}); отправлено заявок {sent}, исполнений {fills}, '
                          f'реальное время {time.perf_counter() - self._wall_started:.0f} с.')

    def _engine_factory(self, cfg: Config, store: Any, venue: Any, hub: Any, builder: Any, notifier: Any, *,
                        live: bool, meta: PairMeta, limiter: Any) -> Any:
        """The real Engine; its preflight is observed (never changed) by the hard-guard check."""
        from .engine import Engine

        engine = Engine(cfg, store, venue, hub, builder, notifier, live=live, meta=meta, limiter=limiter)
        original = engine.preflight

        def preflight(origin: str = 'loop') -> str | None:
            result = original(origin)
            try:
                self._after_preflight(engine)
            except Exception as exc:
                self._violation('INVARIANT_CHECK_FAILED', error=type(exc).__name__)
            return result

        engine.preflight = preflight
        return engine

    def _after_preflight(self, engine: Any) -> None:
        """Right after every preflight: no working order is inside the hard minimum edge of
        the index the guard just used (the index may move later, until the next preflight)."""
        snap = engine.index
        if snap is None or not snap.ok or snap.center is None:
            return
        self.checks['preflight_checks'] += 1
        for o in engine.store.orders():
            if o['cancel_reason'] or o['terminal_status'] or o['cancel_ack']:
                continue
            for price in engine._prices(o):
                if edge_pct(o['side'], price, snap.center) < self._hard_min:
                    self._violation('WORKING_ORDER_INSIDE_HARD_MIN_EDGE', text=o['text'], price=price,
                                    center=snap.center)

    def _violation(self, code: str, **fields: Any) -> None:
        self.violations[code] += 1
        examples = self.examples.setdefault(code, [])
        if len(examples) < EXAMPLES_PER_CODE:
            examples.append({'t_s': round(self.clock.elapsed, 3), **{k: str(v) for k, v in fields.items()}})

    def _probe_index(self) -> None:
        """Independent index evaluation every tick (blocked time even without orders); the
        state found at one probe is held until the next one (REST latency included)."""
        now = self.clock.elapsed
        if self._probe_state is not None:
            held = now - self._probe_state[0]
            self.probe_seconds += held
            if self._probe_state[1] is not None:
                self.probe_blocked_seconds += held
                self.probe_reasons[self._probe_state[1]] += held
        self._probe_state = None
        self.probe_filter.active = True
        try:
            snapshots = {source.id: self.hub.source_snapshot(source.id) for source in self.cfg.sources()}
            snap = self.probe.compute(snapshots, self.hub.fx_snapshot(), time.monotonic(), int(time.time() * 1000))
        except Exception as exc:
            self._violation('INDEX_PROBE_FAILED', error=type(exc).__name__)
            return
        finally:
            self.probe_filter.active = False
        self._probe_state = (now, None if snap.ok else re.sub(r':\d+/\d+$', '', str(snap.reason)))

    def finish(self) -> None:
        self._probe_index()

    def _check_invariants(self) -> None:
        engine, store = self.runner.engine, self.runner.store
        if engine is None or store is None:
            return
        self.checks['ticks'] += 1
        rows = store.orders()
        per_slot = Counter(o['slot'] for o in rows)
        for slot, count in per_slot.items():
            if count > 1:
                self._violation('TWO_ACTIVE_ORDERS_PER_SLOT_STORE', slot=slot, count=count)
        with store.read() as db:
            opens = db.execute("SELECT text, side FROM paper_orders WHERE status='open'").fetchall()
        venue_slots: Counter = Counter()
        for text, side in opens:
            parsed = parse_text(text)
            venue_slots[f"{side}:{parsed[1] if parsed else '?'}"] += 1
        for slot, count in venue_slots.items():
            if count > 1:
                self._violation('TWO_OPEN_ORDERS_PER_SLOT_VENUE', slot=slot, count=count)
        working = [o for o in rows if not o['cancel_reason'] and not o['terminal_status'] and not o['cancel_ack']]
        for side in SIDES:
            ranked = sorted((o for o in working if o['side'] == side),
                            key=lambda o: self._level_rank[side].get(o['level'], 999))
            for a, b in zip(ranked, ranked[1:]):
                if (b['price'] >= a['price']) if side == 'buy' else (b['price'] <= a['price']):
                    self._violation('LADDER_NOT_MONOTONE', side=side, shallow=f"{a['level']}@{a['price']}",
                                    deep=f"{b['level']}@{b['price']}")
        buys = [o['price'] for o in working if o['side'] == 'buy']
        sells = [o['price'] for o in working if o['side'] == 'sell']
        if buys and sells and max(buys) >= min(sells):
            self._violation('OWN_ORDERS_SELF_CROSS', buy=max(buys), sell=min(sells))
        # Money on the (paper) exchange at this frozen instant: every active reservation with its
        # TRUE remaining (open on the venue: its left; finished on the venue: nothing locked any
        # more; not on the venue: the reserved remainder) never exceeds the account totals. (The
        # engine's own view may lag by a fill between the history and balance reads of one sync;
        # that only makes it more conservative, e.g. a LIMIT_REDUCTION cancel.)
        self.checks['reservation_checks'] += 1
        totals = (self.runner.venue.diagnostics() or {}).get('balances') or {}
        quote_total = D(totals.get(self.meta.quote, '0'))
        base_total = D(totals.get(self.meta.base, '0'))
        reserved_quote = reserved_base = ZERO
        with store.read() as db:
            for o in rows:
                found = db.execute('SELECT status, left_amount FROM paper_orders WHERE text=?', (o['text'],)).fetchone()
                if found is None:
                    remaining = max(ZERO, o['qty'] - (o['filled_qty'] or ZERO))
                elif found[0] == 'open':
                    remaining = D(found[1])
                else:
                    remaining = ZERO
                if o['side'] == 'buy':
                    reserved_quote += remaining * engine._reserve_price(o) * self._fee_factor
                else:
                    reserved_base += remaining
        if reserved_quote > quote_total:
            self._violation('BUY_RESERVATIONS_EXCEED_USDT_TOTAL', reserved=reserved_quote, total=quote_total)
        if reserved_base > base_total:
            self._violation('SELL_RESERVATIONS_EXCEED_BASE_TOTAL', reserved=reserved_base, total=base_total)

    # ----- summaries -----
    def status_block(self) -> dict:
        return {'synthetic_market': True, 'note': SYNTHETIC_RU, 'seed': self.seed, 'minutes': self.minutes,
                'virtual_elapsed_seconds': round(self.clock.elapsed, 3), 'ticks': self.ticks,
                'invariant_violations': dict(self.violations),
                'market_events': {key: value for key, value in self.market.stats.items()
                                  if key in ('jump', 'sweep', 'source_dropout', 'taker_trades', 'prints',
                                             'publish_rejected')}}


def _order_rows(path: Path) -> list[dict]:
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        return [dict(row) for row in db.execute(
            'SELECT text, side, level, price, qty, filled_qty, center_price, created_ms, state, reason, prior_prices '
            'FROM orders ORDER BY created_ms, rowid')]
    finally:
        db.close()


def placement_audit(path: Path, cfg: Config) -> dict:
    """Every order ever reserved: edge of its creation price to its placement centre >=
    safety.min_new_edge_pct and, within one creation pass, levels strictly monotone per side."""
    min_new = D(cfg.safety['min_new_edge_pct'])
    rank = {side: {level.id: i for i, level in enumerate(cfg.levels(side))} for side in SIDES}
    rows = _order_rows(path)
    below: list[dict] = []
    edges: list[Decimal] = []
    passes: dict[tuple[int, str], list[dict]] = {}
    for row in rows:
        prior = json.loads(row['prior_prices'] or '[]')      # an amend keeps the creation price first
        price = D(prior[0][0]) if prior else D(row['price'])
        center = D(row['center_price']) if row['center_price'] is not None else None
        if center is None:
            below.append({'text': row['text'], 'reason': 'NO_CENTER'})
            continue
        edge = edge_pct(row['side'], price, center)
        edges.append(edge)
        if edge < min_new:
            below.append({'text': row['text'], 'edge_pct': str(edge)})
        passes.setdefault((int(row['created_ms']), row['side']), []).append({**row, 'price': price})
    non_monotone = []
    for (created, side), group in passes.items():
        group.sort(key=lambda o: rank[side].get(o['level'], 999))
        for a, b in zip(group, group[1:]):
            if (b['price'] >= a['price']) if side == 'buy' else (b['price'] <= a['price']):
                non_monotone.append({'created_ms': created, 'side': side, 'shallow': a['text'], 'deep': b['text']})
    return {'orders': len(rows), 'min_edge_at_placement_pct': min(edges) if edges else None,
            'min_new_edge_pct': min_new, 'below_min_new_edge': below[:20], 'below_min_new_edge_count': len(below),
            'same_pass_non_monotone': non_monotone[:20], 'same_pass_non_monotone_count': len(non_monotone),
            'states': dict(Counter(row['state'] for row in rows))}


@dataclass
class SimulationResult:
    exit_code: int
    run_exit_code: int
    exit_reason: str
    out_dir: Path
    db_path: Path
    status_path: Path
    report_path: Path
    report: dict
    summary: dict
    text: str = field(repr=False, default='')


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def run_simulation(cfg: Config, *, minutes: float = 60.0, seed: int = 1, out_dir: str | Path | None = None,
                   params: MarketParams | None = None, deps: Deps | None = None) -> SimulationResult:
    """Run the real engine on the synthetic market (see module doc). Raises SimulationRefused."""
    from .report import build_report, format_report_ru
    from .status import write_status

    deps = deps or Deps()
    if not (isinstance(minutes, (int, float)) and math.isfinite(minutes) and 0 < minutes <= MAX_MINUTES):
        raise SimulationRefused(f'--minutes должно быть в (0, {MAX_MINUTES}]')
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= MAX_SEED:
        raise SimulationRefused(f'--seed должно быть целым 0..{MAX_SEED}')
    params = params or MarketParams()
    target = Path(out_dir) if out_dir is not None else default_simulation_dir(cfg, seed)
    target = target.resolve()
    check_simulation_dir(cfg, target)
    target.mkdir(parents=True, exist_ok=True)       # a directory: the simulation config's base dir
    sim_cfg = simulation_config(cfg, target, seed)
    clock = VirtualClock()
    wall_started = time.perf_counter()
    try:
        locks = ProcessLocks(sim_cfg, False)
        locks.__enter__()
    except LockBusy as exc:
        raise SimulationRefused(f'Симуляция в {target} уже идёт (занята блокировка {exc.path}).') from None
    try:
        _clear_previous(target)
        with clock.installed():
            handlers = configure_logging(sim_cfg, console_level=logging.ERROR) if deps.logging else []
            try:
                LOG.warning('SIMULATION START seed=%s minutes=%s: %s', seed, minutes, SYNTHETIC_RU)
                sim = Simulation(sim_cfg, clock, minutes=float(minutes), seed=seed, params=params, deps=deps)
                deps.say(f'{SYNTHETIC_BANNER}. seed {seed}, {minutes:g} мин виртуального времени '
                         f'({_utc(clock.wall_ms())} …), волатильность {params.volatility_pct_per_hour:g}%/√ч; '
                         f'папка {target}.')
                run_code = sim.run()
                sim.finish()
                end_ms = clock.wall_ms()
                LOG.warning('SIMULATION END seed=%s exit=%s reason=%s', seed, run_code, sim.runner.exit_reason)
            finally:
                remove_logging(handlers)
    finally:
        locks.release()
    database = db_path(sim_cfg, 'paper')
    slots = [f'{side}:{level.id}' for side in SIDES for level in sim_cfg.levels(side)]
    report = build_report(str(database), hours=None, now_ms=end_ms,
                          initial_avg_price=sim_cfg.pair.get('initial_avg_price'), slots=slots)
    engine = sim.runner.engine
    audit = placement_audit(database, sim_cfg)
    probe_pct = (Decimal(repr(sim.probe_blocked_seconds)) / Decimal(repr(sim.probe_seconds)) * HUNDRED
                 if sim.probe_seconds else None)
    summary = {
        'synthetic_market': True,
        'note': SYNTHETIC_RU,
        'seed': seed,
        'minutes': minutes,
        'virtual_start_utc': _utc(VIRTUAL_START_MS),
        'virtual_end_utc': _utc(end_ms),
        'wall_seconds': round(time.perf_counter() - wall_started, 1),
        'run_exit_code': run_code,
        'exit_reason': sim.runner.exit_reason,
        'engine_state': None if engine is None else engine.last_reason,
        'engine_stop_reason': None if engine is None else engine.stop_reason,
        'engine_counters': {} if engine is None else dict(engine.counters),
        'engine_errors': {} if engine is None else dict(engine._errors),
        'pair_meta': dict(sim.meta.raw),
        'params': asdict(params),
        'market': dict(sorted(sim.market.stats.items())),
        'market_events': sim.market.events,
        'index_probe': {'observed_seconds': round(sim.probe_seconds, 1),
                        'blocked_seconds': round(sim.probe_blocked_seconds, 1),
                        'blocked_pct': probe_pct,
                        'blocked_reasons_seconds': {k: round(v, 1) for k, v in sim.probe_reasons.most_common()}},
        'invariants': {'checks': dict(sim.checks), 'violations': dict(sim.violations), 'examples': sim.examples},
        'placement_audit': audit,
    }
    report['simulation'] = summary
    report_path = target / REPORT_NAME
    write_status(report_path, report)
    violations = sum(sim.violations.values()) + audit['below_min_new_edge_count'] + audit['same_pass_non_monotone_count']
    exit_code = EXIT_ERROR if run_code != EXIT_OK else (EXIT_FAIL if violations else EXIT_OK)
    text = '\n'.join([f'===== {SYNTHETIC_BANNER} (seed {seed}) =====', format_report_ru(report),
                      *summary_lines(summary), f'===== конец отчёта: {SYNTHETIC_RU} ====='])
    return SimulationResult(exit_code=exit_code, run_exit_code=run_code, exit_reason=sim.runner.exit_reason,
                            out_dir=target, db_path=database, status_path=status_path(sim_cfg),
                            report_path=report_path, report=report, summary=summary, text=text)


def summary_lines(summary: dict) -> list[str]:
    market = summary['market']
    dropouts = Counter()
    for event in summary['market_events']:
        if event['kind'] == 'source_dropout':
            dropouts[event['key']] += 1
    probe = summary['index_probe']
    reasons = ', '.join(f'{code} {seconds:g} с' for code, seconds in probe['blocked_reasons_seconds'].items())
    invariants = summary['invariants']
    audit = summary['placement_audit']
    counters = summary['engine_counters']
    violations = ', '.join(f'{code}×{n}' for code, n in invariants['violations'].items()) or 'нет'
    blocked_pct = probe['blocked_pct']
    return [
        f"Синтетический рынок ({SYNTHETIC_RU}): скачков цены {market.get('jump', 0)}, проходов по стакану (sweep) "
        f"{market.get('sweep', 0)}, сделок-тейкеров {market.get('taker_trades', 0)}, принтов в ленте "
        f"{market.get('prints', 0)}, выпадений источников {sum(dropouts.values())}"
        + (f" ({', '.join(f'{k}×{n}' for k, n in sorted(dropouts.items()))})" if dropouts else '')
        + f", отклонённых публикаций {market.get('publish_rejected', 0)}.",
        f"Индекс по независимой проверке каждый тик: заблокирован {probe['blocked_seconds']:g} с из "
        f"{probe['observed_seconds']:g} с ({fmt(blocked_pct, 2) if blocked_pct is not None else 'н/д'}%)"
        + (f'; причины: {reasons}' if reasons else '') + '.',
        f"Движок: шагов {counters.get('steps', 0)}, отправлено заявок {counters.get('orders_sent', 0)}, "
        f"принято {counters.get('creates_accepted', 0)}, post-only отказов {counters.get('post_only_rejects', 0)}, "
        f"отмен {counters.get('cancel_requests', 0)}, исполнений {counters.get('fills', 0)}, продлений DMS "
        f"{counters.get('dms_renewals', 0)}; состояние {summary['engine_state']}, остановка "
        f"{summary['engine_stop_reason'] or 'нет'}; ошибки диагностики {summary['engine_errors'] or 'нет'}.",
        f"Инварианты (проверок {invariants['checks'].get('ticks', 0)} по тикам, "
        f"{invariants['checks'].get('preflight_checks', 0)} после preflight): нарушений {violations}; "
        f"заявок ближе min_new_edge при выставлении {audit['below_min_new_edge_count']} "
        f"(минимальный край {fmt(audit['min_edge_at_placement_pct'], 4) if audit['min_edge_at_placement_pct'] is not None else 'н/д'}%), "
        f"немонотонных пар уровней в одном проходе {audit['same_pass_non_monotone_count']}.",
        f"Завершение: {summary['exit_reason']} (код {summary['run_exit_code']}); виртуальное время "
        f"{summary['virtual_start_utc']} — {summary['virtual_end_utc']}; реальное время {summary['wall_seconds']:g} с.",
    ]


def cmd_simulate(cfg: Config, args: argparse.Namespace, deps: Deps) -> int:
    """CLI entry: prints the report (Russian) and writes status.json + report.json."""
    volatility = DEFAULT_VOLATILITY_PCT if args.volatility is None else args.volatility
    if not (math.isfinite(volatility) and 0 <= volatility < 100):
        deps.error('Ошибка: --volatility должно быть числом 0..100 (% за √часа)')
        return EXIT_ERROR
    params = MarketParams(volatility_pct_per_hour=float(volatility))
    try:
        result = run_simulation(cfg, minutes=args.minutes, seed=args.seed, out_dir=args.state_dir, params=params,
                                deps=deps)
    except SimulationRefused as exc:
        deps.error(f'Симуляция не запущена: {exc}')
        return EXIT_ERROR
    deps.say(result.text)
    deps.say(f'Файлы ({SYNTHETIC_RU}): статус {result.status_path}; отчёт JSON {result.report_path}; '
             f'база симуляции {result.db_path}.')
    if result.exit_code == EXIT_FAIL:
        deps.error('ВНИМАНИЕ: симуляция обнаружила нарушения инвариантов (см. отчёт).')
    return result.exit_code
