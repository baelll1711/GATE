"""Paper venue: a persistent, deterministic simulator of Gate spot (DESIGN §7 "PaperVenue").

``PaperVenue`` implements the same ``Venue`` protocol as the live ``GateVenue`` and returns
the same value types, so the engine runs unchanged in paper mode. State lives in the
campaign's SQLite store (tables ``paper_orders``, ``paper_trades``, ``paper_tape`` and meta
keys ``paper_state`` / ``paper_balances``), so a restarted process sees the same orders,
balances and fills and never re-applies a public trade.

Fill model ``trade_through`` (ported from the reference simulator, adapted to a ladder):

* Inputs are the traded book ``hub.spot_book()`` and the public tape
  ``hub.trades_since(seq)`` / ``hub.last_trade_seq()`` (``PublicTrade`` rows).
* A public trade fills a resting order only if it was received AFTER the order was placed
  (``trade.seq > placement_seq``; the tape boundary is captured at placement, which also
  separates prints within one millisecond) and its exchange time is not before the order's
  priority time (``trade.create_time_ms >= priority_ms``: creation time, or the last amend).
* BUY fills only when ``trade.price < order.price``, SELL only when ``trade.price >
  order.price`` (STRICTLY through: a print at our price never fills, the queue position in
  front of us is unknown). The fill price is the ORDER price (maker).
* A trade's amount is SHARED: it is allocated across all eligible orders in price priority
  (BUY highest price first, SELL lowest first; then time priority: a persisted rank taken
  at creation and renewed by every amend), each order takes ``floor_qty(min(left, trade remainder))`` and the remainder
  shrinks — a print is never reused for a deeper level. One pool serves both sides (a
  single print cannot fill a bid and an ask; only self-crossed own orders could compete).
* Every public trade is applied once: a persisted tape watermark plus a persisted set of
  processed public trade ids (re-delivered/replayed trades after a reconnect or restart are
  ignored). A tape whose numbering went backwards (new feed hub) is detected and handled.
* Fees: ``rate = paper.fee_pct`` (percent; null -> ``portfolio.fee_estimate_pct``). BUY fee
  in the base currency (``qty * rate``), SELL fee in the quote currency (``qty * price *
  rate``). Own fills are returned by ``my_trades`` with role ``maker``.

Gate-like behaviour: post-only orders that would take (BUY >= best ask, SELL <= best bid of
the CURRENT spot book) are created already ``cancelled`` with ``finish_as='poc'``;
precision/minimum/balance problems are ``Rejected`` with Gate labels (``INVALID_PRECISION``,
``AMOUNT_TOO_LITTLE``, ``AMOUNT_TOO_MUCH``, ``BALANCE_NOT_ENOUGH``); structural problems of
the request (bad text/side/time-in-force/types) are ``NotSent`` exactly like the live
venue's local validation. Balances: BUY locks ``left * price`` quote, SELL locks ``left``
base (no fee hold). Lookup by ``text`` works only while the order is open (like Gate).

Every protocol method first runs the lazy dead-man's switch check (``now >= deadline > 0``
-> trades printed up to the deadline are applied first, then every open order is cancelled
with ``finish_as='cancelled'``) and applies pending public trades, so fills and DMS are
deterministic functions of the tape and the call sequence. Reads call ``before_read()``
(the engine checkpoint) first, like the live venue does before each physical read.

Documented limitations: queue position and hidden iceberg priority are NOT modelled (the
``iceberg`` field is stored and echoed, fills treat the order as fully visible); the taker
side of a print is ignored; an amend always resets the priority (Gate keeps it on a pure
amount reduction — the simulator is conservative). Paper results are therefore neither an
upper nor a lower bound of live fills.
"""
from __future__ import annotations

import copy
import json
import logging
import sqlite3
import threading
import time
from decimal import Decimal
from typing import Any, Callable, Sequence

from .dec import HUNDRED, ZERO, D, floor_qty, on_grid
from .errors import NotSentValueError, ReadError, Rejected, Unsafe, not_sent
from .types import (SIDES, Balance, Book, CancelRequest, PairMeta, PlaceRequest, PlaceResult, PublicTrade,
                    VenueOrder, VenueTrade)
from .venue import (MAX_TIME_MS, POST_ONLY_FINISH_AS, POST_ONLY_LABEL, json_safe, parse_order, parse_trade,
                    side_allowed, trade_sort_key, validate_amend_text, validate_order_id, validate_text, wire)

LOG = logging.getLogger(__name__)

STATE_KEY = 'paper_state'
BALANCES_KEY = 'paper_balances'
STATE_VERSION = 1

DMS_MIN_TIMEOUT = 5
DMS_MAX_TIMEOUT = 86400
TAPE_KEEP_MS = 3_600_000          # processed public trade ids are kept at least this long ...
TAPE_SKEW_MARGIN_MS = 60_000      # ... and never pruned within this margin of an open order's priority time
PRUNE_INTERVAL_MS = 600_000

# Gate error labels produced by the simulator (notes/api_gate_rest.md §11).
INVALID_PRECISION = 'INVALID_PRECISION'
AMOUNT_TOO_LITTLE = 'AMOUNT_TOO_LITTLE'
AMOUNT_TOO_MUCH = 'AMOUNT_TOO_MUCH'
BALANCE_NOT_ENOUGH = 'BALANCE_NOT_ENOUGH'
INVALID_PARAM_VALUE = 'INVALID_PARAM_VALUE'
ORDER_NOT_FOUND = 'ORDER_NOT_FOUND'
ORDER_CLOSED = 'ORDER_CLOSED'
ORDER_CANCELLED = 'ORDER_CANCELLED'
HTTP_BAD_REQUEST = 400

DDL = """
CREATE TABLE IF NOT EXISTS paper_orders(
    order_id INTEGER PRIMARY KEY,
    text TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy','sell')),
    time_in_force TEXT NOT NULL,
    price TEXT NOT NULL,
    amount TEXT NOT NULL,
    left_amount TEXT NOT NULL,
    iceberg TEXT NOT NULL DEFAULT '0',
    status TEXT NOT NULL CHECK(status IN ('open','closed','cancelled')),
    finish_as TEXT NOT NULL,
    create_time_ms INTEGER NOT NULL,
    update_time_ms INTEGER NOT NULL,
    finish_time_ms INTEGER,
    placement_seq INTEGER NOT NULL,
    priority_ms INTEGER NOT NULL,
    priority_rank INTEGER NOT NULL,
    fee TEXT NOT NULL DEFAULT '0',
    filled_total TEXT NOT NULL DEFAULT '0',
    amend_text TEXT);
CREATE INDEX IF NOT EXISTS paper_orders_status ON paper_orders(status);
CREATE INDEX IF NOT EXISTS paper_orders_text ON paper_orders(text);
CREATE INDEX IF NOT EXISTS paper_orders_finish ON paper_orders(finish_time_ms);
CREATE TABLE IF NOT EXISTS paper_trades(
    trade_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    text TEXT NOT NULL,
    side TEXT NOT NULL,
    amount TEXT NOT NULL,
    price TEXT NOT NULL,
    fee TEXT NOT NULL,
    fee_currency TEXT NOT NULL,
    create_time_ms INTEGER NOT NULL,
    public_trade_id TEXT NOT NULL,
    tape_seq INTEGER NOT NULL,
    recorded_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS paper_trades_time ON paper_trades(create_time_ms);
CREATE INDEX IF NOT EXISTS paper_trades_order ON paper_trades(order_id);
CREATE TABLE IF NOT EXISTS paper_tape(
    trade_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    create_time_ms INTEGER NOT NULL,
    price TEXT NOT NULL,
    amount TEXT NOT NULL,
    used TEXT NOT NULL,
    processed_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS paper_tape_time ON paper_tape(create_time_ms);
"""

_DECIMAL_COLUMNS = ('price', 'amount', 'left_amount', 'iceberg', 'fee', 'filled_total')


def _noop() -> None:
    return None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _get(db: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return json.loads(row[0]) if row else default


def _put(db: sqlite3.Connection, key: str, value: Any) -> None:
    db.execute('INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)',
               (key, json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)))


def _order_row(row: sqlite3.Row) -> dict:
    out = dict(row)
    for name in _DECIMAL_COLUMNS:
        out[name] = D(out[name])
    return out


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _positive_decimal(value: Any) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def _window(from_ms: Any, to_ms: Any) -> tuple[int, int]:
    for value in (from_ms, to_ms):
        if not _is_int(value) or not 0 <= value < MAX_TIME_MS:
            raise ValueError('INVALID_TIME_WINDOW')
    if from_ms > to_ms:
        raise ValueError('INVALID_TIME_WINDOW')
    return from_ms, to_ms


def _rejected(label: str, message: str) -> Rejected:
    return Rejected(message, label=label, status=HTTP_BAD_REQUEST)


class PaperVenue:
    """Deterministic Gate spot simulator persisted in the campaign store (Venue protocol)."""

    live = False

    def __init__(self, cfg: Any, store: Any, hub: Any, meta: PairMeta):
        market = cfg.market
        if not isinstance(meta, PairMeta):
            raise TypeError('PaperVenue needs PairMeta')
        if (meta.currency_pair, meta.base, meta.quote) != (market['currency_pair'], market['base'], market['quote']):
            raise Unsafe('SPOT_PAIR_MAPPING_MISMATCH')
        self.cfg = cfg
        self.store = store
        self.hub = hub
        self.meta = meta
        self.pair = meta.currency_pair
        self.base = meta.base
        self.quote = meta.quote
        self.account = market.get('account') or 'spot'
        paper = cfg.paper
        fee_pct = paper.get('fee_pct')
        if fee_pct is None:
            fee_pct = cfg.portfolio['fee_estimate_pct']
        self.fee_pct = D(fee_pct)
        if not 0 <= self.fee_pct < HUNDRED:
            raise ValueError('Invalid paper fee percent')
        self.fee_rate = self.fee_pct / HUNDRED
        self.before_read: Callable[[], None] = _noop
        self._lock = threading.RLock()
        self._last_prune_ms = 0
        store.ensure_tables(DDL)
        self._init_state(D(paper['initial_usdt']), D(paper['initial_base']))

    # ------------------------------------------------------------------ persistent state
    def _identity(self) -> dict:
        return {'currency_pair': self.pair, 'base': self.base, 'quote': self.quote}

    def _init_state(self, initial_quote: Decimal, initial_base: Decimal) -> None:
        """Create the paper account ONCE per campaign; later config edits never add money."""
        with self.store.write() as db:
            state = _get(db, STATE_KEY)
            if state is None:
                if initial_quote < 0 or initial_base < 0:
                    raise Unsafe('NEGATIVE_PAPER_INITIAL_BALANCE')
                now = _now_ms()
                state = {
                    'version': STATE_VERSION, 'identity': self._identity(), 'created_ms': now,
                    'initial': {self.quote: wire(initial_quote), self.base: wire(initial_base)},
                    'last_order_id': now, 'last_trade_id': 0, 'last_rank': 0, 'tape_seq': 0,
                    'dms_deadline_ms': 0,
                }
                _put(db, STATE_KEY, state)
                _put(db, BALANCES_KEY, {self.quote: wire(initial_quote), self.base: wire(initial_base)})
                LOG.info('PAPER_ACCOUNT_CREATED %s', json.dumps(json_safe(state['initial']), sort_keys=True))
                return
            if (not isinstance(state, dict) or state.get('version') != STATE_VERSION
                    or state.get('identity') != self._identity()):
                raise Unsafe('PAPER_MARKET_IDENTITY_MISMATCH')
            balances = _get(db, BALANCES_KEY)
            if not isinstance(balances, dict) or set(balances) != {self.base, self.quote}:
                raise Unsafe('PAPER_BALANCES_INVALID')
            initial = state.get('initial') or {}
            if (D(initial.get(self.quote, '0')) != initial_quote
                    or D(initial.get(self.base, '0')) != initial_base):
                LOG.warning('PAPER_INITIAL_BALANCE_IGNORED: the paper account of this campaign already exists; '
                            'paper.initial_usdt/initial_base apply only to a new campaign database')

    def _state(self, db: sqlite3.Connection) -> dict:
        state = _get(db, STATE_KEY)
        if not isinstance(state, dict):
            raise Unsafe('PAPER_STATE_MISSING')
        return state

    def _balances_db(self, db: sqlite3.Connection) -> dict[str, Decimal]:
        raw = _get(db, BALANCES_KEY)
        if not isinstance(raw, dict):
            raise Unsafe('PAPER_BALANCES_INVALID')
        try:
            return {self.base: D(raw[self.base]), self.quote: D(raw[self.quote])}
        except (KeyError, ValueError) as exc:
            raise Unsafe('PAPER_BALANCES_INVALID') from exc

    def _holds(self, db: sqlite3.Connection) -> dict[str, Decimal]:
        holds = {self.base: ZERO, self.quote: ZERO}
        for side, price, left in db.execute(
                "SELECT side, price, left_amount FROM paper_orders WHERE status='open'"):
            if side == 'buy':
                holds[self.quote] += D(left) * D(price)
            else:
                holds[self.base] += D(left)
        return holds

    # ------------------------------------------------------------------ tape / DMS (lazy, every call)
    def _tick(self, *, safe: bool = False) -> list[int]:
        """Lazy DMS check + pending public trades in one transaction. Returns new paper trade ids.

        ``safe`` (cancel paths): a failure is logged and swallowed so it never blocks a cancel.
        """
        now = _now_ms()
        try:
            with self.store.write() as db:
                return self._tick_db(db, now)
        except Exception as exc:
            if not safe:
                raise
            LOG.error('PAPER_TICK_FAILED error=%s', type(exc).__name__)
            return []

    def _read_tick(self) -> None:
        """Tick before a read: invariant violations stay Unsafe, anything else is a ReadError."""
        try:
            self._tick()
        except Unsafe:
            raise
        except Exception as exc:
            raise ReadError(f'PAPER_TICK_FAILED:{type(exc).__name__}') from None

    def _write_tick(self) -> None:
        """Tick before a write: a failure means nothing was performed (NotSent)."""
        try:
            self._tick()
        except Exception as exc:
            raise not_sent(exc) from None

    def _tick_db(self, db: sqlite3.Connection, now: int) -> list[int]:
        state = self._state(db)
        balances = self._balances_db(db)
        state_before = copy.deepcopy(state)
        balances_before = dict(balances)
        fills: list[int] = []
        deadline = int(state.get('dms_deadline_ms') or 0)
        if deadline > 0 and now >= deadline:
            # Prints through a resting order before the timer fired still fill it.
            fills += self._apply_tape(db, state, balances, now, until_ms=deadline)
            cancelled = self._cancel_open(db, deadline, 'cancelled')
            state['dms_deadline_ms'] = 0
            LOG.warning('PAPER_DMS_FIRED deadline_ms=%s cancelled=%s', deadline, cancelled)
        fills += self._apply_tape(db, state, balances, now)
        self._prune(db, now)
        if state != state_before:
            _put(db, STATE_KEY, state)
        if balances != balances_before:
            _put(db, BALANCES_KEY, {name: wire(value) for name, value in balances.items()})
        return fills

    def _apply_tape(self, db: sqlite3.Connection, state: dict, balances: dict[str, Decimal], now: int,
                    until_ms: int | None = None) -> list[int]:
        hub = self.hub
        if hub is None:
            return []
        watermark = int(state.get('tape_seq') or 0)
        last = hub.last_trade_seq()
        if not _is_int(last):
            raise Unsafe('PAPER_TAPE_INVALID_SEQUENCE')
        if last < watermark:
            # A new feed hub numbers its tape below the persisted watermark (clock moved back):
            # everything on the new tape was received after every persisted placement.
            LOG.warning('PAPER_TAPE_RESET last_seq=%s watermark=%s', last, watermark)
            db.execute("UPDATE paper_orders SET placement_seq=0 WHERE status='open'")
            watermark = 0
            state['tape_seq'] = 0
        if last <= watermark:
            return []
        trades = [t for t in hub.trades_since(watermark) if _is_int(getattr(t, 'seq', None))]
        trades.sort(key=lambda t: t.seq)
        if not trades:
            return []
        opens = [_order_row(row) for row in db.execute("SELECT * FROM paper_orders WHERE status='open'")]
        fills: list[int] = []
        for trade in trades:
            if trade.seq <= watermark:
                continue
            if until_ms is not None and _is_int(trade.create_time_ms) and trade.create_time_ms > until_ms:
                break  # the countdown fired before this print
            watermark = trade.seq
            state['tape_seq'] = trade.seq
            if not self._trade_ok(trade):
                LOG.warning('PAPER_TAPE_ROW_SKIPPED seq=%s', trade.seq)
                continue
            if db.execute('SELECT 1 FROM paper_tape WHERE trade_id=?', (trade.trade_id,)).fetchone():
                continue  # already applied (re-delivered or replayed after a restart)
            used = self._match(db, trade, opens, balances, state, now, fills)
            db.execute('INSERT INTO paper_tape(trade_id,seq,create_time_ms,price,amount,used,processed_ms) '
                       'VALUES(?,?,?,?,?,?,?)',
                       (trade.trade_id, trade.seq, trade.create_time_ms, wire(trade.price), wire(trade.amount),
                        wire(used), now))
        return fills

    @staticmethod
    def _trade_ok(trade: Any) -> bool:
        try:
            return (isinstance(trade.trade_id, str) and 0 < len(trade.trade_id) <= 64
                    and _positive_decimal(trade.price) and _positive_decimal(trade.amount)
                    and _is_int(trade.create_time_ms) and 0 < trade.create_time_ms < MAX_TIME_MS)
        except AttributeError:
            return False

    def _match(self, db: sqlite3.Connection, trade: PublicTrade, opens: list[dict], balances: dict[str, Decimal],
               state: dict, now: int, fills: list[int]) -> Decimal:
        """Allocate one public trade across eligible orders; returns the amount used."""
        def eligible(order: dict) -> bool:
            return (order['status'] == 'open' and order['left_amount'] > 0
                    and order['placement_seq'] < trade.seq and order['priority_ms'] <= trade.create_time_ms)

        buys = [o for o in opens if o['side'] == 'buy' and trade.price < o['price'] and eligible(o)]
        sells = [o for o in opens if o['side'] == 'sell' and trade.price > o['price'] and eligible(o)]
        buys.sort(key=lambda o: (-o['price'], o['priority_rank']))
        sells.sort(key=lambda o: (o['price'], o['priority_rank']))
        remaining = trade.amount
        for order in buys + sells:
            if remaining <= 0:
                break
            qty = floor_qty(min(order['left_amount'], remaining), self.meta.amount_precision)
            if qty <= 0:
                continue
            remaining -= qty
            fills.append(self._fill(db, order, qty, trade, balances, state, now))
        return trade.amount - remaining

    def _fill(self, db: sqlite3.Connection, order: dict, qty: Decimal, trade: PublicTrade,
              balances: dict[str, Decimal], state: dict, now: int) -> int:
        price = order['price']
        notional = qty * price
        stamp = min(trade.create_time_ms, now)   # public trade time; never a future local time
        if order['side'] == 'buy':
            fee, fee_currency = qty * self.fee_rate, self.base
            balances[self.quote] -= notional
            balances[self.base] += qty - fee
        else:
            fee, fee_currency = notional * self.fee_rate, self.quote
            balances[self.base] -= qty
            balances[self.quote] += notional - fee
        if balances[self.base] < 0 or balances[self.quote] < 0:
            raise Unsafe('PAPER_BALANCE_INCONSISTENT')
        order['left_amount'] -= qty
        order['fee'] += fee
        order['filled_total'] += notional
        order['update_time_ms'] = max(int(order['update_time_ms']), stamp)
        if order['left_amount'] == 0:
            order['status'] = 'closed'
            order['finish_as'] = 'filled'
            order['finish_time_ms'] = order['update_time_ms']
        db.execute('UPDATE paper_orders SET left_amount=?, fee=?, filled_total=?, status=?, finish_as=?, '
                   'update_time_ms=?, finish_time_ms=? WHERE order_id=?',
                   (wire(order['left_amount']), wire(order['fee']), wire(order['filled_total']), order['status'],
                    order['finish_as'], order['update_time_ms'], order['finish_time_ms'], order['order_id']))
        state['last_trade_id'] = int(state.get('last_trade_id') or 0) + 1
        trade_id = state['last_trade_id']
        db.execute('INSERT INTO paper_trades(trade_id,order_id,text,side,amount,price,fee,fee_currency,'
                   'create_time_ms,public_trade_id,tape_seq,recorded_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (trade_id, order['order_id'], order['text'], order['side'], wire(qty), wire(price), wire(fee),
                    fee_currency, stamp, trade.trade_id, trade.seq, now))
        LOG.info('PAPER_FILL %s', json.dumps({
            'trade_id': str(trade_id), 'order_id': str(order['order_id']), 'text': order['text'],
            'side': order['side'], 'qty': wire(qty), 'price': wire(price), 'fee': wire(fee),
            'fee_currency': fee_currency, 'public_trade_id': trade.trade_id, 'public_price': wire(trade.price),
            'left': wire(order['left_amount'])}, sort_keys=True))
        return trade_id

    def _cancel_open(self, db: sqlite3.Connection, stamp: int, finish_as: str) -> int:
        cursor = db.execute("UPDATE paper_orders SET status='cancelled', finish_as=?, "
                            'update_time_ms=MAX(update_time_ms, ?), finish_time_ms=MAX(update_time_ms, ?) '
                            "WHERE status='open'", (finish_as, stamp, stamp))
        return cursor.rowcount

    def _cancel_one(self, db: sqlite3.Connection, order_id: int, now: int) -> None:
        db.execute("UPDATE paper_orders SET status='cancelled', finish_as='cancelled', "
                   'update_time_ms=MAX(update_time_ms, ?), finish_time_ms=MAX(update_time_ms, ?) '
                   "WHERE order_id=? AND status='open'", (now, now, order_id))

    def _prune(self, db: sqlite3.Connection, now: int) -> None:
        """Forget processed public trade ids that can no longer fill anything."""
        if now - self._last_prune_ms < PRUNE_INTERVAL_MS:
            return
        self._last_prune_ms = now
        cutoff = now - TAPE_KEEP_MS
        oldest = db.execute("SELECT MIN(priority_ms) FROM paper_orders WHERE status='open'").fetchone()[0]
        if oldest is not None:
            cutoff = min(cutoff, int(oldest) - TAPE_SKEW_MARGIN_MS)
        db.execute('DELETE FROM paper_tape WHERE create_time_ms < ?', (cutoff,))

    def _book(self) -> Book | None:
        if self.hub is None:
            return None
        try:
            return self.hub.spot_book()
        except Exception as exc:
            LOG.warning('PAPER_SPOT_BOOK_UNAVAILABLE error=%s', type(exc).__name__)
            return None

    def _tape_seq(self) -> int:
        if self.hub is None:
            return 0
        seq = self.hub.last_trade_seq()
        if not _is_int(seq):
            raise Unsafe('PAPER_TAPE_INVALID_SEQUENCE')
        return seq

    @staticmethod
    def _crosses(side: str, price: Decimal, book: Book | None) -> bool:
        """Post-only check against the current traded book (missing side: nothing to take)."""
        if book is None:
            return False
        if side == 'buy':
            return bool(book.asks) and price >= book.asks[0][0]
        return bool(book.bids) and price <= book.bids[0][0]

    # ------------------------------------------------------------------ value conversion
    def _venue_order(self, row: dict) -> VenueOrder:
        amount, left = row['amount'], row['left_amount']
        filled = amount - left
        filled_total = row['filled_total']
        raw: dict[str, Any] = {
            'id': str(row['order_id']), 'text': row['text'], 'currency_pair': self.pair, 'type': 'limit',
            'account': self.account, 'side': row['side'], 'time_in_force': row['time_in_force'],
            'amount': wire(amount), 'price': wire(row['price']), 'left': wire(left), 'filled_amount': wire(filled),
            'filled_total': wire(filled_total), 'fill_price': wire(filled_total),
            'avg_deal_price': wire(filled_total / filled) if filled > 0 else '0',
            'iceberg': wire(row['iceberg']), 'status': row['status'], 'finish_as': row['finish_as'],
            'create_time': str(row['create_time_ms'] // 1000), 'create_time_ms': row['create_time_ms'],
            'update_time': str(row['update_time_ms'] // 1000), 'update_time_ms': row['update_time_ms'],
            'fee': wire(row['fee']), 'fee_currency': self.base if row['side'] == 'buy' else self.quote,
            'point_fee': '0', 'gt_fee': '0', 'rebated_fee': '0', 'rebated_fee_currency': self.quote,
            'stp_act': '-', 'paper': True, 'paper_finish_time_ms': row['finish_time_ms'],
        }
        if row['amend_text']:
            raw['amend_text'] = row['amend_text']
        return parse_order(raw)

    def _venue_trade(self, row: sqlite3.Row) -> VenueTrade:
        ms = int(row['create_time_ms'])
        raw = {
            'id': str(row['trade_id']), 'order_id': str(row['order_id']), 'text': row['text'],
            'currency_pair': self.pair, 'side': row['side'], 'role': 'maker',
            'amount': row['amount'], 'price': row['price'], 'fee': row['fee'], 'fee_currency': row['fee_currency'],
            'point_fee': '0', 'gt_fee': '0', 'create_time': str(ms // 1000), 'create_time_ms': f'{ms}.000000',
            'sequence_id': str(row['trade_id']), 'amend_text': '-', 'paper': True,
            'paper_public_trade_id': row['public_trade_id'],
        }
        return parse_trade(raw)

    def _order_by_id(self, db: sqlite3.Connection, order_id: int) -> dict | None:
        row = db.execute('SELECT * FROM paper_orders WHERE order_id=?', (order_id,)).fetchone()
        return _order_row(row) if row is not None else None

    def _open_by_text(self, db: sqlite3.Connection, text: str) -> dict | None:
        row = db.execute("SELECT * FROM paper_orders WHERE text=? AND status='open' ORDER BY order_id DESC LIMIT 1",
                         (text,)).fetchone()
        return _order_row(row) if row is not None else None

    # ------------------------------------------------------------------ request validation
    def _check_request(self, req: Any) -> None:
        """Local structural validation (live parity): failures mean nothing was 'sent' (NotSent)."""
        if not isinstance(req, PlaceRequest):
            raise ValueError('INVALID_PLACE_REQUEST')
        validate_text(req.text)
        if req.side not in SIDES:
            raise ValueError('INVALID_ORDER_SIDE')
        if req.time_in_force != 'poc':
            raise ValueError('ONLY_POST_ONLY_SUPPORTED')
        if not isinstance(req.price, Decimal) or not isinstance(req.amount, Decimal):
            raise ValueError('ORDER_PRICE_AND_AMOUNT_MUST_BE_DECIMAL')
        price, amount = D(req.price), D(req.amount)
        if price <= 0 or amount <= 0:
            raise ValueError('NONPOSITIVE_ORDER_PRICE_OR_AMOUNT')
        if req.iceberg is not None:
            if not isinstance(req.iceberg, Decimal):
                raise ValueError('ICEBERG_DISPLAY_INVALID')
            iceberg = D(req.iceberg)
            if not 0 < iceberg < amount or not on_grid(iceberg, self.meta.amount_precision):
                raise ValueError('ICEBERG_DISPLAY_INVALID')
        if not side_allowed(self.meta.trade_status, req.side):
            raise Unsafe(f'PAIR_NOT_TRADABLE:{self.meta.trade_status}')

    def _gate_checks(self, price: Decimal, amount: Decimal) -> None:
        """Exchange-side validation like Gate: precision, then minimums/maximums (Rejected)."""
        meta = self.meta
        if not on_grid(price, meta.precision) or not on_grid(amount, meta.amount_precision):
            raise _rejected(INVALID_PRECISION, 'Invalid price or amount precision')
        notional = amount * price
        if ((meta.min_base_amount is not None and amount < meta.min_base_amount)
                or (meta.min_quote_amount is not None and notional < meta.min_quote_amount)):
            raise _rejected(AMOUNT_TOO_LITTLE, 'Order amount below the minimum')
        if ((meta.max_base_amount is not None and amount > meta.max_base_amount)
                or (meta.max_quote_amount is not None and notional > meta.max_quote_amount)):
            raise _rejected(AMOUNT_TOO_MUCH, 'Order amount above the maximum')

    def _free(self, db: sqlite3.Connection) -> dict[str, Decimal]:
        balances = self._balances_db(db)
        holds = self._holds(db)
        return {name: balances[name] - holds[name] for name in balances}

    @staticmethod
    def _next_rank(state: dict) -> int:
        state['last_rank'] = int(state.get('last_rank') or 0) + 1
        return state['last_rank']

    def _create(self, db: sqlite3.Connection, state: dict, req: PlaceRequest, now: int, book: Book | None,
                tape_seq: int) -> VenueOrder:
        """One create. Raises Rejected BEFORE mutating anything (batch items are independent)."""
        price, amount = D(req.price), D(req.amount)
        self._gate_checks(price, amount)
        if db.execute('SELECT 1 FROM paper_orders WHERE text=?', (req.text,)).fetchone():
            raise _rejected(INVALID_PARAM_VALUE, 'Duplicate paper order text')
        currency, need = (self.quote, amount * price) if req.side == 'buy' else (self.base, amount)
        if self._free(db)[currency] < need:
            raise _rejected(BALANCE_NOT_ENOUGH, 'Not enough balance')
        crosses = self._crosses(req.side, price, book)
        state['last_order_id'] = int(state['last_order_id']) + 1
        order_id = state['last_order_id']
        rank = self._next_rank(state)
        status, finish_as, finish_ms = ('cancelled', POST_ONLY_FINISH_AS, now) if crosses else ('open', 'open', None)
        iceberg = D(req.iceberg) if req.iceberg is not None else ZERO
        db.execute('INSERT INTO paper_orders(order_id,text,side,time_in_force,price,amount,left_amount,iceberg,'
                   'status,finish_as,create_time_ms,update_time_ms,finish_time_ms,placement_seq,priority_ms,'
                   'priority_rank) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (order_id, req.text, req.side, req.time_in_force, wire(price), wire(amount), wire(amount),
                    wire(iceberg), status, finish_as, now, now, finish_ms, tape_seq, now, rank))
        if crosses:
            LOG.info('PAPER_POST_ONLY_CANCELLED text=%s side=%s price=%s', req.text, req.side, wire(price))
        return self._venue_order(self._order_by_id(db, order_id))

    # ------------------------------------------------------------------ Venue protocol: metadata / time
    def pair_meta(self) -> PairMeta:
        self.before_read()
        with self._lock:
            self._read_tick()
            return self.meta

    def server_time_ms(self) -> int:
        """Local wall clock (the simulator is its own exchange)."""
        self.before_read()
        with self._lock:
            self._read_tick()
            return _now_ms()

    def account_check(self) -> dict:
        self.before_read()
        with self._lock:
            self._read_tick()
            return {
                'paper': True, 'key_mode': None, 'unified_mode': None, 'unified_mode_error': None,
                'detected_account': self.account, 'configured_account': self.account, 'account_ok': True,
                'pair_whitelist_ok': True, 'pair_whitelist': [], 'ip_whitelist': [], 'tier': None,
                'problems': [], 'currency_pair': self.pair, 'fee_pct': wire(self.fee_pct),
            }

    # ------------------------------------------------------------------ Venue protocol: writes
    def place(self, req: PlaceRequest) -> VenueOrder:
        """Create one post-only order; ``cancelled``/``poc`` reply when it would take (not an error)."""
        try:
            self._check_request(req)
        except Exception as exc:
            raise not_sent(exc) from None
        with self._lock:
            self._write_tick()
            book = self._book()
            tape_seq = self._tape_seq()
            with self.store.write() as db:
                now = _now_ms()
                state = self._state(db)
                order = self._create(db, state, req, now, book, tape_seq)
                _put(db, STATE_KEY, state)
            return order

    def place_batch(self, reqs: Sequence[PlaceRequest]) -> list[PlaceResult]:
        """All items validated first (NotSent if any is malformed); then each is accepted or rejected."""
        reqs = list(reqs)
        if not reqs:
            return []
        try:
            for req in reqs:
                self._check_request(req)
            texts = [req.text for req in reqs]
            if len(set(texts)) != len(texts):
                raise ValueError('DUPLICATE_CLIENT_TEXT')
        except Exception as exc:
            raise not_sent(exc) from None
        with self._lock:
            self._write_tick()
            book = self._book()
            tape_seq = self._tape_seq()
            results: list[PlaceResult] = []
            with self.store.write() as db:
                now = _now_ms()
                state = self._state(db)
                for req in reqs:
                    try:
                        order = self._create(db, state, req, now, book, tape_seq)
                    except Rejected as exc:
                        results.append(PlaceResult(req.text, 'rejected', None, exc.label, str(exc)))
                        continue
                    results.append(PlaceResult(req.text, 'accepted', order, None, None))
                _put(db, STATE_KEY, state)
            return results

    def cancel_many(self, reqs: Sequence[CancelRequest]) -> dict[str, str]:
        """text -> 'sent' (was open, now cancelled) | 'rejected' (finished or unknown) | 'unknown'.

        By ``order_id`` when given, else by ``text`` among OPEN orders only. Never raises on
        simulator failures (all affected items become 'unknown').
        """
        reqs = list(reqs)
        if not reqs:
            return {}
        texts = [getattr(req, 'text', None) for req in reqs]
        if any(not isinstance(text, str) or not text for text in texts) or len(set(texts)) != len(texts):
            raise ValueError('Cancellation batch must have distinct texts')
        result: dict[str, str] = {}
        with self._lock:
            self._tick(safe=True)
            try:
                with self.store.write() as db:
                    now = _now_ms()
                    for req in reqs:
                        try:
                            if not isinstance(req, CancelRequest):
                                raise ValueError('INVALID_CANCEL_REQUEST')
                            order_id = validate_order_id(req.order_id) if req.order_id is not None else None
                            if order_id is None:
                                validate_text(req.text)
                        except Exception:
                            LOG.warning('PAPER_CANCEL_ITEM_INVALID text=%s', req.text)
                            result[req.text] = 'unknown'
                            continue
                        if order_id is not None:
                            row = self._order_by_id(db, int(order_id))
                        else:
                            row = self._open_by_text(db, req.text)
                        if row is None or row['status'] != 'open':
                            result[req.text] = 'rejected'
                            continue
                        self._cancel_one(db, row['order_id'], now)
                        result[req.text] = 'sent'
            except Exception as exc:
                LOG.error('PAPER_CANCEL_FAILED error=%s', type(exc).__name__)
                return {text: 'unknown' for text in texts}
        return {text: result[text] for text in texts}

    def cancel_all_pair(self) -> None:
        """Cancel every open paper order of the pair (cancel-only fallback)."""
        with self._lock:
            self._tick(safe=True)
            with self.store.write() as db:
                count = self._cancel_open(db, _now_ms(), 'cancelled')
            LOG.info('PAPER_CANCEL_ALL cancelled=%s', count)

    def amend(self, order_id: str, text: str, *, price: Decimal | None = None, amount: Decimal | None = None,
              amend_text: str) -> VenueOrder:
        """Price only or amount only; keeps order_id/text; priority is reset (placement = tape now).

        A new price that would take (post-only) -> Rejected POC_FILL_IMMEDIATELY, order unchanged.
        An amount at or below the filled amount cancels the order (Gate semantics).
        """
        try:
            oid = int(validate_order_id(order_id))
            validate_text(text)
            validate_amend_text(amend_text)
            if (price is None) == (amount is None):
                raise ValueError('AMEND_PRICE_OR_AMOUNT_ONLY')
            value = price if price is not None else amount
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError('INVALID_AMEND_VALUE')
        except Exception as exc:
            raise not_sent(exc) from None
        with self._lock:
            self._write_tick()
            book = self._book()
            tape_seq = self._tape_seq()
            with self.store.write() as db:
                now = _now_ms()
                state = self._state(db)
                row = self._order_by_id(db, oid)
                if row is None or row['text'] != text:
                    raise _rejected(ORDER_NOT_FOUND, 'Order not found')
                if row['status'] != 'open':
                    raise _rejected(ORDER_CLOSED if row['status'] == 'closed' else ORDER_CANCELLED,
                                    'Order already finished')
                free = self._free(db)
                filled = row['amount'] - row['left_amount']
                if price is not None:
                    self._gate_checks(price, row['amount'])
                    if row['side'] == 'buy' and row['left_amount'] * (price - row['price']) > free[self.quote]:
                        raise _rejected(BALANCE_NOT_ENOUGH, 'Not enough balance')
                    if self._crosses(row['side'], price, book):
                        raise _rejected(POST_ONLY_LABEL, 'Order would match and take immediately so its cancelled')
                    new_price, new_amount, new_left = price, row['amount'], row['left_amount']
                else:
                    self._gate_checks(row['price'], amount)
                    new_price, new_amount = row['price'], amount
                    new_left = amount - filled
                    if new_left > row['left_amount']:
                        extra = new_left - row['left_amount']
                        currency, need = ((self.quote, extra * row['price']) if row['side'] == 'buy'
                                          else (self.base, extra))
                        if need > free[currency]:
                            raise _rejected(BALANCE_NOT_ENOUGH, 'Not enough balance')
                if new_left <= 0:
                    db.execute("UPDATE paper_orders SET status='cancelled', finish_as='cancelled', amend_text=?, "
                               'update_time_ms=MAX(update_time_ms, ?), finish_time_ms=MAX(update_time_ms, ?) '
                               'WHERE order_id=?', (amend_text, now, now, oid))
                    LOG.info('PAPER_AMEND_CANCELLED order_id=%s', oid)
                else:
                    db.execute('UPDATE paper_orders SET price=?, amount=?, left_amount=?, amend_text=?, '
                               'update_time_ms=MAX(update_time_ms, ?), placement_seq=?, priority_ms=?, '
                               'priority_rank=? WHERE order_id=?',
                               (wire(new_price), wire(new_amount), wire(new_left), amend_text, now, tape_seq, now,
                                self._next_rank(state), oid))
                    _put(db, STATE_KEY, state)
                    LOG.info('PAPER_AMEND order_id=%s price=%s amount=%s', oid, wire(new_price), wire(new_amount))
                return self._venue_order(self._order_by_id(db, oid))

    def countdown(self, timeout_seconds: int) -> int | None:
        """Paper countdown_cancel_all: deadline = now + timeout; 0 disarms (returns None)."""
        t = timeout_seconds
        if not _is_int(t) or not (t == 0 or DMS_MIN_TIMEOUT <= t <= DMS_MAX_TIMEOUT):
            raise NotSentValueError('INVALID_DMS_TIMEOUT')
        with self._lock:
            self._write_tick()
            with self.store.write() as db:
                now = _now_ms()
                state = self._state(db)
                deadline = now + t * 1000 if t > 0 else 0
                state['dms_deadline_ms'] = deadline
                _put(db, STATE_KEY, state)
            return deadline if t > 0 else None

    # ------------------------------------------------------------------ Venue protocol: reads
    def open_orders(self) -> list[VenueOrder]:
        self.before_read()
        with self._lock:
            self._read_tick()
            with self.store.read() as db:
                rows = [_order_row(r) for r in
                        db.execute("SELECT * FROM paper_orders WHERE status='open' ORDER BY order_id")]
            return [self._venue_order(row) for row in rows]

    def get_order(self, *, order_id: str | None = None, text: str | None = None) -> VenueOrder | None:
        """By order_id (any state) or by text (OPEN orders only, like Gate); None = not found."""
        try:
            if order_id is None and text is None:
                raise ValueError('ORDER_ID_OR_TEXT_REQUIRED')
            oid = int(validate_order_id(order_id)) if order_id is not None else None
            txt = validate_text(text) if text is not None else None
        except Exception as exc:
            raise not_sent(exc) from None
        self.before_read()
        with self._lock:
            self._read_tick()
            with self.store.read() as db:
                row = self._order_by_id(db, oid) if oid is not None else self._open_by_text(db, txt)
            if row is None:
                return None
            order = self._venue_order(row)
            if oid is not None and txt is not None and order.text != txt:
                raise Unsafe('ORDER_TEXT_MISMATCH')
            return order

    def finished_orders(self, from_ms: int, to_ms: int) -> list[VenueOrder]:
        """Closed/cancelled orders with ``from_ms <= finish time <= to_ms``."""
        try:
            from_ms, to_ms = _window(from_ms, to_ms)
        except ValueError as exc:
            raise not_sent(exc) from None
        self.before_read()
        with self._lock:
            self._read_tick()
            with self.store.read() as db:
                rows = [_order_row(r) for r in db.execute(
                    "SELECT * FROM paper_orders WHERE status IN ('closed','cancelled') "
                    'AND finish_time_ms BETWEEN ? AND ?', (from_ms, to_ms))]
            orders = [self._venue_order(row) for row in rows]
            return sorted(orders, key=lambda o: (o.update_time_ms, int(o.order_id)))

    def my_trades(self, from_ms: int, to_ms: int) -> list[VenueTrade]:
        """Own paper fills with ``from_ms <= create_time_ms <= to_ms`` (role maker)."""
        try:
            from_ms, to_ms = _window(from_ms, to_ms)
        except ValueError as exc:
            raise not_sent(exc) from None
        self.before_read()
        with self._lock:
            self._read_tick()
            with self.store.read() as db:
                rows = db.execute('SELECT * FROM paper_trades WHERE create_time_ms BETWEEN ? AND ?',
                                  (from_ms, to_ms)).fetchall()
                trades = [self._venue_trade(row) for row in rows]
            return sorted(trades, key=trade_sort_key)

    def account_open_orders(self) -> list[VenueOrder]:
        """Open orders of OTHER pairs: the simulated account trades one pair only."""
        return []

    def account_trades(self, from_ms: int, to_ms: int) -> list[VenueTrade]:
        """Trades of OTHER pairs in the window: none in the simulated account."""
        try:
            _window(from_ms, to_ms)
        except ValueError as exc:
            raise not_sent(exc) from None
        return []

    def balances(self) -> dict[str, Balance]:
        """{base, quote}: total = available + locked; hold = locked by open orders."""
        self.before_read()
        with self._lock:
            self._read_tick()
            with self.store.read() as db:
                totals = self._balances_db(db)
                holds = self._holds(db)
            return {name: Balance(total=totals[name], hold=holds[name]) for name in (self.base, self.quote)}

    # ------------------------------------------------------------------ extras (not in the protocol)
    def process_trades(self) -> list[VenueTrade]:
        """Apply pending public trades (and the lazy DMS) now; returns the new paper fills."""
        with self._lock:
            ids = self._tick()
            if not ids:
                return []
            with self.store.read() as db:
                marks = ','.join('?' for _ in ids)
                rows = db.execute(f'SELECT * FROM paper_trades WHERE trade_id IN ({marks})', ids).fetchall()
                trades = [self._venue_trade(row) for row in rows]
            return sorted(trades, key=trade_sort_key)

    def diagnostics(self) -> dict:
        """JSON-safe local view for status (no tape processing, no side effects)."""
        try:
            with self.store.read() as db:
                state = _get(db, STATE_KEY) or {}
                balances = _get(db, BALANCES_KEY) or {}
                open_count = db.execute("SELECT COUNT(*) FROM paper_orders WHERE status='open'").fetchone()[0]
            return json_safe({
                'paper': True, 'fill_model': 'trade_through', 'fee_pct': self.fee_pct,
                'balances': balances, 'open_orders': open_count, 'tape_seq': state.get('tape_seq'),
                'dms_deadline_ms': state.get('dms_deadline_ms'), 'last_order_id': state.get('last_order_id'),
                'last_trade_id': state.get('last_trade_id'),
                'limitations': 'queue position and iceberg hidden priority are not modelled',
            })
        except Exception as exc:
            return {'paper': True, 'error': type(exc).__name__}

    def close(self) -> None:
        """The store belongs to the caller; nothing to release."""
        return None
