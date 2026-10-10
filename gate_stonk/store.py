"""Durable SQLite state: slot reservations, execution ledger, outbox, index samples (DESIGN §9).

Durability rules (ported from the reference store):

* WAL journal, ``synchronous=FULL``; every write is one ``BEGIN IMMEDIATE`` transaction
  under an ``RLock`` (the read/check/write sequence is also safe against a second
  connection to the same file). Money, prices and quantities are stored as decimal TEXT
  and summed in Python ``Decimal`` (never REAL, never SQL ``SUM()``).
* An order row (keyed by its client ``text``) is persisted BEFORE anything is sent. One
  active order per ladder slot ``(side, level)`` is enforced by the unique partial index
  ``one_active_slot``. Only the engine releases a reservation (``close_order``) after
  status, fills and absence agree.
* Each execution, its ``orders.filled_qty`` update, the position cache, the durable
  "position snapshot owed" flag and its Telegram outbox event commit in ONE transaction
  (one SAVEPOINT per trade). A failing trade rolls back only itself: the accepted prefix
  of the batch is committed, then the error is raised.
* Executions are deduplicated by ``fill_key`` and by Gate trade id; the same identity with
  different content is ``FILL_IDENTITY_CONFLICT`` (never overwritten or double counted).
* The position is a cache of a pure replay of the ``fills`` table in exchange execution
  order; a late-visible earlier fill triggers a full replay and a correction message.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .dec import ZERO, D, fmt, on_grid, step
from .errors import Unsafe
from .ledger import (FillEffect, Position, apply_fill_effect, apply_transfer_out, edge_text, money, pct,
                     replay_effects, signed, tid_number)
from .marks import MARK_BRACKET_MS, Mark, mark_from_samples
from .types import SIDES, IndexSnapshot, VenueOrder, VenueTrade

LOG = logging.getLogger(__name__)

SCHEMA_VERSION = 1
POSITIONS_ORDER = 'execution_v1'
ACTIVE_STATES = frozenset({'reserved', 'open', 'unknown', 'cancel_pending'})
FINAL_STATES = frozenset({'closed', 'rejected'})
AMEND_PENDING = frozenset({'sent', 'unknown'})
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
FOREIGN_GRACE_MS = 2000          # foreign trade this close before start_ms still counts as campaign activity
HOST_AHEAD_MS = 10_000           # host clock may run up to 10 s ahead of the exchange
EXCHANGE_AHEAD_MS = 2000         # exchange clock may run up to 2 s ahead of the host
COMPONENTS_INTERVAL_MS = 1000    # index_components at most once per second
OWN_LEVEL_GRACE_MS = 2000        # recently closed orders still counted in own_levels
OUTBOX_MAX_DELAY_S = 60

TEXT_RE = re.compile(r't-[0-9A-Za-z_.-]{1,28}')
LEVEL_RE = re.compile(r'[a-z0-9]{1,4}')
ORDER_ID_RE = re.compile(r'[0-9]{1,32}')
TRADE_ID_RE = re.compile(r'[0-9A-Za-z_.:-]{1,64}')
_ACTIVE_SQL = "('reserved','open','unknown','cancel_pending')"

DECIMAL_ORDER_FIELDS = ('qty', 'price', 'iceberg', 'index_price', 'center_price', 'filled_qty',
                        'amend_price', 'amend_qty')
DECIMAL_FILL_FIELDS = ('qty', 'price', 'notional', 'fee', 'point_fee', 'gt_fee')

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders(
    text TEXT PRIMARY KEY,
    side TEXT NOT NULL CHECK(side IN ('buy','sell')),
    level TEXT NOT NULL,
    order_id TEXT UNIQUE,
    qty TEXT NOT NULL, price TEXT NOT NULL, iceberg TEXT,
    index_price TEXT NOT NULL, center_price TEXT NOT NULL,
    created_ms INTEGER NOT NULL, reason TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('reserved','open','unknown','cancel_pending','closed','rejected')),
    filled_qty TEXT NOT NULL DEFAULT '0',
    cancel_reason TEXT,
    cancel_attempts INTEGER NOT NULL DEFAULT 0,
    next_cancel_ms INTEGER NOT NULL DEFAULT 0,
    last_cancel_ms INTEGER NOT NULL DEFAULT 0,
    cancel_ack INTEGER NOT NULL DEFAULT 0,
    terminal_status TEXT, finish_as TEXT, terminal_since_ms INTEGER,
    terminal_observations INTEGER NOT NULL DEFAULT 0,
    last_terminal_ms INTEGER,
    amend_state TEXT CHECK(amend_state IS NULL OR amend_state IN ('sent','unknown')),
    amend_price TEXT, amend_qty TEXT, amend_text TEXT, amend_sent_ms INTEGER,
    prior_prices TEXT NOT NULL DEFAULT '[]',
    diagnostics TEXT,
    sent_ms INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_slot ON orders(side, level) WHERE state IN {_ACTIVE_SQL};
CREATE INDEX IF NOT EXISTS orders_state ON orders(state);
CREATE INDEX IF NOT EXISTS orders_created ON orders(created_ms);
CREATE TABLE IF NOT EXISTS fills(
    fill_key TEXT PRIMARY KEY, trade_id TEXT NOT NULL UNIQUE, order_id TEXT NOT NULL,
    text TEXT NOT NULL, currency_pair TEXT NOT NULL, side TEXT NOT NULL, level TEXT NOT NULL,
    qty TEXT NOT NULL, price TEXT NOT NULL, notional TEXT NOT NULL,
    fee TEXT NOT NULL, fee_currency TEXT NOT NULL, point_fee TEXT NOT NULL, gt_fee TEXT NOT NULL,
    role TEXT, sequence_id TEXT, time_ms INTEGER NOT NULL, recorded_ms INTEGER NOT NULL, raw TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS fills_text ON fills(text);
CREATE INDEX IF NOT EXISTS fills_side_time ON fills(side, time_ms);
CREATE INDEX IF NOT EXISTS fills_time ON fills(time_ms);
CREATE TABLE IF NOT EXISTS positions(pair TEXT PRIMARY KEY, json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS external_fills(
    fill_key TEXT PRIMARY KEY, trade_id TEXT NOT NULL, order_id TEXT NOT NULL,
    time_ms INTEGER NOT NULL, recorded_ms INTEGER NOT NULL, raw TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS external_orders(
    order_id TEXT PRIMARY KEY, first_seen_ms INTEGER NOT NULL, last_seen_ms INTEGER NOT NULL, raw TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ledger_transfers(
    id TEXT PRIMARY KEY, pair TEXT NOT NULL, time_ms INTEGER NOT NULL,
    qty TEXT NOT NULL, cost TEXT NOT NULL, balance TEXT NOT NULL, note TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS order_rebates(
    order_id TEXT PRIMARY KEY, text TEXT, rebated_fee TEXT NOT NULL, rebated_fee_currency TEXT,
    recorded_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(
    key TEXT PRIMARY KEY, text TEXT NOT NULL, created_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, next_ms INTEGER NOT NULL DEFAULT 0,
    sent_ms INTEGER, last_error TEXT, status TEXT NOT NULL DEFAULT 'pending');
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(sent_ms) WHERE sent_ms IS NULL;
CREATE TABLE IF NOT EXISTS index_obs(
    ts_ms INTEGER NOT NULL, "index" TEXT, center TEXT, ok INTEGER NOT NULL, reason TEXT,
    spot_bid TEXT, spot_ask TEXT);
CREATE INDEX IF NOT EXISTS index_obs_ts ON index_obs(ts_ms);
CREATE TABLE IF NOT EXISTS index_components(ts_ms INTEGER NOT NULL, json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS index_components_ts ON index_components(ts_ms);
CREATE TABLE IF NOT EXISTS fill_index_marks(
    fill_key TEXT NOT NULL, horizon_s INTEGER NOT NULL, index_price TEXT NOT NULL,
    sample_ms INTEGER NOT NULL, PRIMARY KEY(fill_key, horizon_s));
CREATE TABLE IF NOT EXISTS actions(kind TEXT NOT NULL, ts_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS actions_ts ON actions(ts_ms);
"""


# ----------------------------------------------------------------------------- helpers

def _integer(value: Any, name: str, *, nullable: bool = False) -> int | None:
    """Non-negative integer; rejects bool, fractions and garbage (ValueError)."""
    if nullable and value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f'Invalid {name}')
    try:
        result = int(value)
        if D(value) != result or result < 0:
            raise ValueError(f'Invalid {name}')
        return result
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(f'Invalid {name}') from exc


def _positive(value: Any, name: str) -> Decimal:
    result = D(value)
    if result <= 0:
        raise ValueError(f'{name} must be positive')
    return result


def _optional_text(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f'Invalid {name}')
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (tuple, set, frozenset)):
        return list(value)
    raise TypeError(f'Not JSON serializable: {type(value).__name__}')


def _dumps(value: Any, **kwargs: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, default=_json_default, **kwargs)


def _canonical(value: Any) -> Any:
    """JSON round trip so tuples/Decimals compare like their stored form."""
    return json.loads(_dumps(value, sort_keys=True))


def utc_day_start_ms(now_ms: int) -> int:
    """UTC midnight at or before ``now_ms`` (daily caps)."""
    now_ms = _integer(now_ms, 'now_ms')
    return now_ms - now_ms % DAY_MS


def utc_text(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def prior_prices_since(prior_prices: Any, since_ms: int) -> set[Decimal]:
    """Previous prices of an amended order (``orders.prior_prices``: JSON text or parsed
    ``[(price, until_ms)]``) that it left at or after ``since_ms``."""
    entries = json.loads(prior_prices) if isinstance(prior_prices, str) else (prior_prices or ())
    return {D(price) for price, until in entries if int(until) >= int(since_ms)}


def fill_key_for(currency_pair: str, order_id: str, trade_id: str, create_time_ms: int) -> str:
    """sha256 of the JSON identity ``[pair, order_id, trade_id, create_time_ms]``."""
    data = json.dumps([currency_pair, order_id, trade_id, int(create_time_ms)], separators=(',', ':'))
    return hashlib.sha256(data.encode()).hexdigest()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clean_error(error: Any) -> str:
    text = ' '.join(str(error).split()) if error is not None else ''
    return text[:200]


# ----------------------------------------------------------------------------- store

class Store:
    """SQLite persistence for one campaign (one Gate spot pair)."""

    def __init__(self, path: str | Path):
        memory = str(path) == ':memory:'
        self.path: Path | str = ':memory:' if memory else Path(path)
        if not memory:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), timeout=10, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            mode = str(self.db.execute('PRAGMA journal_mode=WAL').fetchone()[0]).lower()
            if mode != 'wal' and not memory:
                raise Unsafe('SQLITE_WAL_UNAVAILABLE')
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.executescript(SCHEMA)
            self._migrate()
        except BaseException:
            self.db.close()
            raise
        self._last_components_ms: int | None = None

    def _migrate(self) -> None:
        """Additive, idempotent column migrations of databases created by older versions.

        ``orders.sent_ms`` (wall-clock time stamped right before the create request that
        carries the row was sent): NULL for rows written before the column existed; the
        unknown-create expiry then falls back to ``created_ms`` exactly as before.
        """
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(orders)')}
        if 'sent_ms' not in columns:
            self.db.execute('ALTER TABLE orders ADD COLUMN sent_ms INTEGER')

    # ----- transactions -----
    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self.lock:
            if self.db.in_transaction:
                raise RuntimeError('Nested ledger transaction')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                yield self.db
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute('ROLLBACK')
                raise
            try:
                self.db.execute('COMMIT')
            except BaseException:
                if self.db.in_transaction:
                    self.db.execute('ROLLBACK')
                raise

    def write(self):
        """Public ``BEGIN IMMEDIATE`` transaction (e.g. for the paper venue's own tables)."""
        return self._write()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        """The connection under the store lock (consistent reads across statements)."""
        with self.lock:
            yield self.db

    def ensure_tables(self, ddl: str) -> None:
        """Run ``CREATE ... IF NOT EXISTS`` DDL owned by another module (paper venue, watch)."""
        with self.lock:
            if self.db.in_transaction:
                raise RuntimeError('Nested ledger transaction')
            self.db.executescript(ddl)

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def __enter__(self) -> 'Store':
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ----- meta -----
    def meta(self, key: str, default: Any = None) -> Any:
        with self.lock:
            row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
            return json.loads(row[0]) if row else default

    def _set_meta_sql(self, key: str, value: Any) -> None:
        self.db.execute('INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)', (key, _dumps(value)))

    def set_meta(self, key: str, value: Any) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError('Invalid meta key')
        _dumps(value)  # validate before the transaction
        with self._write():
            self._set_meta_sql(key, value)

    def delete_meta(self, key: str) -> None:
        with self._write():
            self.db.execute('DELETE FROM meta WHERE key=?', (key,))

    # ----- identity -----
    def initialize(self, identity: dict, start_ms: int) -> None:
        """Bind the database to one campaign; never reset an existing one."""
        if not isinstance(identity, dict) or not identity:
            raise ValueError('Identity must be a nonempty mapping')
        start_ms = _integer(start_ms, 'start_ms')
        identity = _canonical(identity)
        with self._write():
            old = self.meta('identity')
            if old is not None:
                if old != identity or self.meta('schema') != SCHEMA_VERSION:
                    raise Unsafe('DATABASE_IDENTITY_MISMATCH_DO_NOT_RESET')
                return
            if (self.db.execute('SELECT 1 FROM orders LIMIT 1').fetchone()
                    or self.db.execute('SELECT 1 FROM fills LIMIT 1').fetchone()):
                raise Unsafe('DATABASE_IDENTITY_MISSING_DO_NOT_RESET')
            self._set_meta_sql('identity', identity)
            self._set_meta_sql('schema', SCHEMA_VERSION)
            self._set_meta_sql('start_ms', start_ms)
            self._set_meta_sql('last_sync_ms', start_ms)

    def identity(self) -> dict | None:
        return self.meta('identity')

    def start_ms(self) -> int | None:
        value = self.meta('start_ms')
        return None if value is None else int(value)

    def _require_identity(self) -> dict:
        identity = self.meta('identity')
        if identity is None:
            raise Unsafe('DATABASE_NOT_INITIALIZED')
        return identity

    # ----- orders -----
    @staticmethod
    def _order(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        result = dict(row)
        for name in DECIMAL_ORDER_FIELDS:
            if result[name] is not None:
                result[name] = D(result[name])
        result['cancel_ack'] = bool(result['cancel_ack'])
        result['prior_prices'] = [(D(price), int(until)) for price, until in json.loads(result['prior_prices'])]
        result['diagnostics'] = json.loads(result['diagnostics']) if result['diagnostics'] else None
        result['slot'] = f"{result['side']}:{result['level']}"
        return result

    def order(self, text: str) -> dict | None:
        with self.lock:
            return self._order(self.db.execute('SELECT * FROM orders WHERE text=?', (text,)).fetchone())

    def order_by_id(self, order_id: str) -> dict | None:
        with self.lock:
            return self._order(self.db.execute('SELECT * FROM orders WHERE order_id=?', (str(order_id),)).fetchone())

    def orders(self, active_only: bool = True) -> list[dict]:
        query = 'SELECT * FROM orders'
        if active_only:
            query += f' WHERE state IN {_ACTIVE_SQL}'
        with self.lock:
            return [self._order(row) for row in self.db.execute(query + ' ORDER BY created_ms, rowid')]

    def slot_order(self, side: str, level: str) -> dict | None:
        """The active order of one ladder slot, if any."""
        with self.lock:
            return self._order(self.db.execute(
                f'SELECT * FROM orders WHERE side=? AND level=? AND state IN {_ACTIVE_SQL}',
                (side, level)).fetchone())

    def possible_open_order_count(self) -> int:
        """Active orders whose cancel was not acknowledged (venue slots for max_open_orders)."""
        return sum(not o['cancel_ack'] for o in self.orders())

    def reserve_order(self, *, text: str, side: str, level: str, qty, price, now_ms: int, reason: str,
                      index_price, center_price, iceberg=None, diagnostics: dict | None = None) -> dict:
        """Durable reservation of a slot BEFORE anything is sent (state ``reserved``)."""
        if not isinstance(text, str) or not TEXT_RE.fullmatch(text):
            raise ValueError('Invalid order text')
        if side not in SIDES:
            raise ValueError('Invalid order side')
        if not isinstance(level, str) or not LEVEL_RE.fullmatch(level):
            raise ValueError('Invalid order level')
        qty, price = _positive(qty, 'qty'), _positive(price, 'price')
        index_price, center_price = _positive(index_price, 'index_price'), _positive(center_price, 'center_price')
        if iceberg is not None:
            iceberg = _positive(iceberg, 'iceberg')
            if iceberg >= qty:
                raise ValueError('Iceberg visible amount must be below the order amount')
        if not isinstance(reason, str) or not reason:
            raise ValueError('Order reason is required')
        now_ms = _integer(now_ms, 'now_ms')
        encoded = _dumps(diagnostics) if diagnostics is not None else None
        with self._write():
            self._require_identity()
            if self.db.execute(f'SELECT 1 FROM orders WHERE side=? AND level=? AND state IN {_ACTIVE_SQL}',
                               (side, level)).fetchone():
                raise Unsafe('UNRESOLVED_ORDER_RESERVATION')
            if self.db.execute('SELECT 1 FROM orders WHERE text=?', (text,)).fetchone():
                raise Unsafe('DUPLICATE_ORDER_TEXT')
            try:
                self.db.execute(
                    'INSERT INTO orders(text,side,level,qty,price,iceberg,index_price,center_price,created_ms,'
                    "reason,state,diagnostics) VALUES(?,?,?,?,?,?,?,?,?,?,'reserved',?)",
                    (text, side, level, str(qty), str(price), None if iceberg is None else str(iceberg),
                     str(index_price), str(center_price), now_ms, reason, encoded))
            except sqlite3.IntegrityError as exc:
                raise Unsafe('DUPLICATE_ORDER_ID_OR_ACTIVE_SLOT') from exc
            return self.order(text)

    _UPDATABLE = frozenset({'order_id', 'state', 'filled_qty', 'cancel_reason', 'cancel_attempts', 'next_cancel_ms',
                            'last_cancel_ms', 'cancel_ack', 'terminal_status', 'finish_as', 'terminal_since_ms',
                            'terminal_observations', 'last_terminal_ms'})

    def update_order(self, text: str, **fields: Any) -> None:
        """Reference invariants: final rows never reopen, order ids never move, filled_qty
        only mirrors the execution ledger. Final states are set by ``close_order`` only."""
        if not fields or not fields.keys() <= self._UPDATABLE:
            raise ValueError('Invalid order update')
        values = dict(fields)
        if 'state' in values and values['state'] not in ACTIVE_STATES:
            raise ValueError('Invalid order state (final states only via close_order)')
        if 'order_id' in values:
            if not isinstance(values['order_id'], str) or not ORDER_ID_RE.fullmatch(values['order_id']):
                raise ValueError('Invalid order_id')
        for name in ('cancel_attempts', 'next_cancel_ms', 'last_cancel_ms', 'terminal_observations'):
            if name in values:
                values[name] = _integer(values[name], name)
        for name in ('terminal_since_ms', 'last_terminal_ms'):
            if name in values:
                values[name] = _integer(values[name], name, nullable=True)
        for name in ('cancel_reason', 'terminal_status', 'finish_as'):
            if name in values:
                values[name] = _optional_text(values[name], name)
        if 'cancel_ack' in values:
            if not isinstance(values['cancel_ack'], bool):
                raise ValueError('cancel_ack must be boolean')
            values['cancel_ack'] = int(values['cancel_ack'])
        if 'filled_qty' in values:
            values['filled_qty'] = D(values['filled_qty'])
        with self._write():
            old = self.order(text)
            if old is None:
                raise Unsafe('MISSING_LOCAL_ORDER')
            if old['state'] in FINAL_STATES and values.get('state', old['state']) != old['state']:
                raise Unsafe('TERMINAL_ORDER_CANNOT_REOPEN')
            if 'order_id' in values and old['order_id'] is not None and values['order_id'] != old['order_id']:
                raise Unsafe('ORDER_ID_ALREADY_BOUND')
            if 'filled_qty' in values:
                if values['filled_qty'] != self.fill_qty(text):
                    raise Unsafe('FILLED_QUANTITY_REQUIRES_EXECUTION_LEDGER')
                values['filled_qty'] = str(values['filled_qty'])
            try:
                self.db.execute('UPDATE orders SET ' + ','.join(f'{key}=?' for key in values) + ' WHERE text=?',
                                list(values.values()) + [text])
            except sqlite3.IntegrityError as exc:
                raise Unsafe('DUPLICATE_ORDER_ID_OR_ACTIVE_SLOT') from exc

    def close_order(self, text: str, status: str, now_ms: int, *, finish_as: str | None = None) -> None:
        """Release only after the engine confirms status + fills + absence agree."""
        now_ms = _integer(now_ms, 'now_ms')
        if not isinstance(status, str) or not status:
            raise ValueError('A terminal status is required')
        finish_as = _optional_text(finish_as, 'finish_as')
        with self._write():
            row = self.order(text)
            if row is None:
                raise Unsafe('MISSING_LOCAL_ORDER')
            if row['state'] in FINAL_STATES:
                return
            rejected = status.lower() in {'rejected', 'reject'}
            if rejected and (row['filled_qty'] or self.fill_qty(text)):
                raise Unsafe('FILLED_ORDER_CANNOT_BE_REJECTED')
            self.db.execute('UPDATE orders SET state=?,terminal_status=?,finish_as=COALESCE(?,finish_as),'
                            'terminal_since_ms=COALESCE(terminal_since_ms,?),last_terminal_ms=? WHERE text=?',
                            ('rejected' if rejected else 'closed', status, finish_as, now_ms, now_ms, text))
            self.db.execute('DELETE FROM meta WHERE key=?', ('terminal_fp:' + text,))

    def mark_sent(self, texts: Sequence[str], now_ms: int) -> None:
        """Stamp ``sent_ms`` on reserved rows right BEFORE the create request carrying them
        is sent (one transaction per request). The unknown-create expiry is measured from
        this stamp: the request is signed after it, so its x-gate-exptime is never earlier
        than ``sent_ms + request_expiry``. Only still-unsent ``reserved`` rows qualify."""
        now_ms = _integer(now_ms, 'now_ms')
        texts = list(texts)
        if not texts or any(not isinstance(text, str) or not text for text in texts) or len(set(texts)) != len(texts):
            raise ValueError('Invalid order texts')
        with self._write():
            for text in texts:
                row = self.db.execute('SELECT state,sent_ms FROM orders WHERE text=?', (text,)).fetchone()
                if row is None:
                    raise Unsafe('MISSING_LOCAL_ORDER')
                if row['state'] != 'reserved' or row['sent_ms'] is not None:
                    raise Unsafe('ORDER_ALREADY_SENT')
                self.db.execute('UPDATE orders SET sent_ms=? WHERE text=?', (now_ms, text))

    def set_order_diagnostics(self, text: str, data: dict) -> None:
        encoded = _dumps(data)
        with self._write():
            if self.db.execute('UPDATE orders SET diagnostics=? WHERE text=?', (encoded, text)).rowcount != 1:
                raise Unsafe('MISSING_LOCAL_ORDER')

    # ----- amend (Gate PATCH keeps the order_id) -----
    def begin_amend(self, text: str, *, price, qty=None, amend_text: str, now_ms: int) -> dict:
        """Record an amend BEFORE it is sent (``amend_state='sent'``)."""
        price = _positive(price, 'amend price')
        qty = None if qty is None else _positive(qty, 'amend qty')
        if not isinstance(amend_text, str) or not 0 < len(amend_text) <= 31:
            raise ValueError('Invalid amend_text')
        now_ms = _integer(now_ms, 'now_ms')
        with self._write():
            row = self.order(text)
            if row is None:
                raise Unsafe('MISSING_LOCAL_ORDER')
            if row['state'] != 'open' or row['order_id'] is None:
                raise Unsafe('AMEND_REQUIRES_OPEN_BOUND_ORDER')
            if row['amend_state'] is not None:
                raise Unsafe('AMEND_ALREADY_PENDING')
            if qty is not None and qty < row['filled_qty']:
                raise Unsafe('AMEND_QTY_BELOW_FILLED')
            self.db.execute("UPDATE orders SET amend_state='sent',amend_price=?,amend_qty=?,amend_text=?,"
                            'amend_sent_ms=? WHERE text=?',
                            (str(price), None if qty is None else str(qty), amend_text, now_ms, text))
            return self.order(text)

    def amend_unknown(self, text: str) -> None:
        """The amend reply was lost: the order may rest at either price until resolved."""
        with self._write():
            row = self.order(text)
            if row is None:
                raise Unsafe('MISSING_LOCAL_ORDER')
            if row['amend_state'] not in AMEND_PENDING:
                raise Unsafe('NO_PENDING_AMEND')
            self.db.execute("UPDATE orders SET amend_state='unknown' WHERE text=?", (text,))

    def confirm_amend(self, text: str, now_ms: int, *, price=None, qty=None) -> dict:
        """Apply a confirmed amend: the order's price/qty become the amended values.

        ``price``/``qty`` default to the recorded amend values (pass the exchange's values
        when known). The old price stays acceptable for fills executed before the amend
        (exchange time <= confirmation + 2 s)."""
        now_ms = _integer(now_ms, 'now_ms')
        with self._write():
            row = self.order(text)
            if row is None:
                raise Unsafe('MISSING_LOCAL_ORDER')
            if row['amend_state'] not in AMEND_PENDING:
                raise Unsafe('NO_PENDING_AMEND')
            if row['state'] in FINAL_STATES:
                raise Unsafe('TERMINAL_ORDER_CANNOT_REOPEN')
            new_price = _positive(price, 'price') if price is not None else (row['amend_price'] or row['price'])
            new_qty = _positive(qty, 'qty') if qty is not None else (row['amend_qty'] or row['qty'])
            if new_qty < self.fill_qty(text):
                raise Unsafe('AMEND_QTY_BELOW_FILLED')
            prior = [[str(p), until] for p, until in row['prior_prices']]
            if new_price != row['price']:
                prior.append([str(row['price']), now_ms])
            self.db.execute('UPDATE orders SET price=?,qty=?,prior_prices=?,amend_state=NULL,amend_price=NULL,'
                            'amend_qty=NULL,amend_text=NULL,amend_sent_ms=NULL WHERE text=?',
                            (str(new_price), str(new_qty), json.dumps(prior), text))
            return self.order(text)

    def clear_amend(self, text: str) -> None:
        """Forget an amend proven not applied (rejected / not sent). No-op if none."""
        with self._write():
            if self.db.execute('UPDATE orders SET amend_state=NULL,amend_price=NULL,amend_qty=NULL,'
                               'amend_text=NULL,amend_sent_ms=NULL WHERE text=?', (text,)).rowcount != 1:
                raise Unsafe('MISSING_LOCAL_ORDER')

    # ----- own volume -----
    def own_levels(self, side: str, snapshot_ms: int) -> dict[Decimal, Decimal]:
        """{price: ORIGINAL qty} of own active orders plus closed (not rejected) orders whose
        first terminal observation is within 2 s of the book snapshot. An order with an
        unresolved amend is counted at both prices, and an amended order also at every
        previous price it left within 2 s of the snapshot (book lag; conservative own-volume
        exclusion)."""
        snapshot_ms = _integer(snapshot_ms, 'snapshot_ms')
        since = snapshot_ms - OWN_LEVEL_GRACE_MS
        with self.lock:
            rows = self.db.execute(
                f'SELECT price,qty,amend_state,amend_price,prior_prices FROM orders WHERE side=? AND '
                f"(state IN {_ACTIVE_SQL} OR (state='closed' AND terminal_since_ms>=?))",
                (side, since)).fetchall()
        result: dict[Decimal, Decimal] = {}
        for row in rows:
            qty = D(row['qty'])
            prices = {D(row['price'])}
            if row['amend_state'] in AMEND_PENDING and row['amend_price'] is not None:
                prices.add(D(row['amend_price']))
            prices.update(prior_prices_since(row['prior_prices'], since))
            for price in prices:
                result[price] = result.get(price, ZERO) + qty
        return result

    # ----- fills -----
    def fill_qty(self, text: str) -> Decimal:
        with self.lock:
            return sum((D(row[0]) for row in self.db.execute('SELECT qty FROM fills WHERE text=?', (text,))), ZERO)

    @staticmethod
    def _fill(row: sqlite3.Row) -> dict:
        result = dict(row)
        for name in DECIMAL_FILL_FIELDS:
            result[name] = D(result[name])
        result['raw'] = json.loads(result['raw'])
        return result

    def fills(self, since_ms: int | None = None, *, text: str | None = None) -> list[dict]:
        """Recorded executions in execution order (time_ms, numeric trade id, fill_key)."""
        clauses, params = [], []
        if since_ms is not None:
            clauses.append('time_ms>=?')
            params.append(_integer(since_ms, 'since_ms'))
        if text is not None:
            clauses.append('text=?')
            params.append(text)
        query = 'SELECT * FROM fills' + (' WHERE ' + ' AND '.join(clauses) if clauses else '')
        with self.lock:
            rows = [self._fill(row) for row in self.db.execute(query, params)]
        rows.sort(key=lambda f: (f['time_ms'], tid_number(f['trade_id']), f['fill_key']))
        return rows

    def count_fills_since(self, time_ms: int) -> int:
        time_ms = _integer(time_ms, 'time_ms')
        with self.lock:
            return int(self.db.execute('SELECT COUNT(*) FROM fills WHERE time_ms>=?', (time_ms,)).fetchone()[0])

    def totals(self, side: str, since_ms: int | None = None) -> tuple[Decimal, Decimal]:
        """Gross (qty, notional) of own fills of a side, optionally since ``since_ms``
        (daily caps: ``utc_day_start_ms(now_ms)``). Summed in Decimal, fees excluded."""
        if side not in SIDES:
            raise ValueError('Invalid fill side')
        query, params = 'SELECT qty,notional FROM fills WHERE side=?', [side]
        if since_ms is not None:
            query += ' AND time_ms>=?'
            params.append(_integer(since_ms, 'since_ms'))
        qty = notional = ZERO
        with self.lock:
            for row in self.db.execute(query, params):
                qty += D(row['qty'])
                notional += D(row['notional'])
        return qty, notional

    # ----- position cache -----
    def _ensure_currencies(self, base: str, quote: str) -> str:
        for value, name in ((base, 'base'), (quote, 'quote')):
            if not isinstance(value, str) or not re.fullmatch(r'[A-Z0-9]{1,20}', value):
                raise ValueError(f'Invalid {name} currency')
        if base == quote:
            raise ValueError('Base and quote must differ')
        pair = f'{base}_{quote}'
        identity = self._require_identity()
        expected = identity.get('currency_pair')
        if expected is not None and expected != pair:
            raise Unsafe('LEDGER_PAIR_MISMATCH')
        stored = self.meta('ledger_currencies')
        wanted = {'pair': pair, 'base': base, 'quote': quote}
        if stored is None:
            self._set_meta_sql('ledger_currencies', wanted)
        elif stored != wanted:
            raise Unsafe('LEDGER_CURRENCY_MISMATCH')
        return pair

    def _replay_rows(self) -> tuple[list[dict], list[dict]]:
        fills = [dict(r) for r in self.db.execute(
            'SELECT time_ms,trade_id,fill_key,side,qty,price,fee,fee_currency,point_fee,gt_fee FROM fills')]
        transfers = [dict(r) for r in self.db.execute('SELECT id,time_ms,qty FROM ledger_transfers')]
        return fills, transfers

    def _replay(self, base: str, quote: str) -> tuple[Position, dict[str, FillEffect]]:
        fills, transfers = self._replay_rows()
        return replay_effects(fills, base, quote, transfers=transfers)

    def _cache_fresh(self) -> bool:
        last = self.db.execute('SELECT COALESCE(MAX(rowid),0) FROM fills').fetchone()[0]
        return self.meta('positions_order') == POSITIONS_ORDER and self.meta('positions_rowid') == last

    def _save_position(self, pair: str, position: Position, rowid: int | None = None) -> None:
        self.db.execute('INSERT OR REPLACE INTO positions(pair,json) VALUES(?,?)', (pair, position.to_json()))
        if rowid is None:
            rowid = self.db.execute('SELECT COALESCE(MAX(rowid),0) FROM fills').fetchone()[0]
        self._set_meta_sql('positions_rowid', int(rowid))
        self._set_meta_sql('positions_order', POSITIONS_ORDER)

    def _load_position(self, pair: str, base: str, quote: str) -> Position:
        """Inside a write transaction: the cached position, rebuilt when stale/invalid."""
        row = self.db.execute('SELECT json FROM positions WHERE pair=?', (pair,)).fetchone()
        if row is not None and self._cache_fresh():
            try:
                return Position.from_json(row[0])
            except (ValueError, TypeError, ArithmeticError):
                LOG.warning('Position cache is invalid; rebuilding from the fills ledger')
        position, _ = self._replay(base, quote)
        self._save_position(pair, position)
        return position

    def position(self) -> Position:
        """Bot position of the campaign pair (replayed in memory if the cache is stale)."""
        with self.lock:
            currencies = self.meta('ledger_currencies')
            if currencies is None:
                return Position()
            row = self.db.execute('SELECT json FROM positions WHERE pair=?', (currencies['pair'],)).fetchone()
            if row is not None and self._cache_fresh():
                try:
                    return Position.from_json(row[0])
                except (ValueError, TypeError, ArithmeticError):
                    pass
            return self._replay(currencies['base'], currencies['quote'])[0]

    def sync_position(self, *, base: str, quote: str) -> Position:
        """Bring the cache up to date (engine start) and return it."""
        with self._write():
            pair = self._ensure_currencies(base, quote)
            return self._load_position(pair, base, quote)

    def pending_position(self) -> bool:
        """True when a post-reconciliation position snapshot is owed (set with each fill)."""
        return bool(self.meta('pending_position', False))

    def clear_pending_position(self) -> None:
        with self._write():
            self.db.execute("DELETE FROM meta WHERE key='pending_position'")

    # ----- execution ingestion -----
    def _check_duplicate(self, existing: sqlite3.Row, fill: dict) -> None:
        for name in ('fill_key', 'trade_id', 'order_id', 'currency_pair', 'side', 'fee_currency', 'time_ms'):
            if existing[name] != fill[name]:
                raise Unsafe('FILL_IDENTITY_CONFLICT')
        for name in ('qty', 'price', 'fee', 'point_fee', 'gt_fee'):
            if D(existing[name]) != fill[name]:
                raise Unsafe('FILL_IDENTITY_CONFLICT')
        for name in ('role', 'sequence_id'):
            if existing[name] is not None and fill[name] is not None and existing[name] != fill[name]:
                raise Unsafe('FILL_IDENTITY_CONFLICT')
        if fill['trade_text'] and fill['trade_text'] != existing['text']:
            raise Unsafe('FILL_IDENTITY_CONFLICT')

    @staticmethod
    def _parse_trade(trade: Any) -> dict:
        """Field-level validation of a VenueTrade (no ownership yet)."""
        if not isinstance(trade, VenueTrade):
            raise Unsafe('INVALID_FILL')
        try:
            time_ms = trade.create_time_ms
            if isinstance(time_ms, bool) or not isinstance(time_ms, int) or time_ms < 0:
                raise ValueError('time')
            if not isinstance(trade.order_id, str) or not ORDER_ID_RE.fullmatch(trade.order_id):
                raise ValueError('order_id')
            if not isinstance(trade.trade_id, str) or not TRADE_ID_RE.fullmatch(trade.trade_id):
                raise ValueError('trade_id')
        except ValueError as exc:
            raise Unsafe('INVALID_FILL_ID_OR_TIMESTAMP') from exc
        try:
            fill = {
                'trade_id': trade.trade_id, 'order_id': trade.order_id, 'time_ms': time_ms,
                'currency_pair': trade.currency_pair, 'side': trade.side,
                'qty': D(trade.amount), 'price': D(trade.price), 'fee': D(trade.fee),
                'fee_currency': trade.fee_currency,
                'point_fee': D(trade.point_fee if trade.point_fee is not None else ZERO),
                'gt_fee': D(trade.gt_fee if trade.gt_fee is not None else ZERO),
                'role': trade.role if isinstance(trade.role, str) and trade.role else None,
                'sequence_id': (str(trade.sequence_id) if trade.sequence_id not in (None, '') else None),
                'trade_text': trade.text if isinstance(trade.text, str) and trade.text else None,
            }
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise Unsafe('INVALID_FILL') from exc
        if not isinstance(fill['currency_pair'], str):
            raise Unsafe('INVALID_FILL')
        fill['fill_key'] = fill_key_for(fill['currency_pair'], fill['order_id'], fill['trade_id'], time_ms)
        return fill

    def _own_row(self, fill: dict) -> dict | None:
        """Ownership by order_id (any local row, closed included); else bind by text when the
        local row has no order_id yet (inside the caller's savepoint)."""
        row = self.order_by_id(fill['order_id'])
        if row is not None:
            return row
        text = fill['trade_text']
        if text is None or not TEXT_RE.fullmatch(text):
            return None
        row = self.order(text)
        if row is None:
            return None
        if row['order_id'] is not None:
            raise Unsafe('FILL_ORDER_ID_TEXT_CONFLICT')
        try:
            self.db.execute('UPDATE orders SET order_id=? WHERE text=? AND order_id IS NULL',
                            (fill['order_id'], text))
        except sqlite3.IntegrityError as exc:
            raise Unsafe('DUPLICATE_ORDER_ID_OR_ACTIVE_SLOT') from exc
        LOG.info('ORDER_BOUND_BY_TRADE %s', _dumps({'text': text, 'order_id': fill['order_id']}))
        return self.order(text)

    def _validate_own_fill(self, fill: dict, row: dict, pair: str, base: str, now_ms: int,
                           amount_precision: int | None) -> None:
        if row['state'] == 'rejected':
            raise Unsafe('FILL_FOR_REJECTED_ORDER')
        if fill['currency_pair'] != pair:
            raise Unsafe('FILL_MARKET_MISMATCH')
        if fill['side'] != row['side']:
            raise Unsafe('FILL_SIDE_MISMATCH')
        if fill['trade_text'] is not None and fill['trade_text'] != row['text']:
            raise Unsafe('FILL_TEXT_MISMATCH')
        qty, price, fee, currency = fill['qty'], fill['price'], fill['fee'], fill['fee_currency']
        if qty <= 0 or price <= 0 or not isinstance(currency, str) or not currency.strip() or currency != currency.strip():
            raise Unsafe('INVALID_FILL_AMOUNT_OR_FEE_TOKEN')
        if row['side'] == 'sell' and currency == base and qty + fee <= 0:
            raise Unsafe('INVALID_FILL_AMOUNT_OR_FEE_TOKEN')
        if amount_precision is not None and not on_grid(qty, amount_precision):
            raise Unsafe('INVALID_FILL_QUANTITY_PRECISION')
        if fill['time_ms'] < row['created_ms'] - HOST_AHEAD_MS or fill['time_ms'] > now_ms + EXCHANGE_AHEAD_MS:
            raise Unsafe('INVALID_FILL_TIMESTAMP')
        limits = [row['price']]
        limits += [p for p, until in row['prior_prices'] if fill['time_ms'] <= until + EXCHANGE_AHEAD_MS]
        if row['amend_state'] in AMEND_PENDING and row['amend_price'] is not None:
            limits.append(row['amend_price'])
        if (row['side'] == 'buy' and price > max(limits)) or (row['side'] == 'sell' and price < min(limits)):
            raise Unsafe('FILL_PRICE_VIOLATES_LIMIT')

    def _quarantine_external_fill(self, fill: dict, trade: VenueTrade, now_ms: int) -> None:
        """Inside the trade savepoint; never adds inventory, profit or cap capacity."""
        try:
            if fill['side'] not in SIDES or not isinstance(fill['fee_currency'], str) or not fill['fee_currency']:
                raise ValueError('fields')
            if fill['qty'] <= 0 or fill['price'] <= 0:
                raise ValueError('amounts')
            if fill['time_ms'] > now_ms + EXCHANGE_AHEAD_MS:
                raise ValueError('future time')
        except (ValueError, TypeError, ArithmeticError) as exc:
            raise Unsafe('INVALID_EXTERNAL_FILL') from exc
        key = fill['order_id'] + ':' + fill['trade_id']
        canonical = {name: (str(fill[name].normalize()) if isinstance(fill[name], Decimal) else fill[name])
                     for name in ('trade_id', 'order_id', 'time_ms', 'currency_pair', 'side', 'qty', 'price',
                                  'fee', 'fee_currency', 'point_fee', 'gt_fee')}
        existing = self.db.execute('SELECT raw FROM external_fills WHERE fill_key=?', (key,)).fetchone()
        if existing is not None:
            saved = json.loads(existing[0]).get('canonical', {})
            if saved != canonical:
                raise Unsafe('EXTERNAL_FILL_IDENTITY_CONFLICT')
            return
        self.db.execute('INSERT INTO external_fills(fill_key,trade_id,order_id,time_ms,recorded_ms,raw) '
                        'VALUES(?,?,?,?,?,?)',
                        (key, fill['trade_id'], fill['order_id'], fill['time_ms'], now_ms,
                         _dumps({'canonical': canonical, 'text': fill['trade_text'], 'raw': trade.raw},
                                sort_keys=True)))
        self.db.execute('INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)',
                        ('external_activity', _dumps({'reason': 'FOREIGN_FILL', 'detected_ms': now_ms})))

    def quarantine_external_trades(self, trades: Sequence[VenueTrade], now_ms: int) -> int:
        """Executions on OTHER pairs of the account (account-wide check, never own): kept in
        ``external_fills`` as evidence with meta ``external_activity`` FOREIGN_FILL, in ONE
        transaction. Trades older than ``start_ms - 2 s`` are pre-campaign and ignored.
        Returns the number of campaign-time trades (new or already quarantined)."""
        now_ms = _integer(now_ms, 'now_ms')
        trades = list(trades)
        count = 0
        with self._write():
            self._require_identity()
            start = self.meta('start_ms')
            if start is None:
                raise Unsafe('DATABASE_NOT_INITIALIZED')
            start = _integer(start, 'start_ms')
            for trade in trades:
                fill = self._parse_trade(trade)
                if fill['time_ms'] < start - FOREIGN_GRACE_MS:
                    continue
                self._quarantine_external_fill(fill, trade, now_ms)
                count += 1
        return count

    def record_trades(self, trades: Sequence[VenueTrade], *, base: str, quote: str, now_ms: int,
                      start_ms: int | None = None, quarantine_foreign: bool = False,
                      amount_precision: int | None = None, initial_avg_price=None) -> list[dict]:
        """Ingest own executions (``GET /spot/my_trades`` rows) in ONE transaction.

        Trades are processed in execution order ``(create_time_ms, numeric trade id)``; each
        has its own SAVEPOINT. A failing trade is rolled back alone, the accepted prefix is
        committed, then the error is raised. Ownership: a local row with that order_id (any
        state), else a local row with the trade's ``text`` and no order_id yet (bound here).
        A trade without an own row is foreign: ignored if older than ``start_ms - 2 s``,
        else ``Unsafe('FOREIGN_FILL')`` or, with ``quarantine_foreign``, kept in
        ``external_fills`` with meta ``external_activity`` (account review required).
        Returns the newly recorded fills (dicts with Decimal amounts, ``late`` flag).
        """
        now_ms = _integer(now_ms, 'now_ms')
        if amount_precision is not None:
            step(amount_precision)  # validates the precision (ValueError)
        if initial_avg_price is not None:
            initial_avg_price = _positive(initial_avg_price, 'initial_avg_price')

        def order_key(trade: Any) -> tuple:
            try:
                return (int(trade.create_time_ms), tid_number(trade.trade_id), str(trade.trade_id))
            except (AttributeError, TypeError, ValueError):
                return (-1, -1, '')

        ordered = sorted(trades, key=order_key)
        added: list[dict] = []
        failure: BaseException | None = None
        with self._write():
            identity = self._require_identity()
            start = start_ms if start_ms is not None else self.meta('start_ms')
            if start is None:
                raise Unsafe('DATABASE_NOT_INITIALIZED')
            start = _integer(start, 'start_ms')
            pair = self._ensure_currencies(base, quote)
            prefix = '[PAPER] ' if identity.get('mode') == 'paper' else ''
            position = self._load_position(pair, base, quote)
            for trade in ordered:
                self.db.execute('SAVEPOINT one_fill')
                try:
                    result = self._record_one(trade, position, pair=pair, base=base, quote=quote, start=start,
                                              now_ms=now_ms, quarantine_foreign=quarantine_foreign,
                                              amount_precision=amount_precision,
                                              initial_avg_price=initial_avg_price, prefix=prefix)
                    self.db.execute('RELEASE one_fill')
                except Exception as exc:
                    try:
                        self.db.execute('ROLLBACK TO one_fill')
                        self.db.execute('RELEASE one_fill')
                    except sqlite3.Error:
                        # SQLite already aborted the whole transaction (e.g. disk full):
                        # nothing of this batch is committed; report the original error.
                        raise exc
                    failure = exc
                    break
                if result is not None:
                    fill, position = result
                    added.append(fill)
        if failure is not None:
            LOG.warning('FILL_REJECTED %s', _dumps({'reason': str(failure) or type(failure).__name__,
                                                    'accepted': len(added)}))
            raise failure
        return added

    def _record_one(self, trade: Any, position: Position, *, pair: str, base: str, quote: str, start: int,
                    now_ms: int, quarantine_foreign: bool, amount_precision: int | None,
                    initial_avg_price: Decimal | None, prefix: str) -> tuple[dict, Position] | None:
        fill = self._parse_trade(trade)
        existing = self.db.execute('SELECT * FROM fills WHERE fill_key=? OR trade_id=?',
                                   (fill['fill_key'], fill['trade_id'])).fetchall()
        if existing:
            if len(existing) != 1:
                raise Unsafe('FILL_IDENTITY_CONFLICT')
            self._check_duplicate(existing[0], fill)
            return None  # identical overlap: idempotent, no second outbox event
        row = self._own_row(fill)
        if row is None:
            if fill['time_ms'] < start - FOREIGN_GRACE_MS:
                return None  # pre-campaign foreign activity is not ours to judge
            if not quarantine_foreign:
                raise Unsafe('FOREIGN_FILL')
            self._quarantine_external_fill(fill, trade, now_ms)
            return None
        self._validate_own_fill(fill, row, pair, base, now_ms, amount_precision)
        filled = row['filled_qty'] + fill['qty']
        cap = row['qty']
        if row['amend_state'] in AMEND_PENDING and row['amend_qty'] is not None:
            cap = max(cap, row['amend_qty'])
        if filled > cap:
            raise Unsafe('FILLS_EXCEED_ORDER_QUANTITY')
        fill['text'], fill['level'] = row['text'], row['level']
        fill['notional'] = fill['qty'] * fill['price']
        raw = trade.raw if isinstance(trade.raw, dict) and trade.raw else {
            k: fill[k] for k in ('trade_id', 'order_id', 'time_ms', 'currency_pair', 'side', 'qty', 'price', 'fee',
                                 'fee_currency', 'point_fee', 'gt_fee', 'role', 'sequence_id', 'trade_text')}
        try:
            raw_json = _dumps(raw)
        except (TypeError, ValueError) as exc:
            raise Unsafe('INVALID_FILL') from exc
        cursor = self.db.execute(
            'INSERT INTO fills(fill_key,trade_id,order_id,text,currency_pair,side,level,qty,price,notional,fee,'
            'fee_currency,point_fee,gt_fee,role,sequence_id,time_ms,recorded_ms,raw) '
            'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (fill['fill_key'], fill['trade_id'], fill['order_id'], fill['text'], fill['currency_pair'], fill['side'],
             fill['level'], str(fill['qty']), str(fill['price']), str(fill['notional']), str(fill['fee']),
             fill['fee_currency'], str(fill['point_fee']), str(fill['gt_fee']), fill['role'], fill['sequence_id'],
             fill['time_ms'], now_ms, raw_json))
        self.db.execute('UPDATE orders SET filled_qty=? WHERE text=?', (str(filled), row['text']))
        key = (fill['time_ms'], tid_number(fill['trade_id']), fill['fill_key'])
        correction = None
        try:
            if key > position.watermark():
                new_position, effect = apply_fill_effect(
                    position, fill['side'], fill['qty'], fill['price'], fill['fee'], fill['fee_currency'],
                    base, quote, point_fee=fill['point_fee'], gt_fee=fill['gt_fee'], time_ms=fill['time_ms'],
                    trade_id=fill['trade_id'], fill_key=fill['fill_key'])
            else:
                # An earlier execution became visible late: rebuild in execution order.
                new_position, effects = self._replay(base, quote)
                effect = effects[fill['fill_key']]
                correction = (position.realized, new_position.realized)
        except (ValueError, ArithmeticError, KeyError) as exc:
            raise Unsafe('INVALID_FILL') from exc
        self._save_position(pair, new_position, cursor.lastrowid)
        self._set_meta_sql('pending_position', True)
        order_view = dict(row, filled_qty=filled)
        self._emit_sql('fill:' + fill['fill_key'], prefix + fill_text(
            fill, order_view, filled, effect, new_position, base, quote, initial_avg_price, correction), now_ms)
        result = {k: fill[k] for k in ('fill_key', 'trade_id', 'order_id', 'text', 'level', 'currency_pair', 'side',
                                       'qty', 'price', 'notional', 'fee', 'fee_currency', 'point_fee', 'gt_fee',
                                       'role', 'sequence_id', 'time_ms')}
        result['late'] = correction is not None
        return result, new_position

    def record_transfer_out(self, qty, balance, now_ms: int, *, base: str, quote: str,
                            note: str = 'unexplained balance decrease') -> tuple[Decimal, Decimal]:
        """Book a CONFIRMED withdrawal of bot-bought base at average cost (no profit).

        The transfer is placed after every applied fill (time ``max(now, last fill)``), so a
        later-visible fill with an earlier exchange time is applied through a replay. Only
        exact when no fills happened between the last clean reconciliation and the withdrawal;
        the caller decides that (otherwise it must require an account review instead).
        """
        qty, balance = D(qty), D(balance)
        now_ms = _integer(now_ms, 'now_ms')
        with self._write():
            pair = self._ensure_currencies(base, quote)
            prefix = '[PAPER] ' if self._require_identity().get('mode') == 'paper' else ''
            current = self._load_position(pair, base, quote)
            if qty <= 0 or current.qty <= 0:
                return ZERO, ZERO
            at = max(now_ms, current.last_time_ms)
            position, removed, cost = apply_transfer_out(current, qty, at)
            ident = f'transfer:{pair}:{at}:{secrets.token_hex(4)}'
            self.db.execute('INSERT INTO ledger_transfers(id,pair,time_ms,qty,cost,balance,note) VALUES(?,?,?,?,?,?,?)',
                            (ident, pair, at, str(removed), str(cost), str(balance), str(note)))
            self._save_position(pair, position)
            avg = position.avg_cost
            text = (f'{base}: на балансе {fmt(balance)}, а куплено ботом и не продано {fmt(current.qty)}. '
                    f'Похоже на вывод токенов: позиция бота уменьшена на {fmt(removed)} '
                    f'(себестоимость списания {money(cost)} {quote}); прибыль не начислена. '
                    f'Позиция бота: {fmt(position.qty)} {base}'
                    + (f' по средней {fmt(avg, 8)}.' if avg is not None else '.'))
            self._emit_sql(ident, prefix + text, now_ms)
            return removed, cost

    def record_order_rebate(self, order: VenueOrder, now_ms: int) -> None:
        """Order-level ``rebated_fee`` (not allocated to fills) kept for reporting; latest wins."""
        now_ms = _integer(now_ms, 'now_ms')
        if not isinstance(order, VenueOrder) or order.rebated_fee is None or not order.rebated_fee:
            return
        amount = D(order.rebated_fee)
        with self._write():
            self.db.execute('INSERT OR REPLACE INTO order_rebates(order_id,text,rebated_fee,rebated_fee_currency,'
                            'recorded_ms) VALUES(?,?,?,?,?)',
                            (order.order_id, order.text, str(amount), order.rebated_fee_currency, now_ms))

    def order_rebates(self) -> list[dict]:
        with self.lock:
            rows = [dict(r) for r in self.db.execute('SELECT * FROM order_rebates ORDER BY recorded_ms, order_id')]
        for row in rows:
            row['rebated_fee'] = D(row['rebated_fee'])
        return rows

    # ----- external activity -----
    def external_activity(self) -> dict | None:
        return self.meta('external_activity')

    def require_account_review(self, reason: str, now_ms: int | None = None) -> None:
        """Permanent STOP_ACCOUNT_REVIEW marker (only the first reason is kept)."""
        if not isinstance(reason, str) or not reason:
            raise ValueError('A review reason is required')
        now_ms = _now_ms() if now_ms is None else _integer(now_ms, 'now_ms')
        with self._write():
            self.db.execute('INSERT OR IGNORE INTO meta(key,value) VALUES(?,?)',
                            ('external_activity', _dumps({'reason': reason, 'detected_ms': now_ms})))

    def record_external_orders(self, orders: Sequence[VenueOrder], now_ms: int) -> None:
        """Foreign open orders seen on the account (evidence; never cancelled by the bot)."""
        now_ms = _integer(now_ms, 'now_ms')
        rows = []
        for order in orders:
            if not isinstance(order, VenueOrder) or not isinstance(order.order_id, str) or not order.order_id:
                raise ValueError('Invalid external order')
            rows.append((order.order_id, now_ms, now_ms, _dumps({'text': order.text, 'raw': order.raw})))
        with self._write():
            for order_id, first, last, raw in rows:
                self.db.execute('INSERT INTO external_orders(order_id,first_seen_ms,last_seen_ms,raw) VALUES(?,?,?,?) '
                                'ON CONFLICT(order_id) DO UPDATE SET last_seen_ms=excluded.last_seen_ms',
                                (order_id, first, last, raw))
            self._set_meta_sql('external_open_orders_last_seen', [r[0] for r in rows])

    def external_fills(self) -> list[dict]:
        with self.lock:
            rows = [dict(r) for r in self.db.execute('SELECT * FROM external_fills ORDER BY time_ms, fill_key')]
        for row in rows:
            row['raw'] = json.loads(row['raw'])
        return rows

    # ----- outbox -----
    def _emit_sql(self, key: str, text: str, now_ms: int | None = None) -> bool:
        cursor = self.db.execute('INSERT OR IGNORE INTO outbox(key,text,created_ms) VALUES(?,?,?)',
                                 (str(key), str(text), _now_ms() if now_ms is None else int(now_ms)))
        return cursor.rowcount == 1

    def outbox_add(self, key: str, text: str, now_ms: int | None = None) -> bool:
        """Idempotent by key: a key already queued or delivered is ignored. True if added."""
        if not isinstance(key, str) or not key or not isinstance(text, str):
            raise ValueError('Invalid outbox event')
        now_ms = _now_ms() if now_ms is None else _integer(now_ms, 'now_ms')
        with self._write():
            return self._emit_sql(key, text, now_ms)

    emit = outbox_add

    @staticmethod
    def _outbox_row(row: sqlite3.Row) -> dict:
        return dict(row)

    def outbox_head(self) -> dict | None:
        """Oldest undelivered event (delivery is FIFO; newer events never overtake it)."""
        with self.lock:
            row = self.db.execute('SELECT * FROM outbox WHERE sent_ms IS NULL ORDER BY rowid LIMIT 1').fetchone()
            return self._outbox_row(row) if row else None

    def outbox_due(self, now_ms: int, limit: int = 20) -> list[dict]:
        """FIFO prefix of undelivered events that are due (stops at the first backing-off one)."""
        now_ms = _integer(now_ms, 'now_ms')
        limit = _integer(limit, 'limit')
        result = []
        with self.lock:
            for row in self.db.execute('SELECT * FROM outbox WHERE sent_ms IS NULL ORDER BY rowid LIMIT ?', (limit,)):
                if row['next_ms'] > now_ms:
                    break
                result.append(self._outbox_row(row))
        return result

    def outbox_mark_sent(self, key: str, now_ms: int) -> None:
        now_ms = _integer(now_ms, 'now_ms')
        with self._write():
            self.db.execute("UPDATE outbox SET sent_ms=?,status='sent',last_error=NULL WHERE key=? AND sent_ms IS NULL",
                            (now_ms, key))

    def outbox_mark_skipped(self, key: str, now_ms: int) -> None:
        """Telegram disabled: the event stays recorded but is not sent."""
        now_ms = _integer(now_ms, 'now_ms')
        with self._write():
            self.db.execute("UPDATE outbox SET sent_ms=?,status='skipped' WHERE key=? AND sent_ms IS NULL",
                            (now_ms, key))

    def outbox_mark_failed(self, key: str, now_ms: int, error: Any = None,
                           retry_after: float | None = None) -> int | None:
        """Count a failed delivery; next attempt after max(retry_after, min(60, 2**min(n, 6))) s.
        ``error`` must not contain secrets (pass exception type names). Returns next_ms."""
        now_ms = _integer(now_ms, 'now_ms')
        if retry_after is not None:
            retry_after = float(retry_after)
            if not retry_after >= 0 or retry_after == float('inf'):
                raise ValueError('Invalid retry_after')
        with self._write():
            row = self.db.execute('SELECT attempts FROM outbox WHERE key=? AND sent_ms IS NULL', (key,)).fetchone()
            if row is None:
                return None
            attempts = int(row[0]) + 1
            delay = max(retry_after or 0.0, float(min(OUTBOX_MAX_DELAY_S, 2 ** min(attempts, 6))))
            next_ms = now_ms + int(delay * 1000)
            self.db.execute('UPDATE outbox SET attempts=?,next_ms=?,last_error=? WHERE key=?',
                            (attempts, next_ms, _clean_error(error) or None, key))
            return next_ms

    def outbox_pending_count(self) -> int:
        with self.lock:
            return int(self.db.execute('SELECT COUNT(*) FROM outbox WHERE sent_ms IS NULL').fetchone()[0])

    def outbox_rows(self, limit: int = 100, *, pending_only: bool = False) -> list[dict]:
        limit = _integer(limit, 'limit')
        query = 'SELECT * FROM outbox' + (' WHERE sent_ms IS NULL' if pending_only else '') + ' ORDER BY rowid LIMIT ?'
        with self.lock:
            return [self._outbox_row(r) for r in self.db.execute(query, (limit,))]

    # ----- signed-action cap -----
    def action_count(self, now_ms: int, window_ms: int = HOUR_MS) -> int:
        now_ms = _integer(now_ms, 'now_ms')
        with self.lock:
            return int(self.db.execute('SELECT COUNT(*) FROM actions WHERE ts_ms>?',
                                       (now_ms - int(window_ms),)).fetchone()[0])

    def action(self, kind: str, now_ms: int, max_per_hour: int, emergency: bool = False) -> None:
        """Durable local cap on signed actions per rolling hour; emergency actions (cancels,
        DMS) are never refused but are recorded and count."""
        now_ms = _integer(now_ms, 'now_ms')
        maximum = _integer(max_per_hour, 'max_per_hour')
        if not isinstance(kind, str) or not kind:
            raise ValueError('Action kind is required')
        with self._write():
            if not emergency and self.action_count(now_ms) >= maximum:
                raise Unsafe('SIGNED_ACTION_HOURLY_LIMIT')
            self.db.execute('INSERT INTO actions(kind,ts_ms) VALUES(?,?)', (kind, now_ms))
            self.db.execute('DELETE FROM actions WHERE ts_ms<=?', (now_ms - HOUR_MS,))

    # ----- index samples and execution marks -----
    def record_index(self, ts_ms: int, index, center, ok: bool, reason: str | None = None, *,
                     components: Any = None, spot_bid=None, spot_ask=None) -> None:
        """One compact index observation; ``components`` (JSON-able) kept at most once per second."""
        ts_ms = _integer(ts_ms, 'ts_ms')
        if not isinstance(ok, bool):
            raise ValueError('ok must be boolean')
        reason = _optional_text(reason, 'reason')
        values = []
        for value, name in ((index, 'index'), (center, 'center'), (spot_bid, 'spot_bid'), (spot_ask, 'spot_ask')):
            values.append(None if value is None else str(_positive(value, name)))
        if ok and (values[0] is None or values[1] is None):
            raise ValueError('A valid index observation needs index and center')
        encoded = None if components is None else _dumps(components)
        with self._write():
            self.db.execute('INSERT INTO index_obs(ts_ms,"index",center,ok,reason,spot_bid,spot_ask) '
                            'VALUES(?,?,?,?,?,?,?)',
                            (ts_ms, values[0], values[1], int(ok), reason, values[2], values[3]))
            if encoded is not None:
                if self._last_components_ms is None:
                    self._last_components_ms = self.db.execute(
                        'SELECT MAX(ts_ms) FROM index_components').fetchone()[0]
                last = self._last_components_ms
                if last is None or ts_ms - last >= COMPONENTS_INTERVAL_MS or ts_ms < last:
                    self.db.execute('INSERT INTO index_components(ts_ms,json) VALUES(?,?)', (ts_ms, encoded))
                    self._last_components_ms = ts_ms

    def record_index_snapshot(self, snapshot: IndexSnapshot, *, spot_bid=None, spot_ask=None,
                              with_components: bool = True) -> None:
        components = [asdict(c) for c in snapshot.components] if with_components else None
        self.record_index(int(snapshot.at_ms), snapshot.index if snapshot.ok else None,
                          snapshot.center if snapshot.ok else None, bool(snapshot.ok), snapshot.reason,
                          components=components, spot_bid=spot_bid, spot_ask=spot_ask)

    def index_obs(self, from_ms: int | None = None, to_ms: int | None = None) -> list[dict]:
        """Rows ``{ts_ms, index, center, ok, reason, spot_bid, spot_ask}`` ordered by time."""
        clauses, params = [], []
        if from_ms is not None:
            clauses.append('ts_ms>=?')
            params.append(_integer(from_ms, 'from_ms'))
        if to_ms is not None:
            clauses.append('ts_ms<=?')
            params.append(_integer(to_ms, 'to_ms'))
        query = ('SELECT ts_ms,"index" AS idx,center,ok,reason,spot_bid,spot_ask FROM index_obs'
                 + (' WHERE ' + ' AND '.join(clauses) if clauses else '') + ' ORDER BY ts_ms, rowid')
        with self.lock:
            rows = self.db.execute(query, params).fetchall()
        return [{'ts_ms': r['ts_ms'], 'index': None if r['idx'] is None else D(r['idx']),
                 'center': None if r['center'] is None else D(r['center']), 'ok': bool(r['ok']),
                 'reason': r['reason'], 'spot_bid': None if r['spot_bid'] is None else D(r['spot_bid']),
                 'spot_ask': None if r['spot_ask'] is None else D(r['spot_ask'])} for r in rows]

    def index_samples(self, from_ms: int, to_ms: int) -> list[tuple[int, Decimal]]:
        """Valid observations ``[(ts_ms, index)]`` in [from_ms, to_ms] (for marks)."""
        return [(r['ts_ms'], r['index']) for r in self.index_obs(from_ms, to_ms) if r['ok'] and r['index'] is not None]

    def index_mark(self, ms: int, bracket_ms: int = MARK_BRACKET_MS) -> Mark | None:
        """Execution-time mark from persisted samples (same ±1 s bracket rule as IndexTrail)."""
        ms = _integer(ms, 'ms')
        return mark_from_samples(self.index_samples(max(0, ms - bracket_ms), ms + bracket_ms), ms, bracket_ms)

    def index_components(self, from_ms: int | None = None, to_ms: int | None = None) -> list[dict]:
        clauses, params = [], []
        if from_ms is not None:
            clauses.append('ts_ms>=?')
            params.append(_integer(from_ms, 'from_ms'))
        if to_ms is not None:
            clauses.append('ts_ms<=?')
            params.append(_integer(to_ms, 'to_ms'))
        query = ('SELECT ts_ms,json FROM index_components' + (' WHERE ' + ' AND '.join(clauses) if clauses else '')
                 + ' ORDER BY ts_ms, rowid')
        with self.lock:
            return [{'ts_ms': r['ts_ms'], 'components': json.loads(r['json'])} for r in self.db.execute(query, params)]

    def prune_index(self, before_ms: int) -> int:
        """Delete index observations/components older than ``before_ms``; returns rows removed."""
        before_ms = _integer(before_ms, 'before_ms')
        with self._write():
            removed = self.db.execute('DELETE FROM index_obs WHERE ts_ms<?', (before_ms,)).rowcount
            removed += self.db.execute('DELETE FROM index_components WHERE ts_ms<?', (before_ms,)).rowcount
            return removed

    def record_marks(self, rows: Sequence[tuple]) -> None:
        """``(fill_key, horizon_s, index, sample_ms)``; the first value recorded wins."""
        values = []
        for fill_key, horizon, index, sample_ms in rows:
            if not isinstance(fill_key, str) or not fill_key:
                raise ValueError('Invalid fill_key')
            values.append((fill_key, _integer(horizon, 'horizon_s'), str(_positive(index, 'index')),
                           _integer(sample_ms, 'sample_ms')))
        if not values:
            return
        with self._write():
            self.db.executemany('INSERT OR IGNORE INTO fill_index_marks(fill_key,horizon_s,index_price,sample_ms) '
                                'VALUES(?,?,?,?)', values)

    def marks(self, fill_key: str | None = None) -> list[dict]:
        query, params = 'SELECT * FROM fill_index_marks', []
        if fill_key is not None:
            query += ' WHERE fill_key=?'
            params.append(fill_key)
        with self.lock:
            rows = [dict(r) for r in self.db.execute(query + ' ORDER BY fill_key, horizon_s', params)]
        for row in rows:
            row['index_price'] = D(row['index_price'])
        return rows


# ----------------------------------------------------------------------------- messages

def fill_text(fill: Mapping[str, Any], order: Mapping[str, Any], filled: Decimal, effect: FillEffect,
              position: Position, base: str, quote: str, initial_price: Decimal | None = None,
              correction: tuple[Decimal, Decimal] | None = None) -> str:
    """Human (Russian) message for one execution: size, price, fees, order, PnL, position."""
    side, qty, price = fill['side'], fill['qty'], fill['price']
    lines = [f"{base} {side.upper()} [{order['level']}]: исполнено {fmt(qty)} {base} по {fmt(price)} {quote} "
             f"= {money(fill['notional'])} {quote}."]
    fee_line = f"Комиссия: {fmt(fill['fee'])} {fill['fee_currency']}"
    if fill['fee_currency'] == base and fill['fee']:
        fee_line += f' (≈{money(fill["fee"] * price)} {quote})'
    extra = [f'{fmt(fill[name])} {label}' for name, label in (('point_fee', 'POINT'), ('gt_fee', 'GT'))
             if fill.get(name)]
    if extra:
        fee_line += f"; также {', '.join(extra)} (в {quote} не пересчитывается)"
    lines.append(fee_line + '.')
    index, center = order.get('index_price'), order.get('center_price')
    edge = ''
    if index:
        edge = f'; индекс при выставлении {fmt(index)}, цена исполнения {edge_text(side, price, index)}'
        if center and center != index:
            edge += f' (центр {fmt(center)}: {edge_text(side, price, center, "центра")})'
    lines.append(f"Заявка {fmt(order['price'])}: исполнено {fmt(filled)} из {fmt(order['qty'])}, "
                 f"осталось {fmt(order['qty'] - filled)}{edge}.")
    if side == 'buy':
        lines.append(f'Получено {fmt(effect.net_qty)} {base}, оплачено {money(effect.net_quote)} {quote}.')
    else:
        lines.append(f'Отдано {fmt(effect.net_qty)} {base}, получено {money(effect.net_quote)} {quote}.')
        if effect.matched_qty > 0:
            lines.append(f'Прибыль по сделке: {signed(effect.realized)} {quote} ({pct(effect.realized_pct)}) '
                         f'к себестоимости {money(effect.cost_removed)} {quote}; '
                         f'всего реализовано {signed(position.realized)} {quote}.')
        if effect.unmatched_qty > 0:
            if initial_price is None:
                lines.append(f'Из запаса, купленного не ботом (себестоимость не учтена): '
                             f'{fmt(effect.unmatched_qty)} {base}.')
            else:
                share = effect.net_quote * effect.unmatched_qty / effect.net_qty
                gain = share - effect.unmatched_qty * D(initial_price)
                lines.append(f'Из прежнего запаса {fmt(effect.unmatched_qty)} {base} по initial_avg_price '
                             f'{fmt(initial_price)}: {signed(gain)} {quote}.')
    if correction is not None:
        lines.append(f'Исполнение стало видно с опозданием: позиция и прибыль пересчитаны в порядке времени '
                     f'исполнения; всего реализовано было {signed(correction[0])}, стало {signed(correction[1])} '
                     f'{quote}.')
    avg = position.avg_cost
    lines.append(f'Позиция бота: {fmt(position.qty)} {base}'
                 + (f', средняя цена {fmt(avg, 8)}, себестоимость {money(position.cost)} {quote}.'
                    if avg is not None else '.'))
    lines.append(f'Кампания: куплено {fmt(position.bought_qty)} {base} на {money(position.bought_cost)} {quote}; '
                 f'продано {fmt(position.sold_qty)} {base} на {money(position.sold_proceeds)} {quote}.')
    lines.append(f"order_id: {fill['order_id']}; trade_id: {fill['trade_id']}; роль: {fill.get('role') or 'н/д'}; "
                 f"время: {utc_text(fill['time_ms'])}.")
    return '\n'.join(lines)
