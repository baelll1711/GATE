"""Shared plumbing of the index-source venue adapters (DESIGN §16).

Private helper of ``bybit.py``, ``bitget.py``, ``bingx.py``, ``lighter.py`` and
``gate_futures.py``. Nothing venue-specific lives here, only the rules every
adapter must apply identically:

* strict parsing: JSON floats become ``Decimal`` (never ``float``), ``NaN`` /
  ``Infinity`` are refused, timestamps/ids must be non-negative integers;
* book levels become immutable ``(Decimal, Decimal)`` tuples sorted best-first
  (bids descending, asks ascending). Sorting is stable, so a duplicated price
  stays adjacent and the hub's ``Book.check`` still reports
  ``UNSORTED_OR_DUPLICATE_BOOK_LEVEL``; nothing is merged, dropped or repaired;
* per-frame error discipline: a data problem in one frame becomes
  ``Update(kind='error', error=CODE)`` for the adapter's key (the socket stays
  up); only socket-fatal conditions raise ``Unsafe`` (the worker reconnects);
* ``rest_snapshot`` either returns complete updates or raises: transport, HTTP
  and venue-envelope failures raise ``TransientUnsafe(<PREFIX>_REST_...)`` (nothing
  was obtained), a malformed body raises ``Unsafe(INVALID_REST_BOOK:<key>)`` /
  ``Unsafe(INVALID_REST_TICKER:<key>)``. Nothing is published from a failed poll;
  the previous WS data simply ages out through the normal freshness checks;
* ``instrument_info`` never raises. Reason codes are ported from the reference
  ``verify_references``: ``REFERENCE_LOOKUP_FAILED:*`` (transient),
  ``REFERENCE_HTTP_ERROR``, ``<VENUE>_REFERENCE_NOT_LISTED:<sym>``,
  ``<VENUE>_REFERENCE_NOT_ACTIVE_EXACT_PERP:<sym>`` (details in ``problems``).
  Exact symbols only; scaled contracts (``1000STONK``-style) are refused.

Logs carry reason codes and exception type names only, never payloads or URLs.
"""
from __future__ import annotations

import json
import logging
import math
import re
import zlib
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from typing import Any

from ..dec import D
from ..errors import ConfigError, TransientUnsafe, Unsafe
from ..types import Book, Ticker
from .base import Update

LOG = logging.getLogger(__name__)

Level = tuple[Decimal, Decimal]

# Marker returned by ``decode`` for a binary frame that cannot be decoded. ``on_text``
# turns it into an error update for the key instead of dropping the connection.
DECODE_ERROR = '\x00DECODE_ERROR'
MAX_FRAME_BYTES = 4 * 1024 * 1024          # gzip output cap (a corrupt/huge frame is refused)
TICKER_PRICE_KINDS = frozenset({'mark', 'last'})
DATA_ERRORS = (KeyError, TypeError, ValueError, AttributeError, IndexError, ArithmeticError)
REFERENCE_RETRY_STATUSES = frozenset({403, 418, 429})

_DETAIL = re.compile(r'[^0-9A-Za-z_.\-]')
_DIGITS = re.compile(r'[0-9]+')
_SIGNED_DIGITS = re.compile(r'-?[0-9]+')
# A base like 1000STONK, 10000STONK, 1MSTONK, 1KSTONK is a price-scaled contract.
_SCALED_BASE = re.compile(r'^(?:10+|1[KMB])(?=[A-Z])')


class FrameError(ValueError):
    """A data problem in one frame or payload; ``code`` is the reason to publish."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class TransportError(Exception):
    """The HTTP client raised (timeout, connection reset, ...). ``name`` = exception type."""

    def __init__(self, name: str):
        super().__init__(name)
        self.name = name


class ReferenceFailure(Exception):
    """An instrument check ended with a definitive or transient reason code."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


# --------------------------------------------------------------------------- parsing
def _reject_constant(name: str) -> Any:
    raise ValueError('Non-finite JSON constant')


def loads(text: str) -> Any:
    """JSON with exact decimals: floats -> Decimal, NaN/Infinity refused."""
    return json.loads(text, parse_float=Decimal, parse_constant=_reject_constant)


def dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=True)


def as_int(value: Any) -> int:
    """Strict non-negative integer (timestamps, update ids, nonces). bool refused."""
    if isinstance(value, bool):
        raise ValueError('Boolean supplied instead of integer')
    if isinstance(value, int):
        result = value
    elif isinstance(value, str):
        text = value.strip()
        if not _DIGITS.fullmatch(text):
            raise ValueError('Invalid integer')
        result = int(text)
    elif isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise ValueError('Invalid integer')
        result = int(value)
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError('Invalid integer')
        result = int(value)
    else:
        raise ValueError('Invalid integer')
    if result < 0:
        raise ValueError('Negative integer')
    return result


def as_code(value: Any) -> int:
    """Venue status code (int or digit string, sign allowed). bool refused."""
    if isinstance(value, bool):
        raise ValueError('Boolean supplied instead of code')
    if isinstance(value, int):
        return value
    if isinstance(value, str) and _SIGNED_DIGITS.fullmatch(value.strip()):
        return int(value.strip())
    if isinstance(value, Decimal) and value.is_finite() and value == value.to_integral_value():
        return int(value)
    raise ValueError('Invalid code')


def ms_from_seconds(value: Any) -> int:
    """Seconds (int/decimal, e.g. Gate futures ``current`` 1623898993.123) -> int ms (floor)."""
    if isinstance(value, bool):
        raise ValueError('Invalid timestamp')
    seconds = D(value)
    if seconds < 0:
        raise ValueError('Invalid timestamp')
    return int(seconds * 1000)


def opt_price(value: Any) -> Decimal | None:
    """Ticker price: absent/empty/zero -> None (missing); negative or garbage -> ValueError."""
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return None
    price = D(value)
    if price < 0:
        raise ValueError('Negative price')
    return price if price > 0 else None


def positive_decimal(value: Any) -> Decimal | None:
    """Metadata number (tick, multiplier) or None when absent/invalid/non-positive."""
    try:
        number = D(value)
    except (ValueError, TypeError):
        return None
    return number if number > 0 else None


def plain(value: Decimal | None) -> str | None:
    """JSON-safe plain decimal text (no exponent)."""
    if value is None:
        return None
    text = format(value.normalize(), 'f')
    return '0' if text in ('-0', '') else text


def detail(value: Any) -> str:
    """Safe ``:detail`` suffix for a reason code (ASCII, bounded, no separators)."""
    text = _DETAIL.sub('_', str(value))[:40]
    return text or 'UNKNOWN'


def is_scaled_base(base: Any) -> bool:
    return isinstance(base, str) and bool(_SCALED_BASE.match(base.upper()))


def parse_levels(rows: Any, shape: str = 'pair') -> tuple[Level, ...]:
    """Rows -> immutable Decimal pairs in venue order (deep copy of the decoder's data).

    ``shape``: ``pair`` = ``[price, size]``; ``ps`` = ``{"p": .., "s": ..}``;
    ``price_size`` = ``{"price": .., "size": ..}``. Reference codes: an empty or
    non-list side -> ``EMPTY_BOOK_SIDE``; a row of the wrong shape -> ``INVALID_BOOK_SCHEMA``.
    """
    if not isinstance(rows, list) or not rows:
        raise FrameError('EMPTY_BOOK_SIDE')
    result = []
    for row in rows:
        if shape == 'pair':
            if not isinstance(row, (list, tuple)) or len(row) != 2:
                raise FrameError('INVALID_BOOK_SCHEMA')
            price, size = row
        else:
            price_key, size_key = ('p', 's') if shape == 'ps' else ('price', 'size')
            if not isinstance(row, dict) or price_key not in row or size_key not in row:
                raise FrameError('INVALID_BOOK_SCHEMA')
            price, size = row[price_key], row[size_key]
        result.append((D(price), D(size)))
    return tuple(result)


def best_first(levels: Iterable[Level], *, bids: bool) -> tuple[Level, ...]:
    """Bids descending, asks ascending; stable (duplicates stay visible to Book.check)."""
    return tuple(sorted(levels, key=lambda level: level[0], reverse=bids))


def make_book(bids: Iterable[Level], asks: Iterable[Level], received: float, event_ms: int,
              update_id: int | None = None) -> Book:
    return Book(bids=best_first(bids, bids=True), asks=best_first(asks, bids=False),
                received=float(received), event_ms=int(event_ms), update_id=update_id)


def gunzip(data: bytes, limit: int = MAX_FRAME_BYTES) -> bytes:
    """Bounded gzip decompression; truncated, corrupt or oversized input -> ValueError."""
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        output = decompressor.decompress(data, limit)
    except zlib.error as exc:
        raise ValueError('Corrupt gzip frame') from exc
    if decompressor.unconsumed_tail or not decompressor.eof:
        raise ValueError('Truncated or oversized gzip frame')
    return output


# --------------------------------------------------------------------------- HTTP
def _body(response: Any) -> Any:
    """Parse a response body with exact decimals when the raw text is available."""
    text = getattr(response, 'text', None)
    if isinstance(text, str):
        try:
            return loads(text)
        except (ValueError, RecursionError):
            return None
    try:
        return response.json()
    except Exception:  # requests raises its own JSONDecodeError (a ValueError) or others
        return None


def http_get(http: Any, url: str, *, params: Mapping[str, Any] | None = None,
             headers: Mapping[str, str] | None = None, timeout: float) -> tuple[int, Any]:
    """One GET -> (status_code, parsed body or None). Client exceptions -> TransportError."""
    try:
        response = http.get(url, params=dict(params or {}), headers=dict(headers or {}), timeout=timeout)
    except Exception as exc:
        raise TransportError(type(exc).__name__) from exc
    status = getattr(response, 'status_code', None)
    if isinstance(status, bool) or not isinstance(status, int):
        raise TransportError('InvalidStatusCode')
    return status, _body(response)


def rest_payload(prefix: str, status: int, payload: Any) -> Any:
    """REST refresher classification: anything but a parsed HTTP 200 raises TransientUnsafe."""
    if status == 429:
        raise TransientUnsafe(f'{prefix}_REST_RATE_LIMITED')
    if status != 200:
        label = payload.get('label') if isinstance(payload, dict) else None
        suffix = f':{detail(label)}' if isinstance(label, str) and label else ''
        raise TransientUnsafe(f'{prefix}_REST_HTTP_{status}{suffix}')
    if payload is None:
        raise TransientUnsafe(f'{prefix}_REST_INVALID_JSON')
    return payload


def reference_payload(status: int, payload: Any) -> dict:
    """Doctor/reference classification, ported verbatim from the reference."""
    if status in REFERENCE_RETRY_STATUSES or status >= 500:
        # Rate limit/WAF ban or maintenance: transient, retried with backoff.
        raise ReferenceFailure(f'REFERENCE_LOOKUP_FAILED:HTTP_{status}')
    if status != 200:
        raise ReferenceFailure('REFERENCE_HTTP_ERROR')
    if not isinstance(payload, dict):
        raise ReferenceFailure('REFERENCE_LOOKUP_FAILED:INVALID_RESPONSE')
    return payload


# --------------------------------------------------------------------------- adapter base
class AdapterBase:
    """Common VenueAdapter behaviour for one configured index source (one key per socket).

    Thread model: ``on_text``/``on_reset`` run on the worker thread only (they own any
    delta state); ``rest_snapshot`` and ``instrument_info`` only build fresh objects and
    may run on other threads.
    """

    VENUE = ''                     # config venue name (SUPPORTED_VENUES)
    PREFIX = ''                    # UPPER prefix of venue-specific reason codes
    DEFAULT_WS_URL = ''
    DEFAULT_REST_URL = ''
    DECODE_ERROR_CODE = 'INVALID_WS_MESSAGE'
    ping_interval: float = 0.0
    idle_timeout: float = 60.0

    def __init__(self, source: Any, *, timeout: float, rest_url: str | None = None,
                 expected_base: str | None = None) -> None:
        source_id = getattr(source, 'id', '?')
        if getattr(source, 'venue', None) != self.VENUE:
            raise ConfigError(f'index.sources[{source_id}].venue: {type(self).__name__} serves {self.VENUE}')
        try:
            timeout = float(timeout)
        except (TypeError, ValueError) as exc:
            raise ConfigError('app.request_timeout_seconds: must be a positive number') from exc
        if not math.isfinite(timeout) or timeout <= 0:
            raise ConfigError('app.request_timeout_seconds: must be a positive number')
        symbol = getattr(source, 'symbol', None)
        if not isinstance(symbol, str) or not symbol:
            raise ConfigError(f'index.sources[{source_id}].symbol: required')
        if expected_base is not None and (not isinstance(expected_base, str) or not expected_base):
            raise ConfigError('market.base: must be a non-empty string')
        self.source = source
        self.venue: str = source.venue
        self.key: str = source.id
        self.symbol: str = symbol
        self.quote: str = source.quote
        self.timeout = timeout
        self.rest_url = (rest_url or self.DEFAULT_REST_URL).rstrip('/')
        self.wants_ticker: bool = source.price in TICKER_PRICE_KINDS
        self._expected_base = expected_base
        self._check_symbol()

    # -- configuration hooks ------------------------------------------------
    def _check_symbol(self) -> None:
        raise NotImplementedError

    def _derive_base(self) -> str | None:
        raise NotImplementedError

    def _symbol_error(self, rule: str) -> ConfigError:
        return ConfigError(f'index.sources[{self.key}].symbol: {rule}')

    def expected_base(self) -> str | None:
        """Base coin the venue must report: ``expected_base`` (market.base) or the symbol's base."""
        return self._expected_base or self._derive_base()

    # -- websocket ----------------------------------------------------------
    def ws_url(self) -> str:
        return getattr(self.source, 'url', None) or self.DEFAULT_WS_URL

    def ws_headers(self) -> dict[str, str]:
        return {}

    def decode(self, raw: bytes | str) -> str:
        if isinstance(raw, str):
            return raw
        if isinstance(raw, (bytes, bytearray, memoryview)):
            data = bytes(raw)
            try:
                if data[:2] == b'\x1f\x8b':
                    data = gunzip(data)
                return data.decode('utf-8')
            except ValueError:
                LOG.warning('%s: undecodable binary frame', self.key)
                return DECODE_ERROR
        return DECODE_ERROR

    def on_reset(self) -> None:
        return None

    def on_text(self, text: str, now: float, wall_ms: int) -> list:
        if not isinstance(text, str):
            return [self.error('INVALID_WS_MESSAGE')]
        if text == DECODE_ERROR:
            return [self.error(self.DECODE_ERROR_CODE)]
        return self._on_text(text, now, wall_ms)

    def _on_text(self, text: str, now: float, wall_ms: int) -> list:
        raise NotImplementedError

    @staticmethod
    def parse(text: str) -> dict | None:
        """A JSON object or None (non-JSON / non-object frame)."""
        try:
            msg = loads(text)
        except (ValueError, RecursionError):
            return None
        return msg if isinstance(msg, dict) else None

    # -- updates ------------------------------------------------------------
    def error(self, code: str, source: str = 'ws') -> Update:
        return Update(kind='error', key=self.key, error=code, source=source)

    def subscribed(self) -> Update:
        return Update(kind='subscribed', key=self.key)

    def book_update(self, book: Book, source: str = 'ws') -> Update:
        return Update(kind='book', key=self.key, book=book, source=source)

    def ticker_update(self, ticker: Ticker, source: str = 'ws') -> Update:
        return Update(kind='ticker', key=self.key, ticker=ticker, source=source)

    @property
    def book_code(self) -> str:
        return f'INVALID_WS_BOOK:{self.key}'

    @property
    def ticker_code(self) -> str:
        return f'INVALID_WS_TICKER:{self.key}'

    def guard(self, code: str, fn: Callable[..., list], *args: Any) -> list:
        """Run a frame parser; a data problem becomes an error update for this key."""
        try:
            return fn(*args)
        except FrameError as exc:
            return [self.error(exc.code)]
        except DATA_ERRORS:
            return [self.error(code)]

    def fatal(self, code: str) -> Unsafe:
        LOG.warning('%s: %s', self.key, code)
        return Unsafe(code)

    # -- REST snapshot --------------------------------------------------------
    def rest_snapshot(self, http: Any, timeout: float, now: float, wall_ms: int) -> list[Update]:
        """One complete REST poll: the book, plus the ticker for price kinds mark/last.

        All-or-nothing: any failure raises (see module docstring); the returned updates
        carry ``source='rest'`` and go through the hub's normal validation path.
        """
        timeout = float(timeout)
        updates = [self._rest_parse(f'INVALID_REST_BOOK:{self.key}', self._rest_book, http, timeout, now, wall_ms)]
        if self.wants_ticker:
            updates.append(self._rest_parse(f'INVALID_REST_TICKER:{self.key}', self._rest_ticker,
                                            http, timeout, now, wall_ms))
        return updates

    def _rest_book(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        raise NotImplementedError

    def _rest_ticker(self, http: Any, timeout: float, now: float, wall_ms: int) -> Update:
        raise NotImplementedError

    @staticmethod
    def _rest_parse(code: str, fn: Callable[..., Update], *args: Any) -> Update:
        try:
            return fn(*args)
        except FrameError as exc:
            raise Unsafe(exc.code) from exc
        except DATA_ERRORS as exc:
            raise Unsafe(code) from exc

    def rest_get(self, http: Any, path: str, params: Mapping[str, Any] | None, timeout: float,
                 headers: Mapping[str, str] | None = None) -> Any:
        try:
            status, payload = http_get(http, self.rest_url + path, params=params, headers=headers, timeout=timeout)
        except TransportError as exc:
            raise TransientUnsafe(f'{self.PREFIX}_REST_TRANSPORT:{exc.name}') from exc
        return rest_payload(self.PREFIX, status, payload)

    # -- instrument check (doctor) ------------------------------------------
    def ref_get(self, http: Any, path: str, params: Mapping[str, Any] | None, timeout: float,
                headers: Mapping[str, str] | None = None) -> dict:
        status, payload = http_get(http, self.rest_url + path, params=params, headers=headers, timeout=timeout)
        return reference_payload(status, payload)

    @property
    def not_listed(self) -> str:
        return f'{self.PREFIX}_REFERENCE_NOT_LISTED:{self.symbol}'

    def instrument_info(self, http: Any, timeout: float) -> dict:
        """Exact-symbol instrument check for the doctor. Never raises.

        Keys: ok, venue, source_id, symbol, status, base, quote, tick (plain text),
        contract_size, multiplier, raw_keys, reason, problems, warnings, transient.
        """
        info: dict[str, Any] = {
            'ok': False, 'venue': self.venue, 'source_id': self.key, 'symbol': self.symbol,
            'status': None, 'base': None, 'quote': None, 'tick': None, 'contract_size': None,
            'multiplier': None, 'raw_keys': [], 'reason': None, 'problems': [], 'warnings': [],
            'transient': False,
        }
        try:
            self._instrument(http, float(timeout), info)
        except ReferenceFailure as exc:
            info['reason'] = exc.reason
        except TransportError as exc:
            info['reason'] = 'REFERENCE_LOOKUP_FAILED:' + exc.name
        except DATA_ERRORS as exc:
            info['reason'] = 'REFERENCE_LOOKUP_FAILED:' + type(exc).__name__
        else:
            if info['problems']:
                info['reason'] = f'{self.PREFIX}_REFERENCE_NOT_ACTIVE_EXACT_PERP:{self.symbol}'
            info['ok'] = info['reason'] is None
        info['transient'] = bool(info['reason']) and info['reason'].startswith('REFERENCE_LOOKUP_FAILED')
        return info

    def _instrument(self, http: Any, timeout: float, info: dict) -> None:
        raise NotImplementedError

    def check_base(self, venue_base: Any, problems: list[str]) -> None:
        """Exact base coin; scaled contracts (1000STONK, 1MSTONK, ...) are never accepted."""
        expected = self.expected_base()
        if is_scaled_base(venue_base) or is_scaled_base(expected):
            problems.append(f'SCALED_CONTRACT:{detail(venue_base)}')
        if expected is None:
            problems.append('BASE_UNKNOWN')
        elif venue_base != expected:
            problems.append(f'BASE:{detail(venue_base)}')

    @staticmethod
    def check_equal(problems: list[str], name: str, actual: Any, expected: Any) -> None:
        if actual != expected:
            problems.append(f'{name}:{detail(actual)}')

    @staticmethod
    def raw_keys(row: Mapping[str, Any]) -> list[str]:
        return sorted(str(key) for key in row)
