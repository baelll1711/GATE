"""Signed Gate APIv4 REST client (DESIGN §10, §15; notes/api_gate_rest.md §1, §2, §11, §12).

Transport discipline ported from the reference (zec_spot/transport.py, spot_portfolio/venue.py):

* Everything that can fail BEFORE the request leaves the process (argument
  validation, missing keys, the read checkpoint, the rate limiter, the local clock
  guard) fails before anything is signed or sent: ``NotSent`` (or ``BudgetWait`` /
  ``ReadInterrupted``, which also prove that nothing was sent).
* Local clock guard: Gate has no nonce (only a ``Timestamp`` with a +-60 s window), so a
  wall clock that stepped BACKWARDS below the last signed request refuses (NotSent) only
  the non-emergency writes that CREATE or AMEND orders, where NotSent is safe. Cancels,
  the DMS countdown and reads are still sent (refusing them would delay protective
  cancels); the step is recorded in ``diagnostics()['clock_backwards']`` and logged.
  Larger skew is caught by the periodic ``/spot/time`` check (``max_clock_skew_ms``).
* After the request may have left: only an explicit Gate refusal (a 4xx with a JSON
  ``label``) is ``Rejected``. Timeouts, connection errors, 5xx, ``INTERNAL`` /
  ``SERVER_ERROR`` / ``TOO_BUSY``, unlabelled errors and non-JSON replies are
  ``UnknownResult`` for writes and ``ReadError`` for reads. ``TOO_MANY_REQUESTS`` /
  ``TOO_FAST`` (or HTTP 429) is ``RateLimited`` on reads and ``Rejected`` on writes
  (unlabelled 429 on a write stays ``UnknownResult``); the limiter bucket is blocked.
* No automatic retries. Exception texts and logs carry only method, endpoint group,
  HTTP status, Gate label and exception type names: never keys, signatures, headers
  or bodies.

Signing is exactly the official SDK's (``api_client.gen_sign``): HMAC-SHA512 (hex) of
``METHOD\\n/api/v4<path>\\n<unquoted query>\\n<sha512 hex of the body bytes>\\n<ts>``
with headers ``KEY``, ``Timestamp``, ``SIGN``. ``x-gate-exptime`` (ms epoch, not
signed) is sent on order create / batch create / cancel / batch cancel / cancel-all /
amend only — not on ``countdown_cancel_all`` and not on reads. Its value is always
``now + request_expiry_seconds``, except in ``probe_expired_exptime`` (``smoke-test
--probe-exptime`` only), which sends one cancel of a nonexistent order id with an expired value.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping
from urllib.parse import unquote_plus, urlencode, urlsplit

from .errors import (NotSentUnsafe, RateLimited, ReadError, Rejected, UnknownResult, Unsafe,
                     not_sent)
from .ratelimit import CANCEL, DMS, EMERGENCY_BUCKETS, ORDER, PRIVATE_READ, PUBLIC, RateLimiter

LOG = logging.getLogger(__name__)

API_PREFIX = '/api/v4'
EXPTIME_HEADER = 'x-gate-exptime'
EMPTY_BODY_SHA512 = hashlib.sha512(b'').hexdigest()
READ_METHODS = ('GET',)
WRITE_METHODS = ('POST', 'DELETE', 'PATCH')

# Error labels whose outcome is unknown (the request may have been executed).
UNKNOWN_RESULT_LABELS = frozenset({'INTERNAL', 'SERVER_ERROR', 'TOO_BUSY'})
# Rate-limit labels: the request was refused.
RATE_LIMIT_LABELS = frozenset({'TOO_MANY_REQUESTS', 'TOO_FAST'})
# Authentication / permission labels (Rejected; the engine stops). Informational set.
AUTH_LABELS = frozenset({'REQUEST_EXPIRED', 'INVALID_SIGNATURE', 'INVALID_KEY', 'INVALID_CREDENTIALS',
                         'AUTHENTICATION_FAILED', 'MISSING_REQUIRED_HEADER', 'IP_FORBIDDEN', 'READ_ONLY',
                         'FORBIDDEN', 'ACCOUNT_LOCKED'})

# Response headers (case-insensitive) recorded when present; not confirmed for REST (notes §12).
RATE_LIMIT_HEADERS = {
    'x-gate-ratelimit-requests-remain': 'remain',
    'x-gate-ratelimit-limit': 'limit',
    'x-gate-ratelimit-reset-timestamp': 'reset_timestamp',
}

# (method, path regex) of the requests that carry x-gate-exptime (SDK spot_api.py, notes §2).
_EXPTIME_ROUTES = (
    ('POST', re.compile(r'/spot/orders')),
    ('POST', re.compile(r'/spot/batch_orders')),
    ('DELETE', re.compile(r'/spot/orders')),
    ('DELETE', re.compile(r'/spot/orders/[^/]+')),
    ('POST', re.compile(r'/spot/cancel_batch_orders')),
    ('PATCH', re.compile(r'/spot/orders/[^/]+')),
    ('POST', re.compile(r'/spot/amend_batch_orders')),
)
# Write endpoints this client may call, and their limiter bucket. Anything else is refused.
_WRITE_ROUTES = (
    ('POST', re.compile(r'/spot/orders'), ORDER),
    ('POST', re.compile(r'/spot/batch_orders'), ORDER),
    ('PATCH', re.compile(r'/spot/orders/[^/]+'), ORDER),
    ('POST', re.compile(r'/spot/amend_batch_orders'), ORDER),
    ('DELETE', re.compile(r'/spot/orders'), CANCEL),
    ('DELETE', re.compile(r'/spot/orders/[^/]+'), CANCEL),
    ('POST', re.compile(r'/spot/cancel_batch_orders'), CANCEL),
    ('POST', re.compile(r'/spot/countdown_cancel_all'), DMS),
)
_GROUPS = (
    (re.compile(r'/spot/orders/[^/]+'), '/spot/orders/{order_id}'),
    (re.compile(r'/spot/currency_pairs/[^/]+'), '/spot/currency_pairs/{currency_pair}'),
)
_PATH_RE = re.compile(r'(?:/[A-Za-z0-9_.\-]+)+')
_LABEL_RE = re.compile(r'[A-Z][A-Z0-9_]{0,63}')
_PARAM_KEY_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,63}')
_HEX_RUN_RE = re.compile(r'[0-9a-fA-F]{32,}')


def requires_exptime(method: str, path: str) -> bool:
    """True for the requests on which DESIGN §15 sends ``x-gate-exptime``."""
    return any(method == m and rx.fullmatch(path) for m, rx in _EXPTIME_ROUTES)


def endpoint_group(path: str) -> str:
    """Path with ids replaced by placeholders (rate-limit grouping, logs)."""
    for rx, group in _GROUPS:
        if rx.fullmatch(path):
            return group
    return path


def default_bucket(method: str, path: str, *, public: bool = False) -> str:
    """Limiter bucket for a request; raises ValueError for an unsupported write endpoint."""
    if public:
        return f'{PUBLIC}:{endpoint_group(path)}'
    if method in READ_METHODS:
        return f'{PRIVATE_READ}:{endpoint_group(path)}'
    for m, rx, bucket in _WRITE_ROUTES:
        if method == m and rx.fullmatch(path):
            return bucket
    raise ValueError(f'Unsupported write endpoint: {method} {endpoint_group(path)}')


def body_sha512(payload: str | None) -> str:
    return hashlib.sha512((payload or '').encode('utf-8')).hexdigest()


def sign_request(secret: str, method: str, sign_path: str, query: str, payload: str | None,
                 timestamp: str) -> str:
    """SDK ``gen_sign``: hex HMAC-SHA512 of the 5-line string (no trailing newline)."""
    message = '%s\n%s\n%s\n%s\n%s' % (method, sign_path, query or '', body_sha512(payload), timestamp)
    return hmac.new(secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha512).hexdigest()


def _reject_constant(value: str):
    raise ValueError('Non-finite JSON number')


def loads_json(content: bytes | str) -> Any:
    """Strict JSON: floats become Decimal, NaN/Infinity are rejected."""
    if isinstance(content, bytes):
        content = content.decode('utf-8')
    return json.loads(content, parse_float=Decimal, parse_constant=_reject_constant)


@dataclass(frozen=True)
class _Prepared:
    method: str
    path: str
    group: str
    url: str
    sign_path: str
    sign_query: str
    payload: str | None
    bucket: str
    emergency: bool
    expire: bool
    signed: bool
    write: bool
    order_write: bool       # creates or amends an order (ORDER bucket endpoint)


class GateRestClient:
    """Gate APIv4 REST transport: signing, expiry header, limiter, error classification.

    ``before_read`` (callable or None) is the venue/engine checkpoint: it is called
    before every physical READ (public or private) and may raise ReadInterrupted or
    BudgetWait, in which case nothing is sent.
    """

    def __init__(self, base_url: str, key: str | None, secret: str | None, *,
                 timeout: float, request_expiry_seconds: float, limiter: RateLimiter | None,
                 session: Any = None, clock_guard: bool = True):
        parts = urlsplit(base_url) if isinstance(base_url, str) else None
        if (parts is None or parts.scheme != 'https' or not parts.netloc or parts.query or parts.fragment
                or parts.path.rstrip('/') != API_PREFIX):
            raise ValueError('base_url must be https://<host>/api/v4')
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float, Decimal)) or not 0 < float(timeout) <= 60:
            raise ValueError('timeout must be in (0, 60] seconds')
        if (isinstance(request_expiry_seconds, bool) or not isinstance(request_expiry_seconds, (int, float, Decimal))
                or not 1 <= float(request_expiry_seconds) <= 600):
            raise ValueError('request_expiry_seconds must be in [1, 600]')
        for name, value in (('key', key), ('secret', secret)):
            if value is not None and (not isinstance(value, str) or not value or value != value.strip()):
                raise ValueError(f'Invalid API {name} format')
        self._origin = f'{parts.scheme}://{parts.netloc}'
        self._key = key
        self._secret = secret
        self.timeout = float(timeout)
        self.request_expiry_ms = int(float(request_expiry_seconds) * 1000)
        self.limiter = limiter
        self._session = session
        self._owns_session = session is None
        self.clock_guard = bool(clock_guard)
        self.before_read: Callable[[], None] | None = None
        self._lock = threading.Lock()
        self._last_signed_ms = 0
        self._diag_lock = threading.Lock()
        self._diag: dict[str, Any] = {'requests': 0, 'outcomes': {}, 'last_error': None, 'rate_limit_headers': {},
                                      'clock_backwards': {'count': 0, 'last': None}}

    # ------------------------------------------------------------------ construction helpers
    @classmethod
    def from_config(cls, cfg: Any, *, limiter: RateLimiter | None = None, session: Any = None) -> 'GateRestClient':
        secrets = cfg.secrets
        return cls(cfg.market['rest_url'], secrets.gate_key, secrets.gate_secret,
                   timeout=float(cfg.app['request_timeout_seconds']),
                   request_expiry_seconds=float(cfg.app['request_expiry_seconds']),
                   limiter=limiter, session=session)

    def __repr__(self) -> str:  # never leak keys
        return (f'GateRestClient(origin={self._origin!r}, key={"set" if self._key else None}, '
                f'secret={"set" if self._secret else None})')

    @property
    def has_keys(self) -> bool:
        return bool(self._key and self._secret)

    @property
    def last_signed_ms(self) -> int:
        return self._last_signed_ms

    @property
    def session(self) -> Any:
        if self._session is None:
            import requests  # local import: tests inject a fake session
            self._session = requests.Session()
        return self._session

    def close(self) -> None:
        session, self._session = self._session, None
        if session is not None and self._owns_session:
            try:
                session.close()
            except Exception:
                pass

    # ------------------------------------------------------------------ public API
    def public(self, method: str, path: str, params: Any = None, *, bucket: str | None = None) -> Any:
        """Unsigned GET (market data, pair metadata, server time)."""
        return self._request(method, path, params, None, signed=False, bucket=bucket, emergency=None,
                             expire=None, cost=1)

    def private(self, method: str, path: str, params: Any = None, body: Any = None, *,
                bucket: str | None = None, emergency: bool | None = None, expire: bool | None = None,
                cost: int = 1) -> Any:
        """Signed request. ``bucket``/``emergency``/``expire`` default from the endpoint tables;
        an explicit ``expire`` must agree with DESIGN §15 (else NotSent)."""
        return self._request(method, path, params, body, signed=True, bucket=bucket, emergency=emergency,
                             expire=expire, cost=cost)

    def probe_expired_exptime(self, order_id: str, params: Any, *, expired_ms: int) -> Any:
        """``smoke-test --probe-exptime`` ONLY (API_NOTES §9.2, step 5.2): one signed
        ``DELETE /spot/orders/{order_id}`` whose ``x-gate-exptime`` is deliberately in the PAST,
        to learn which label / HTTP status Gate returns for an expired request.

        Narrow by construction: only a single-order cancel of a numeric id, ``currency_pair``
        required, and ``expired_ms`` must be at least 1 s in the past. Nothing else in the
        bot can set the header; every other request keeps ``now + request_expiry_seconds``.
        The reply is classified as usual (the expected outcome is ``Rejected``).
        """
        oid = str(order_id) if isinstance(order_id, int) and not isinstance(order_id, bool) else order_id
        if not isinstance(oid, str) or not re.fullmatch(r'[1-9][0-9]{0,31}', oid):
            raise not_sent(ValueError('PROBE_ORDER_ID_INVALID'))
        pairs = list(params.items()) if isinstance(params, Mapping) else list(params or ())
        if not any(isinstance(p, (tuple, list)) and len(p) == 2 and p[0] == 'currency_pair' for p in pairs):
            raise not_sent(ValueError('PROBE_CURRENCY_PAIR_REQUIRED'))
        if (isinstance(expired_ms, bool) or not isinstance(expired_ms, int) or expired_ms <= 0
                or expired_ms > int(time.time() * 1000) - 1000):
            raise not_sent(ValueError('PROBE_EXPTIME_MUST_BE_IN_THE_PAST'))
        return self._request('DELETE', f'/spot/orders/{oid}', pairs, None, signed=True, bucket=None,
                             emergency=None, expire=None, cost=1, exptime_override_ms=expired_ms)

    def diagnostics(self) -> dict:
        with self._diag_lock:
            return {
                'requests': self._diag['requests'],
                'outcomes': dict(self._diag['outcomes']),
                'last_error': dict(self._diag['last_error']) if self._diag['last_error'] else None,
                'rate_limit_headers': {k: dict(v) for k, v in self._diag['rate_limit_headers'].items()},
                'last_signed_ms': self._last_signed_ms,
                'clock_backwards': {'count': self._diag['clock_backwards']['count'],
                                    'last': (dict(self._diag['clock_backwards']['last'])
                                             if self._diag['clock_backwards']['last'] else None)},
            }

    # ------------------------------------------------------------------ internals
    def _prepare(self, method: Any, path: Any, params: Any, body: Any, *, signed: bool, bucket: str | None,
                 emergency: bool | None, expire: bool | None) -> _Prepared:
        if not isinstance(method, str) or method.upper() not in READ_METHODS + WRITE_METHODS:
            raise ValueError('Unsupported HTTP method')
        method = method.upper()
        write = method in WRITE_METHODS
        if not signed and write:
            raise ValueError('Public requests are GET only')
        if (not isinstance(path, str) or not _PATH_RE.fullmatch(path) or path.startswith('/api/')
                or any(segment in ('.', '..') for segment in path.split('/'))):
            raise ValueError('Invalid API path')
        if body is not None and not write:
            raise ValueError('GET requests have no body')
        if signed and not self.has_keys:
            raise Unsafe('GATE_API_KEYS_MISSING')
        query = self._encode_params(params)
        payload = None
        if body is not None:
            if not isinstance(body, (dict, list)):
                raise ValueError('Request body must be a JSON object or array')
            try:
                payload = json.dumps(body)  # SDK: json.dumps defaults; the hash covers these exact bytes
            except (TypeError, ValueError):
                raise ValueError('Request body is not JSON serializable') from None
        if bucket is None:
            bucket = default_bucket(method, path, public=not signed)
        elif write:
            default_bucket(method, path)  # still refuse an unsupported write endpoint
        if emergency is None:
            emergency = bucket.split(':', 1)[0] in EMERGENCY_BUCKETS
        needs_exptime = signed and requires_exptime(method, path)
        if expire is not None and bool(expire) != needs_exptime:
            raise ValueError('x-gate-exptime policy mismatch for this endpoint')
        url = self._origin + API_PREFIX + path + ('?' + query if query else '')
        order_write = write and default_bucket(method, path) == ORDER
        return _Prepared(method=method, path=path, group=endpoint_group(path), url=url,
                         sign_path=API_PREFIX + path, sign_query=unquote_plus(query), payload=payload,
                         bucket=bucket, emergency=bool(emergency), expire=needs_exptime, signed=signed,
                         write=write, order_write=order_write)

    @staticmethod
    def _encode_params(params: Any) -> str:
        if params is None:
            return ''
        items = list(params.items()) if isinstance(params, Mapping) else list(params)
        pairs: list[tuple[str, str]] = []
        for item in items:
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                raise ValueError('Query parameters must be (key, value) pairs')
            key, value = item
            if not isinstance(key, str) or not _PARAM_KEY_RE.fullmatch(key):
                raise ValueError('Invalid query parameter name')
            if value is None:
                continue
            if isinstance(value, bool):
                raise ValueError('Boolean query parameter values are not supported')
            if isinstance(value, int):
                value = str(value)
            if not isinstance(value, str) or not value or any(ch in value for ch in '\r\n\t'):
                raise ValueError('Invalid query parameter value')
            pairs.append((key, value))
        return urlencode(pairs)

    def _sign_headers(self, prepared: _Prepared, now: float) -> dict[str, str]:
        timestamp = str(int(now))
        sign = sign_request(self._secret or '', prepared.method, prepared.sign_path, prepared.sign_query,
                            prepared.payload, timestamp)
        return {'KEY': self._key or '', 'Timestamp': timestamp, 'SIGN': sign}

    def _request(self, method: Any, path: Any, params: Any, body: Any, *, signed: bool, bucket: str | None,
                 emergency: bool | None, expire: bool | None, cost: int,
                 exptime_override_ms: int | None = None) -> Any:
        try:
            prepared = self._prepare(method, path, params, body, signed=signed, bucket=bucket,
                                     emergency=emergency, expire=expire)
            if exptime_override_ms is not None and not (
                    prepared.method == 'DELETE' and prepared.expire and prepared.group == '/spot/orders/{order_id}'):
                raise ValueError('EXPTIME_OVERRIDE_NOT_ALLOWED')   # probe_expired_exptime only
        except Exception as exc:
            raise not_sent(exc) from None
        if not prepared.write and self.before_read is not None:
            self.before_read()          # ReadInterrupted / BudgetWait: nothing sent
        if self.limiter is not None:
            try:
                self.limiter.admit(prepared.bucket, cost, emergency=prepared.emergency)  # BudgetWait: nothing sent
            except ValueError as exc:
                raise not_sent(exc) from None
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        with self._lock:
            now = time.time()
            now_ms = int(now * 1000)
            if prepared.signed:
                if now_ms < self._last_signed_ms:
                    # Gate has no nonce: only a create/amend is refused (NotSent is safe there);
                    # cancels, DMS renewals and reads go out, the step is diagnostic only.
                    if self.clock_guard and prepared.order_write and not prepared.emergency:
                        self._note(prepared, 'not_sent', error='LOCAL_CLOCK_MOVED_BACKWARDS')
                        raise NotSentUnsafe('LOCAL_CLOCK_MOVED_BACKWARDS')
                    self._note_clock_backwards(prepared, self._last_signed_ms - now_ms)
                self._last_signed_ms = max(self._last_signed_ms, now_ms)
                headers.update(self._sign_headers(prepared, now))
            if prepared.expire:
                expires = now_ms + self.request_expiry_ms if exptime_override_ms is None else exptime_override_ms
                headers[EXPTIME_HEADER] = str(expires)
        started = time.monotonic()
        try:
            data = None if prepared.payload is None else prepared.payload.encode('utf-8')
            response = self.session.request(prepared.method, prepared.url, data=data, headers=headers,
                                            timeout=self.timeout, allow_redirects=False)
        except Exception as exc:
            kind = type(exc).__name__
            self._note(prepared, 'transport_error', error=kind, started=started)
            if prepared.write:
                raise UnknownResult(f'GATE_TRANSPORT_ERROR:{prepared.method} {prepared.group}:{kind}') from None
            raise ReadError(f'GATE_TRANSPORT_ERROR:{prepared.method} {prepared.group}:{kind}') from None
        return self._handle(prepared, response, started)

    def _handle(self, prepared: _Prepared, response: Any, started: float) -> Any:
        where = f'{prepared.method} {prepared.group}'
        status = getattr(response, 'status_code', None)
        if isinstance(status, bool) or not isinstance(status, int):
            self._note(prepared, 'invalid_response', started=started)
            if prepared.write:
                raise UnknownResult(f'GATE_INVALID_RESPONSE:{where}')
            raise ReadError(f'GATE_INVALID_RESPONSE:{where}')
        self._record_rate_headers(prepared.bucket, getattr(response, 'headers', None))
        try:
            content = getattr(response, 'content', None)
            if content is None:
                content = getattr(response, 'text', '')
            data, parsed = loads_json(content), True
        except Exception:
            data, parsed = None, False
        if 200 <= status < 300:
            if not parsed:
                self._note(prepared, 'non_json', status=status, started=started)
                if prepared.write:
                    raise UnknownResult(f'GATE_NON_JSON_REPLY:{where}:HTTP_{status}')
                raise ReadError(f'GATE_NON_JSON_REPLY:{where}:HTTP_{status}')
            self._note(prepared, 'ok', status=status, started=started)
            return data
        label, message = self._error_fields(data)
        if label in RATE_LIMIT_LABELS or status == 429:
            if self.limiter is not None:
                try:
                    self.limiter.rate_limited(prepared.bucket)
                except Exception:
                    pass
        code = label or f'HTTP_{status}'
        if status >= 500 or label in UNKNOWN_RESULT_LABELS:
            self._note(prepared, 'server_error', status=status, label=label, started=started)
            if prepared.write:
                raise UnknownResult(f'GATE_RESULT_UNKNOWN:{where}:{code}:HTTP_{status}')
            raise ReadError(f'GATE_READ_FAILED:{where}:{code}:HTTP_{status}')
        if label in RATE_LIMIT_LABELS or status == 429:
            self._note(prepared, 'rate_limited', status=status, label=label, started=started)
            if not prepared.write:
                raise RateLimited(f'GATE_RATE_LIMITED:{where}:{code}:HTTP_{status}')
            if label:
                raise Rejected(self._rejected_text(label, message), label=label, status=status)
            raise UnknownResult(f'GATE_RESULT_UNKNOWN:{where}:{code}:HTTP_{status}')
        if 400 <= status < 500 and label:
            self._note(prepared, 'rejected', status=status, label=label, started=started)
            raise Rejected(self._rejected_text(label, message), label=label, status=status)
        self._note(prepared, 'http_error', status=status, label=label, started=started)
        if prepared.write:
            raise UnknownResult(f'GATE_RESULT_UNKNOWN:{where}:{code}:HTTP_{status}')
        raise ReadError(f'GATE_READ_FAILED:{where}:{code}:HTTP_{status}')

    @staticmethod
    def _error_fields(data: Any) -> tuple[str | None, str | None]:
        if not isinstance(data, dict):
            return None, None
        label = data.get('label')
        label = label.strip() if isinstance(label, str) else None
        if not label or not _LABEL_RE.fullmatch(label):
            label = None
        detail, message = data.get('detail'), data.get('message')
        text = detail if isinstance(detail, str) and detail else message if isinstance(message, str) else None
        return label, text

    def _safe_text(self, value: str | None) -> str:
        if not isinstance(value, str):
            return ''
        for secret in (self._key, self._secret):
            if secret:
                value = re.sub(re.escape(secret), '[REDACTED]', value, flags=re.I)
        value = _HEX_RUN_RE.sub('[REDACTED]', value)
        return ' '.join(value.split())[:160]

    def _rejected_text(self, label: str, message: str | None) -> str:
        text = self._safe_text(message)
        return f'{label}: {text}' if text else label

    def _record_rate_headers(self, bucket: str, headers: Any) -> None:
        try:
            if not headers:
                return
            info: dict[str, Any] = {}
            for name, value in headers.items():
                field = RATE_LIMIT_HEADERS.get(str(name).lower())
                if field is None:
                    continue
                text = str(value).strip()
                info[field] = int(text) if text.isdigit() else text[:32]
            if not info:
                return
            info['at_ms'] = int(time.time() * 1000)
            with self._diag_lock:
                self._diag['rate_limit_headers'][bucket] = info
            if self.limiter is not None:
                self.limiter.record_headers(bucket, info)
        except Exception:
            pass  # diagnostics never change a trading result

    def _note_clock_backwards(self, prepared: _Prepared, behind_ms: int) -> None:
        """Diagnostic only: a cancel / DMS / read signed while the wall clock is behind the
        last signed request (Gate has no nonce, so the request is still sent)."""
        try:
            with self._diag_lock:
                entry = self._diag['clock_backwards']
                entry['count'] += 1
                entry['last'] = {'endpoint': f'{prepared.method} {prepared.group}', 'behind_ms': int(behind_ms),
                                 'at_ms': int(time.time() * 1000)}
            LOG.warning('REST %s %s: local clock is %s ms behind the last signed request '
                        '(LOCAL_CLOCK_MOVED_BACKWARDS); sent anyway (cancel/DMS/read)',
                        prepared.method, prepared.group, behind_ms)
        except Exception:
            pass

    def _note(self, prepared: _Prepared, outcome: str, *, status: int | None = None, label: str | None = None,
              error: str | None = None, started: float | None = None) -> None:
        try:
            elapsed = None if started is None else round((time.monotonic() - started) * 1000, 1)
            with self._diag_lock:
                self._diag['requests'] += 1
                self._diag['outcomes'][outcome] = self._diag['outcomes'].get(outcome, 0) + 1
                if outcome != 'ok':
                    self._diag['last_error'] = {'endpoint': f'{prepared.method} {prepared.group}',
                                                'outcome': outcome, 'status': status, 'label': label,
                                                'error': error, 'at_ms': int(time.time() * 1000)}
            if outcome == 'ok':
                LOG.debug('REST %s %s -> %s (%s ms)', prepared.method, prepared.group, status, elapsed)
            else:
                LOG.warning('REST %s %s -> %s status=%s label=%s error=%s (%s ms)', prepared.method,
                            prepared.group, outcome, status, label, error, elapsed)
        except Exception:
            pass
