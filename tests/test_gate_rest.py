"""Gate REST client + rate limiter (DESIGN §10, §15; notes/api_gate_rest.md §1, §2, §11, §12).

Offline: a FakeSession records every request (method, url, query, headers, body) and returns
scripted responses. Signature vectors from notes §1.3 are recomputed here with an independent
snippet (hashlib/hmac only) and compared with what the client actually sends.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlsplit

import pytest
import requests

from gate_stonk.errors import (BudgetWait, NotSent, NotSentUnsafe, RateLimited, ReadError, ReadInterrupted,
                               Rejected, UnknownResult, Unsafe)
from gate_stonk.gate_rest import (EMPTY_BODY_SHA512, GateRestClient, default_bucket, endpoint_group,
                                  requires_exptime, sign_request)
from gate_stonk.ratelimit import RateLimiter, default_buckets

BASE_URL = 'https://api.gateio.ws/api/v4'
KEY = 'KEYxTESTxVALUE123'
SECRET = 'SECRET_TEST'
VECTOR_TS = 1760000000.0
VECTOR_BODY = ('{"text": "t-stonk1", "currency_pair": "STONK_USDT", "type": "limit", "account": "spot", '
               '"side": "buy", "amount": "100", "price": "0.1355", "time_in_force": "poc"}')
VECTOR_BODY_SHA512 = ('72b3b938ea4a7603b3814ae8bee50ffac1bbcccdb99e234d05f8866a2cb214b2'
                      'd83462a59d6bb6dee9dcce91f46800a5f025861b4748f99811ea12534ec2b16e')
VECTOR_SIGN_POST = ('d744128d089b6333ccf6136e235b55934c81f5f3b7e01fb80b98c468d29ad7f1'
                    '3ab5c5ce7f9d00115d79ce070635b43b76941752f5321a3804923d1edebf1352')
VECTOR_SIGN_GET = ('1a50511f4d72070e2a8613a4ad4afc332def58d7192cb36834a8ba5dde6a7d86'
                   '2e48fa62c495bdb97a66cd53f5de4b6a34ff7384f807cee7bee92f4e459bfe4d')
VECTOR_SIGN_DELETE = ('6c68029245937e6807f63583b8cd730c94c193f770d16050f5fbb33aa4dc37e8'
                      '7c42a8a83f2cf09f29ab74382ff7b3814387aa40889f6398ccaa17ed22cca795')


# ----------------------------------------------------------------------------- fakes

class FakeResponse:
    def __init__(self, status: int = 200, body=None, headers=None, raw: bytes | str | None = None):
        self.status_code = status
        self.headers = headers or {}
        if raw is not None:
            self.content = raw.encode() if isinstance(raw, str) else raw
        else:
            self.content = json.dumps(body).encode()


class FakeSession:
    """Records requests; returns scripted responses (FakeResponse, exception, or callable(record))."""

    def __init__(self, *responses):
        self.calls: list[dict] = []
        self.script = list(responses)
        self.closed = False

    def queue(self, *responses):
        self.script.extend(responses)

    def request(self, method, url, data=None, headers=None, timeout=None, allow_redirects=True):
        parts = urlsplit(url)
        record = {'method': method, 'url': url, 'path': parts.path, 'query': parts.query,
                  'params': parse_qsl(parts.query, keep_blank_values=True), 'headers': dict(headers or {}),
                  'body': None if data is None else data.decode() if isinstance(data, bytes) else data,
                  'timeout': timeout, 'allow_redirects': allow_redirects}
        self.calls.append(record)
        if not self.script:
            raise AssertionError('unexpected request ' + method + ' ' + parts.path)
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            return item(record)
        return item

    def close(self):
        self.closed = True


def ok(body=None, status=200, headers=None):
    return FakeResponse(status, {} if body is None else body, headers)


def make_client(session=None, limiter=None, key=KEY, secret=SECRET, **kwargs):
    session = session if session is not None else FakeSession()
    client = GateRestClient(BASE_URL, key, secret, timeout=5, request_expiry_seconds=10, limiter=limiter,
                            session=session, **kwargs)
    return client, session


def independent_sign(secret: str, method: str, path: str, query: str, body: str | None, ts: str) -> str:
    payload_hash = hashlib.sha512((body or '').encode('utf-8')).hexdigest()
    message = f'{method}\n{path}\n{query}\n{payload_hash}\n{ts}'
    return hmac.new(secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha512).hexdigest()


# ----------------------------------------------------------------------------- signing

def test_independent_snippet_reproduces_notes_vectors():
    assert hashlib.sha512(VECTOR_BODY.encode()).hexdigest() == VECTOR_BODY_SHA512
    assert hashlib.sha512(b'').hexdigest() == EMPTY_BODY_SHA512
    cases = [('POST', '/api/v4/spot/orders', '', VECTOR_BODY, VECTOR_SIGN_POST),
             ('GET', '/api/v4/spot/orders', 'currency_pair=STONK_USDT&status=open', None, VECTOR_SIGN_GET),
             ('DELETE', '/api/v4/spot/orders/12345', 'currency_pair=STONK_USDT', None, VECTOR_SIGN_DELETE)]
    for method, path, query, body, expected in cases:
        assert independent_sign('SECRET_TEST', method, path, query, body, '1760000000') == expected
        assert sign_request('SECRET_TEST', method, path, query, body, '1760000000') == expected


def test_post_signature_headers_and_exact_body(clock):
    clock[0] = VECTOR_TS
    client, session = make_client()
    body = {'text': 't-stonk1', 'currency_pair': 'STONK_USDT', 'type': 'limit', 'account': 'spot', 'side': 'buy',
            'amount': '100', 'price': '0.1355', 'time_in_force': 'poc'}
    session.queue(ok({'id': '1'}, 201))
    assert client.private('POST', '/spot/orders', body=body) == {'id': '1'}
    call = session.calls[0]
    assert call['method'] == 'POST'
    assert call['url'] == 'https://api.gateio.ws/api/v4/spot/orders'
    assert call['body'] == VECTOR_BODY                      # hashed bytes == sent bytes
    assert call['headers']['KEY'] == KEY
    assert call['headers']['Timestamp'] == '1760000000'
    assert call['headers']['SIGN'] == VECTOR_SIGN_POST
    assert call['headers']['SIGN'] == independent_sign(SECRET, 'POST', '/api/v4/spot/orders', '', call['body'],
                                                       call['headers']['Timestamp'])
    assert call['headers']['x-gate-exptime'] == str(1760000000000 + 10_000)
    assert call['headers']['Content-Type'] == 'application/json'
    assert call['allow_redirects'] is False
    assert call['timeout'] == 5.0


def test_get_and_delete_signatures_match_vectors(clock):
    clock[0] = VECTOR_TS
    client, session = make_client()
    session.queue(ok([]), ok({'id': '12345'}))
    client.private('GET', '/spot/orders', [('currency_pair', 'STONK_USDT'), ('status', 'open')])
    client.private('DELETE', '/spot/orders/12345', [('currency_pair', 'STONK_USDT')])
    get, delete = session.calls
    assert get['query'] == 'currency_pair=STONK_USDT&status=open'
    assert get['body'] is None
    assert get['headers']['SIGN'] == VECTOR_SIGN_GET
    assert 'x-gate-exptime' not in get['headers']
    assert delete['path'] == '/api/v4/spot/orders/12345'
    assert delete['headers']['SIGN'] == VECTOR_SIGN_DELETE
    assert delete['headers']['x-gate-exptime'] == '1760000010000'


def test_signature_uses_unquoted_query_like_sdk(clock):
    clock[0] = 1800000000.25
    client, session = make_client()
    session.queue(ok([]))
    client.private('GET', '/spot/my_trades', [('currency_pair', 'STONK_USDT'), ('text', 't-a b/c')])
    call = session.calls[0]
    assert call['query'] == 'currency_pair=STONK_USDT&text=t-a+b%2Fc'      # sent encoded
    assert call['headers']['Timestamp'] == '1800000000'                     # integer seconds
    expected = independent_sign(SECRET, 'GET', '/api/v4/spot/my_trades', 'currency_pair=STONK_USDT&text=t-a b/c',
                                None, '1800000000')
    assert call['headers']['SIGN'] == expected


def test_public_requests_are_unsigned_and_need_no_keys(clock):
    client, session = make_client(key=None, secret=None)
    session.queue(ok({'server_time': 1}))
    assert client.public('GET', '/spot/time') == {'server_time': 1}
    headers = session.calls[0]['headers']
    assert not {'KEY', 'SIGN', 'Timestamp', 'x-gate-exptime'} & set(headers)
    with pytest.raises(NotSentUnsafe, match='GATE_API_KEYS_MISSING'):
        client.private('GET', '/spot/accounts')
    assert len(session.calls) == 1


# ----------------------------------------------------------------------------- x-gate-exptime

@pytest.mark.parametrize('method,path,expected', [
    ('POST', '/spot/orders', True),
    ('POST', '/spot/batch_orders', True),
    ('DELETE', '/spot/orders', True),
    ('DELETE', '/spot/orders/912345678901', True),
    ('POST', '/spot/cancel_batch_orders', True),
    ('PATCH', '/spot/orders/912345678901', True),
    ('POST', '/spot/amend_batch_orders', True),
    ('POST', '/spot/countdown_cancel_all', False),
    ('GET', '/spot/orders', False),
    ('GET', '/spot/orders/912345678901', False),
    ('GET', '/spot/my_trades', False),
])
def test_exptime_only_on_listed_writes(clock, method, path, expected):
    assert requires_exptime(method, path) is expected
    client, session = make_client()
    session.queue(ok([]))
    body = {'x': 1} if method in ('POST', 'PATCH') else None
    client.private(method, path, body=body)
    headers = session.calls[0]['headers']
    assert ('x-gate-exptime' in headers) is expected
    if expected:
        assert headers['x-gate-exptime'] == str(int(clock[0] * 1000) + 10_000)


def test_explicit_expire_must_match_policy(clock):
    client, session = make_client()
    with pytest.raises(NotSent):
        client.private('POST', '/spot/countdown_cancel_all', body={'timeout': 0}, expire=True)
    with pytest.raises(NotSent):
        client.private('POST', '/spot/orders', body={}, expire=False)
    session.queue(ok({}))
    client.private('POST', '/spot/orders', body={}, expire=True)
    assert len(session.calls) == 1


# ----------------------------------------------------------------------------- classification

HTML = '<html>bad gateway</html>'


@pytest.mark.parametrize('method,status,body,raw,exc_type,label', [
    ('POST', 400, {'label': 'BALANCE_NOT_ENOUGH', 'message': 'Not enough balance'}, None, Rejected, 'BALANCE_NOT_ENOUGH'),
    ('POST', 400, {'label': 'POC_FILL_IMMEDIATELY', 'message': 'would take'}, None, Rejected, 'POC_FILL_IMMEDIATELY'),
    ('POST', 401, {'label': 'INVALID_SIGNATURE', 'message': 'Signature mismatch'}, None, Rejected, 'INVALID_SIGNATURE'),
    ('POST', 400, {'label': 'REQUEST_EXPIRED', 'detail': 'too old'}, None, Rejected, 'REQUEST_EXPIRED'),
    ('DELETE', 400, {'label': 'ORDER_CLOSED', 'message': 'closed'}, None, Rejected, 'ORDER_CLOSED'),
    ('GET', 404, {'label': 'ORDER_NOT_FOUND', 'message': 'Order not found'}, None, Rejected, 'ORDER_NOT_FOUND'),
    ('POST', 500, {'label': 'INTERNAL', 'message': 'x'}, None, UnknownResult, None),
    ('POST', 400, {'label': 'SERVER_ERROR', 'message': 'x'}, None, UnknownResult, None),
    ('POST', 503, {'label': 'TOO_BUSY', 'message': 'x'}, None, UnknownResult, None),
    ('PATCH', 500, {'label': 'BALANCE_NOT_ENOUGH'}, None, UnknownResult, None),      # any 5xx: maybe performed
    ('GET', 500, {'label': 'INTERNAL'}, None, ReadError, None),
    ('GET', 400, {'label': 'TOO_BUSY'}, None, ReadError, None),
    ('POST', 502, None, HTML, UnknownResult, None),
    ('GET', 502, None, HTML, ReadError, None),
    ('POST', 429, {'label': 'TOO_MANY_REQUESTS', 'message': 'slow down'}, None, Rejected, 'TOO_MANY_REQUESTS'),
    ('POST', 400, {'label': 'TOO_FAST'}, None, Rejected, 'TOO_FAST'),
    ('GET', 429, {'label': 'TOO_MANY_REQUESTS'}, None, RateLimited, None),
    ('GET', 400, {'label': 'TOO_FAST'}, None, RateLimited, None),
    ('GET', 429, None, HTML, RateLimited, None),
    ('POST', 429, None, HTML, UnknownResult, None),                                   # unlabelled 429 on a write
    ('POST', 403, None, HTML, UnknownResult, None),                                   # unlabelled 4xx
    ('GET', 403, None, HTML, ReadError, None),
    ('POST', 400, {'message': 'no label'}, None, UnknownResult, None),
    ('POST', 400, {'label': 'not-a-label', 'message': 'x'}, None, UnknownResult, None),
    ('GET', 400, {'message': 'no label'}, None, ReadError, None),
    ('POST', 200, None, 'not json', UnknownResult, None),
    ('GET', 200, None, 'not json', ReadError, None),
    ('POST', 201, None, '{"price": NaN}', UnknownResult, None),
    ('GET', 200, None, '{"price": Infinity}', ReadError, None),
    ('POST', 302, None, '', UnknownResult, None),
    ('GET', 301, None, '', ReadError, None),
])
def test_error_classification_table(clock, method, status, body, raw, exc_type, label):
    client, session = make_client()
    session.queue(FakeResponse(status, body, raw=raw))
    path = '/spot/orders' if method != 'PATCH' else '/spot/orders/1'
    request_body = {'a': '1'} if method in ('POST', 'PATCH') else None
    with pytest.raises(exc_type) as info:
        client.private(method, path, body=request_body)
    assert type(info.value) is exc_type          # e.g. a plain ReadError is not RateLimited
    if exc_type is Rejected:
        assert info.value.label == label
        assert info.value.status == status
        assert str(info.value).startswith(label)
    assert len(session.calls) == 1               # never retried


@pytest.mark.parametrize('error', [requests.Timeout('read timed out'), requests.ConnectionError('reset'),
                                   OSError('broken pipe'), ValueError('weird')])
def test_transport_errors_unknown_for_writes_readerror_for_reads(clock, error):
    client, session = make_client()
    session.queue(error, error)
    with pytest.raises(UnknownResult) as write_info:
        client.private('POST', '/spot/orders', body={'a': '1'})
    with pytest.raises(ReadError) as read_info:
        client.private('GET', '/spot/accounts')
    for exc in (write_info.value, read_info.value):
        assert type(error).__name__ in str(exc)
        assert str(error) not in str(exc)        # only the exception type name is reported
    assert type(read_info.value) is ReadError
    assert len(session.calls) == 2


def test_success_json_floats_become_decimal(clock):
    client, session = make_client()
    session.queue(FakeResponse(200, raw='{"price": 0.1345, "n": 7}'))
    data = client.public('GET', '/spot/time')
    assert data == {'price': Decimal('0.1345'), 'n': 7}
    assert isinstance(data['price'], Decimal)


# ----------------------------------------------------------------------------- pre-send refusals

@pytest.mark.parametrize('call', [
    lambda c: c.private('POST', '/api/v4/spot/orders', body={}),
    lambda c: c.private('POST', 'spot/orders', body={}),
    lambda c: c.private('POST', '/spot/orders/../x', body={}),
    lambda c: c.private('POST', '/spot/orders?x=1', body={}),
    lambda c: c.private('POST', '/spot/unknown_write', body={}),
    lambda c: c.private('PUT', '/spot/orders', body={}),
    lambda c: c.private('GET', '/spot/orders', body={'x': 1}),
    lambda c: c.private('GET', '/spot/orders', [('flag', True)]),
    lambda c: c.private('GET', '/spot/orders', [('bad key', '1')]),
    lambda c: c.private('GET', '/spot/orders', [('x', 'a\nb')]),
    lambda c: c.private('POST', '/spot/orders', body={'p': Decimal('1')}),   # not JSON serializable
    lambda c: c.private('POST', '/spot/orders', body='raw string'),
    lambda c: c.public('POST', '/spot/orders'),
    lambda c: c.private('GET', '/spot/orders', bucket='nonexistent'),
])
def test_invalid_requests_are_not_sent(clock, call):
    client, session = make_client(limiter=RateLimiter.from_settings(8))
    with pytest.raises(NotSent):
        call(client)
    assert session.calls == []


def test_constructor_validation():
    session = FakeSession()
    for url in ('http://api.gateio.ws/api/v4', 'https://api.gateio.ws/api/v3', 'https://api.gateio.ws',
                'https://api.gateio.ws/api/v4?x=1', 'not a url'):
        with pytest.raises(ValueError):
            GateRestClient(url, KEY, SECRET, timeout=5, request_expiry_seconds=10, limiter=None, session=session)
    with pytest.raises(ValueError):
        GateRestClient(BASE_URL, KEY, SECRET, timeout=0, request_expiry_seconds=10, limiter=None, session=session)
    with pytest.raises(ValueError):
        GateRestClient(BASE_URL, KEY, SECRET, timeout=5, request_expiry_seconds=0, limiter=None, session=session)
    with pytest.raises(ValueError):
        GateRestClient(BASE_URL, ' padded', SECRET, timeout=5, request_expiry_seconds=10, limiter=None,
                       session=session)
    client = GateRestClient(BASE_URL + '/', KEY, SECRET, timeout=5, request_expiry_seconds=10, limiter=None,
                            session=session)
    assert KEY not in repr(client) and SECRET not in repr(client)


def test_from_config_reads_market_app_and_secrets():
    cfg = SimpleNamespace(market={'rest_url': BASE_URL}, app={'request_timeout_seconds': 4.0,
                                                              'request_expiry_seconds': 12.0},
                          secrets=SimpleNamespace(gate_key=KEY, gate_secret=SECRET))
    client = GateRestClient.from_config(cfg, session=FakeSession())
    assert client.timeout == 4.0 and client.request_expiry_ms == 12_000 and client.has_keys


def test_local_clock_moving_backwards_is_not_sent(clock):
    client, session = make_client()
    session.queue(ok({}), ok({}), ok({}))
    clock[0] = 1800000000.500
    client.private('POST', '/spot/orders', body={})
    clock[0] = 1800000000.499
    with pytest.raises(NotSentUnsafe, match='LOCAL_CLOCK_MOVED_BACKWARDS') as info:
        client.private('POST', '/spot/cancel_batch_orders', body=[])
    assert isinstance(info.value, Unsafe) and isinstance(info.value, NotSent)
    with pytest.raises(NotSentUnsafe):
        client.private('GET', '/spot/accounts')
    assert len(session.calls) == 1
    client.public('GET', '/spot/time')           # unsigned: no clock guard
    clock[0] = 1800000000.500                    # same millisecond again is fine
    client.private('GET', '/spot/accounts')
    assert len(session.calls) == 3
    assert client.last_signed_ms == 1800000000500


# ----------------------------------------------------------------------------- limiter + checkpoint integration

def test_budget_wait_before_send_and_emergency_bypass(clock):
    limiter = RateLimiter({'order': (2, 10.0), 'cancel': (1, 1.0), 'dms': (1, 10.0), 'private_read': (5, 10.0),
                           'public': (5, 10.0)})
    client, session = make_client(limiter=limiter)
    session.queue(*[ok({}) for _ in range(6)])
    client.private('POST', '/spot/orders', body={})
    client.private('PATCH', '/spot/orders/1', body={})          # amend shares the order bucket
    with pytest.raises(BudgetWait) as info:
        client.private('POST', '/spot/batch_orders', body=[])
    assert info.value.bucket == 'order' and info.value.wait_seconds == pytest.approx(10.0)
    assert len(session.calls) == 2                              # nothing sent
    for _ in range(3):                                          # cancels: emergency, never refused
        client.private('POST', '/spot/cancel_batch_orders', body=[])
    client.private('POST', '/spot/countdown_cancel_all', body={'timeout': 30})
    assert len(session.calls) == 6
    status = limiter.status()
    assert status['cancel']['emergency_over_limit'] == 2
    assert status['order']['refused'] == 1
    clock[0] += 10.0
    session.queue(ok([]))
    client.private('POST', '/spot/batch_orders', body=[])
    assert len(session.calls) == 7


def test_rate_limited_reply_blocks_the_bucket(clock):
    limiter = RateLimiter.from_settings(8)
    client, session = make_client(limiter=limiter)
    session.queue(FakeResponse(429, {'label': 'TOO_MANY_REQUESTS'}))
    with pytest.raises(RateLimited):
        client.private('GET', '/spot/orders', [('status', 'open')])
    assert limiter.blocked_for('private_read:/spot/orders') == pytest.approx(15.0)
    with pytest.raises(BudgetWait):
        client.private('GET', '/spot/orders', [('status', 'open')])
    session.queue(ok([]), FakeResponse(400, {'label': 'TOO_FAST'}), ok([]))
    client.private('GET', '/spot/accounts')                     # other endpoint group unaffected
    with pytest.raises(Rejected) as info:                       # write: refused -> Rejected + block
        client.private('POST', '/spot/orders', body={})
    assert info.value.label == 'TOO_FAST'
    assert limiter.blocked_for('order') == pytest.approx(15.0)
    client.private('POST', '/spot/cancel_batch_orders', body=[])  # emergency still goes
    assert len(session.calls) == 4
    clock[0] += 15.0
    session.queue(ok([]))
    client.private('GET', '/spot/orders', [('status', 'open')])
    assert len(session.calls) == 5


def test_checkpoint_runs_before_every_read_but_not_writes(clock):
    limiter = RateLimiter.from_settings(8)
    client, session = make_client(limiter=limiter)
    calls = []
    client.before_read = lambda: calls.append('checkpoint')
    session.queue(ok({}), ok([]), ok({}))
    client.public('GET', '/spot/time')
    client.private('GET', '/spot/orders')
    client.private('POST', '/spot/orders', body={})
    assert calls == ['checkpoint', 'checkpoint']

    def interrupt():
        raise ReadInterrupted('CANCEL_PENDING')

    client.before_read = interrupt
    with pytest.raises(ReadInterrupted):
        client.private('GET', '/spot/orders')
    assert len(session.calls) == 3
    assert limiter.used('private_read:/spot/orders') == 1        # interrupted read not charged


def test_rate_limit_headers_recorded(clock):
    limiter = RateLimiter.from_settings(8)
    client, session = make_client(limiter=limiter)
    headers = {'X-Gate-RateLimit-Requests-Remain': '9', 'X-Gate-RateLimit-Limit': '10',
               'x-gate-ratelimit-reset-timestamp': '1800000001000', 'Content-Type': 'application/json'}
    session.queue(ok({'id': '1'}, 201, headers))
    client.private('POST', '/spot/orders', body={})
    recorded = client.diagnostics()['rate_limit_headers']['order']
    assert recorded['remain'] == 9 and recorded['limit'] == 10 and recorded['reset_timestamp'] == 1800000001000
    assert limiter.status()['order']['last_headers']['remain'] == 9
    json.dumps(client.diagnostics())


def test_bucket_and_group_derivation():
    assert endpoint_group('/spot/orders/912345678901') == '/spot/orders/{order_id}'
    assert endpoint_group('/spot/orders/t-stk1-b1-x') == '/spot/orders/{order_id}'
    assert endpoint_group('/spot/currency_pairs/STONK_USDT') == '/spot/currency_pairs/{currency_pair}'
    assert default_bucket('GET', '/spot/orders/1') == 'private_read:/spot/orders/{order_id}'
    assert default_bucket('GET', '/spot/time', public=True) == 'public:/spot/time'
    assert default_bucket('POST', '/spot/orders') == 'order'
    assert default_bucket('POST', '/spot/batch_orders') == 'order'
    assert default_bucket('PATCH', '/spot/orders/1') == 'order'
    assert default_bucket('POST', '/spot/cancel_batch_orders') == 'cancel'
    assert default_bucket('DELETE', '/spot/orders') == 'cancel'
    assert default_bucket('DELETE', '/spot/orders/1') == 'cancel'
    assert default_bucket('POST', '/spot/countdown_cancel_all') == 'dms'
    with pytest.raises(ValueError):
        default_bucket('POST', '/wallet/transfers')


# ----------------------------------------------------------------------------- secrets

def test_secrets_never_in_exceptions_or_logs(clock, caplog):
    caplog.set_level(logging.DEBUG)
    key, secret = 'KEYxLEAKxCHECKx777', 'SECRETxLEAKxCHECKx888'
    client, session = make_client(key=key, secret=secret, limiter=RateLimiter.from_settings(8))
    session.queue(
        ok({'id': '1'}),
        FakeResponse(400, {'label': 'INVALID_KEY', 'message': f'key {key} secret {secret} sig ' + 'ab' * 64}),
        requests.ConnectionError(f'proxy said {key}:{secret}'),
        FakeResponse(500, {'label': 'INTERNAL', 'message': secret}),
        FakeResponse(200, raw=f'garbage {secret}'),
        requests.Timeout(secret),
    )
    errors = []
    client.private('POST', '/spot/orders', body={'text': 't-1'})
    for call in (lambda: client.private('GET', '/spot/accounts'),
                 lambda: client.private('POST', '/spot/orders', body={}),
                 lambda: client.private('DELETE', '/spot/orders/1'),
                 lambda: client.private('GET', '/spot/orders'),
                 lambda: client.private('POST', '/spot/countdown_cancel_all', body={'timeout': 0})):
        with pytest.raises(Exception) as info:
            call()
        errors.append(info.value)
    texts = [str(e) for e in errors] + [repr(e) for e in errors] + [caplog.text, repr(client),
                                                                    json.dumps(client.diagnostics())]
    for text in texts:
        assert key not in text and secret not in text
        assert 'ab' * 64 not in text
    assert errors[0].label == 'INVALID_KEY' and '[REDACTED]' in str(errors[0])
    # The signature header itself never appears in logs either.
    sign = session.calls[0]['headers']['SIGN']
    assert sign not in caplog.text


def test_close_closes_only_own_session():
    session = FakeSession()
    client, _ = make_client(session=session)
    client.close()
    assert session.closed is False                # injected session belongs to the caller


# ----------------------------------------------------------------------------- RateLimiter unit tests

def test_default_buckets_from_settings():
    assert default_buckets(8, Decimal('0.5')) == {
        'order': (8, 10.0), 'cancel': (100, 1.0), 'dms': (100, 10.0), 'private_read': (100, 10.0),
        'public': (100, 10.0)}
    assert default_buckets(3, '1')['cancel'] == (200, 1.0)
    assert default_buckets(3, Decimal('0.001'))['public'] == (1, 10.0)
    for bad in ((0, '0.5'), (True, '0.5'), (8, '0'), (8, '1.5'), (8, 'nan'), (Decimal('8'), '0.5')):
        with pytest.raises(ValueError):
            default_buckets(*bad)
    cfg = SimpleNamespace(safety={'max_order_requests_per_10s': Decimal('8'),
                                  'rate_limit_utilization': Decimal('0.5')})
    assert RateLimiter.from_config(cfg).limit('order') == 8


def test_sliding_window_refuses_then_frees(clock):
    limiter = RateLimiter({'order': (3, 10.0)})
    start = clock[0]
    for i in range(3):
        clock[0] = start + i
        limiter.admit('order')
    with pytest.raises(BudgetWait) as info:
        limiter.admit('order')
    assert info.value.wait_seconds == pytest.approx(8.0)        # first event at start, now start+2
    assert info.value.bucket == 'order'
    assert str(info.value).startswith('REST_BUDGET_WAIT:order:')
    assert limiter.wait('order') == pytest.approx(8.0)
    assert limiter.wait('order', 2) == pytest.approx(9.0)
    clock[0] = start + 9.999
    with pytest.raises(BudgetWait):
        limiter.admit('order')
    clock[0] = start + 10.0
    limiter.admit('order')
    assert limiter.used('order') == 3
    status = limiter.status()['order']
    assert status['refused'] == 2 and status['admitted'] == 4 and status['peak'] == 3


def test_emergency_never_refused_but_recorded(clock):
    limiter = RateLimiter({'cancel': (2, 1.0)})
    for _ in range(5):
        limiter.admit('cancel', emergency=True)
    assert limiter.used('cancel') == 5
    assert limiter.wait('cancel', emergency=True) == 0.0
    with pytest.raises(BudgetWait):
        limiter.admit('cancel')                                  # ordinary admits see the emergency load
    status = limiter.status()['cancel']
    assert status['emergency_admits'] == 5 and status['emergency_over_limit'] == 3
    clock[0] += 1.0
    limiter.admit('cancel')


def test_rate_limited_blocks_ordinary_for_15_seconds(clock):
    limiter = RateLimiter.from_settings(8)
    limiter.rate_limited('order')
    with pytest.raises(BudgetWait) as info:
        limiter.admit('order')
    assert info.value.wait_seconds == pytest.approx(15.0)
    limiter.admit('order', emergency=True)
    clock[0] += 14.9
    with pytest.raises(BudgetWait):
        limiter.admit('order')
    clock[0] += 0.1
    limiter.admit('order')
    limiter.rate_limited('private_read')                         # template blocks every endpoint group
    with pytest.raises(BudgetWait):
        limiter.admit('private_read:/spot/orders')
    with pytest.raises(BudgetWait):
        limiter.admit('private_read:/spot/my_trades')
    limiter.admit('public:/spot/time')


def test_endpoint_groups_have_independent_windows(clock):
    limiter = RateLimiter({'private_read': (2, 10.0)})
    for _ in range(2):
        limiter.admit('private_read:/spot/orders')
        limiter.admit('private_read:/spot/my_trades')
    with pytest.raises(BudgetWait) as info:
        limiter.admit('private_read:/spot/orders')
    assert info.value.bucket == 'private_read:/spot/orders'
    assert limiter.used('private_read:/spot/my_trades') == 2


def test_limiter_argument_validation(clock):
    limiter = RateLimiter({'order': (2, 10.0)})
    for bad in ('nope', '', 'nope:/x'):
        with pytest.raises(ValueError):
            limiter.admit(bad)
    for cost in (0, -1, True, 1.5):
        with pytest.raises(ValueError):
            limiter.admit('order', cost)
    with pytest.raises(ValueError):
        limiter.admit('order', 3)                                # can never fit
    for buckets in ({}, {'a:b': (1, 1.0)}, {'a': (0, 1.0)}, {'a': (1, 0)}, {'a': (True, 1.0)}, {'a': 5}):
        with pytest.raises(ValueError):
            RateLimiter(buckets)
    json.dumps(limiter.status())
