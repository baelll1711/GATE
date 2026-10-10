"""Live Gate venue on scripted HTTP (DESIGN §7, §15; notes/api_gate_rest.md).

All replies are SYNTHETIC (tests/fixtures/gate/README.md). A FakeSession records every request
(method, path, query params, headers, body) so the tests pin exactly what would be sent.
"""
from __future__ import annotations

import copy
import json
import logging
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlsplit

import pytest
import requests

from gate_stonk.errors import (NotSent, NotSentUnsafe, NotSentValueError, ReadError, ReadInterrupted, Rejected,
                               UnknownResult, Unsafe)
from gate_stonk.gate_rest import GateRestClient
from gate_stonk.ratelimit import RateLimiter
from gate_stonk.types import Balance, CancelRequest, PlaceRequest, VenueOrder
from gate_stonk.venue import (BudgetNotSent, Venue, base36, is_own_text, is_post_only_rejection, make_text,
                              parse_order, parse_pair_meta, parse_text, parse_trade, validate_text)
from gate_stonk.venue_gate import GateVenue, GateVenueSettings

FIX = Path(__file__).parent / 'fixtures' / 'gate'
BASE_URL = 'https://api.gateio.ws/api/v4'
KEY = 'KEYxVENUExTEST001'
SECRET = 'SECRETxVENUExTEST002'
NOW_MS = 1_800_000_000_000


def fx(name: str):
    data = json.loads((FIX / name).read_text())
    assert data['_comment'].startswith('SYNTHETIC')
    return copy.deepcopy(data['data'])


# ----------------------------------------------------------------------------- fakes

class FakeResponse:
    def __init__(self, status: int = 200, body=None, raw: str | None = None, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.content = (raw if raw is not None else json.dumps(body)).encode()


class FakeSession:
    def __init__(self):
        self.calls: list[dict] = []
        self.script: list = []

    def queue(self, *items):
        self.script.extend(items)

    def request(self, method, url, data=None, headers=None, timeout=None, allow_redirects=True):
        parts = urlsplit(url)
        record = {'method': method, 'path': parts.path, 'params': dict(parse_qsl(parts.query)),
                  'pairs': parse_qsl(parts.query), 'headers': dict(headers or {}),
                  'body': None if data is None else json.loads(data)}
        self.calls.append(record)
        if not self.script:
            raise AssertionError(f'unexpected request {method} {parts.path}')
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            item = item(record)
        if isinstance(item, FakeResponse):
            return item
        return FakeResponse(200, item)

    def close(self):
        pass


def make_venue(*, account='spot', limiter=None, with_meta=True, meta_fixture='currency_pair_stonk_usdt.json',
               **settings):
    session = FakeSession()
    client = GateRestClient(BASE_URL, KEY, SECRET, timeout=5, request_expiry_seconds=10, limiter=limiter,
                            session=session)
    meta = parse_pair_meta(fx(meta_fixture)) if with_meta else None
    venue = GateVenue(GateVenueSettings('STONK_USDT', 'STONK', 'USDT', account=account, **settings), client, meta)
    return venue, session


def order_row(**changes):
    row = fx('order_open.json')
    row.update(changes)
    return row


def req(level='b1', n=0, side='buy', price='0.1345', amount='150', iceberg=None, tif='poc'):
    return PlaceRequest(text=make_text('stk1', level, NOW_MS + 123, n), side=side, price=Decimal(price),
                        amount=Decimal(amount), iceberg=None if iceberg is None else Decimal(iceberg),
                        time_in_force=tif)


def reply_for(r: PlaceRequest, oid='912345678901', **changes):
    row = order_row(id=str(oid), text=r.text, side=r.side, price=format(r.price, 'f'),
                    amount=format(r.amount, 'f'), left=format(r.amount, 'f'))
    row.update(changes)
    return row


def trade_row(tid, ms, **changes):
    row = fx('my_trades_row.json')
    row.update(id=str(tid), create_time_ms=f'{ms}.250000', create_time=str(ms // 1000))
    row.update(changes)
    return row


# ----------------------------------------------------------------------------- settings / protocol

def test_settings_validation_and_from_config():
    with pytest.raises(ValueError):
        GateVenueSettings('STONK_USDT', 'STONK', 'USDC')
    with pytest.raises(ValueError):
        GateVenueSettings('stonk_usdt', 'stonk', 'usdt')
    with pytest.raises(ValueError):
        GateVenueSettings('STONK_USDT', 'STONK', 'USDT', account='margin')
    with pytest.raises(ValueError):
        GateVenueSettings('STONK_USDT', 'STONK', 'USDT', batch_create_max=11)
    with pytest.raises(ValueError):
        GateVenueSettings('STONK_USDT', 'STONK', 'USDT', batch_cancel_max=21)
    with pytest.raises(ValueError):
        GateVenueSettings('STONK_USDT', 'STONK', 'USDT', orders_page_limit=101)
    cfg = SimpleNamespace(market={'currency_pair': 'STONK_USDT', 'base': 'STONK', 'quote': 'USDT',
                                  'account': 'unified', 'expected_precision': 4, 'expected_amount_precision': None})
    settings = GateVenueSettings.from_config(cfg)
    assert settings.account == 'unified' and settings.expected_precision == 4
    client = GateRestClient(BASE_URL, KEY, SECRET, timeout=5, request_expiry_seconds=10, limiter=None,
                            session=FakeSession())
    venue = GateVenue(cfg, client)
    assert venue.settings == settings and venue.live is True
    assert isinstance(venue, Venue)


def test_before_read_hook_is_the_venue_attribute(clock):
    venue, session = make_venue()
    seen = []
    venue.before_read = lambda: seen.append('x')       # set by the engine after construction
    session.queue(fx('spot_accounts.json'))
    venue.balances()
    assert seen == ['x']


# ----------------------------------------------------------------------------- pair metadata / time

def test_pair_meta_parses_fixture(clock):
    venue, session = make_venue(with_meta=False)
    session.queue(fx('currency_pair_stonk_usdt.json'))
    meta = venue.pair_meta()
    call = session.calls[0]
    assert call['method'] == 'GET' and call['path'] == '/api/v4/spot/currency_pairs/STONK_USDT'
    assert 'KEY' not in call['headers'] and 'SIGN' not in call['headers']
    assert (meta.currency_pair, meta.base, meta.quote) == ('STONK_USDT', 'STONK', 'USDT')
    assert (meta.precision, meta.amount_precision) == (4, 2)
    assert meta.tick == Decimal('0.0001') and meta.lot == Decimal('0.01')
    assert meta.min_base_amount == Decimal('1') and meta.min_quote_amount == Decimal('3')
    assert meta.max_base_amount is None and meta.max_quote_amount == Decimal('5000000')
    assert meta.trade_status == 'tradable' and meta.fee_pct == Decimal('0.2')
    json.dumps(meta.raw)
    assert venue.meta is meta


def test_pair_meta_null_minimums_and_side_gating(clock):
    venue, session = make_venue(meta_fixture='currency_pair_null_minimums.json')
    meta = venue.meta
    assert meta.min_base_amount is None and meta.min_quote_amount is None and meta.max_quote_amount is None
    assert meta.trade_status == 'buyable'
    with pytest.raises(NotSentUnsafe, match='PAIR_NOT_TRADABLE:buyable'):
        venue.place(req(side='sell', level='s1'))
    small = req(amount='0.01')                         # no minimums: tiny order allowed locally
    session.queue(reply_for(small))
    assert venue.place(small).amount == Decimal('0.01')
    assert len(session.calls) == 1


@pytest.mark.parametrize('change,error', [
    ({'id': 'STONK_USDC', 'quote': 'USDC'}, 'SPOT_PAIR_MAPPING_MISMATCH'),
    ({'base': 'STONKS'}, 'SPOT_PAIR_MAPPING_MISMATCH'),
])
def test_pair_meta_mapping_mismatch(clock, change, error):
    venue, session = make_venue(with_meta=False)
    row = fx('currency_pair_stonk_usdt.json')
    row.update(change)
    session.queue(row)
    with pytest.raises(Unsafe, match=error):
        venue.pair_meta()
    assert venue.meta is None


def test_pair_meta_pinned_and_changed_precision(clock):
    venue, session = make_venue(with_meta=False, expected_precision=5)
    session.queue(fx('currency_pair_stonk_usdt.json'))
    with pytest.raises(Unsafe, match='PAIR_PRECISION_MISMATCH'):
        venue.pair_meta()
    venue, session = make_venue()
    session.queue(dict(fx('currency_pair_stonk_usdt.json'), amount_precision=3))
    with pytest.raises(Unsafe, match='PAIR_PRECISION_CHANGED'):
        venue.pair_meta()
    assert venue.meta.amount_precision == 2


@pytest.mark.parametrize('change', [
    {'trade_status': 'paused'}, {'precision': True}, {'precision': 19}, {'amount_precision': -1},
    {'min_base_amount': '-1'}, {'min_quote_amount': 'abc'}, {'id': None}, {'max_base_amount': '0.5'},
    {'fee': 'NaN'},
])
def test_pair_meta_invalid_is_read_error(clock, change):
    venue, session = make_venue(with_meta=False)
    row = fx('currency_pair_stonk_usdt.json')
    row.update(change)
    session.queue(row)
    with pytest.raises(ReadError, match='INVALID_PAIR_METADATA'):
        venue.pair_meta()


def test_server_time(clock):
    venue, session = make_venue()
    session.queue(fx('server_time.json'), {'server_time': '1800000000000'}, {'server_time': True}, {})
    assert venue.server_time_ms() == NOW_MS
    for _ in range(3):
        with pytest.raises(ReadError, match='INVALID_SERVER_TIME'):
            venue.server_time_ms()
    assert session.calls[0]['path'] == '/api/v4/spot/time'


# ----------------------------------------------------------------------------- place

def test_place_sends_exact_post_only_body(clock):
    venue, session = make_venue()
    r = req()
    session.queue(FakeResponse(201, reply_for(r)))
    order = venue.place(r)
    call = session.calls[0]
    assert call['method'] == 'POST' and call['path'] == '/api/v4/spot/orders' and call['params'] == {}
    assert list(call['body']) == ['text', 'currency_pair', 'type', 'account', 'side', 'amount', 'price',
                                  'time_in_force']
    assert call['body'] == {'text': r.text, 'currency_pair': 'STONK_USDT', 'type': 'limit', 'account': 'spot',
                            'side': 'buy', 'amount': '150', 'price': '0.1345', 'time_in_force': 'poc'}
    assert call['headers']['x-gate-exptime'] == str(NOW_MS + 10_000)
    assert isinstance(order, VenueOrder)
    assert (order.order_id, order.text, order.status, order.finish_as) == ('912345678901', r.text, 'open', 'open')
    assert order.price == Decimal('0.1345') and order.amount == Decimal('150') and order.left == Decimal('150')
    assert order.filled_amount == 0 and order.iceberg == 0 and order.create_time_ms == 1800000000123


def test_place_iceberg_and_unified_account(clock):
    venue, session = make_venue(account='unified')
    r = req(iceberg='30.5')
    session.queue(reply_for(r, account='unified', iceberg='30.5'))
    order = venue.place(r)
    assert session.calls[0]['body']['iceberg'] == '30.5'
    assert session.calls[0]['body']['account'] == 'unified'
    assert order.iceberg == Decimal('30.5') and order.account == 'unified'


def test_place_post_only_cancelled_reply_is_returned_not_raised(clock):
    venue, session = make_venue()
    r = req()
    poc = fx('order_poc_cancelled.json')
    poc['text'] = r.text
    session.queue(poc)
    order = venue.place(r)
    assert order.status == 'cancelled' and order.finish_as == 'poc' and order.left == order.amount
    assert is_post_only_rejection(order)
    assert not is_post_only_rejection(parse_order(order_row()))


def test_place_post_only_error_label_is_rejected(clock):
    venue, session = make_venue()
    session.queue(FakeResponse(400, fx('error_post_only.json')))
    with pytest.raises(Rejected) as info:
        venue.place(req())
    assert info.value.label == 'POC_FILL_IMMEDIATELY' and info.value.status == 400
    assert is_post_only_rejection(label=info.value.label)


@pytest.mark.parametrize('bad', [
    dict(price='0.13455'),               # off the 4-decimal price grid
    dict(amount='150.005'),              # off the 2-decimal amount grid
    dict(amount='0.5'),                  # below min_base_amount 1
    dict(amount='10'),                   # 10 * 0.1345 = 1.345 < min_quote_amount 3
    dict(amount='40000000', price='0.2'),  # above max_quote_amount
    dict(price='0'), dict(amount='-1'),
    dict(tif='gtc'),
    dict(side='BUY'),
    dict(iceberg='150'), dict(iceberg='0'), dict(iceberg='1.005'),
])
def test_place_invalid_is_not_sent(clock, bad):
    venue, session = make_venue()
    with pytest.raises(NotSent):
        venue.place(req(**bad))
    assert session.calls == []


def test_place_invalid_text_and_types_not_sent(clock):
    venue, session = make_venue()
    for text in ('t-' + 'x' * 29, 'stk1-b1-abc', 't-a b', 't-é', 't-'):
        with pytest.raises(NotSentValueError):
            venue.place(PlaceRequest(text=text, side='buy', price=Decimal('0.1345'), amount=Decimal('150')))
    with pytest.raises(NotSent):
        venue.place(PlaceRequest(text=req().text, side='buy', price=0.1345, amount=Decimal('150')))
    with pytest.raises(NotSent):
        venue.place({'text': 't-x'})
    assert session.calls == []
    venue_without_meta, session2 = make_venue(with_meta=False)
    with pytest.raises(NotSentUnsafe, match='PAIR_METADATA_REQUIRED'):
        venue_without_meta.place(req())
    assert session2.calls == []


def test_place_budget_wait_is_not_sent(clock):
    limiter = RateLimiter.from_settings(1)
    venue, session = make_venue(limiter=limiter)
    r1, r2 = req(n=1), req(n=2)
    session.queue(reply_for(r1))
    venue.place(r1)
    with pytest.raises(BudgetNotSent) as info:
        venue.place(r2)
    assert isinstance(info.value, NotSent)
    assert info.value.bucket == 'order' and info.value.wait_seconds == pytest.approx(10.0)
    assert len(session.calls) == 1


@pytest.mark.parametrize('failure', [requests.Timeout('t'), requests.ConnectionError('c'),
                                     FakeResponse(502, raw='<html>'), FakeResponse(500, {'label': 'INTERNAL'}),
                                     FakeResponse(201, raw='not json')])
def test_place_transport_failures_are_unknown_and_not_retried(clock, failure):
    venue, session = make_venue()
    session.queue(failure)
    with pytest.raises(UnknownResult):
        venue.place(req())
    assert len(session.calls) == 1


@pytest.mark.parametrize('change', [
    {'text': 't-other'}, {'currency_pair': 'BTC_USDT'}, {'side': 'sell'}, {'price': '0.1346'},
    {'amount': '151', 'left': '151'}, {'time_in_force': 'gtc'}, {'type': 'market'}, {'account': 'margin'},
    {'id': None}, {'status': 'weird'}, {'left': '-1'}, {'create_time_ms': None, 'create_time': None},
    {'filled_amount': '5'},
])
def test_place_reply_mismatch_is_unknown(clock, change):
    venue, session = make_venue()
    r = req()
    session.queue(reply_for(r, **change))
    with pytest.raises(UnknownResult, match='CREATE_REPLY_INVALID'):
        venue.place(r)


def test_place_non_object_reply_is_unknown(clock):
    venue, session = make_venue()
    session.queue([], {'label': 'X'})
    for _ in range(2):
        with pytest.raises(UnknownResult):
            venue.place(req())


# ----------------------------------------------------------------------------- place_batch

def test_place_batch_chunks_of_ten_and_matches_by_text(clock):
    venue, session = make_venue()
    reqs = [req(level='b1', n=i, price=f'0.13{40 - i:02d}') for i in range(12)]

    def first_chunk(record):
        assert len(record['body']) == 10
        items = []
        for i, r in enumerate(reqs[:10]):
            if i == 1:
                items.append({'text': r.text, 'succeeded': False, 'label': 'BALANCE_NOT_ENOUGH',
                              'message': 'Not enough balance'})
            elif i == 2:
                items.append({'text': r.text, 'succeeded': False, 'label': 'POC_FILL_IMMEDIATELY',
                              'message': 'would take'})
            elif i == 3:
                items.append({'text': r.text, 'succeeded': False, 'label': 'INTERNAL', 'message': ''})
            elif i == 4:
                continue                                        # missing from the reply
            elif i == 5:
                items.append(dict(reply_for(r, oid=900 + i), price='0.1', succeeded=True))   # mismatch
            elif i == 6:
                items.append({'text': r.text, 'succeeded': None})
            else:
                items.append(dict(reply_for(r, oid=900 + i), succeeded=True, label='', message=''))
        items.reverse()                                          # order not guaranteed by Gate
        items.append({'user_id': 1, 'id': 'id', 'succeeded': False, 'label': 'INVALID_PROTOCOL'})
        return FakeResponse(200, items)

    second = [dict(reply_for(reqs[10], oid=910), succeeded=True),
              dict(reply_for(reqs[11], oid=911), succeeded=True),
              dict(reply_for(reqs[11], oid=911), succeeded=True)]   # duplicate -> unknown
    session.queue(first_chunk, second)
    results = venue.place_batch(reqs)
    assert [r.text for r in results] == [r.text for r in reqs]
    outcome = {i: (res.outcome, res.label) for i, res in enumerate(results)}
    assert outcome[0] == ('accepted', None) and results[0].order.order_id == '900'
    assert outcome[1] == ('rejected', 'BALANCE_NOT_ENOUGH')
    assert outcome[2] == ('rejected', 'POC_FILL_IMMEDIATELY')
    assert outcome[3][0] == 'unknown' and outcome[4][0] == 'unknown'
    assert outcome[5][0] == 'unknown' and outcome[6][0] == 'unknown'
    assert all(outcome[i] == ('accepted', None) for i in (7, 8, 9, 10))
    assert outcome[11][0] == 'unknown'
    assert len(session.calls) == 2
    first, second_call = session.calls
    assert first['path'] == second_call['path'] == '/api/v4/spot/batch_orders'
    assert [item['text'] for item in first['body']] == [r.text for r in reqs[:10]]
    assert [item['text'] for item in second_call['body']] == [r.text for r in reqs[10:]]
    assert all(item['account'] == 'spot' and item['time_in_force'] == 'poc' and 'iceberg' not in item
               for item in first['body'])
    assert first['headers']['x-gate-exptime'] == str(NOW_MS + 10_000)


def test_place_batch_fixture_reply(clock):
    venue, session = make_venue()
    reply = fx('batch_orders_reply.json')
    reqs = [PlaceRequest(text=item['text'], side='buy', price=Decimal('0.1345'), amount=Decimal('150'))
            for item in reply[::-1]]
    reqs[2] = PlaceRequest(text=reply[0]['text'], side='sell', price=Decimal('0.1372'), amount=Decimal('100'))
    session.queue(reply)
    results = {r.text: r for r in venue.place_batch(reqs)}
    assert results['t-stk1-s1-mgk2x1a01'].outcome == 'accepted'
    assert results['t-stk1-s1-mgk2x1a01'].order.side == 'sell'
    assert (results['t-stk1-b2-mgk2x1a02'].outcome, results['t-stk1-b2-mgk2x1a02'].label) == (
        'rejected', 'BALANCE_NOT_ENOUGH')
    assert (results['t-stk1-b1-mgk2x1a00'].outcome, results['t-stk1-b1-mgk2x1a00'].label) == (
        'rejected', 'POC_FILL_IMMEDIATELY')


def test_place_batch_transport_failure_marks_only_that_chunk_unknown(clock):
    venue, session = make_venue()
    reqs = [req(n=i) for i in range(13)]
    session.queue([dict(reply_for(r, oid=100 + i), succeeded=True) for i, r in enumerate(reqs[:10])],
                  requests.Timeout('t'))
    results = venue.place_batch(reqs)
    assert [r.outcome for r in results] == ['accepted'] * 10 + ['unknown'] * 3
    assert len(session.calls) == 2


def test_place_batch_whole_request_rejected_or_garbage(clock):
    venue, session = make_venue()
    reqs = [req(n=i) for i in range(3)]
    session.queue(FakeResponse(400, {'label': 'TOO_MANY_ORDERS', 'message': 'x'}), {'not': 'a list'})
    assert [(r.outcome, r.label) for r in venue.place_batch(reqs)] == [('rejected', 'TOO_MANY_ORDERS')] * 3
    assert [r.outcome for r in venue.place_batch(reqs)] == ['unknown'] * 3


def test_place_batch_nothing_sent_cases(clock):
    venue, session = make_venue()
    assert venue.place_batch([]) == []
    with pytest.raises(NotSent):
        venue.place_batch([req(n=1), req(n=1)])                       # duplicate text
    with pytest.raises(NotSent):
        venue.place_batch([req(n=1), req(n=2, price='0.13455')])      # one invalid -> nothing sent
    assert session.calls == []
    limiter = RateLimiter.from_settings(1)
    venue, session = make_venue(limiter=limiter)
    with pytest.raises(NotSent):
        venue.place_batch([req(n=i) for i in range(11)])              # 2 requests can never fit in 1/10 s
    limiter2 = RateLimiter.from_settings(2)
    venue, session = make_venue(limiter=limiter2)
    limiter2.admit('order')
    with pytest.raises(BudgetNotSent):
        venue.place_batch([req(n=i) for i in range(11)])              # 2 needed, 1 left
    assert session.calls == []


def test_place_batch_first_chunk_not_sent_raises_later_chunk_marks_not_sent(clock):
    venue, session = make_venue()
    reqs = [req(n=i) for i in range(12)]
    venue.client._last_signed_ms = NOW_MS + 5_000                     # local clock behind the last signature
    with pytest.raises(NotSentUnsafe, match='LOCAL_CLOCK_MOVED_BACKWARDS'):
        venue.place_batch(reqs)
    assert session.calls == []

    venue, session = make_venue()

    def first(record):
        clock[0] -= 1.0                                               # clock steps back after chunk 1
        return FakeResponse(200, [dict(reply_for(r, oid=100 + i), succeeded=True) for i, r in enumerate(reqs[:10])])

    session.queue(first)
    results = venue.place_batch(reqs)
    assert [r.outcome for r in results[:10]] == ['accepted'] * 10
    assert [(r.outcome, r.label) for r in results[10:]] == [('rejected', 'NOT_SENT')] * 2
    assert len(session.calls) == 1


# ----------------------------------------------------------------------------- cancel_many

def test_cancel_many_chunks_of_twenty_by_order_id_else_text(clock):
    venue, session = make_venue()
    reqs = [CancelRequest(text=make_text('stk1', 'b1', NOW_MS, i), order_id=str(5000 + i) if i % 5 else None)
            for i in range(25)]

    def answer(record):
        items = []
        for k, item in enumerate(record['body']):
            assert set(item) == {'currency_pair', 'id', 'account'}
            assert item['currency_pair'] == 'STONK_USDT' and item['account'] == 'spot'
            if k == 0:
                items.append({'currency_pair': 'STONK_USDT', 'id': item['id'], 'succeeded': False,
                              'label': 'ORDER_NOT_FOUND', 'message': 'Order not found'})
            elif k == 1:
                items.append({'currency_pair': 'STONK_USDT', 'id': item['id'], 'succeeded': False,
                              'label': 'ORDER_CLOSED'})
            elif k == 2:
                items.append({'currency_pair': 'STONK_USDT', 'id': item['id'], 'succeeded': False,
                              'label': 'ORDER_CANCELLED'})
            elif k == 3:
                items.append({'currency_pair': 'STONK_USDT', 'id': item['id'], 'succeeded': False,
                              'label': 'INTERNAL'})
            elif k == 4:
                continue
            elif k == 5:
                items.append({'currency_pair': 'BTC_USDT', 'id': item['id'], 'succeeded': True})
            else:
                items.append({'currency_pair': 'STONK_USDT', 'id': item['id'], 'text': '', 'succeeded': True,
                              'label': '', 'message': ''})
        return FakeResponse(200, items[::-1])

    session.queue(answer, answer)
    result = venue.cancel_many(reqs)
    assert list(result) == [r.text for r in reqs]
    first, second = session.calls
    assert first['method'] == 'POST' and first['path'] == '/api/v4/spot/cancel_batch_orders'
    assert len(first['body']) == 20 and len(second['body']) == 5
    assert [item['id'] for item in first['body']] == [r.order_id or r.text for r in reqs[:20]]
    assert first['headers']['x-gate-exptime'] == str(NOW_MS + 10_000)
    texts = [r.text for r in reqs]
    for offset in (0, 20):
        assert result[texts[offset + 0]] == 'rejected'
        assert result[texts[offset + 1]] == 'rejected'
        assert result[texts[offset + 2]] == 'rejected'
        assert result[texts[offset + 3]] == 'unknown'
        assert result[texts[offset + 4]] == 'unknown'
    assert result[texts[5]] == 'unknown'                               # other pair in the reply
    assert all(result[t] == 'sent' for t in texts[6:20])


def test_cancel_many_fixture_reply_and_text_fallback(clock):
    venue, session = make_venue()
    reply = fx('cancel_batch_reply.json')
    reqs = [CancelRequest(text='t-stk1-b1-mgk2x1a00', order_id='912345678901'),
            CancelRequest(text='t-stk1-s1-mgk2x1a01', order_id=None)]
    session.queue(reply)
    assert venue.cancel_many(reqs) == {'t-stk1-b1-mgk2x1a00': 'sent', 't-stk1-s1-mgk2x1a01': 'rejected'}
    # Gate answered with the numeric id although we sent the text: matched by the reply's text.
    session.queue([{'currency_pair': 'STONK_USDT', 'id': '777', 'text': 't-stk1-s1-mgk2x1a01', 'succeeded': True}])
    assert venue.cancel_many(reqs[1:]) == {'t-stk1-s1-mgk2x1a01': 'sent'}


@pytest.mark.parametrize('failure,expected', [
    (requests.Timeout('t'), 'unknown'), (requests.ConnectionError('c'), 'unknown'),
    (FakeResponse(502, raw='<html>'), 'unknown'), (FakeResponse(500, {'label': 'SERVER_ERROR'}), 'unknown'),
    (FakeResponse(200, raw='nope'), 'unknown'), (FakeResponse(200, {'x': 1}), 'unknown'),
    (FakeResponse(400, {'label': 'INVALID_PARAM_VALUE'}), 'rejected'),
    (FakeResponse(429, {'label': 'TOO_MANY_REQUESTS'}), 'rejected'),
])
def test_cancel_many_never_raises_on_transport(clock, failure, expected):
    venue, session = make_venue()
    reqs = [CancelRequest(text=make_text('stk1', 's1', NOW_MS, i), order_id=str(10 + i)) for i in range(3)]
    session.queue(failure)
    assert venue.cancel_many(reqs) == {r.text: expected for r in reqs}
    assert len(session.calls) == 1


def test_cancel_many_transport_failure_of_one_chunk_only(clock):
    venue, session = make_venue()
    reqs = [CancelRequest(text=make_text('stk1', 's1', NOW_MS, i), order_id=str(10 + i)) for i in range(22)]
    session.queue(requests.Timeout('t'),
                  [{'currency_pair': 'STONK_USDT', 'id': r.order_id, 'succeeded': True} for r in reqs[20:]])
    result = venue.cancel_many(reqs)
    assert [result[r.text] for r in reqs] == ['unknown'] * 20 + ['sent'] * 2


def test_cancel_many_is_emergency_and_input_checks(clock):
    limiter = RateLimiter({'order': (1, 10.0), 'cancel': (1, 1.0), 'dms': (1, 10.0), 'private_read': (1, 10.0),
                           'public': (1, 10.0)})
    venue, session = make_venue(limiter=limiter)
    limiter.rate_limited('cancel')
    reqs = [CancelRequest(text=make_text('stk1', 's1', NOW_MS, i), order_id=str(10 + i)) for i in range(2)]
    session.queue([{'id': '10', 'succeeded': True}, {'id': '11', 'succeeded': True}],
                  [{'id': '10', 'succeeded': True}, {'id': '11', 'succeeded': True}])
    assert set(venue.cancel_many(reqs).values()) == {'sent'}
    assert set(venue.cancel_many(reqs).values()) == {'sent'}          # over the limit, still sent
    assert venue.cancel_many([]) == {}
    with pytest.raises(ValueError):
        venue.cancel_many([reqs[0], reqs[0]])
    bad = CancelRequest(text='t-bad', order_id='12abc')
    session.queue([{'id': '10', 'succeeded': True}])
    assert venue.cancel_many([bad, reqs[0]]) == {'t-bad': 'unknown', reqs[0].text: 'sent'}
    assert [item['id'] for item in session.calls[-1]['body']] == ['10']
    venue.client._last_signed_ms = NOW_MS + 1_000                     # local clock went backwards
    calls = len(session.calls)
    assert venue.cancel_many(reqs) == {r.text: 'unknown' for r in reqs}
    assert len(session.calls) == calls


def test_cancel_all_pair(clock):
    venue, session = make_venue(account='unified')
    session.queue(fx('cancel_all_reply.json') + [{'succeeded': False, 'label': 'ORDER_CLOSED'}])
    assert venue.cancel_all_pair() is None
    call = session.calls[0]
    assert call['method'] == 'DELETE' and call['path'] == '/api/v4/spot/orders'
    assert call['pairs'] == [('currency_pair', 'STONK_USDT'), ('account', 'unified')]
    assert call['headers']['x-gate-exptime'] == str(NOW_MS + 10_000)
    assert venue.last_cancel_all['succeeded'] == 1 and venue.last_cancel_all['failed'] == {'ORDER_CLOSED': 1}
    session.queue({'x': 1}, requests.Timeout('t'))
    with pytest.raises(UnknownResult):
        venue.cancel_all_pair()
    with pytest.raises(UnknownResult):
        venue.cancel_all_pair()


# ----------------------------------------------------------------------------- amend

def test_amend_price_only(clock):
    venue, session = make_venue()
    session.queue(order_row(price='0.1340', amend_text='t-rp-1'))
    order = venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=Decimal('0.1340'), amount=None,
                        amend_text='t-rp-1')
    call = session.calls[0]
    assert call['method'] == 'PATCH' and call['path'] == '/api/v4/spot/orders/912345678901'
    assert call['pairs'] == [('currency_pair', 'STONK_USDT'), ('account', 'spot')]
    assert call['body'] == {'currency_pair': 'STONK_USDT', 'account': 'spot', 'price': '0.1340',
                            'amend_text': 't-rp-1'}
    assert 'x-gate-exptime' in call['headers']
    assert order.price == Decimal('0.134')


def test_amend_amount_only_and_refusals(clock):
    venue, session = make_venue()
    session.queue(order_row(amount='100', left='100'))
    order = venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=None, amount=Decimal('100'),
                        amend_text='t-rd-1')
    assert 'price' not in session.calls[0]['body'] and session.calls[0]['body']['amount'] == '100'
    assert order.amount == Decimal('100')
    for kwargs in ({'price': Decimal('0.134'), 'amount': Decimal('100')}, {'price': None, 'amount': None},
                   {'price': Decimal('0.13401'), 'amount': None}, {'price': None, 'amount': Decimal('1.001')}):
        with pytest.raises(NotSent):
            venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', amend_text='t-x', **kwargs)
    with pytest.raises(NotSent):
        venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=Decimal('0.134'), amount=None, amend_text='x' * 32)
    with pytest.raises(NotSent):
        venue.amend('t-stk1-b1-mgk2x1a00', 't-stk1-b1-mgk2x1a00', price=Decimal('0.134'), amount=None,
                    amend_text='t-x')
    assert len(session.calls) == 1


def test_amend_reply_checks(clock):
    venue, session = make_venue()
    session.queue(order_row(price='0.1345'), FakeResponse(400, {'label': 'NO_CHANGE', 'message': 'No change'}),
                  requests.Timeout('t'), order_row(price='0.1340', text='t-other'))
    with pytest.raises(UnknownResult, match='AMEND_PRICE_NOT_APPLIED'):
        venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=Decimal('0.1340'), amount=None, amend_text='t-a')
    with pytest.raises(Rejected) as info:
        venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=Decimal('0.1340'), amount=None, amend_text='t-a')
    assert info.value.label == 'NO_CHANGE'
    with pytest.raises(UnknownResult):
        venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=Decimal('0.1340'), amount=None, amend_text='t-a')
    with pytest.raises(UnknownResult, match='ORDER_TEXT_MISMATCH'):
        venue.amend('912345678901', 't-stk1-b1-mgk2x1a00', price=Decimal('0.1340'), amount=None, amend_text='t-a')


# ----------------------------------------------------------------------------- countdown (DMS)

def test_countdown_arm_and_disarm(clock):
    venue, session = make_venue()
    session.queue(fx('countdown_reply.json'), {'triggerTime': 0})
    assert venue.countdown(30) == NOW_MS + 30_000
    call = session.calls[0]
    assert call['method'] == 'POST' and call['path'] == '/api/v4/spot/countdown_cancel_all'
    assert call['body'] == {'timeout': 30, 'currency_pair': 'STONK_USDT'}
    assert 'x-gate-exptime' not in call['headers']
    assert venue.last_dms['outcome'] == 'acknowledged' and venue.last_dms['trigger_ms'] == NOW_MS + 30_000
    assert venue.countdown(0) is None
    assert session.calls[1]['body'] == {'timeout': 0, 'currency_pair': 'STONK_USDT'}


@pytest.mark.parametrize('reply', [
    {}, {'triggerTime': True}, {'triggerTime': '1800000030000'}, {'triggerTime': None}, [],
    {'triggerTime': NOW_MS + 30_000 + 61_000}, {'triggerTime': NOW_MS - 1}, {'label': 'X'},
    FakeResponse(200, raw='{"triggerTime": 1800000030000.5}'),
])
def test_countdown_unacknowledged_is_unknown(clock, reply):
    venue, session = make_venue()
    session.queue(reply)
    with pytest.raises(UnknownResult, match='DMS_NOT_ACKNOWLEDGED'):
        venue.countdown(30)
    assert venue.last_dms['outcome'] == 'unexpected_response'


def test_countdown_disarm_requires_trigger_time(clock):
    venue, session = make_venue()
    session.queue({})
    with pytest.raises(UnknownResult):
        venue.countdown(0)


@pytest.mark.parametrize('timeout', [3, 4, -1, True, 30.0, '30', 86401, None])
def test_countdown_invalid_timeout_not_sent(clock, timeout):
    venue, session = make_venue()
    with pytest.raises(NotSent):
        venue.countdown(timeout)
    assert session.calls == []


def test_countdown_refusal_and_transport_are_unknown(clock):
    limiter = RateLimiter({'order': (1, 10.0), 'cancel': (1, 1.0), 'dms': (1, 10.0), 'private_read': (1, 10.0),
                           'public': (1, 10.0)})
    venue, session = make_venue(limiter=limiter)
    session.queue(FakeResponse(403, {'label': 'READ_ONLY', 'message': 'read only'}), requests.Timeout('t'),
                  fx('countdown_reply.json'))
    with pytest.raises(UnknownResult, match='DMS_NOT_ACKNOWLEDGED:READ_ONLY'):
        venue.countdown(30)
    assert venue.last_dms['outcome'] == 'rejected' and venue.last_dms['label'] == 'READ_ONLY'
    with pytest.raises(UnknownResult):
        venue.countdown(30)
    assert venue.last_dms['outcome'] == 'transport_error'
    assert venue.countdown(30) == NOW_MS + 30_000                     # dms bucket full: emergency, still sent
    assert len(session.calls) == 3


def test_probe_write_permission(clock):
    venue, session = make_venue()
    session.queue({'triggerTime': 0}, FakeResponse(403, {'label': 'READ_ONLY'}), requests.Timeout('t'), {})
    assert venue.probe_write_permission() == {'ok': True, 'outcome': 'acknowledged', 'label': None}
    assert session.calls[0]['body'] == {'timeout': 0, 'currency_pair': 'STONK_USDT'}
    denied = venue.probe_write_permission()
    assert denied['ok'] is False and denied['label'] == 'READ_ONLY' and denied['http_status'] == 403
    assert venue.probe_write_permission()['outcome'] == 'unknown'
    assert venue.probe_write_permission()['outcome'] == 'unexpected_response'


# ----------------------------------------------------------------------------- open orders / get_order / finished

def _rows(start, count, **changes):
    return [order_row(id=str(start + i), text=f't-stk1-b1-r{start + i}', **changes) for i in range(count)]


def test_open_orders_pages_until_short_page(clock):
    venue, session = make_venue()
    checkpoints = []
    venue.before_read = lambda: checkpoints.append(1)
    session.queue(_rows(1000, 100), _rows(2000, 3))
    orders = venue.open_orders()
    assert len(orders) == 103 and len(checkpoints) == 2
    for page, call in enumerate(session.calls, start=1):
        assert call['method'] == 'GET' and call['path'] == '/api/v4/spot/orders'
        assert call['params'] == {'currency_pair': 'STONK_USDT', 'status': 'open', 'account': 'spot',
                                  'page': str(page), 'limit': '100'}
        assert 'x-gate-exptime' not in call['headers'] and 'SIGN' in call['headers']


def test_open_orders_full_last_page_then_empty_and_dedup(clock):
    venue, session = make_venue()
    page2 = _rows(1099, 1)                                             # repeats the last row of page 1
    session.queue(_rows(1000, 100), page2)
    assert len(venue.open_orders()) == 100
    session.queue(_rows(1000, 100), [])
    assert len(venue.open_orders()) == 100
    assert len(session.calls) == 4


def test_open_orders_foreign_rows_allowed_but_validated(clock):
    venue, session = make_venue()
    foreign = order_row(id='55', text='apiv4', time_in_force='gtc')
    session.queue([foreign])
    assert venue.open_orders()[0].time_in_force == 'gtc'
    for row, error in ((order_row(currency_pair='BTC_USDT'), Unsafe), (order_row(status='cancelled'), ReadError),
                       (order_row(status='weird'), Unsafe), (order_row(amount='abc'), ReadError),
                       (order_row(account='margin'), Unsafe), (order_row(side='both'), Unsafe)):
        session.queue([row])
        with pytest.raises(error):
            venue.open_orders()
    session.queue({'not': 'a list'})
    with pytest.raises(ReadError, match='INVALID_OPEN_ORDERS'):
        venue.open_orders()


def test_open_orders_page_guard_and_interrupt(clock):
    venue, session = make_venue(max_pages=2)
    session.queue(_rows(1000, 100), _rows(2000, 100))
    with pytest.raises(Unsafe, match='OPEN_ORDERS_TOO_MANY_PAGES'):
        venue.open_orders()
    venue, session = make_venue()
    count = []

    def checkpoint():
        count.append(1)
        if len(count) == 2:
            raise ReadInterrupted('CANCEL_PENDING')

    venue.before_read = checkpoint
    session.queue(_rows(1000, 100))
    with pytest.raises(ReadInterrupted):
        venue.open_orders()
    assert len(session.calls) == 1                                    # page 2 never sent


def test_get_order_by_id_and_text(clock):
    venue, session = make_venue()
    session.queue(order_row(), order_row())
    order = venue.get_order(order_id='912345678901')
    assert order.order_id == '912345678901'
    call = session.calls[0]
    assert call['path'] == '/api/v4/spot/orders/912345678901'
    assert call['pairs'] == [('currency_pair', 'STONK_USDT'), ('account', 'spot')]
    assert venue.get_order(text='t-stk1-b1-mgk2x1a00').text == 't-stk1-b1-mgk2x1a00'
    assert session.calls[1]['path'] == '/api/v4/spot/orders/t-stk1-b1-mgk2x1a00'


def test_get_order_not_found_is_none(clock):
    venue, session = make_venue()
    session.queue(FakeResponse(404, fx('error_order_not_found.json')),
                  FakeResponse(400, {'label': 'CLIENT_ID_NOT_FOUND', 'message': 'x'}),
                  FakeResponse(401, {'label': 'INVALID_KEY'}), FakeResponse(500, {'label': 'INTERNAL'}))
    assert venue.get_order(order_id='1') is None
    assert venue.get_order(text='t-stk1-b1-x') is None
    with pytest.raises(Rejected):
        venue.get_order(order_id='1')
    with pytest.raises(ReadError):
        venue.get_order(order_id='1')


@pytest.mark.parametrize('row,kwargs,error', [
    (dict(text='t-other'), {'text': 't-stk1-b1-mgk2x1a00'}, 'ORDER_TEXT_MISMATCH'),
    (dict(id='2'), {'order_id': '912345678901'}, 'ORDER_ID_MISMATCH'),
    (dict(time_in_force='gtc'), {'order_id': '912345678901'}, 'UNEXPECTED_ORDER_TIME_IN_FORCE'),
    (dict(type='market'), {'order_id': '912345678901'}, 'UNEXPECTED_ORDER_TYPE'),
    (dict(status='closed', left='5', filled_amount='145'), {'order_id': '912345678901'}, 'FILLED_ORDER_HAS_REMAINING'),
    (dict(left='100', filled_amount='10'), {'order_id': '912345678901'}, 'ORDER_FILL_AMOUNTS_INCONSISTENT'),
    (dict(currency_pair='BTC_USDT'), {'order_id': '912345678901'}, 'UNEXPECTED_ORDER_PAIR'),
])
def test_get_order_mismatch_is_unsafe(clock, row, kwargs, error):
    venue, session = make_venue()
    session.queue(order_row(**row))
    with pytest.raises(Unsafe, match=error):
        venue.get_order(**kwargs)


def test_get_order_argument_checks(clock):
    venue, session = make_venue()
    with pytest.raises(NotSent):
        venue.get_order()
    with pytest.raises(NotSent):
        venue.get_order(order_id='../x')
    with pytest.raises(NotSent):
        venue.get_order(text='no-prefix')
    assert session.calls == []


def test_finished_orders_window_paging_and_validation(clock):
    venue, session = make_venue()
    page1 = [order_row(id=str(3000 + i), status='cancelled', finish_as='cancelled',
                       update_time_ms=1800000050000 - i) for i in range(100)]
    page2 = [order_row(id='4000', status='closed', finish_as='filled', left='0', filled_amount='150',
                       update_time_ms=1800000000001)]
    session.queue(page1, page2)
    orders = venue.finished_orders(1800000000500, 1800000060500)
    assert len(orders) == 101
    assert orders[0].order_id == '4000'                                # sorted by update time
    call = session.calls[0]
    assert call['params'] == {'currency_pair': 'STONK_USDT', 'status': 'finished', 'from': '1799999999',
                              'to': '1800000061', 'account': 'spot', 'page': '1', 'limit': '100'}
    session.queue([order_row()])
    with pytest.raises(ReadError, match='INVALID_FINISHED_ORDER_STATUS'):
        venue.finished_orders(1, 2)
    with pytest.raises(NotSent):
        venue.finished_orders(10, 5)


# ----------------------------------------------------------------------------- my_trades

def test_my_trades_paging_dedup_window_and_decimal_time(clock):
    venue, session = make_venue()
    base = 1800000010000
    page1 = [trade_row(5000 + i, base + i * 10) for i in range(1000)]
    page2 = [trade_row(5999, base + 9990),                             # repeat of page 1 (same content)
             trade_row(7000, base + 20_000),                           # after the window
             trade_row(7001, base - 5_000)]                            # before the window
    session.queue(page1, page2)
    trades = venue.my_trades(base, base + 10_000)
    assert len(trades) == 1000
    assert trades[0].trade_id == '5000' and trades[0].create_time_ms == base
    assert [t.create_time_ms for t in trades] == sorted(t.create_time_ms for t in trades)
    assert venue.last_trades_read['outside_window'] == 2 and venue.last_trades_read['rows'] == 1002
    first, second = session.calls
    assert first['path'] == '/api/v4/spot/my_trades'
    assert first['params'] == {'currency_pair': 'STONK_USDT', 'from': str(base // 1000 - 1),
                               'to': str((base + 10_000) // 1000 + 1), 'page': '1', 'limit': '1000'}
    assert second['params']['page'] == '2'
    assert 'account' not in first['params']


def test_trade_row_parsing_fixture():
    trade = parse_trade(fx('my_trades_row.json'))
    assert trade.create_time_ms == 1800000010777                       # "1800000010777.399200"
    assert trade.trade_id == '2876130500' and trade.order_id == '912345678901'
    assert trade.text == 't-stk1-b1-mgk2x1a00' and trade.role == 'maker' and trade.side == 'buy'
    assert trade.amount == Decimal('10.97') and trade.fee == Decimal('0.01097') and trade.fee_currency == 'STONK'
    assert trade.point_fee == 0 and trade.gt_fee == 0 and trade.sequence_id == '588018'
    json.dumps(trade.raw)
    row = fx('my_trades_row.json')
    del row['point_fee'], row['gt_fee'], row['text']
    trade = parse_trade(row)
    assert trade.point_fee == 0 and trade.gt_fee == 0 and trade.text is None


@pytest.mark.parametrize('change', [
    {'create_time_ms': 'abc'}, {'create_time_ms': None, 'create_time': None}, {'amount': '0'}, {'price': '-1'},
    {'side': 'long'}, {'role': 'both'}, {'fee': None}, {'fee_currency': ''}, {'order_id': 'x1'}, {'id': ''},
    {'create_time_ms': True},
])
def test_my_trades_malformed_row_is_read_error(clock, change):
    venue, session = make_venue()
    session.queue([trade_row(1, 1800000010000, **change)])
    with pytest.raises(ReadError, match='INVALID_TRADE_ROW'):
        venue.my_trades(1800000000000, 1800000020000)


def test_my_trades_conflicts_and_pair(clock):
    venue, session = make_venue()
    session.queue([trade_row(1, 1800000010000), trade_row(1, 1800000010000, amount='11')])
    with pytest.raises(Unsafe, match='CONFLICTING_TRADE_ROWS'):
        venue.my_trades(1800000000000, 1800000020000)
    session.queue([trade_row(1, 1800000010000, currency_pair='BTC_USDT')])
    with pytest.raises(Unsafe, match='UNEXPECTED_TRADE_PAIR'):
        venue.my_trades(1800000000000, 1800000020000)


def test_my_trades_long_window_is_sliced(clock):
    venue, session = make_venue()
    start, end = 1800000000000, 1800000000000 + 10 * 86400 * 1000
    session.queue([trade_row(1, start + 1000)], [trade_row(1, start + 1000), trade_row(2, end - 1000)])
    trades = venue.my_trades(start, end)
    assert [t.trade_id for t in trades] == ['1', '2']
    first, second = session.calls
    assert int(first['params']['to']) - int(first['params']['from']) == 7 * 86400
    assert second['params']['from'] == first['params']['to']           # boundary second read twice
    assert second['params']['to'] == str(end // 1000 + 1)
    assert venue.last_trades_read['slices'] == 2
    with pytest.raises(NotSent):
        venue.my_trades(5, 1)


def test_order_trades(clock):
    venue, session = make_venue()
    session.queue([trade_row(2, 1800000010000), trade_row(1, 1800000005000)])
    trades = venue.order_trades('912345678901')
    assert [t.trade_id for t in trades] == ['1', '2']
    assert session.calls[0]['params'] == {'currency_pair': 'STONK_USDT', 'order_id': '912345678901', 'page': '1',
                                          'limit': '1000'}
    session.queue([trade_row(3, 1800000010000, order_id='5')])
    with pytest.raises(Unsafe, match='UNEXPECTED_TRADE_ORDER'):
        venue.order_trades('912345678901')


# ----------------------------------------------------------------------------- balances / account

def test_balances_spot(clock):
    venue, session = make_venue()
    session.queue(fx('spot_accounts.json'))
    balances = venue.balances()
    assert session.calls[0]['path'] == '/api/v4/spot/accounts' and session.calls[0]['params'] == {}
    assert balances['USDT'] == Balance(Decimal('300.12'), Decimal('50'))
    assert balances['USDT'].free == Decimal('250.12')
    assert balances['STONK'] == Balance(Decimal('0'), Decimal('0'))
    assert balances['GT'] == Balance(Decimal('1.5'), Decimal('0'))
    session.queue([{'currency': 'USDT', 'available': '1', 'locked': '0'},
                   {'currency': 'JUNK', 'available': 'NaN', 'locked': '0'}])
    balances = venue.balances()
    assert balances['STONK'] == Balance(Decimal(0), Decimal(0)) and 'JUNK' not in balances


@pytest.mark.parametrize('rows,error', [
    ([{'currency': 'USDT', 'available': '-1', 'locked': '0'}], 'NEGATIVE_OR_INCONSISTENT_SPOT_BALANCE'),
    ([{'currency': 'STONK', 'available': '1', 'locked': '-0.1'}], 'NEGATIVE_OR_INCONSISTENT_SPOT_BALANCE'),
    ([{'currency': 'USDT', 'available': '1', 'locked': '0'}, {'currency': 'USDT', 'available': '1', 'locked': '0'}],
     'BALANCE_TOKEN_MISMATCH_OR_DUPLICATE'),
])
def test_balances_fail_closed_unsafe(clock, rows, error):
    venue, session = make_venue()
    session.queue(rows)
    with pytest.raises(Unsafe, match=error):
        venue.balances()


@pytest.mark.parametrize('reply', [
    {'not': 'a list'}, [{'currency': 'USDT', 'available': 'abc', 'locked': '0'}],
    [{'currency': 'USDT', 'available': '1'}], [{'currency': 'USDT', 'available': 'NaN', 'locked': '0'}], ['x'],
])
def test_balances_malformed_is_read_error(clock, reply):
    venue, session = make_venue()
    session.queue(reply)
    with pytest.raises(ReadError, match='INVALID_BALANCES'):
        venue.balances()


def test_balances_unified(clock):
    venue, session = make_venue(account='unified')
    session.queue(fx('unified_accounts.json'))
    balances = venue.balances()
    assert session.calls[0]['path'] == '/api/v4/unified/accounts'
    assert balances['USDT'] == Balance(Decimal('500.0'), Decimal('99.5'))
    assert balances['STONK'] == Balance(Decimal('150'), Decimal('30'))
    assert 'balances' in venue.last_balance_keys
    session.queue({'balances': {'USDT': {'available': '5'}}}, {'mode': 'x'})
    assert venue.balances()['USDT'] == Balance(Decimal('5'), Decimal('0'))   # missing freeze -> 0
    with pytest.raises(ReadError):
        venue.balances()


@pytest.mark.parametrize('key_mode,unified,account,detected,problems', [
    (1, 'classic', 'spot', 'spot', []),
    (1, 'classic', 'unified', 'spot', ['ACCOUNT_MODE_MISMATCH:spot']),
    (2, 'classic', 'unified', 'unified', []),
    (1, 'multi_currency', 'unified', 'unified', []),
    (1, 'portfolio', 'spot', 'unified', ['ACCOUNT_MODE_MISMATCH:unified']),
    (3, 'classic', 'spot', 'unknown', ['ACCOUNT_MODE_MISMATCH:unknown']),
])
def test_account_check_mode_detection(clock, key_mode, unified, account, detected, problems):
    venue, session = make_venue(account=account)
    detail = fx('account_detail_classic.json')
    detail['key']['mode'] = key_mode
    mode = fx('unified_mode_classic.json')
    mode['mode'] = unified
    session.queue(detail, mode)
    info = venue.account_check()
    assert [c['path'] for c in session.calls] == ['/api/v4/account/detail', '/api/v4/unified/unified_mode']
    assert info['key_mode'] == key_mode and info['unified_mode'] == unified
    assert info['detected_account'] == detected and info['problems'] == problems
    assert info['account_ok'] is (detected == account)
    assert info['pair_whitelist_ok'] is True and info['ip_whitelist'] == [] and info['tier'] == 0
    json.dumps(info)


def test_account_check_whitelist_and_failures(clock):
    venue, session = make_venue()
    detail = fx('account_detail_classic.json')
    detail['currency_pairs'] = ['BTC_USDT']
    detail['ip_whitelist'] = ['10.0.0.1']
    session.queue(detail, FakeResponse(400, {'label': 'UNIFIED_ACCOUNT_NOT_OPENED'}))
    info = venue.account_check()
    assert info['pair_whitelist_ok'] is False and info['ip_whitelist'] == ['10.0.0.1']
    assert info['unified_mode'] is None and info['unified_mode_error'] == 'UNIFIED_ACCOUNT_NOT_OPENED'
    assert info['detected_account'] == 'unknown'
    assert info['problems'] == ['ACCOUNT_MODE_MISMATCH:unknown', 'PAIR_NOT_IN_KEY_WHITELIST']
    detail['currency_pairs'] = ['BTC_USDT', 'STONK_USDT']
    session.queue(detail, fx('unified_mode_classic.json'))
    assert venue.account_check()['pair_whitelist_ok'] is True
    for bad in ({'key': {}}, {'key': {'mode': True}}, [], {'key': {'mode': 1}, 'currency_pairs': 'x'}):
        session.queue(bad)
        with pytest.raises(ReadError, match='INVALID_ACCOUNT_DETAIL'):
            venue.account_check()


def test_fee_info(clock):
    venue, session = make_venue()
    session.queue(fx('wallet_fee.json'), fx('debit_fee.json'))
    info = venue.fee_info()
    assert session.calls[0]['path'] == '/api/v4/wallet/fee'
    assert session.calls[0]['params'] == {'currency_pair': 'STONK_USDT'}
    assert session.calls[1]['path'] == '/api/v4/account/debit_fee'
    assert info['maker_fee'] == Decimal('0.002') and info['maker_fee_pct'] == Decimal('0.2')
    assert info['taker_fee_pct'] == Decimal('0.2') and info['gt_discount'] is False
    assert info['debit_fee'] == 3 and info['gt_deduction_enabled'] is False and info['point_type'] == '1'
    session.queue(dict(fx('wallet_fee.json'), maker_fee='0.5'))
    with pytest.raises(ReadError, match='INVALID_FEE_INFO'):
        venue.fee_info()
    session.queue(fx('wallet_fee.json'), FakeResponse(403, {'label': 'FORBIDDEN'}))
    info = venue.fee_info()
    assert info['gt_deduction_enabled'] is None and info['debit_fee_error'] == 'FORBIDDEN'


# ----------------------------------------------------------------------------- client ids

def test_make_text_format_and_limits():
    text = make_text('stk1', 'b1', 1800000000123, 5)
    assert text == 't-stk1-b1-' + base36(1800000000123) + '05'
    assert validate_text(text) == text
    longest = make_text('abcdef', 'abcd', 10 ** 14 - 1, 1295)
    assert longest.endswith('zz') and len(longest.encode()) - 2 <= 28
    assert parse_text(longest) == ('abcdef', 'abcd')
    assert is_own_text(text, 'stk1') and not is_own_text(text, 'stk2') and not is_own_text('apiv4', 'stk1')
    assert make_text('stk1', 'b1', 1800000000123, 1296) == make_text('stk1', 'b1', 1800000000123, 0)
    texts = {make_text('stk1', 'b1', 1800000000123, n) for n in range(1296)}
    assert len(texts) == 1296
    assert base36(0) == '0' and base36(35) == 'z' and base36(36) == '10'


@pytest.mark.parametrize('args', [
    ('STK1', 'b1', 1800000000123, 0), ('stk_1', 'b1', 1800000000123, 0), ('', 'b1', 1800000000123, 0),
    ('abcdefg', 'b1', 1800000000123, 0), ('stk1', 'B1', 1800000000123, 0), ('stk1', 'abcde', 1800000000123, 0),
    ('stk1', '', 1800000000123, 0), ('stk1', 'b-1', 1800000000123, 0), ('stk1', 'b1', 0, 0),
    ('stk1', 'b1', -1, 0), ('stk1', 'b1', True, 0), ('stk1', 'b1', 1.5, 0), ('stk1', 'b1', 10 ** 14, 0),
    ('stk1', 'b1', 1800000000123, -1), ('stk1', 'b1', 1800000000123, True), ('stk1', 'b1', 1800000000123, 1.0),
    (None, 'b1', 1800000000123, 0),
])
def test_make_text_rejects_invalid_parts(args):
    with pytest.raises(ValueError):
        make_text(*args)


@pytest.mark.parametrize('text,valid', [
    ('t-' + 'a' * 28, True), ('t-' + 'a' * 29, False), ('t-A.b_c-9', True), ('t-', False), ('t-a b', False),
    ('t-é', False), ('t-a/b', False), ('x-abc', False), ('abc', False), (None, False), (5, False),
])
def test_validate_text_charset_and_length(text, valid):
    if valid:
        assert validate_text(text) == text
    else:
        with pytest.raises(ValueError):
            validate_text(text)


# ----------------------------------------------------------------------------- secrets

def test_no_secret_in_logs_or_errors_across_venue_flows(clock, caplog):
    caplog.set_level(logging.DEBUG)
    venue, session = make_venue()
    session.queue(
        FakeResponse(400, {'label': 'INVALID_KEY', 'message': f'bad {KEY}/{SECRET}'}),
        requests.ConnectionError(f'{KEY}:{SECRET}'),
        FakeResponse(403, {'label': 'READ_ONLY', 'message': SECRET}),
        requests.Timeout(SECRET),
        FakeResponse(500, {'label': 'INTERNAL', 'message': KEY}),
    )
    errors = []
    for call in (lambda: venue.place(req()), lambda: venue.place(req(n=1)), lambda: venue.countdown(30),
                 lambda: venue.open_orders(), lambda: venue.amend('1', req().text, price=Decimal('0.1'),
                                                                  amount=None, amend_text='t-a')):
        with pytest.raises(Exception) as info:
            call()
        errors.append(info.value)
    session.queue(requests.Timeout(SECRET))
    venue.cancel_many([CancelRequest(text=req().text, order_id='1')])
    blob = ' '.join(str(e) for e in errors) + caplog.text + json.dumps(venue.diagnostics()) + repr(venue.client)
    assert KEY not in blob and SECRET not in blob
