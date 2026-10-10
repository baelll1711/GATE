"""status.json: atomic replace, strict JSON conversion, Windows PermissionError retry."""
from __future__ import annotations

import collections
import json
import logging
import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import pytest

from gate_stonk import status as status_mod
from gate_stonk.config import Secrets
from gate_stonk.ledger import Position
from gate_stonk.status import dumps, read_status, status_age_seconds, to_jsonable, write_status
from gate_stonk.types import Balance

D = Decimal


@dataclass(frozen=True)
class Inner:
    price: Decimal
    tags: tuple


@dataclass(frozen=True)
class Outer:
    name: str
    inner: Inner
    secret: str = field(default='TOP_SECRET', repr=False)


def files(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir())


def test_to_jsonable_converts_decimals_dataclasses_tuples_sets():
    value = {
        'price': D('1E-7'), 'big': D('1.5E+3'), 'neg': D('-0.00010'),
        'outer': Outer('x', Inner(D('2.50'), (1, D('3')))),
        'tuple': (1, 2, (3, 4)), 'set': {'b', 'a', 'c'}, 'frozen': frozenset({3, 1, 2}),
        'deque': collections.deque([D('1')]), 'path': Path('a') / 'b',
        D('1.5'): 'decimal key', 7: 'int key', ('buy', 'b1'): 'tuple key',
        'balance': Balance(D('10'), D('2.5')), 'none': None, 'flag': True, 'f': 0.5,
    }
    out = to_jsonable(value)
    assert out['price'] == '0.0000001'          # plain text, never exponent notation
    assert out['big'] == '1500'
    assert out['neg'] == '-0.00010'
    assert out['outer'] == {'name': 'x', 'inner': {'price': '2.50', 'tags': [1, '3']}}
    assert 'secret' not in out['outer']          # repr=False fields (secrets) are omitted
    assert out['tuple'] == [1, 2, [3, 4]]
    assert out['set'] == ['a', 'b', 'c'] and out['frozen'] == [1, 2, 3]
    assert out['deque'] == ['1'] and out['path'] == str(Path('a') / 'b')
    assert out['1.5'] == 'decimal key' and out['7'] == 'int key' and out['buy:b1'] == 'tuple key'
    assert out['balance'] == {'total': '10', 'hold': '2.5'}
    assert out['none'] is None and out['flag'] is True and out['f'] == 0.5
    json.dumps(out, allow_nan=False)


def test_secrets_dataclass_never_serialized():
    out = to_jsonable({'secrets': Secrets(gate_key='K', gate_secret='S', telegram_token='1:T', telegram_chat='9')})
    assert out == {'secrets': {}}


def test_position_dataclass_round_trips_as_text():
    pos = Position(qty=D('1.5'), cost=D('150.15'), fees_other={'GT': D('0.01')})
    out = to_jsonable(pos)
    assert out['qty'] == '1.5' and out['cost'] == '150.15' and out['fees_other'] == {'GT': '0.01'}


@pytest.mark.parametrize('bad', [D('NaN'), D('Infinity'), float('nan'), float('inf'), {'k': D('-Infinity')}])
def test_non_finite_values_are_rejected(bad):
    with pytest.raises(ValueError):
        to_jsonable(bad)


def test_unknown_objects_are_rejected():
    class Thing:
        pass
    with pytest.raises(TypeError):
        to_jsonable({'x': Thing()})
    with pytest.raises(TypeError):
        to_jsonable(b'bytes')


def test_write_status_atomic_and_readable(tmp_path):
    path = tmp_path / 'state' / 'status.json'
    assert read_status(path) is None and status_age_seconds(path) is None
    assert write_status(path, {'updated_ms': 1, 'price': D('0.00000123'), 'levels': ('b1', 'b2')}) is True
    assert read_status(path) == {'updated_ms': 1, 'price': '0.00000123', 'levels': ['b1', 'b2']}
    assert files(path.parent) == ['status.json']
    assert write_status(path, {'updated_ms': 2, 'text': 'Сводка'}) is True
    assert read_status(path) == {'updated_ms': 2, 'text': 'Сводка'}
    assert 'Сводка' in path.read_text(encoding='utf-8')     # ensure_ascii=False
    assert files(path.parent) == ['status.json']
    assert status_age_seconds(path) is not None


def test_unserializable_value_leaves_previous_document(tmp_path):
    path = tmp_path / 'status.json'
    write_status(path, {'ok': 1})
    with pytest.raises(ValueError):
        write_status(path, {'bad': D('NaN')})
    assert read_status(path) == {'ok': 1}
    assert files(tmp_path) == ['status.json']


def test_permission_error_is_retried_then_succeeds(tmp_path, monkeypatch):
    """Windows: a reader holds status.json; the replace succeeds on the second try."""
    path = tmp_path / 'status.json'
    write_status(path, {'v': 1})
    real_replace = os.replace
    calls, sleeps = [], []

    def flaky_replace(src, dst):
        calls.append((src, dst))
        if len(calls) == 1:
            raise PermissionError('locked by monitor')
        return real_replace(src, dst)

    monkeypatch.setattr(status_mod.os, 'replace', flaky_replace)
    monkeypatch.setattr(status_mod.time, 'sleep', sleeps.append)
    assert write_status(path, {'v': 2}) is True
    assert len(calls) == 2 and sleeps == [0.02]
    assert read_status(path) == {'v': 2}
    assert files(tmp_path) == ['status.json']        # no partial or temp files left


def test_permission_error_three_times_logged_not_raised(tmp_path, monkeypatch, caplog):
    path = tmp_path / 'status.json'
    write_status(path, {'v': 1})
    sleeps = []

    def locked(src, dst):
        raise PermissionError('locked')

    monkeypatch.setattr(status_mod.os, 'replace', locked)
    monkeypatch.setattr(status_mod.time, 'sleep', sleeps.append)
    with caplog.at_level(logging.WARNING, logger='gate_stonk.status'):
        assert write_status(path, {'v': 2}) is False
    assert sleeps == [0.02, 0.05]
    assert 'status.json unavailable: PermissionError' in caplog.text
    assert 'locked' not in caplog.text                # type name only
    assert read_status(path) == {'v': 1}
    assert files(tmp_path) == ['status.json']


def test_other_os_error_is_logged_not_raised(tmp_path, caplog):
    blocker = tmp_path / 'not_a_dir'
    blocker.write_text('x', encoding='utf-8')
    with caplog.at_level(logging.WARNING, logger='gate_stonk.status'):
        assert write_status(blocker / 'status.json', {'v': 1}) is False
    assert 'status.json unavailable:' in caplog.text


def test_fsync_failure_cleans_temp_file(tmp_path, monkeypatch):
    def broken_fsync(fd):
        raise OSError('disk')

    monkeypatch.setattr(status_mod.os, 'fsync', broken_fsync)
    assert write_status(tmp_path / 'status.json', {'v': 1}) is False
    assert files(tmp_path) == []


def test_dumps_is_strict_json():
    text = dumps({'a': D('1.10'), 'b': (1,)}, indent=None, sort_keys=True)
    assert text == '{"a": "1.10", "b": [1]}'


def test_read_status_rejects_non_object(tmp_path):
    path = tmp_path / 'status.json'
    path.write_text('[1, 2]', encoding='utf-8')
    with pytest.raises(ValueError):
        read_status(path)
