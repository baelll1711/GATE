"""Atomic ``status.json`` writer and JSON-safe conversion (DESIGN §14, notes/ref_config_cli_ops.md §2.3).

``write_status`` serializes first (a value that cannot be represented as strict JSON
raises before the file is touched, so the previous document stays intact), then writes
a temporary file in the SAME directory, flushes and fsyncs it and atomically replaces
the target with ``os.replace``. Observers (``monitor_stonk.ps1``, ``status`` command)
always see either the complete old or the complete new document.

On Windows a reader (monitor script, antivirus, an editor) can hold the target open
for a moment and ``os.replace`` fails with ``PermissionError``; the replace is retried
after 0.02 s and 0.05 s. A file-system failure (``OSError``, including a final
``PermissionError``) is logged with its type name only and reported by the return
value; it never stops trading. No temporary file is left behind in any case.

``to_jsonable`` is the single conversion used for status/report output:

* ``Decimal`` -> plain decimal text (``format(d, 'f')``: never exponent notation);
  NaN/Infinity (Decimal or float) -> ``ValueError`` (strict JSON, ``allow_nan=False``);
* dataclasses -> dicts of their fields, EXCEPT fields declared ``repr=False`` (the
  convention for secrets, e.g. ``config.Secrets``), which are omitted;
* mappings -> dicts with string keys; tuples/lists/deques -> lists;
  sets/frozensets -> sorted lists; ``Path`` -> str; ``Enum`` -> its value;
  ``datetime``/``date`` -> ISO text;
* anything else -> ``TypeError`` (no silent ``str()`` of arbitrary objects).
"""
from __future__ import annotations

import collections
import dataclasses
import datetime as _dt
import enum
import json
import logging
import math
import os
import tempfile
import time
from decimal import Decimal
from pathlib import Path, PurePath
from typing import Any, Mapping

LOG = logging.getLogger(__name__)

REPLACE_RETRY_DELAYS = (0.02, 0.05)


def _plain_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError('Non-finite Decimal is not JSON-safe')
    return format(value, 'f')


def _key(key: Any) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, Decimal):
        return _plain_decimal(key)
    if isinstance(key, enum.Enum):
        return _key(key.value)
    if isinstance(key, bool) or key is None:
        return json.dumps(key)
    if isinstance(key, int):
        return str(key)
    if isinstance(key, float):
        if not math.isfinite(key):
            raise ValueError('Non-finite float key is not JSON-safe')
        return repr(key)
    if isinstance(key, PurePath):
        return str(key)
    if isinstance(key, tuple):
        return ':'.join(_key(part) for part in key)
    raise TypeError(f'Unsupported mapping key type: {type(key).__name__}')


def to_jsonable(obj: Any) -> Any:
    """Recursively convert ``obj`` into strict-JSON values (see module docstring)."""
    if obj is None or isinstance(obj, (bool, str)):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError('Non-finite float is not JSON-safe')
        return obj
    if isinstance(obj, Decimal):
        return _plain_decimal(obj)
    if isinstance(obj, enum.Enum):
        return to_jsonable(obj.value)
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_jsonable(getattr(obj, f.name)) for f in dataclasses.fields(obj) if f.repr}
    if isinstance(obj, Mapping):
        return {_key(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, collections.deque)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, (set, frozenset)):
        items = [to_jsonable(v) for v in obj]
        return sorted(items, key=lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False))
    if isinstance(obj, PurePath):
        return str(obj)
    if isinstance(obj, (_dt.datetime, _dt.date, _dt.time)):
        return obj.isoformat()
    raise TypeError(f'Not JSON serializable: {type(obj).__name__}')


def dumps(obj: Any, *, indent: int | None = 2, sort_keys: bool = False) -> str:
    """Strict JSON text (UTF-8 friendly, NaN forbidden) of ``to_jsonable(obj)``."""
    return json.dumps(to_jsonable(obj), ensure_ascii=False, allow_nan=False, indent=indent, sort_keys=sort_keys)


def _fsync_dir(directory: Path) -> None:
    """Best effort: persist the rename itself (POSIX); not possible on Windows."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_status(path: str | Path, data: Any) -> bool:
    """Atomically replace ``path`` with the JSON of ``data``.

    Returns True when the new document is in place, False when a file-system error
    prevented it (logged as ``status.json unavailable: <ExceptionType>``). A value that
    is not JSON-safe raises ``ValueError``/``TypeError`` before the file is touched.
    """
    path = Path(path)
    text = dumps(data) + '\n'
    tmp_name: str | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + '.', suffix='.tmp')
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        delays = iter(REPLACE_RETRY_DELAYS)
        while True:
            try:
                os.replace(tmp_name, path)
                break
            except PermissionError:
                delay = next(delays, None)
                if delay is None:
                    raise
                time.sleep(delay)
        tmp_name = None
        _fsync_dir(path.parent)
        return True
    except OSError as exc:
        LOG.warning('status.json unavailable: %s', type(exc).__name__)
        return False
    finally:
        if tmp_name is not None:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass


def read_status(path: str | Path) -> dict | None:
    """The status document, or None when the file does not exist yet.

    A malformed document raises ``ValueError`` (cannot happen with ``write_status``,
    which never leaves a partial file)."""
    path = Path(path)
    try:
        text = path.read_text(encoding='utf-8')
    except FileNotFoundError:
        return None
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError('status.json must contain a JSON object')
    return data


def status_age_seconds(path: str | Path) -> float | None:
    """Seconds since the status file was last replaced (wall clock), None if missing."""
    try:
        mtime = Path(path).stat().st_mtime
    except FileNotFoundError:
        return None
    return max(0.0, time.time() - mtime)
