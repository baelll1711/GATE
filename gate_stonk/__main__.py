"""``python -m gate_stonk``: make console output unable to crash, then run the CLI.

On Windows a redirected or piped stdout (``> bot.log``, ``Tee-Object``, Task Scheduler,
NSSM) is encoded with the ANSI code page (cp1252 / cp1251) and ``errors='strict'``: the
first Russian line or a sign such as ``≤ → ≈ ×`` would raise UnicodeEncodeError and stop
the bot. The streams are switched to ``errors='backslashreplace'`` here, NOT in
``cli.main()``, so tests that call ``main()`` keep pytest's own capture untouched.
``run_stonk.ps1`` / ``setup_windows.ps1`` also set ``PYTHONUTF8=1``.
"""
from __future__ import annotations

import sys
from typing import Any, Iterable


def configure_console(streams: Iterable[Any] | None = None) -> None:
    """``reconfigure(errors='backslashreplace')`` on stdout/stderr; never raises."""
    for stream in (sys.stdout, sys.stderr) if streams is None else streams:
        reconfigure = getattr(stream, 'reconfigure', None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(errors='backslashreplace')
        except Exception:
            pass


if __name__ == '__main__':
    configure_console()
    from .cli import main

    raise SystemExit(main())
