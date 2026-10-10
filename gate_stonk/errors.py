"""Exception taxonomy (ported from the 12-chagpt reference).

The message of ``Unsafe`` and its subclasses IS the reason code (UPPER_SNAKE).

Boundary between "sent" and "not sent" — the core safety contract:

* ``NotSent`` is raised by a venue ONLY before anything was signed or
  transmitted. It is the single proof that an order cannot exist.
* ``Rejected`` is an explicit, unambiguous refusal by the exchange: the
  request reached Gate and Gate answered that it did not perform it.
* ``UnknownResult`` is everything after the request may have left the process
  (timeout, connection drop, 5xx, unparseable or incomplete reply). A create
  with an unknown result keeps its reservation, is never retried blindly and
  is searched for by its ``text`` at the next reconciliation.
"""
from __future__ import annotations


class Unsafe(RuntimeError):
    """Input stale/inconsistent or an invariant is violated; do not quote."""


class TransientUnsafe(Unsafe):
    """Expected to clear on its own (e.g. history lags order status)."""


class ConfigError(ValueError):
    """Configuration file is invalid; the message names the offending key."""


class ReadError(RuntimeError):
    """A read failed or returned an invalid shape. No automatic retry."""


class RateLimited(ReadError):
    """HTTP 429 (or a documented rate-limit label) on a read."""


class Rejected(RuntimeError):
    """An explicit, unambiguous rejection by the exchange."""

    def __init__(self, message: str = "", label: str | None = None, status: int | None = None):
        super().__init__(message)
        self.label = label
        self.status = status


class UnknownResult(RuntimeError):
    """The request may have reached the exchange. Never blindly retry."""


class NotSent(RuntimeError):
    """Raised ONLY before anything was signed or transmitted."""


class NotSentUnsafe(NotSent, Unsafe):
    """A local safety refusal before sending (still an Unsafe for callers)."""


class NotSentValueError(NotSent, ValueError):
    """A local precision/argument refusal before sending (still a ValueError)."""


def not_sent(exc: BaseException) -> NotSent:
    text = str(exc) or type(exc).__name__
    if isinstance(exc, Unsafe):
        return NotSentUnsafe(text)
    if isinstance(exc, ValueError):
        return NotSentValueError(text)
    return NotSent(text)


class BudgetWait(Exception):
    """The local REST budget refused a request; nothing was sent."""

    def __init__(self, wait_seconds: float, bucket: str = ""):
        super().__init__(f"REST_BUDGET_WAIT:{bucket}:{wait_seconds:.3f}")
        self.wait_seconds = float(wait_seconds)
        self.bucket = bucket


class ReadInterrupted(Exception):
    """A local checkpoint stopped a read before it was sent (e.g. pending cancel)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason
