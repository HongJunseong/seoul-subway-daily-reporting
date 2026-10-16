"""Wall-clock collection limits for the Linux main-thread collector runtime."""
from contextlib import contextmanager
import math
import signal
import threading

from src.common.http import SourceError


class _DeadlineExpired(BaseException):
    """Bypass HTTP retry handlers; a collection deadline must never be retried internally."""


@contextmanager
def collection_deadline(seconds):
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("collection deadline must be positive and finite")
    if not hasattr(signal, "setitimer") or threading.current_thread() is not threading.main_thread():
        raise RuntimeError("source collection requires the Linux main-thread runtime")
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise RuntimeError("source collection cannot replace an active process timer")
    previous = signal.getsignal(signal.SIGALRM)

    def expire(signum, frame):
        raise _DeadlineExpired()

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _DeadlineExpired:
        raise SourceError("Source collection exceeded its wall-clock deadline; partial input discarded") from None
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
