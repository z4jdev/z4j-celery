"""Dedicated, bounded thread pool for Celery broker / result-backend I/O.

1.7.1 (M8/M10/M11/M13): every synchronous broker call (revoke, purge,
send_task, control.broadcast, AsyncResult reads) is offloaded HERE, not
onto the event loop's default executor.

* **Isolation.** The default executor also runs the agent's heartbeat
  providers (``asyncio.to_thread``) and ``getaddrinfo`` for WS reconnect.
  A hung broker parking threads there would starve agent liveness during
  the exact incident an operator is responding to. This pool is separate
  and bounded, so a broker stall can at worst exhaust THIS pool; the
  agent's heartbeat/reconnect path stays alive on the default executor.

* **Honest timeouts.** ``asyncio.wait_for`` cancels the FUTURE on timeout,
  but the underlying thread keeps running -- the broker op may STILL
  complete. ``offload()`` raises :class:`OffloadTimeoutError`; callers report
  the outcome as INDETERMINATE (never a definitive "failed", which would
  invite a retry that double-applies the side effect).

The pool is kept per-adapter (not shared via z4j-core) on purpose: a
1.7.1 adapter must not hard-require a z4j-core symbol newer than its
declared ``>=1.8.0`` floor.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import os
import threading
from collections.abc import Callable
from typing import Any, TypeVar

from z4j_core.models import CommandResult

_T = TypeVar("_T")

#: Bounded so a blackholed broker can never spawn unbounded threads. Eight
#: is ample for the agent's command concurrency (commands are dispatched
#: serially on the receive loop) while leaving the pool small enough that
#: exhaustion is observable rather than a slow leak.
_MAX_WORKERS = 8

_executor: concurrent.futures.ThreadPoolExecutor | None = None
_lock = threading.Lock()


class OffloadTimeoutError(Exception):
    """A broker call outlived its timeout.

    The underlying thread cannot be cancelled, so the operation may still
    be in progress or have already completed: the outcome is INDETERMINATE.
    Callers must NOT report a definitive failure and must NOT blindly retry.
    """


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _executor  # noqa: PLW0603  module-level lazy singleton
    if _executor is None:
        with _lock:
            if _executor is None:
                _executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_MAX_WORKERS,
                    thread_name_prefix="z4j-celery-offload",
                )
    return _executor


async def offload(
    fn: Callable[..., _T],
    *args: Any,
    timeout: float,  # noqa: ASYNC109  timeout param is the intended offload API
    **kwargs: Any,
) -> _T:
    """Run a blocking broker call on the dedicated pool under ``timeout``.

    Raises :class:`OffloadTimeoutError` when the call outlives ``timeout`` (the
    thread keeps running; the outcome is indeterminate).
    """
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(
        _get_executor(),
        functools.partial(fn, *args, **kwargs),
    )
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except TimeoutError as exc:  # asyncio.TimeoutError is TimeoutError on 3.11+
        raise OffloadTimeoutError from exc


def indeterminate_timeout_result(
    operation: str,
    timeout: float,
    *,
    hint: str = "",
) -> CommandResult:
    """Standard result for an offload timeout.

    ``status`` stays ``"failed"`` (CommandResult has no third status, and
    adding one would break a 1.7.0 brain that validates the pattern), but
    the message and the ``result["indeterminate"]`` flag mark the outcome
    as INDETERMINATE so an operator (or a 1.7.1 dashboard) does not treat
    it as a clean failure and blindly retry.
    """
    tail = f" -- {hint}" if hint else ""
    return CommandResult(
        status="failed",
        error=(
            f"{operation}: INDETERMINATE -- the broker call timed out after "
            f"{timeout:g}s. The operation MAY have already completed on the "
            "broker (the agent cannot cancel an in-flight broker call). Do "
            f"NOT blindly retry (risk of a duplicate side effect){tail}; "
            "verify broker state first."
        ),
        result={"indeterminate": True, "operation": operation},
    )


def reset_offload_executor() -> None:
    """Drop the pool after a ``fork()``. Child-context ONLY.

    Runs in the just-forked child, which is single-threaded (fork copies only
    the calling thread). It must therefore NOT acquire ``_lock`` and must NOT
    call ``shutdown()`` on the inherited executor: the inherited ``_lock`` --
    or the executor's internal shutdown lock -- may have been held by a
    thread that does not exist in the child, so waiting on either would
    DEADLOCK the child (1.7.1 H5). Instead we abandon the inherited pool
    object outright (its worker threads were not copied across the fork;
    CPython's own concurrent.futures fork handler clears the global thread
    bookkeeping, so the dead threads are never joined) and install a FRESH
    lock. The next :func:`offload` lazily recreates the pool.
    """
    global _executor, _lock  # noqa: PLW0603  module-level lazy singletons
    _executor = None
    _lock = threading.Lock()


# Reset the pool in a forked child so it never reuses the parent's dead
# threads (the OS does not copy threads across fork()). Fires after every
# fork -- harmless in a Celery prefork child (the pool is lazily recreated
# on the next offload). POSIX only; Windows has neither register_at_fork
# nor fork.
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=reset_offload_executor)


__all__ = [
    "OffloadTimeoutError",
    "indeterminate_timeout_result",
    "offload",
    "reset_offload_executor",
]
