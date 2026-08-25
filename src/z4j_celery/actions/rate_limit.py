"""Set the per-task rate limit on a Celery worker.

Wraps ``celery_app.control.rate_limit(task_name, rate, destination=...)``
in a structured action with input validation, broadcast timeout, and
fire-and-forget semantics matching the rest of the worker-control
surface.

Rate format follows Celery's own grammar: an integer optionally
suffixed with ``/s`` (per second), ``/m`` (per minute), or ``/h``
(per hour). ``"0"`` removes the limit. Accepted examples:
``"100/m"``, ``"5/s"``, ``"1000/h"``, ``"0"``.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from z4j_core.models import CommandResult

from z4j_celery._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)

logger = logging.getLogger("z4j.adapter.celery.actions.rate_limit")

_BROADCAST_TIMEOUT = 10.0

# Celery's rate-limit grammar. ``"0"`` is a sentinel meaning "no
# limit". Otherwise: integer + optional ``/s|/m|/h`` suffix. We
# validate locally before hitting the broker so an obvious typo
# fails fast with a clear error rather than disappearing into a
# fire-and-forget broadcast.
_RATE_RE = re.compile(r"^(?:0|[1-9]\d*(?:/[smh])?)$")


async def rate_limit_action(
    celery_app: Any,
    *,
    task_name: str,
    rate: str,
    worker_name: str | None = None,
) -> CommandResult:
    """Set or clear a per-task rate limit on one or every worker.

    Args:
        celery_app: Live Celery application.
        task_name: Fully-qualified task name, e.g.
                   ``"myapp.tasks.send_email"``.
        rate: Celery rate string. ``"0"`` removes the limit.
              Supported formats: ``"<n>"``, ``"<n>/s"``, ``"<n>/m"``,
              ``"<n>/h"``.
        worker_name: Target worker (``celery@web-01``). When
                     ``None`` the rate-limit broadcast goes to
                     EVERY worker subscribed to the broker - useful
                     for an emergency global throttle, dangerous if
                     unintended.

    Returns:
        ``success`` when the local fire-and-forget broadcast call returns
        without error. This does not confirm that any worker received or
        applied the rate, and z4j currently has no effective-rate readback.
    """
    if not task_name:
        return CommandResult(
            status="failed",
            error="rate_limit: task_name is required",
        )
    if not _RATE_RE.match(rate or ""):
        return CommandResult(
            status="failed",
            error=(
                f"rate_limit: invalid rate {rate!r} - "
                "use '0' to clear, or '<n>', '<n>/s', '<n>/m', '<n>/h'"
            ),
        )

    destination = [worker_name] if worker_name else None

    try:
        await offload(
            celery_app.control.rate_limit,
            task_name,
            rate,
            destination=destination,
            timeout=_BROADCAST_TIMEOUT,
        )
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "rate_limit broadcast",
            _BROADCAST_TIMEOUT,
            hint="the rate limit may still be applied to some workers",
        )
    except Exception as exc:
        return CommandResult(
            status="failed",
            error=f"rate_limit broadcast failed: {type(exc).__name__}",
        )

    return CommandResult(
        status="success",
        result={
            "task_name": task_name,
            "rate": rate,
            "worker": worker_name,
        },
    )


__all__ = ["rate_limit_action"]
