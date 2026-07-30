"""Cancel a running or pending Celery task.

Uses ``celery_app.control.revoke`` with ``terminate=True`` so running
tasks are sent SIGTERM. Safe to call on tasks that have already
completed - Celery no-ops that case.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_celery._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)

logger = logging.getLogger("z4j.adapter.celery.actions.cancel")

#: Cap on the synchronous broker call. Matches the worker/rate-limit
#: actions. Bounds how long a broker slowdown / failover can stall the
#: offloaded call before we give up and report failure.
_REVOKE_TIMEOUT = 10.0


async def cancel_task_action(
    celery_app: Any,
    *,
    task_id: str,
    terminate: bool = True,
    signal: str = "SIGTERM",
) -> CommandResult:
    """Revoke one Celery task.

    Args:
        celery_app: Live Celery application.
        task_id: Engine-native task id to cancel.
        terminate: If True, send a signal to actively running workers.
                   If False, only mark the task revoked in the broker
                   (the default Celery behavior for "pending" tasks).
        signal: Signal to send when ``terminate=True``. Always
                ``SIGTERM`` in v1 - ``SIGKILL`` can be enabled in a
                later release behind an admin toggle.

    Returns:
        ``success`` in all non-exceptional cases. Celery's revoke
        API does not report per-task outcomes.
    """
    # ``control.revoke`` is a synchronous kombu broadcast. Run it on the
    # dedicated broker-offload pool under a timeout so a broker slowdown /
    # failover cannot freeze the agent's event loop OR starve its heartbeat
    # providers (isolated from the default executor) -- exactly when an
    # operator reaches for Cancel. Mirrors the worker.py / rate_limit.py
    # actions.
    try:
        await offload(
            celery_app.control.revoke,
            task_id,
            terminate=terminate,
            signal=signal,
            timeout=_REVOKE_TIMEOUT,
        )
    except OffloadTimeoutError:
        # The revoke broadcast may still reach the workers; report
        # indeterminate rather than a clean failure.
        return indeterminate_timeout_result(
            "celery_app.control.revoke",
            _REVOKE_TIMEOUT,
            hint="the task may still be revoked",
        )
    except Exception as exc:
        return CommandResult(
            status="failed",
            error=f"celery_app.control.revoke failed: {exc}",
        )

    return CommandResult(
        status="success",
        result={"task_id": task_id, "terminated": terminate},
    )


__all__ = ["cancel_task_action"]
