"""Cancel a running or pending Celery task.

Uses ``celery_app.control.revoke`` with ``terminate=True`` so running
tasks are sent SIGTERM. Safe to call on tasks that have already
completed - Celery no-ops that case.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.agent.celery.actions.cancel")


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
    try:
        celery_app.control.revoke(task_id, terminate=terminate, signal=signal)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(
            status="failed",
            error=f"celery_app.control.revoke failed: {exc}",
        )

    return CommandResult(
        status="success",
        result={"task_id": task_id, "terminated": terminate},
    )


__all__ = ["cancel_task_action"]
