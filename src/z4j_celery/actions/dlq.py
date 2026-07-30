"""Re-queue a task from the dead-letter queue.

Celery does not ship a first-class dead-letter concept - users
usually implement it via a custom retry policy that routes
permanently-failed tasks into a ``dead_letter`` queue. The v1 DLQ
action is therefore a thin convenience: it takes a task id,
attempts to look it up in the result backend, and re-enqueues it
on its original queue.

Users with a bespoke DLQ layout should override this with a custom
adapter if the default does not fit.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_celery.actions.retry import retry_task_action

logger = logging.getLogger("z4j.adapter.celery.actions.dlq")


async def requeue_dead_letter_action(
    celery_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Re-enqueue a task from the dead-letter queue.

    v1 implementation delegates to :func:`retry_task_action` because
    the Celery data model does not distinguish "retry" from "DLQ
    requeue" - both re-send the task signature to the broker. A
    DLQ-aware implementation can land in Phase 2 once we know what
    user conventions look like in the wild.

    ``task_name`` (brain-forwarded, contract) is threaded through:
    on a default-config Celery app the result backend does not carry the
    name, so without it the delegated retry failed "could not resolve
    task name" and the requeue was a no-op.
    """
    result = await retry_task_action(
        celery_app,
        task_id=task_id,
        task_name=task_name,
        override_args=override_args,
        override_kwargs=override_kwargs,
    )
    if result.status == "success" and result.result is not None:
        enriched = dict(result.result)
        enriched["source"] = "dlq"
        return CommandResult(status="success", result=enriched)
    return result


__all__ = ["requeue_dead_letter_action"]
