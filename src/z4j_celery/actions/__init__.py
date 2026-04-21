"""Action implementations for :class:`CeleryEngineAdapter`.

Each action is a small async function that takes a Celery ``app`` and
the action-specific parameters, and returns a
:class:`z4j_core.models.CommandResult`.

The adapter layer (``engine.py``) is a thin orchestrator that calls
into these helpers. Keeping the actions separate makes them trivially
unit-testable with a stub Celery app.
"""

from __future__ import annotations

from z4j_celery.actions.cancel import cancel_task_action
from z4j_celery.actions.dlq import requeue_dead_letter_action
from z4j_celery.actions.purge import purge_queue_action
from z4j_celery.actions.rate_limit import rate_limit_action
from z4j_celery.actions.retry import bulk_retry_action, retry_task_action
from z4j_celery.actions.worker import restart_worker_action

__all__ = [
    "bulk_retry_action",
    "cancel_task_action",
    "purge_queue_action",
    "rate_limit_action",
    "requeue_dead_letter_action",
    "restart_worker_action",
    "retry_task_action",
]
