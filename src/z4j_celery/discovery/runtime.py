"""Runtime task discovery - Layer 1 of the discovery pipeline.

Reads the Celery app's ``tasks`` registry directly. Whatever is in
``celery_app.tasks`` is authoritative - those are the tasks this
process knows how to execute right now.

Also extracts per-task metadata attached via ``@z4j_meta`` and the
default queue binding (when present in the task options).
"""

from __future__ import annotations

import inspect
import logging
from typing import Any

from z4j_core.models import TaskDefinition

from z4j_celery.meta import get_meta

logger = logging.getLogger("z4j.adapter.celery.discovery.runtime")

_ENGINE = "celery"


def discover_runtime(celery_app: Any) -> list[TaskDefinition]:
    """Return the list of task definitions registered with ``celery_app``.

    Args:
        celery_app: A Celery application instance. Duck-typed - any
                    object with a ``tasks`` attribute that behaves
                    like a dict works. This lets tests pass a stub
                    without importing the real Celery.

    Returns:
        A list of :class:`TaskDefinition`. Always marks ``loaded=True``
        because by definition these are loaded into the process.
    """
    result: list[TaskDefinition] = []
    tasks_attr = getattr(celery_app, "tasks", None)
    if tasks_attr is None:
        logger.warning("celery_app has no 'tasks' attribute; runtime discovery empty")
        return result

    try:
        items = list(tasks_attr.items())  # type: ignore[union-attr]
    except Exception:
        logger.exception("failed to iterate celery_app.tasks")
        return result

    for name, task_obj in items:
        # Skip Celery's internal tasks (they start with "celery.").
        if name.startswith("celery."):
            continue
        try:
            definition = _definition_for(name, task_obj)
        except Exception:
            logger.exception("failed to build TaskDefinition for %s", name)
            continue
        result.append(definition)
    return result


def _definition_for(name: str, task_obj: Any) -> TaskDefinition:
    """Build a single TaskDefinition for one Celery task."""
    module = _get_module(task_obj)
    queue = _get_default_queue(task_obj)
    signature = _get_signature(task_obj)
    meta = get_meta(task_obj)
    tags = list(meta.tags) if meta is not None else []

    return TaskDefinition(
        name=name,
        module=module,
        engine=_ENGINE,
        queue=queue,
        signature=signature,
        declared_in=None,
        loaded=True,
        tags=tags,
    )


def _get_module(task_obj: Any) -> str | None:
    """Best-effort resolution of a Celery task's source module."""
    # Celery Task instances proxy ``run`` to the decorated callable.
    target = getattr(task_obj, "run", task_obj)
    module = getattr(target, "__module__", None)
    if isinstance(module, str) and module:
        return module
    return None


def _get_default_queue(task_obj: Any) -> str | None:
    """Extract the task's default queue if it has one."""
    # Celery task objects have ``.queue`` or ``.options["queue"]`` or
    # ``.app.conf.task_default_queue`` as fallbacks. We try the most
    # specific first.
    queue = getattr(task_obj, "queue", None)
    if isinstance(queue, str) and queue:
        return queue

    options = getattr(task_obj, "options", None)
    if isinstance(options, dict):
        opt_queue = options.get("queue")
        if isinstance(opt_queue, str) and opt_queue:
            return opt_queue

    return None


def _get_signature(task_obj: Any) -> str | None:
    """Best-effort human-readable signature for display in the dashboard."""
    target = getattr(task_obj, "run", task_obj)
    try:
        sig = inspect.signature(target)
    except (TypeError, ValueError):
        return None
    rendered = str(sig)
    if len(rendered) > 2000:
        return rendered[:1997] + "..."
    return rendered


__all__ = ["discover_runtime"]
