"""Celery signal kwargs → :class:`z4j_core.models.Event` mappers.

Celery's signals pass their arguments as a loosely-typed bag of
``kwargs``. This module translates those bags into strictly-typed
:class:`Event` instances, applying redaction, truncation, and
per-task metadata as it goes.

All functions here are PURE (no I/O, no side effects), so they are
easy to unit-test against hand-crafted signal payloads.

See ``docs/API.md §5.4`` for the event wire format and
``docs/SECURITY.md §5`` for redaction requirements.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_celery.meta import TaskMeta, get_meta

_ENGINE = "celery"


def _now() -> datetime:
    return datetime.now(UTC)


def _placeholder_uuid() -> UUID:
    """Generate a placeholder UUID for the ``project_id`` / ``agent_id`` fields.

    The agent runtime replaces these with the real project/agent ids
    before forwarding the event to the brain. Having non-null values
    here keeps the model valid without requiring the mapper to know
    about the runtime.
    """
    return uuid4()


def _scrub_args(
    engine: RedactionEngine,
    args: Any,
    meta: TaskMeta | None,
) -> Any:
    """Apply redaction to positional args.

    If ``meta.keep_kwargs`` is set, positional args are redacted
    wholesale (we can't map positional args to names). This matches
    the user-facing behavior documented in ``docs/ADAPTER.md §8.1``.
    """
    if args is None:
        return None
    if meta is not None and meta.keep_kwargs is not None:
        # Whitelist mode: we don't know what positional args mean, so drop them.
        return "[REDACTED]"
    return engine.scrub(list(args))


def _scrub_kwargs(
    engine: RedactionEngine,
    kwargs: dict[str, Any] | None,
    meta: TaskMeta | None,
) -> dict[str, Any] | None:
    """Apply redaction + meta overrides to keyword args."""
    if kwargs is None:
        return None

    source: dict[str, Any] = dict(kwargs)

    # Apply whitelist first (if specified).
    if meta is not None and meta.keep_kwargs is not None:
        source = {k: v for k, v in source.items() if k in meta.keep_kwargs}

    # Force-redact the task-specific "always redact" list.
    if meta is not None and meta.redact_kwargs:
        for key in list(source):
            if key in meta.redact_kwargs:
                source[key] = "[REDACTED]"

    scrubbed = engine.scrub(source)
    assert isinstance(scrubbed, dict)
    return scrubbed


def _resolve_task_name(  # noqa: PLR0911  name-source fallbacks
    task: Any,
    *,
    fallback_hint: str | None = None,
    task_id: str | None = None,
    queue: str | None = None,
) -> str:
    """Extract the task name from a Celery task object or string.

    Celery signals pass the task as either:
    - A ``Task`` class/instance (has ``.name``)
    - A bare string (the dotted task name from the broker message)
    - ``None`` (some failure signals don't carry the task, esp.
      ``task-failed`` for a task the worker didn't have registered)

    Precedence when the primary lookup fails:

    1. ``fallback_hint`` - the raw ``task`` field from the Celery
       broker event (``event.get('name')``) if the caller preserved
       it. Most ``task-failed`` events carry this even when the
       task class isn't registered.
    2. ``<unknown:{queue}:{short_id}>`` when only a queue is known -
       readable in the dashboard, unique enough to group on.
    3. ``<unknown:{short_id}>`` when only a task_id is known.
    4. ``<unknown>`` as a last resort.

    The previous bare ``<unknown>`` ate too much information: a
    failing task whose name we DID have in the broker event was
    still rendered as the opaque placeholder, giving evaluators a
    "nothing works" impression on the Tasks page. This revision
    keeps the placeholder tag (so downstream filters keep working)
    but attaches enough context to make it actionable.
    """
    if task is not None:
        if isinstance(task, str) and task:
            return task
        name = getattr(task, "name", None) or getattr(task, "__name__", None)
        if name:
            return str(name)

    hint = fallback_hint.strip() if isinstance(fallback_hint, str) else ""
    if hint:
        return hint

    short = (task_id or "")[:8]
    if queue and short:
        return f"<unknown:{queue}:{short}>"
    if queue:
        return f"<unknown:{queue}>"
    if short:
        return f"<unknown:{short}>"
    return "<unknown>"


def _base_data(
    *,
    task_name: str,
    queue: str | None,
    worker: str | None,
    meta: TaskMeta | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        # Key must be ``task_name`` - the brain's EventIngestor reads
        # ``data.get("task_name")`` for the tasks-table projection.
        "task_name": task_name,
        "queue": queue,
        "worker": worker,
    }
    if meta is not None:
        if meta.tags:
            data["tags"] = list(meta.tags)
        if meta.priority is not None:
            data["priority"] = meta.priority
        if meta.expected_duration_ms is not None:
            data["expected_duration_ms"] = meta.expected_duration_ms
        if meta.deadline_ms is not None:
            data["deadline_ms"] = meta.deadline_ms
    if extra:
        data.update(extra)
    return data


# ---------------------------------------------------------------------------
# Signal → Event builders
# ---------------------------------------------------------------------------


def build_task_received_event(
    *,
    redaction: RedactionEngine,
    task_id: str,
    task: Any,
    args: Any = None,
    kwargs: dict[str, Any] | None = None,
    queue: str | None = None,
    parent_task_id: str | None = None,
    root_task_id: str | None = None,
) -> Event:
    """Build an Event for ``task_received``.

    Emitted by Celery when a worker picks up a task from the broker
    (``celery.signals.task_received``).

    ``parent_task_id`` / ``root_task_id`` come from Celery's
    canvas linkage on the request and are persisted on the brain
    side so the dashboard can render the chain / group / chord
    tree on the task detail page.
    """
    meta = get_meta(task)
    task_name = _resolve_task_name(task, task_id=task_id, queue=queue)
    extra: dict[str, Any] = {
        "args": _scrub_args(redaction, args, meta),
        "kwargs": _scrub_kwargs(redaction, kwargs, meta),
    }
    if parent_task_id:
        extra["parent_task_id"] = parent_task_id
    if root_task_id:
        extra["root_task_id"] = root_task_id
    data = _base_data(
        task_name=task_name,
        queue=queue,
        worker=None,
        meta=meta,
        extra=extra,
    )
    return Event(
        id=uuid4(),
        project_id=_placeholder_uuid(),
        agent_id=_placeholder_uuid(),
        engine=_ENGINE,
        task_id=task_id,
        kind=EventKind.TASK_RECEIVED,
        occurred_at=_now(),
        data=data,
    )


def build_task_started_event(
    *,
    redaction: RedactionEngine,
    task_id: str,
    task: Any,
    args: Any = None,
    kwargs: dict[str, Any] | None = None,
    worker: str | None = None,
    queue: str | None = None,
) -> Event:
    """Build an Event for ``task_prerun``.

    Emitted by Celery immediately before the task body runs
    (``celery.signals.task_prerun``).
    """
    meta = get_meta(task)
    task_name = _resolve_task_name(task, task_id=task_id, queue=queue)
    data = _base_data(
        task_name=task_name,
        queue=queue,
        worker=worker,
        meta=meta,
        extra={
            "args": _scrub_args(redaction, args, meta),
            "kwargs": _scrub_kwargs(redaction, kwargs, meta),
        },
    )
    return Event(
        id=uuid4(),
        project_id=_placeholder_uuid(),
        agent_id=_placeholder_uuid(),
        engine=_ENGINE,
        task_id=task_id,
        kind=EventKind.TASK_STARTED,
        occurred_at=_now(),
        data=data,
    )


def build_task_succeeded_event(
    *,
    redaction: RedactionEngine,
    task_id: str,
    task: Any,
    result: Any = None,
    runtime_ms: int | None = None,
    worker: str | None = None,
    queue: str | None = None,
) -> Event:
    """Build an Event for ``task_postrun`` (successful path).

    Emitted by Celery after a successful run
    (``celery.signals.task_postrun`` with ``state == "SUCCESS"``).
    """
    meta = get_meta(task)
    task_name = _resolve_task_name(task, task_id=task_id, queue=queue)

    if meta is not None and meta.redact_result:
        scrubbed_result: Any = "[REDACTED]"
    else:
        scrubbed_result = redaction.scrub(result) if result is not None else None

    data = _base_data(
        task_name=task_name,
        queue=queue,
        worker=worker,
        meta=meta,
        extra={
            "result": scrubbed_result,
            "runtime_ms": runtime_ms,
        },
    )
    return Event(
        id=uuid4(),
        project_id=_placeholder_uuid(),
        agent_id=_placeholder_uuid(),
        engine=_ENGINE,
        task_id=task_id,
        kind=EventKind.TASK_SUCCEEDED,
        occurred_at=_now(),
        data=data,
    )


def build_task_failed_event(
    *,
    redaction: RedactionEngine,
    task_id: str,
    task: Any,
    exception: BaseException,
    traceback: str | None = None,
    worker: str | None = None,
    queue: str | None = None,
) -> Event:
    """Build an Event for ``task_failure``.

    Emitted by Celery when a task raises
    (``celery.signals.task_failure``).
    """
    meta = get_meta(task)
    task_name = _resolve_task_name(task, task_id=task_id, queue=queue)

    # Truncate traceback to keep a single failure from blowing the
    # per-field size limit. The redaction engine takes care of its
    # own truncation inside ``scrub``, but we prefer a shorter one
    # for tracebacks since they can be very long.
    if traceback is not None and len(traceback) > 4096:
        traceback = traceback[:4096] + "\n[... traceback truncated ...]"

    # Tracebacks routinely contain repr() of locals - including any
    # secret that lived in the failing function's frame. Redact the
    # traceback through the same engine as the exception message,
    # otherwise we leak credentials to the brain on every failure.
    traceback_scrubbed: Any = redaction.scrub(traceback) if traceback is not None else None

    data = _base_data(
        task_name=task_name,
        queue=queue,
        worker=worker,
        meta=meta,
        extra={
            "exception": type(exception).__name__,
            "exception_message": redaction.scrub(str(exception)),
            "traceback": traceback_scrubbed,
        },
    )
    return Event(
        id=uuid4(),
        project_id=_placeholder_uuid(),
        agent_id=_placeholder_uuid(),
        engine=_ENGINE,
        task_id=task_id,
        kind=EventKind.TASK_FAILED,
        occurred_at=_now(),
        data=data,
    )


def build_task_retried_event(
    *,
    redaction: RedactionEngine,
    task_id: str,
    task: Any,
    reason: str | None = None,
    einfo: Any = None,
    worker: str | None = None,
    queue: str | None = None,
) -> Event:
    """Build an Event for ``task_retry``.

    Emitted when a task is retried
    (``celery.signals.task_retry``).
    """
    meta = get_meta(task)
    task_name = _resolve_task_name(task, task_id=task_id, queue=queue)

    reason_str = reason
    if reason_str is None and einfo is not None:
        reason_str = str(einfo)
    if reason_str is not None:
        reason_scrubbed: Any = redaction.scrub(reason_str)
    else:
        reason_scrubbed = None

    data = _base_data(
        task_name=task_name,
        queue=queue,
        worker=worker,
        meta=meta,
        extra={"reason": reason_scrubbed},
    )
    return Event(
        id=uuid4(),
        project_id=_placeholder_uuid(),
        agent_id=_placeholder_uuid(),
        engine=_ENGINE,
        task_id=task_id,
        kind=EventKind.TASK_RETRIED,
        occurred_at=_now(),
        data=data,
    )


def build_task_revoked_event(
    *,
    task_id: str,
    task: Any,
    terminated: bool = False,
    signum: int | None = None,
    expired: bool = False,
    worker: str | None = None,
    queue: str | None = None,
) -> Event:
    """Build an Event for ``task_revoked``.

    Emitted when a task is cancelled or expired
    (``celery.signals.task_revoked``).
    """
    meta = get_meta(task)
    task_name = _resolve_task_name(task, task_id=task_id, queue=queue)
    data = _base_data(
        task_name=task_name,
        queue=queue,
        worker=worker,
        meta=meta,
        extra={
            "terminated": terminated,
            "signum": signum,
            "expired": expired,
        },
    )
    return Event(
        id=uuid4(),
        project_id=_placeholder_uuid(),
        agent_id=_placeholder_uuid(),
        engine=_ENGINE,
        task_id=task_id,
        kind=EventKind.TASK_REVOKED,
        occurred_at=_now(),
        data=data,
    )


__all__ = [
    "build_task_failed_event",
    "build_task_received_event",
    "build_task_retried_event",
    "build_task_revoked_event",
    "build_task_started_event",
    "build_task_succeeded_event",
]
