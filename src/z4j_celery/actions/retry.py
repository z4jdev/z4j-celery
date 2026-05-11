"""Retry action implementations - single-task and bulk.

``retry_task_action`` looks up a previously-executed task in the
Celery result backend, reconstructs its call signature, and
re-enqueues it via ``celery_app.send_task``. Optional overrides
replace the original args/kwargs and/or schedule the retry for a
future time.

``bulk_retry_action`` is v1's simpler implementation: it takes a
filter dict, walks the brain's view of matching tasks (passed in
from the dispatcher), and issues individual retries. A
broker-inspecting implementation that batches via
``celery_app.control.inspect`` can land in Phase 2.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.adapter.celery.actions.retry")


#: Celery's broker-level priority is an integer in 0-9 (RabbitMQ)
#: or 0-255 (Redis with ``x-max-priority``). z4j's user-facing
#: priority enum (critical / high / normal / low) maps onto these
#: ranges so a "high" task preserved across retry actually lands
#: in the right priority slot. Mapping picked to align with
#: Celery's own conventions: 9 = highest, 0 = lowest. Using 9 / 6
#: / 3 / 0 (rather than 9 / 8 / 7 / 6) leaves explicit headroom
#: for operators who set raw integer priorities directly on
#: ``apply_async`` without going through z4j.
_PRIORITY_LABEL_TO_INT: dict[str, int] = {
    "critical": 9,
    "high": 6,
    "normal": 3,
    "low": 0,
}


def _coerce_priority(raw: object) -> int | None:
    """Normalise a priority value coming from the brain payload.

    Accepts the user-facing label strings AND raw integers (so
    operators using a custom priority scale via the REST API can
    pass ``priority=7`` directly). Out-of-range or unrecognised
    values resolve to ``None`` so we DON'T accidentally demote a
    high-priority task by sending garbage through to the broker.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        # ``bool`` is an ``int`` in Python; reject it explicitly
        # so ``priority=True`` doesn't sneak through as priority 1.
        return None
    if isinstance(raw, int):
        return raw if 0 <= raw <= 255 else None
    if isinstance(raw, str):
        return _PRIORITY_LABEL_TO_INT.get(raw.strip().lower())
    return None


async def retry_task_action(
    celery_app: Any,
    *,
    task_id: str,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
    eta: float | None = None,
    priority: object = None,
) -> CommandResult:
    """Re-enqueue a previously-executed Celery task.

    Args:
        celery_app: Live Celery application instance.
        task_id: Original task id to retry.
        override_args: Optional replacement positional args. If None,
                       the original args from the result backend
                       are reused.
        override_kwargs: Optional replacement keyword args.
        eta: Optional POSIX timestamp for a delayed retry.
        priority: Optional priority for the new task. Accepts the
                  user-facing labels (``"critical" | "high" |
                  "normal" | "low"``) OR a raw integer in 0-255.
                  When the brain looks up the original task's
                  priority before issuing the retry, this
                  preserves the original priority on the
                  re-enqueue (was previously dropped to broker
                  default, silently demoting high-priority work).

    Returns:
        A :class:`CommandResult`:
        - ``success`` with ``{"new_task_id": "<new id>"}`` if the
          retry was accepted by the broker.
        - ``failed`` with an ``error`` explaining why not.
    """
    try:
        async_result = celery_app.AsyncResult(task_id)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"AsyncResult lookup failed: {exc}")

    task_name = _resolve_task_name(async_result)
    if not task_name:
        return CommandResult(
            status="failed",
            error=f"could not resolve task name for {task_id!r}",
        )

    args, kwargs = _resolve_args(async_result, override_args, override_kwargs)

    # Validate eta BEFORE calling send_task so an out-of-range
    # Value never reaches the broker.
    try:
        eta_dt = _eta_from_timestamp(eta)
    except ValueError as exc:
        return CommandResult(status="failed", error=f"invalid eta: {exc}")

    priority_int = _coerce_priority(priority)

    send_kwargs: dict[str, Any] = {
        "args": args,
        "kwargs": kwargs,
        "eta": eta_dt,
    }
    if priority_int is not None:
        # Celery accepts ``priority`` as a kwarg on
        # ``send_task`` / ``apply_async``. The broker plugin
        # (RabbitMQ x-max-priority, Redis sorted-set queues, …)
        # actually does the routing; we just tag the message.
        send_kwargs["priority"] = priority_int

    try:
        new_async = celery_app.send_task(task_name, **send_kwargs)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"send_task failed: {exc}")

    new_task_id = getattr(new_async, "id", None) or getattr(new_async, "task_id", None)
    if not new_task_id:
        return CommandResult(
            status="failed",
            error="send_task returned without a task id",
        )
    return CommandResult(
        status="success",
        result={
            "new_task_id": str(new_task_id),
            "task_name": task_name,
            "priority": priority_int,
        },
    )


#: Hard ceiling on the number of tasks a single ``bulk_retry``
#: command can touch, regardless of the caller-supplied ``max``.
#: A misbehaving (or compromised) brain command with
#: ``max=10_000_000`` would otherwise loop 10M times in one
#: coroutine, blocking the event loop and flooding both the broker
#: and the result backend. 10k is large enough for any reasonable
#: bulk operation; larger fan-outs should be paginated by the
#: brain into multiple commands so each one is observable +
#: cancellable independently.
_BULK_RETRY_HARD_CAP: int = 10_000


async def bulk_retry_action(
    celery_app: Any,
    *,
    filter: dict[str, Any],
    max: int = 1000,
    task_ids: list[str] | None = None,
    task_priorities: dict[str, object] | None = None,
) -> CommandResult:
    """Retry many tasks matching a filter.

    v1 strategy: accept an explicit ``task_ids`` list from the
    caller (the brain computes it from its view of the tasks
    table). We then issue individual retries in a loop, yielding
    to the event loop every batch so a long fan-out doesn't starve
    the heartbeat / receive paths. A more efficient broker-walking
    strategy lands in Phase 2.

    Args:
        celery_app: Live Celery application.
        filter: The filter that produced ``task_ids``. Echoed back
                in the result for correlation.
        max: Safety cap requested by the caller. Hard-clamped to
             :data:`_BULK_RETRY_HARD_CAP` regardless.
        task_ids: Concrete list of task ids to retry. If the
                  caller provides this, we use it directly;
                  otherwise we return a failed result (v1 does not
                  walk the broker independently).

    Returns:
        :class:`CommandResult` with a summary dict containing
        ``requested``, ``succeeded``, ``failed``, ``capped`` (true
        if the input was truncated), and a ``new_task_ids``
        mapping.
    """
    ids = task_ids or []
    if not ids:
        return CommandResult(
            status="failed",
            error=(
                "bulk_retry requires an explicit task_ids list in v1; "
                "the brain should supply one based on the filter. "
                "Broker-walking support lands in Phase 2."
            ),
        )

    # Clamp BOTH the caller-provided cap AND the input
    # length to a hard ceiling. A compromised caller cannot bypass
    # this by passing ``max=2**31``.
    effective_cap = min(int(max), _BULK_RETRY_HARD_CAP)
    capped = False
    if len(ids) > effective_cap:
        ids = ids[:effective_cap]
        capped = True

    import asyncio as _asyncio

    new_task_ids: dict[str, str] = {}
    failures: dict[str, str] = {}

    for batch_start in range(0, len(ids), 100):
        # Yield to the event loop every 100 retries so heartbeats /
        # incoming frames don't starve while a long bulk runs.
        await _asyncio.sleep(0)
        for tid in ids[batch_start:batch_start + 100]:
            tid_priority = (task_priorities or {}).get(tid)
            single = await retry_task_action(
                celery_app, task_id=tid, priority=tid_priority,
            )
            if single.status == "success" and single.result is not None:
                new_id = single.result.get("new_task_id")
                if isinstance(new_id, str):
                    new_task_ids[tid] = new_id
                else:
                    failures[tid] = "missing new_task_id"
            else:
                failures[tid] = single.error or "unknown error"

    return CommandResult(
        status="success",
        result={
            "filter": filter,
            "requested": len(ids),
            "succeeded": len(new_task_ids),
            "failed": len(failures),
            "capped": capped,
            "hard_cap": _BULK_RETRY_HARD_CAP,
            "new_task_ids": new_task_ids,
            "failures": failures,
        },
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_task_name(async_result: Any) -> str | None:
    """Best-effort resolve the original task name from an AsyncResult."""
    # Celery 5+ stores the name on the result if the backend supports it.
    name = getattr(async_result, "name", None)
    if isinstance(name, str) and name:
        return name

    info = getattr(async_result, "info", None)
    if isinstance(info, dict):
        candidate = info.get("name") or info.get("task")
        if isinstance(candidate, str):
            return candidate

    return None


def _resolve_args(
    async_result: Any,
    override_args: tuple[Any, ...] | None,
    override_kwargs: dict[str, Any] | None,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Compute the effective args+kwargs for a retry.

    Overrides win; anything not overridden is pulled from the
    original task's stored args if the result backend carries them.
    """
    original_args: tuple[Any, ...] = ()
    original_kwargs: dict[str, Any] = {}

    stored = getattr(async_result, "args", None)
    if isinstance(stored, (list, tuple)):
        original_args = tuple(stored)

    stored_kwargs = getattr(async_result, "kwargs", None)
    if isinstance(stored_kwargs, dict):
        original_kwargs = dict(stored_kwargs)

    args = override_args if override_args is not None else original_args
    kwargs = override_kwargs if override_kwargs is not None else original_kwargs
    return args, kwargs


#: Hard window for the ``eta`` parameter on retry. Audit H14:
#: the unbounded float was sent straight to Celery's ``send_task``,
#: which would happily accept ``eta=year-9999`` and pin a broker
#: row for centuries (or negative values that pin queues
#: indefinitely on some backends). 60 seconds in the past
#: tolerates clock skew between brain and worker; 365 days in the
#: future is generous for any realistic retry-after-maintenance
#: window.
_ETA_MAX_PAST_SECONDS: int = 60
_ETA_MAX_FUTURE_SECONDS: int = 365 * 24 * 3600


def _eta_from_timestamp(eta: float | None) -> Any:
    """Convert a POSIX timestamp to a bounds-checked ``datetime``.

    Raises :class:`ValueError` (caller turns into a failed
    :class:`CommandResult`) if ``eta`` is outside the allowed
    window. We never silently clip - a clipped retry is worse than
    a refused retry because the caller doesn't know.
    """
    if eta is None:
        return None
    from datetime import UTC, datetime

    try:
        target = datetime.fromtimestamp(float(eta), tz=UTC)
    except (OverflowError, OSError, TypeError, ValueError) as exc:
        raise ValueError(
            f"retry eta {eta!r} cannot be parsed as a POSIX timestamp",
        ) from exc

    now = datetime.now(UTC)
    delta = (target - now).total_seconds()
    if delta < -_ETA_MAX_PAST_SECONDS:
        raise ValueError(
            f"retry eta is {-delta:.0f}s in the past "
            f"(max allowed: {_ETA_MAX_PAST_SECONDS}s)",
        )
    if delta > _ETA_MAX_FUTURE_SECONDS:
        raise ValueError(
            f"retry eta is {delta:.0f}s in the future "
            f"(max allowed: {_ETA_MAX_FUTURE_SECONDS}s = 365 days)",
        )
    return target


__all__ = ["bulk_retry_action", "retry_task_action"]
