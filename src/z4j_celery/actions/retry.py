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

from z4j_celery._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)

logger = logging.getLogger("z4j.adapter.celery.actions.retry")

#: Cap on the offloaded broker/result-backend interaction per retry.
_RETRY_TIMEOUT = 10.0


class _RetryError(Exception):
    """A retry failed for a reportable reason (message becomes the error)."""


#: RabbitMQ consumes larger numeric priorities first. Kombu's Redis
#: transport does the opposite: it checks priority_steps [0, 3, 6, 9]
#: in ascending order. Named z4j priorities therefore need a
#: transport-aware mapping. Explicit integer priorities are never remapped.
_AMQP_PRIORITY_LABEL_TO_INT: dict[str, int] = {
    "critical": 9,
    "high": 6,
    "normal": 3,
    "low": 0,
}
_REDIS_PRIORITY_LABEL_TO_INT: dict[str, int] = {
    "critical": 0,
    "high": 3,
    "normal": 6,
    "low": 9,
}


def _coerce_priority(raw: object, *, driver_type: str | None = None) -> int | None:
    """Normalise a priority value coming from the brain payload.

    Raw integers are preserved exactly. Named labels follow Redis's
    ascending buckets for the Redis transport and AMQP's higher-first
    semantics for RabbitMQ. Unknown transports retain the historical AMQP
    mapping, with a warning from :func:`_resolve_priority`, because silently
    dropping a previously accepted priority would be a breaking change.
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
        mapping = (
            _REDIS_PRIORITY_LABEL_TO_INT if driver_type == "redis" else _AMQP_PRIORITY_LABEL_TO_INT
        )
        return mapping.get(raw.strip().lower())
    return None


def _write_transport_driver_type(celery_app: Any) -> str | None:
    """Read Kombu's configured write-transport kind without connecting."""
    try:
        connection = celery_app.connection_for_write()
        transport = connection.transport
        raw = getattr(transport, "driver_type", None)
    except Exception:
        return None
    value = str(raw or "").strip().lower()
    if "redis" in value:
        return "redis"
    if value in {"amqp", "pyamqp", "librabbitmq", "rabbitmq"}:
        return "amqp"
    return value or None


def _resolve_priority(celery_app: Any, raw: object) -> int | None:
    """Resolve one raw/label priority for the configured write transport."""
    driver_type = _write_transport_driver_type(celery_app)
    if isinstance(raw, str) and driver_type not in {"amqp", "redis"}:
        logger.warning(
            "z4j-celery: named priority %r on unknown transport %r uses "
            "the historical AMQP numeric ordering; pass an explicit integer "
            "if this transport orders priorities differently",
            raw,
            driver_type,
        )
    return _coerce_priority(raw, driver_type=driver_type)


async def retry_task_action(
    celery_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
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
    # Validate eta BEFORE touching the broker so an out-of-range value
    # never reaches send_task. Pure CPU -- stays on the loop.
    try:
        eta_dt = _eta_from_timestamp(eta)
    except ValueError as exc:
        return CommandResult(status="failed", error=f"invalid eta: {exc}")

    # The AsyncResult name/args reads and send_task are synchronous
    # result-backend + kombu broker I/O. Run them in a thread under a
    # timeout so a broker slowdown / failover cannot freeze the agent's
    # single event loop (heartbeat, send loop, ack watchdog, WS
    # ping/pong). ``bulk_retry`` awaits these one at a time, so the pool
    # is never oversubscribed.
    try:
        resolved_name, new_task_id, priority_int = await offload(
            _do_retry_io,
            celery_app,
            task_id=task_id,
            task_name=task_name,
            override_args=override_args,
            override_kwargs=override_kwargs,
            eta_dt=eta_dt,
            priority=priority,
            timeout=_RETRY_TIMEOUT,
        )
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "retry",
            _RETRY_TIMEOUT,
            hint="the task may still have been re-enqueued",
        )
    except _RetryError as exc:
        return CommandResult(status="failed", error=str(exc))
    except Exception as exc:
        return CommandResult(status="failed", error=f"send_task failed: {exc}")

    return CommandResult(
        status="success",
        result={
            "new_task_id": new_task_id,
            "task_name": resolved_name,
            "priority": priority_int,
        },
    )


def _do_retry_io(
    celery_app: Any,
    *,
    task_id: str,
    task_name: str | None,
    override_args: tuple[Any, ...] | None,
    override_kwargs: dict[str, Any] | None,
    eta_dt: Any,
    priority: object,
) -> tuple[str, str, int | None]:
    """Synchronous retry I/O: resolve name/args from the result backend
    (fallback only -- the brain-supplied ``task_name`` is authoritative,
    same contract as the other adapters) and publish via
    ``send_task``. Returns ``(resolved_name, new_task_id, priority)``; raises
    :class:`_RetryError` for reportable failures. Runs in an executor
    thread (see the caller) so the kombu/result-backend blocking stays
    off the event loop.
    """
    priority_int = _resolve_priority(celery_app, priority)
    async_result = celery_app.AsyncResult(task_id)

    name = task_name or _resolve_task_name(async_result)
    if not name:
        # result_extended is off by default, so a default-config Celery
        # app can never resolve the name from the backend -- which is why
        # the brain-supplied name is authoritative.
        raise _RetryError(f"could not resolve task name for {task_id!r}")

    resolved = _resolve_args(async_result, override_args, override_kwargs)
    if resolved is None:
        # 1.7.1 (H3/M7): no operator overrides AND the Celery result
        # backend did not store the original arguments (result_extended is
        # off by default). Celery has no failed-job registry to requeue by
        # reference, and the brain stores task arguments REDACTED and can no
        # longer forward them. Re-running with an empty payload would
        # silently execute the task with the wrong inputs, so fail closed.
        raise _RetryError(
            f"cannot retry {task_id!r}: no operator override_args / "
            "override_kwargs, and the Celery result backend did not store "
            "the original arguments (result_extended is off by default). "
            "Enable result_extended so Celery preserves the arguments, or "
            "use 'retry with different inputs' to supply them explicitly."
        )
    args, kwargs = resolved

    send_kwargs: dict[str, Any] = {"args": args, "kwargs": kwargs, "eta": eta_dt}
    if priority_int is not None:
        # Celery accepts ``priority`` as a send_task kwarg; the broker
        # plugin (RabbitMQ x-max-priority, Redis sorted-set queues) routes.
        send_kwargs["priority"] = priority_int

    new_async = celery_app.send_task(name, **send_kwargs)
    new_task_id = getattr(new_async, "id", None) or getattr(new_async, "task_id", None)
    if not new_task_id:
        raise _RetryError("send_task returned without a task id")
    return name, str(new_task_id), priority_int


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
    filter: dict[str, Any],  # noqa: A002  public bulk_retry signature
    max: int = 1000,  # noqa: A002  public bulk_retry signature
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

    task_names_raw = filter.get("task_names")
    task_names: dict[str, str] = task_names_raw if isinstance(task_names_raw, dict) else {}

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
    # M10: circuit breaker. Each retry against a hung broker burns the full
    # per-task offload timeout (~10s) inline on the receive loop, so a large
    # batch would freeze the agent for minutes -- ignoring every other command
    # and stalling event-batch acks -- while heartbeats keep flowing and it
    # looks healthy. Abort after a short run of CONSECUTIVE broker timeouts
    # rather than grinding through every id; the broker is clearly unhealthy.
    circuit_break_after = 3
    consecutive_timeouts = 0
    broker_unhealthy = False
    processed = 0

    for batch_start in range(0, len(ids), 100):
        if broker_unhealthy:
            break
        # Yield to the event loop every 100 retries so heartbeats /
        # incoming frames don't starve while a long bulk runs.
        await _asyncio.sleep(0)
        for tid in ids[batch_start : batch_start + 100]:
            tid_priority = (task_priorities or {}).get(tid)
            # ``filter["task_names"]`` is the server-owned
            # {task_id: task_name} map the brain forwards with the
            # command (same contract the RQ adapter consumes). Without
            # it every per-task retry fails on a default-config Celery
            # app ("result_extended" off means the result backend never
            # stored the name).
            tid_name = task_names.get(tid) if task_names else None
            single = await retry_task_action(
                celery_app,
                task_id=tid,
                task_name=tid_name if isinstance(tid_name, str) else None,
                priority=tid_priority,
            )
            processed += 1
            # An offload timeout tags result["indeterminate"] -- the
            # broker-hang signal the breaker counts. M2: only a genuine
            # SUCCESS resets the counter; a determinate failure is neutral
            # (neither trips nor resets), so an alternating timeout/failure
            # pattern cannot starve the breaker into grinding the whole batch.
            if single.result and single.result.get("indeterminate"):
                consecutive_timeouts += 1
            elif single.status == "success":
                consecutive_timeouts = 0
            if single.status == "success" and single.result is not None:
                new_id = single.result.get("new_task_id")
                if isinstance(new_id, str):
                    new_task_ids[tid] = new_id
                else:
                    failures[tid] = "missing new_task_id"
            else:
                failures[tid] = single.error or "unknown error"
            if consecutive_timeouts >= circuit_break_after:
                broker_unhealthy = True
                break

    return CommandResult(
        status="success",
        result={
            "filter": filter,
            "requested": len(ids),
            "succeeded": len(new_task_ids),
            "failed": len(failures),
            # Ids never attempted because the breaker tripped on a hung broker.
            "skipped": len(ids) - processed,
            "circuit_broken": broker_unhealthy,
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
) -> tuple[tuple[Any, ...], dict[str, Any]] | None:
    """Compute the effective args+kwargs for a retry, or ``None`` if
    they cannot be safely resolved.

    Safe argument sources are (a) operator-supplied overrides and (b)
    what the Celery RESULT BACKEND itself stored (``result_extended``
    on) -- Celery's own authoritative storage, NOT the brain's redacted
    Task snapshot. 1.7.1 (H3/M7): when NEITHER source exists we return
    ``None`` so the caller fails closed, instead of the pre-1.7.1
    behavior of silently falling back to an empty ``()`` / ``{}`` and
    re-running the task with the wrong inputs on a default-config app.
    """
    stored = getattr(async_result, "args", None)
    stored_kwargs = getattr(async_result, "kwargs", None)
    backend_has_args = isinstance(stored, (list, tuple))
    backend_has_kwargs = isinstance(stored_kwargs, dict)

    # 1.7.1 (H3/M7): EACH half must come from an authoritative source --
    # an operator override or Celery's own result-backend storage
    # (result_extended on). If EITHER half has neither source we fail
    # closed (return None) rather than silently substituting an empty
    # () / {} and re-running the task with that half erased. This covers
    # the partial-override case: on a default-config app (result_extended
    # off) an args-only override leaves kwargs unresolvable (and vice
    # versa), so the operator must supply BOTH halves explicitly.
    args_resolvable = override_args is not None or backend_has_args
    kwargs_resolvable = override_kwargs is not None or backend_has_kwargs
    if not args_resolvable or not kwargs_resolvable:
        return None

    args: tuple[Any, ...] = tuple(override_args) if override_args is not None else tuple(stored)
    kwargs = dict(override_kwargs) if override_kwargs is not None else dict(stored_kwargs)
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
            f"retry eta is {-delta:.0f}s in the past (max allowed: {_ETA_MAX_PAST_SECONDS}s)",
        )
    if delta > _ETA_MAX_FUTURE_SECONDS:
        raise ValueError(
            f"retry eta is {delta:.0f}s in the future "
            f"(max allowed: {_ETA_MAX_FUTURE_SECONDS}s = 365 days)",
        )
    return target


__all__ = ["bulk_retry_action", "retry_task_action"]
