"""The ``@z4j_meta`` decorator.

Optional metadata helper users can stack on top of ``@shared_task``
or ``@app.task`` to give z4j per-task hints - redaction overrides,
tags, expected duration, skip/sample flags.

The key property of this decorator is that it is **pure metadata**.
It attaches a ``__z4j_meta__`` attribute to the decorated function
and returns the function unchanged. It does not wrap the function.
It does not change its signature. It does not affect Celery
behavior in any way. If the user uninstalls z4j entirely, every
``@z4j_meta`` call becomes a no-op.

Example::

    from celery import shared_task
    from z4j_celery import z4j_meta


    @shared_task(bind=True)
    @z4j_meta(redact_kwargs=["email"], tags=["billing"], deadline_ms=5000)
    def charge(self, user_id, email, amount): ...

See ``docs/ADAPTER.md §3.7`` for the full user-facing documentation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

META_ATTR = "__z4j_meta__"
"""Name of the attribute ``@z4j_meta`` attaches to a task function.

Other z4j code (event mapper, discovery) reads the attribute directly
to apply per-task overrides. Clients should not rely on this
attribute name - it is an internal contract between ``z4j-celery``
modules.
"""


@dataclass(frozen=True, slots=True)
class TaskMeta:
    """Normalized per-task z4j metadata attached via ``@z4j_meta``.

    Every field is optional. Code that reads ``TaskMeta`` must be
    prepared for the task not to have one at all.

    Attributes:
        redact_kwargs: Extra kwarg keys to always redact for this task.
                       Merged with the engine's default redaction rules.
        keep_kwargs: If set, only these kwargs are forwarded to the
                     brain - everything else is redacted. Use with
                     care; this is a whitelist, not a blacklist.
        redact_result: If True, the task's return value is replaced
                       with ``[REDACTED]`` in task.succeeded events.
        tags: Tags attached to every event emitted for this task.
              Surface in the dashboard's filter pills.
        expected_duration_ms: UI displays a warning badge when a run
                              exceeds this; the brain also emits an
                              alert when wired up in Phase 2.
        deadline_ms: Hard deadline. Runs exceeding this are flagged
                     as "overran deadline" in the dashboard.
        skip: If True, individual runs of this task are not reported.
              The task definition is still discovered and shown in
              the registry, so users know it exists.
        sample_rate: Fraction of runs to report, in ``[0.0, 1.0]``.
                     1.0 = all, 0.0 = none, 0.1 = 10%. Deterministic
                     by task id hash so either a specific run is
                     reported or it isn't - no "sometimes flaky"
                     drops.
    """

    redact_kwargs: frozenset[str] = field(default_factory=frozenset)
    keep_kwargs: frozenset[str] | None = None
    redact_result: bool = False
    tags: tuple[str, ...] = ()
    priority: str | None = None
    expected_duration_ms: int | None = None
    deadline_ms: int | None = None
    skip: bool = False
    sample_rate: float = 1.0


def z4j_meta(
    *,
    redact_kwargs: Iterable[str] | None = None,
    keep_kwargs: Iterable[str] | None = None,
    redact_result: bool = False,
    tags: Iterable[str] | None = None,
    priority: str | None = None,
    expected_duration_ms: int | None = None,
    deadline_ms: int | None = None,
    skip: bool = False,
    sample_rate: float = 1.0,
) -> Callable[[F], F]:
    """Attach z4j metadata to a task function.

    See :class:`TaskMeta` for the meaning of each argument.

    The decorator is a **no-op** at call time - the wrapped function
    runs exactly as if the decorator were not there. The only thing
    the decorator does is set a single attribute on the function
    object.

    Args:
        priority: Business-level priority classification.
            One of ``"critical"``, ``"high"``, ``"normal"``,
            ``"low"``. Defaults to ``"normal"`` if not set.
            Controls notification routing, dashboard prominence,
            and SLA tracking. Does NOT affect Celery queue order.

    Raises:
        ValueError: ``sample_rate`` is not in ``[0.0, 1.0]``.
        ValueError: ``priority`` is not a recognized level.
    """
    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be in [0.0, 1.0]")
    valid_priorities = {"critical", "high", "normal", "low", None}
    if priority not in valid_priorities:
        raise ValueError(
            f"priority must be one of {valid_priorities - {None}}, got {priority!r}",
        )

    meta = TaskMeta(
        redact_kwargs=frozenset(redact_kwargs or ()),
        keep_kwargs=frozenset(keep_kwargs) if keep_kwargs is not None else None,
        redact_result=redact_result,
        tags=tuple(tags or ()),
        priority=priority,
        expected_duration_ms=expected_duration_ms,
        deadline_ms=deadline_ms,
        skip=skip,
        sample_rate=sample_rate,
    )

    def decorator(func: F) -> F:
        setattr(func, META_ATTR, meta)
        return func

    return decorator


def get_meta(func: Any) -> TaskMeta | None:
    """Return the :class:`TaskMeta` attached to ``func``, or None.

    Accepts raw functions, Celery ``Task`` instances (which proxy
    ``run`` to the underlying function), and bound methods.
    """
    if func is None:
        return None
    # Celery Task objects: look at ``.run``.
    target = getattr(func, "run", func)
    return getattr(target, META_ATTR, None)


__all__ = ["META_ATTR", "TaskMeta", "get_meta", "z4j_meta"]
