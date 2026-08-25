"""Fail-closed placeholder for Celery dead-letter requeue.

Celery does not ship a first-class dead-letter concept - users
usually implement it via a custom retry policy that routes
permanently-failed tasks into a broker-specific queue or exchange. A generic
retry cannot acknowledge/remove that entry safely. The default adapter does
not advertise this action and direct calls fail without publishing anything.
"""

from __future__ import annotations

from typing import Any

from z4j_core.models import CommandResult


async def requeue_dead_letter_action(
    celery_app: Any,
    *,
    task_id: str,
    task_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Refuse generic DLQ replay without touching Celery or its broker."""
    return CommandResult(
        status="failed",
        error=(
            "z4j-celery cannot safely requeue a broker-specific dead-letter "
            "entry: use tooling that can atomically consume/ack that DLQ and "
            "preserve its original routing"
        ),
    )


__all__ = ["requeue_dead_letter_action"]
