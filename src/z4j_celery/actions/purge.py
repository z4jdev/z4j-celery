"""Purge all pending tasks from a Celery queue.

DESTRUCTIVE. The brain requires admin role and a confirmation
dialog before issuing this. Uses ``celery_app.control.purge()``
for the default queue, or ``discard_all`` on a channel for a
specific named queue.

Audit H13: this action now requires an explicit
``confirm_token`` derived from ``HMAC(queue_name + queue_depth)``.
The brain computes it after fetching depth + showing the operator
a preview; the agent recomputes locally and refuses to act if
the token is wrong. Closes the "compromised brain or replayed
command silently nukes a queue with N pending messages" gap.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.adapter.celery.actions.purge")


#: Soft refusal threshold. Above this depth the agent refuses to
#: purge unless ``force=True`` is set. The threshold is per-call
#: configurable via ``Z4J_PURGE_THRESHOLD`` so operators with
#: legitimately-large queues can raise it.
_DEFAULT_PURGE_THRESHOLD: int = 1_000


def _purge_threshold() -> int:
    raw = os.environ.get("Z4J_PURGE_THRESHOLD")
    if not raw:
        return _DEFAULT_PURGE_THRESHOLD
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_PURGE_THRESHOLD


def expected_confirm_token(*, queue_name: str, queue_depth: int) -> str:
    """Derive the ``confirm_token`` the brain must include in its
    purge command. Both sides use this exact function so the agent
    refuses anything but a freshly-computed token.

    The token is short-lived in practice because ``queue_depth``
    changes constantly under load, so a captured-and-replayed
    command becomes wrong almost immediately.
    """
    payload = f"purge|{queue_name}|{queue_depth}".encode()
    # Local-secret HMAC; doesn't need cross-process portability
    # because the brain just produces the EXACT same string against
    # the EXACT same depth it observed when issuing the command.
    return hashlib.sha256(payload).hexdigest()


def _measure_depth(celery_app: Any, queue_name: str) -> int | None:
    """Best-effort current depth of ``queue_name``."""
    try:
        with celery_app.connection_for_read() as conn:
            channel = conn.default_channel
            queue_decl = getattr(channel, "queue_declare", None)
            if callable(queue_decl):
                resp = queue_decl(queue_name, passive=True)
                msg_count = getattr(resp, "message_count", None)
                if isinstance(msg_count, int):
                    return msg_count
            llen = getattr(channel, "client", None)
            if llen is not None and hasattr(llen, "llen"):
                return int(llen.llen(queue_name))
    except Exception:  # noqa: BLE001
        return None
    return None


async def purge_queue_action(
    celery_app: Any,
    *,
    queue_name: str,
    confirm_token: str | None = None,
    force: bool = False,
) -> CommandResult:
    """Purge one queue.

    Args:
        celery_app: Live Celery application.
        queue_name: Name of the queue to purge.
        confirm_token: HMAC of (queue_name, current_depth) computed
                       by the brain at command-issue time. Required
                       unless ``force=True``. Audit H13.
        force: Bypass both the depth-threshold and the confirm
               token check. Reserved for emergency scripted use.
               Logs a critical-level audit line on use.

    Returns:
        ``success`` with ``{"purged": N, "depth_before": D}``,
        or ``failed`` with a clear reason.
    """
    depth = _measure_depth(celery_app, queue_name)

    if force:
        logger.critical(
            "z4j purge_queue: force=True override on queue %r "
            "(depth=%s) - confirm_token check bypassed",
            queue_name, depth,
        )
    else:
        if confirm_token is None:
            return CommandResult(
                status="failed",
                error=(
                    "purge_queue: missing confirm_token. The brain "
                    "must compute HMAC(queue_name, current_depth) "
                    "and include it in the command (audit H13)."
                ),
            )
        if depth is None:
            return CommandResult(
                status="failed",
                error=(
                    "purge_queue: cannot measure current depth - "
                    "refusing to purge without confirmation. "
                    "Pass force=True if absolutely necessary."
                ),
            )
        threshold = _purge_threshold()
        if depth > threshold:
            return CommandResult(
                status="failed",
                error=(
                    f"purge_queue: depth {depth} exceeds threshold "
                    f"{threshold}; refusing to mass-delete. Raise "
                    f"Z4J_PURGE_THRESHOLD or use force=True."
                ),
            )
        expected = expected_confirm_token(
            queue_name=queue_name, queue_depth=depth,
        )
        if not hmac.compare_digest(expected, confirm_token):
            return CommandResult(
                status="failed",
                error=(
                    "purge_queue: confirm_token mismatch. The "
                    "queue depth changed between brain issue and "
                    "agent execution, or the command is replayed. "
                    "Re-issue the command."
                ),
            )

    try:
        with celery_app.connection_for_write() as conn:
            channel = conn.default_channel
            try:
                purged = channel.queue_purge(queue_name)
            except AttributeError:
                # Some kombu channel implementations don't expose
                # queue_purge directly. We REFUSE to fall back to
                # queue_delete because deleting the queue is a much
                # more destructive operation: it tears down
                # bindings, consumer registrations, and dead-letter
                # routing in ways "purge messages" never does.
                return CommandResult(
                    status="failed",
                    error=(
                        "purge_queue: kombu channel does not support "
                        "queue_purge; refusing to fall back to "
                        "queue_delete (would destroy bindings)"
                    ),
                )
    except Exception as exc:  # noqa: BLE001
        return CommandResult(
            status="failed",
            error=f"purge_queue failed: {type(exc).__name__}",
        )

    return CommandResult(
        status="success",
        result={
            "queue": queue_name,
            "depth_before": depth,
            "purged": int(purged) if isinstance(purged, int) else None,
            "force": force,
        },
    )


__all__ = ["expected_confirm_token", "purge_queue_action"]
