"""Purge all pending tasks from a Celery queue.

DESTRUCTIVE. The brain requires admin role and a confirmation
dialog before issuing this. Uses ``celery_app.control.purge()``
for the default queue, or ``discard_all`` on a channel for a
specific named queue.

Audit H13 / M-7: this action requires an explicit ``confirm_token``,
a keyed ``HMAC(project_secret, "purge|queue|depth")`` (see
``z4j_core.purge_token``). The brain computes it server-side after
fetching depth + showing the operator a preview; the agent recomputes
locally against its own per-project secret and refuses to act if the
token is wrong. Keying (M-7) means a party that can only observe the
depth cannot forge or refresh a token. A pre-1.7 UNKEYED token is still
accepted during a grace window (with a warning) for rolling upgrades.
Closes the "compromised brain or replayed command silently nukes a
queue with N pending messages" gap.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from z4j_core.models import CommandResult
from z4j_core.purge_token import (
    accept_legacy_from_env,
    compute_purge_confirm_token,
    legacy_purge_confirm_token,
    verify_purge_confirm_token,
)
from z4j_core.transport.hmac import decode_agent_hmac_secret

logger = logging.getLogger("z4j.adapter.celery.actions.purge")


def _resolve_agent_secret() -> bytes | None:
    """Best-effort raw per-project secret for keying the confirm token.

    Reads ``Z4J_HMAC_SECRET`` (the value the brain returns on agent mint)
    from the environment and decodes it to the same raw bytes frame
    signing uses. Returns None when it is absent or undecodable, in which
    case only the legacy unkeyed token can be verified (grace window).
    """
    raw = os.environ.get("Z4J_HMAC_SECRET")
    if not raw:
        return None
    try:
        return decode_agent_hmac_secret(raw)
    except ValueError:
        return None


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


def expected_confirm_token(
    *,
    queue_name: str,
    queue_depth: int,
    secret: bytes | str | None = None,
) -> str:
    """Derive the ``confirm_token`` for a purge of ``queue_name`` at
    ``queue_depth``.

    Prefer :func:`z4j_core.purge_token.compute_purge_confirm_token`
    directly. This shim keys with the per-project ``secret`` when given
    (the real, keyed HMAC); with no secret it returns the pre-1.7
    UNKEYED token, kept only so existing callers/tests keep working
    during the grace window.

    The token is short-lived in practice because ``queue_depth`` changes
    constantly under load, so a captured-and-replayed command becomes
    wrong almost immediately -- and, keyed, it cannot be recomputed for
    the new depth by anyone lacking the project secret.
    """
    if secret:
        return compute_purge_confirm_token(
            secret=secret,
            queue_name=queue_name,
            queue_depth=queue_depth,
        )
    return legacy_purge_confirm_token(
        queue_name=queue_name,
        queue_depth=queue_depth,
    )


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
    except Exception:
        return None
    return None


async def purge_queue_action(  # noqa: PLR0911  guard-and-dispatch branches
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
            queue_name,
            depth,
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
        accepted, used_legacy = verify_purge_confirm_token(
            provided=confirm_token,
            queue_name=queue_name,
            queue_depth=depth,
            secret=_resolve_agent_secret(),
            accept_legacy=accept_legacy_from_env(),
        )
        if not accepted:
            return CommandResult(
                status="failed",
                error=(
                    "purge_queue: confirm_token mismatch. The "
                    "queue depth changed between brain issue and "
                    "agent execution, or the command is replayed. "
                    "Re-issue the command."
                ),
            )
        if used_legacy:
            logger.warning(
                "z4j purge_queue: accepted a LEGACY unkeyed confirm_token "
                "for queue %r -- the issuer is pre-1.7. Upgrade the brain "
                "so it sends a keyed HMAC token; legacy acceptance is "
                "removed in a future release.",
                queue_name,
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
    except Exception as exc:
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
