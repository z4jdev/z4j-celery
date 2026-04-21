"""Restart a Celery worker.

Uses ``celery_app.control.broadcast("pool_restart")`` with the
worker's name as the destination. Safe to call on a prefork pool;
the worker drains in-flight tasks and respawns its pool processes.

Gevent and eventlet pools do not support pool_restart reliably -
those installations should avoid this action or switch to a full
process restart handled by their supervisor.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.agent.celery.actions.worker")

#: Hard timeout for Celery control broadcasts (seconds).
_BROADCAST_TIMEOUT = 10.0


async def restart_worker_action(
    celery_app: Any,
    *,
    worker_name: str,
) -> CommandResult:
    """Ask a Celery worker to restart its pool.

    Always uses ``pool_restart`` - the soft path that respawns the
    pool's child processes while keeping the parent worker alive.
    The destructive ``shutdown`` broadcast is intentionally NOT
    exposed via this action: a worker that does not come back up
    on its own (because the host has no supervisor configured)
    silently disappears from the dashboard with no recovery path.
    Users who need a true shutdown can do it through their host
    init system.

    Args:
        celery_app: Live Celery application.
        worker_name: Target worker, e.g. ``celery@web-01``.

    Returns:
        ``success`` in the happy path. Celery's broadcast API is
        fire-and-forget; we do not receive a confirmation that the
        worker actually restarted. Users should watch the dashboard's
        worker card for state changes.
    """
    try:
        loop = asyncio.get_running_loop()
        await asyncio.wait_for(
            loop.run_in_executor(
                None,
                functools.partial(
                    celery_app.control.broadcast,
                    "pool_restart",
                    destination=[worker_name],
                    arguments={"reload": True},
                ),
            ),
            timeout=_BROADCAST_TIMEOUT,
        )
    except TimeoutError:
        return CommandResult(
            status="failed",
            error=f"worker restart broadcast timed out after {_BROADCAST_TIMEOUT}s",
        )
    except Exception as exc:  # noqa: BLE001
        return CommandResult(
            status="failed",
            error=f"worker restart broadcast failed: {type(exc).__name__}",
        )

    return CommandResult(
        status="success",
        result={"worker": worker_name},
    )


__all__ = ["restart_worker_action"]
