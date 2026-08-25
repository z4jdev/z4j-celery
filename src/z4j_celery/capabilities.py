"""Capability tokens advertised by the Celery engine adapter.

Kept in a dedicated module so future tuning (e.g. disabling a
capability behind a feature flag for a specific Celery version) has
one clear place to happen.
"""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        "retry_task",
        "cancel_task",
        "bulk_retry",
        "purge_queue",
        "restart_worker",
        "pool_grow",
        "pool_shrink",
        "add_consumer",
        "cancel_consumer",
        "rate_limit",
    },
)
"""Actions supported by :class:`z4j_celery.engine.CeleryEngineAdapter` out
of the box. These map directly onto method names in
:class:`z4j_core.protocols.QueueEngineAdapter`."""


__all__ = ["DEFAULT_CAPABILITIES"]
