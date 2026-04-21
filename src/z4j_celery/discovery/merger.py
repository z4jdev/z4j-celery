"""Merge runtime + static discovery results.

Rule: **runtime is authoritative, static is supplementary.** If a
task name appears in both, the runtime entry wins - it has the real
module path, signature, queue binding, and ``loaded=True``.

Tasks that only appear in the static list are included with
``loaded=False`` so the dashboard can flag them as "declared, not
yet loaded." See ``docs/ARCHITECTURE.md §7`` for the rationale.
"""

from __future__ import annotations

from z4j_core.models import TaskDefinition


def merge_discoveries(
    runtime: list[TaskDefinition],
    static: list[TaskDefinition],
) -> list[TaskDefinition]:
    """Combine runtime and static task definitions.

    Args:
        runtime: Result of :func:`z4j_celery.discovery.runtime.discover_runtime`.
        static: Result of :func:`z4j_celery.discovery.static.discover_static`.

    Returns:
        Deduplicated list of task definitions, sorted by task name.
        Runtime entries always win on conflict.
    """
    by_name: dict[str, TaskDefinition] = {}

    # Static first - they may be replaced below.
    for definition in static:
        by_name[definition.name] = definition

    # Runtime wins.
    for definition in runtime:
        by_name[definition.name] = definition

    return sorted(by_name.values(), key=lambda d: d.name)


__all__ = ["merge_discoveries"]
