"""Celery task discovery - the five-layer pipeline for Celery.

Exports:

- :func:`discover_runtime` - read ``celery_app.tasks``, the
  authoritative list of what this process currently knows about.
- :func:`discover_static` - walk filesystem paths and AST-scan
  ``tasks.py`` files for tasks that are declared but not yet
  imported.
- :func:`merge_discoveries` - combine runtime and static results,
  with runtime winning on conflicts.

See ``docs/ARCHITECTURE.md §7`` for the five-layer spec.
"""

from __future__ import annotations

from z4j_celery.discovery.merger import merge_discoveries
from z4j_celery.discovery.runtime import discover_runtime
from z4j_celery.discovery.static import discover_static

__all__ = [
    "discover_runtime",
    "discover_static",
    "merge_discoveries",
]
