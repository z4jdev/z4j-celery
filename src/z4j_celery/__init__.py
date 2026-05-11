"""z4j-celery - Celery queue engine adapter for z4j.

Public API:

- :class:`CeleryEngineAdapter` - the adapter to pass to
  :func:`z4j_bare.install_agent` or to the Django / Flask / FastAPI
  framework adapters.
- :func:`z4j_meta` - optional per-task metadata decorator for
  redaction overrides, tagging, sampling, and skip flags.
- :class:`TaskMeta` - normalized per-task metadata struct.

Licensed under Apache License 2.0.
"""

from __future__ import annotations

from z4j_celery.engine import CeleryEngineAdapter
from z4j_celery.meta import TaskMeta, z4j_meta
from z4j_celery.worker_bootstrap import register_worker_bootstrap

# Register the Celery worker_init auto-bootstrap at import time.
# This makes FastAPI / Flask / bare-Python Celery workers first-
# class z4j agents the moment ``z4j-celery`` is installed and the
# host's celery module is imported (which happens automatically
# under ``celery -A app:celery_app worker``).
#
# Django gets this for free via Z4JConfig; FastAPI / Flask used to
# be second-class because their lifespan only fires under uvicorn /
# the WSGI server, not under ``celery worker``. The signal below
# closes that gap without requiring any user code change.
#
# Opt-out: set ``Z4J_DISABLED=1`` in the worker's environment.
register_worker_bootstrap()

__version__ = "1.5.0"

__all__ = [
    "CeleryEngineAdapter",
    "TaskMeta",
    "__version__",
    "register_worker_bootstrap",
    "z4j_meta",
]
