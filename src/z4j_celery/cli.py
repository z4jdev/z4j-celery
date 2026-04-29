"""z4j-celery CLI: ``z4j-celery doctor | check | status | version``.

Engines are libraries (not runtimes), so the CLI surface is
intentionally narrower than a framework's: no ``run``, no
``restart`` - those verbs only make sense for adapters that
manage a connection to the brain.

Probes:

1. Upstream ``celery`` library importable + version
2. ``z4j-celery`` adapter importable + version
3. ``CELERY_BROKER_URL`` env var presence (informational)

Run from any host process to confirm the engine adapter is
correctly installed; the framework's doctor (z4j-django,
z4j-flask, z4j-fastapi) calls into this same set of probes
automatically when Celery is the detected engine.
"""

from __future__ import annotations

from z4j_bare.cli import make_engine_main

main = make_engine_main(
    "celery",
    upstream_package="celery",
    broker_env="CELERY_BROKER_URL",
)


__all__ = ["main"]
