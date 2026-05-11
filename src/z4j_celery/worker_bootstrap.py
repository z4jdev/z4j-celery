"""Auto-bootstrap the z4j agent inside a Celery worker process.

Django and FastAPI / Flask / bare-Python deployments all land in the
same place here: when a Celery worker process starts, it must
register itself as a z4j agent so the brain sees ``workers.online``,
broker events, and task lifecycle in real time.

Django already got this for free because :class:`z4j_django.apps
.Z4JConfig` loads on every ``manage.py`` entry point (web AND
worker). FastAPI and Flask have no such entry point - the lifespan
only runs under ``uvicorn`` / the WSGI server, NOT under
``celery worker``. Historically this meant FastAPI shops running
Celery workers saw a half-empty dashboard.

This module closes that gap. A :func:`celery.signals.worker_init`
handler, registered at import time, spins up the full z4j agent
(transport, buffer, dispatcher, heartbeat, Celery engine adapter)
in the worker process. The handler is idempotent, opt-out via
``Z4J_DISABLED``, and self-guarded against non-worker Celery
invocations (``celery inspect``, ``celery control``, ``celery
purge`` etc.) that would otherwise pollute the agent registry.

The bootstrap is triggered automatically by importing
:mod:`z4j_celery` (this module is imported from ``__init__``). In
a FastAPI or Flask app the chain is:

    celery -A app:celery_app worker     # celery imports `app`
    → app imports z4j_fastapi           # your app.py does this
    → z4j_fastapi imports z4j_celery    # ← NEW: see __init__.py
    → z4j_celery registers worker_init  # ← this module
    → worker_init fires on boot
    → z4j agent starts inside the worker

For operators who want the opposite - Celery workers that should
NOT register as agents despite the package being installed - set
``Z4J_DISABLED=1`` in the environment.

Public entry points:

- :func:`register_worker_bootstrap` - idempotent signal registration.
  Safe to call multiple times.
- :func:`_on_worker_init` - the handler itself. Exported for tests.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from typing import Any

from z4j_bare.safety import safe_boundary

logger = logging.getLogger("z4j.adapter.celery.worker_bootstrap")

#: Guard against double-registration if the module is imported more
#: than once (re-imports under pytest, importlib.reload, etc.).
_signal_connected = False

#: Module-level reference to the running agent runtime. The worker
#: lifetime owns this; ``worker_shutdown`` calls ``stop()`` on it.
_runtime: Any = None


def _is_celery_worker_invocation() -> bool:
    """Return True if the current process is running ``celery worker``.

    Celery has many sub-commands - ``inspect``, ``control``,
    ``purge``, ``beat``, ``shell``, ``events`` - and only ``worker``
    is a long-running process that should own an agent WebSocket
    slot. Short-lived commands would register, immediately exit, and
    leave stale entries in the brain's ``agents`` table that the
    dashboard has to clean up.

    The detection is best-effort by inspecting ``sys.argv``. We
    recognise every common invocation we have seen in the wild:

        celery -A app worker ...
        celery --app=app worker ...
        celery worker ...                       (legacy positional)
        python -m celery -A app worker ...
        python3 -m celery worker ...
        pypy -m celery worker ...               (PyPy interpreter)
        pypy3 -m celery worker ...
        uv run celery worker ...                (Astral uv launcher)
        uv run python -m celery worker ...
        uvx celery worker ...                   (uv "tool run")
        pipx run celery worker ...              (pipx ephemeral env)
        poetry run celery worker ...            (poetry shim)
        hatch run celery worker ...             (hatch script runner)

    We do NOT auto-bootstrap when the command is anything else, or
    when the process isn't running under a ``celery`` CLI at all
    (e.g. a plain Python shell that happened to ``import
    z4j_celery`` - tests, ad-hoc scripts).
    """
    argv = sys.argv or []
    if not argv:
        return False
    prog = os.path.basename(argv[0])

    # Python interpreters the celery module might be loaded under:
    # CPython is ``python``/``python3``/``python3.14``; PyPy is
    # ``pypy``/``pypy3``/``pypy3.10``; uv ships its own
    # interpreter shim under various names.
    _PYTHON_PROGS = ("python", "pypy")

    def _is_python(name: str) -> bool:
        return any(name.startswith(p) for p in _PYTHON_PROGS)

    # Direct celery entry point.
    if prog in {"celery", "celery.exe"}:
        return _argv_has_worker_subcommand(argv[1:])

    # ``<python> -m celery [args] worker [...]``
    if _is_python(prog) and len(argv) > 2 and argv[1] == "-m" and argv[2] == "celery":
        return _argv_has_worker_subcommand(argv[3:])

    # ``uvx celery worker ...`` - uvx is a thin alias for ``uv tool
    # run``; its argv[0] is literally "uvx" or "uvx.exe".
    if prog in {"uvx", "uvx.exe"} and len(argv) > 1 and argv[1] == "celery":
        return _argv_has_worker_subcommand(argv[2:])

    # Wrapper launchers that REPLACE argv[0] with the wrapper but
    # still expose ``celery`` (or ``python -m celery``) downstream:
    # ``uv run celery worker``, ``pipx run celery worker``, ``poetry
    # run celery worker``, ``hatch run celery worker``. We probe
    # the next few tokens for either ``celery`` or ``python -m
    # celery``.
    _WRAPPERS = {
        "uv", "uv.exe", "pipx", "pipx.exe", "poetry", "poetry.exe",
        "hatch", "hatch.exe", "rye", "rye.exe", "pdm", "pdm.exe",
    }
    if prog in _WRAPPERS:
        # Walk past wrapper sub-commands ("run", "exec", "tool",
        # "run-script") until we hit a token we recognise.
        for i, arg in enumerate(argv[1:], start=1):
            if arg in {"celery", "celery.exe"}:
                return _argv_has_worker_subcommand(argv[i + 1:])
            if _is_python(os.path.basename(arg)) and (
                len(argv) > i + 2 and argv[i + 1] == "-m"
                and argv[i + 2] == "celery"
            ):
                return _argv_has_worker_subcommand(argv[i + 3:])

    return False


def _argv_has_worker_subcommand(remaining: list[str]) -> bool:
    """Return True if the first non-flag token in ``remaining`` is ``worker``.

    Skips ``-A app`` / ``--app=app`` and any other dash-prefixed
    flags so ``celery -A myapp.celery worker -l info`` and
    ``celery worker`` both return True.
    """
    it = iter(remaining)
    for arg in it:
        if arg in ("-A", "--app"):
            next(it, None)
            continue
        if arg.startswith("-"):
            continue
        return arg == "worker"
    return False


def _env_flag(name: str) -> bool:
    """Truthy parse of an environment flag.

    Accepts ``1``, ``true``, ``yes``, ``on`` (case-insensitive).
    """
    return os.environ.get(name, "").lower() in ("1", "true", "yes", "on")


def _resolve_framework_adapter(celery_app: Any) -> Any | None:
    """Return the best available framework adapter, or ``None``.

    Precedence: FastAPI → Flask → Django → bare. We prefer adapters
    whose package is actually installed. The adapter is only used
    so the brain can label this worker's agent row with the host
    framework - functionality is identical across the choices.
    """
    # Bare is always available (it's in z4j-bare which z4j-celery
    # depends on). Anything more specific is optional.
    from z4j_bare.framework import BareFrameworkAdapter

    # Narrow to ImportError: these probes are meant to detect
    # "is the package installed?", not to swallow arbitrary
    # runtime errors that would hide real bugs in the adapter
    # itself. Any non-ImportError propagates.
    try:
        from z4j_fastapi.framework import FastAPIFrameworkAdapter  # type: ignore[import-not-found]

        return FastAPIFrameworkAdapter
    except ImportError:
        pass
    try:
        from z4j_flask.framework import FlaskFrameworkAdapter  # type: ignore[import-not-found]

        return FlaskFrameworkAdapter
    except ImportError:
        pass
    try:
        # z4j-django reports itself via its AppConfig already when
        # loaded under Django; this branch is for scripts outside
        # Django that explicitly import z4j_django for the adapter.
        from z4j_django.framework import DjangoFrameworkAdapter  # type: ignore[import-not-found]

        return DjangoFrameworkAdapter
    except ImportError:
        pass
    return BareFrameworkAdapter


@safe_boundary
def _on_worker_init(*, sender: Any = None, **_: Any) -> None:
    """Celery ``worker_init`` signal handler.

    Starts the z4j agent runtime in the current worker process.
    Idempotent: if the runtime is already running in this process
    (e.g. signal fired twice), does nothing.

    Wrapped in :func:`safe_boundary` so even a ``BaseException``
    inside our boot path (``KeyboardInterrupt`` during startup, an
    ``asyncio.CancelledError`` from a deep adapter, ...) cannot
    leak into Celery's worker-bootstrap and crash the worker. The
    ``except (SystemExit, KeyboardInterrupt): raise`` carve-out
    inside ``safe_call`` still lets the operator Ctrl-C cleanly.
    """
    global _runtime
    if _runtime is not None:
        return
    if _env_flag("Z4J_DISABLED"):
        logger.info("z4j worker bootstrap: Z4J_DISABLED set, skipping")
        return
    if not _is_celery_worker_invocation():
        # Defensive - the signal should only fire under ``celery
        # worker`` anyway, but some deep celery-internal code paths
        # can trigger it (e.g. test harnesses). Skip to avoid
        # minting a ghost agent for a 200ms process.
        return

    try:
        from celery import current_app  # type: ignore[import-not-found]

        celery_app = getattr(sender, "app", None) or current_app._get_current_object()  # noqa: SLF001
    except Exception:  # noqa: BLE001
        logger.exception("z4j worker bootstrap: cannot resolve Celery app")
        return

    try:
        from z4j_bare.install import install_agent

        from z4j_celery.engine import CeleryEngineAdapter

        engine = CeleryEngineAdapter(celery_app=celery_app)
        # Detect the host framework (Django / Flask / FastAPI / bare)
        # so the agent's hello frame reports the right framework_name
        # and the brain dashboard's Framework column shows the
        # operator's actual stack instead of "bare". install_agent
        # accepts a class and instantiates it with the resolved Config.
        # See z4j-bare 1.0.5 for the kwarg.
        framework_cls = _resolve_framework_adapter(celery_app)
        # ``install_agent`` reads brain_url / token / project_id /
        # hmac_secret from env vars when not passed explicitly - but
        # ``Z4J_DEV_MODE`` is intentionally NOT env-read (security
        # audit C3 - the kwarg is the only trusted source, because a
        # compromised env var must not silently disable the
        # ``wss://`` / signed-envelope invariants). The worker opts
        # in explicitly when the env var is truthy, and a Python
        # reviewer sees the decision in git.
        import os as _os
        dev_mode = _os.environ.get("Z4J_DEV_MODE", "").lower() in (
            "1", "true", "yes", "on",
        )
        runtime = install_agent(
            engines=[engine],
            framework=framework_cls,
            dev_mode=dev_mode,
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "z4j worker bootstrap: failed to start agent runtime - "
            "worker will continue without z4j observability",
        )
        return

    _runtime = runtime
    logger.info(
        "z4j worker bootstrap: agent runtime started "
        "(celery_app=%s, framework=%s)",
        getattr(celery_app, "main", None) or celery_app,
        framework_cls.__name__ if framework_cls else "bare",
    )


@safe_boundary
def _on_worker_shutdown(*_: Any, **__: Any) -> None:
    """Stop the runtime on worker shutdown.

    Celery fires ``worker_shutdown`` AFTER the worker main loop
    exits but BEFORE the process dies - the right place to flush
    buffered events and close the WebSocket. Best-effort; a crash
    here must not block worker exit. ``safe_boundary`` covers
    ``BaseException`` so a misbehaving adapter cannot keep the
    worker process alive past its intended exit.
    """
    global _runtime
    if _runtime is None:
        return
    try:
        _runtime.stop()
    except Exception:  # noqa: BLE001
        logger.exception("z4j worker bootstrap: error during shutdown")
    finally:
        _runtime = None


def register_worker_bootstrap() -> None:
    """Wire the Celery ``worker_init`` / ``worker_shutdown`` signals.

    Idempotent: repeated calls are no-ops after the first. Safe to
    call from module-level code in any package that depends on
    ``z4j-celery``; the signal only fires inside ``celery worker``
    processes.

    Does nothing if Celery is not installed - :mod:`celery.signals`
    import is lazy so the module can still be imported in
    environments without Celery (e.g. Django apps that don't use
    Celery yet, tests, docs builds).
    """
    global _signal_connected
    if _signal_connected:
        return
    # Held under a lock so concurrent imports (rare but possible in
    # threaded test harnesses) don't race to connect twice.
    with _register_lock:
        if _signal_connected:
            return
        try:
            from celery.signals import (  # type: ignore[import-not-found]
                worker_init,
                worker_shutdown,
            )
        except Exception:  # noqa: BLE001
            # Celery isn't installed. Nothing to do - this process
            # will never be a Celery worker.
            return
        worker_init.connect(_on_worker_init, weak=False)
        worker_shutdown.connect(_on_worker_shutdown, weak=False)
        _signal_connected = True


_register_lock = threading.Lock()


__all__ = [
    "register_worker_bootstrap",
]
