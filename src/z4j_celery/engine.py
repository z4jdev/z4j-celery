"""The :class:`CeleryEngineAdapter` - z4j's Celery queue engine adapter.

Implements :class:`z4j_core.protocols.QueueEngineAdapter` on top of
a live Celery ``app`` instance. Wires up:

- Signal hooks that capture task lifecycle events
- Discovery pipeline that enumerates known tasks
- Action implementations (retry, cancel, bulk, purge, dlq, restart)

The adapter is constructed once per agent runtime. Users typically
do not instantiate it directly - framework adapters like
``z4j-django`` discover Celery, import the engine adapter, and pass
it to the agent runtime.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

from z4j_core.errors import NotFoundError
from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.redaction.engine import RedactionEngine
from z4j_core.version import PROTOCOL_VERSION

from z4j_celery.actions import (
    bulk_retry_action,
    cancel_task_action,
    purge_queue_action,
    rate_limit_action,
    requeue_dead_letter_action,
    restart_worker_action,
    retry_task_action,
)
from z4j_celery.capabilities import DEFAULT_CAPABILITIES
from z4j_celery.discovery import discover_runtime, discover_static, merge_discoveries
from z4j_celery.events.signals import CelerySignalHooks

logger = logging.getLogger("z4j.adapter.celery.engine")

_ENGINE_NAME = "celery"

# SECURITY: ``inspector.conf()`` returns the FULL Celery app.conf dict,
# including credentialed values: ``broker_url`` (commonly contains
# ``redis://:password@...`` / ``amqp://user:pw@...`` / SQS access keys),
# ``result_backend`` (``db+postgresql://user:pw@...``),
# ``broker_transport_options`` (TLS keys, IAM creds), and
# ``beat_schedule`` (raw schedule kwargs that may embed PII or tokens).
#
# We allowlist (not denylist) the keys we send to the brain: operationally
# useful tuning knobs that operators legitimately want visible in the
# dashboard. A future Celery release adding a new credentialed key would
# silently leak under a denylist, so allowlist is the safer posture.
#
# Round-7 audit finding R7-H1: the brain persists worker_metadata.conf
# verbatim and exposes it to ProjectRole.VIEWER over
# ``GET /api/v1/projects/{slug}/workers/{worker_id}``. Without this
# filter at source, every viewer-role member could read broker / backend
# credentials. The brain re-applies the same allowlist defense-in-depth
# in ``z4j_brain.websocket.frame_router``.
_CONF_ALLOWLIST: frozenset[str] = frozenset({
    # Serialization
    "task_serializer",
    "result_serializer",
    "accept_content",
    # Queue routing
    "task_default_queue",
    # Worker concurrency / lifecycle
    "worker_concurrency",
    "worker_prefetch_multiplier",
    "worker_max_tasks_per_child",
    "worker_max_memory_per_child",
    # Reliability semantics
    "task_acks_late",
    "task_reject_on_worker_lost",
    # Time limits
    "task_time_limit",
    "task_soft_time_limit",
    # Broker pooling (knobs, not creds; broker_url is excluded)
    "broker_pool_limit",
    "broker_heartbeat",
    # Time zone
    "timezone",
    "enable_utc",
})


def _redact_worker_conf(cfg: Any) -> dict[str, Any]:
    """Strip credentialed keys from a Celery worker conf dict.

    Apply the source-side allowlist defined in ``_CONF_ALLOWLIST``. Any
    key not in the allowlist is dropped, even if it looks benign. The
    return value is always a plain ``dict`` (empty if ``cfg`` is not a
    dict-like), so downstream JSON serialization is total.
    """
    if not isinstance(cfg, dict):
        return {}
    return {k: v for k, v in cfg.items() if k in _CONF_ALLOWLIST}


class CeleryEngineAdapter:
    """Queue-engine adapter for Celery.

    Args:
        celery_app: Live Celery application instance. Duck-typed - any
                    object with the ``tasks``, ``control``,
                    ``connection_for_write``, ``AsyncResult``, and
                    ``send_task`` attributes used by the action helpers
                    works. This is what makes unit tests trivial.
        redaction: Optional redaction engine. If None, a default one
                   is constructed. The agent runtime shares its own
                   engine here so per-project config propagates.
    """

    name: str = _ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    def __init__(
        self,
        *,
        celery_app: Any,
        redaction: RedactionEngine | None = None,
    ) -> None:
        self.celery_app = celery_app
        self.redaction = redaction or RedactionEngine()
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        self._signal_hooks: CelerySignalHooks | None = None
        self._broker_monitor: Any = None
        # Registry-delta queue, fed by the optional dev-mode
        # filesystem watcher (``Z4J_DEV_MODE=true`` +
        # ``z4j-bare[watcher]`` installed). ``subscribe_registry_changes``
        # consumes it; in production this stays empty forever.
        self._registry_delta_queue: asyncio.Queue[TaskRegistryDelta] = (
            asyncio.Queue(maxsize=100)
        )
        self._fs_watcher: Any = None
        self._discovery_hints: DiscoveryHints | None = None
        self._registry_loop: asyncio.AbstractEventLoop | None = None
        # Worker stats cache for get_health(). MUST be instance-scoped
        # (R8-L2): pre-1.6.7 these lived at class scope (engine.py:463-464),
        # which let two CeleryEngineAdapter instances in the same Python
        # process see each other's cached worker_details payload. In single-
        # tenant prod that was harmless; in multi-tenant tests and in the
        # forthcoming 1.7 soak rig it lets project A's worker conf leak
        # into project B's heartbeat. Mutable class-level defaults are
        # never safe; the previous declaration also tripped ruff RUF012.
        self._last_worker_stats_at: float = 0.0
        self._cached_worker_stats: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def connect_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Subscribe to Celery task lifecycle events.

        Automatically chooses the right capture strategy based on
        the worker pool type:

        - ``solo`` - uses in-process Celery signals. Signals fire
          in the main process where the z4j runtime lives, so
          ``call_soon_threadsafe`` reaches the async queue directly.

        - ``prefork`` (default), ``gevent``, ``eventlet`` - uses
          Celery's **broker events** system. The worker publishes
          lifecycle events to the broker; the monitor receives them
          on a dedicated connection in the main process. This
          bypasses the fork boundary entirely. Same approach as
          Flower.

        The caller (AgentRuntime) doesn't need to know which
        strategy is in use - both feed the same ``_event_queue``.
        """
        target_loop = loop
        # In celery prefork pools, signals fire in forked CHILD
        # processes whose
        # ``target_loop`` reference is to a loop that lives only in
        # the PARENT. Calling ``call_soon_threadsafe`` on it from the
        # child either no-ops silently or, on some pythons, raises and
        # tears down the worker. Capture the connect-time PID; if the
        # sink fires in a different process we drop the event and let
        # the broker-events monitor (running in the parent) pick it up.
        _connect_pid = os.getpid()

        def sink(event: Event) -> None:
            if os.getpid() != _connect_pid:
                # Forked child - target_loop is stale. Broker-events
                # monitor in the parent process is the source of truth.
                return
            current_loop = target_loop
            if current_loop is None:
                try:
                    current_loop = asyncio.get_running_loop()
                except RuntimeError:
                    logger.debug("no running loop for celery event; dropping")
                    return
            current_loop.call_soon_threadsafe(self._enqueue_event, event)

        # Always connect signals - they work in solo and are a
        # reliable fallback for any pool type. In prefork/gevent/eventlet,
        # signals fire in the main process for task-sent but not for
        # task-succeeded/failed (those fire in child processes). The
        # broker events monitor covers those.
        logger.info("z4j celery: connecting signal hooks")
        self._signal_hooks = CelerySignalHooks(
            sink=sink,
            redaction=self.redaction,
        )
        self._signal_hooks.connect()

        # Also start the broker events monitor. It captures events
        # published by worker children in prefork mode. For solo pools
        # this is redundant (signals already cover everything) but
        # harmless - the dedup in the brain's EventIngestor handles it.
        pool = self._detect_pool_type()
        logger.info(
            "z4j celery: starting broker events monitor (pool=%s)",
            pool,
        )
        from z4j_celery.events.broker import CeleryBrokerEventsMonitor

        # When running side-by-side with a
        # specific celery worker process (the typical "1 z4j-bare per
        # worker container" topology), pair the broker monitor with
        # the LOCAL celery hostname so it ignores events emitted by
        # OTHER workers on the same broker. Without this filter, every
        # agent receives every event via celery's fanout exchange and
        # the brain sees N copies (one per agent) of every task event,
        # multiplying ingest contention by N.
        #
        # Auto-detection precedence:
        # 1. ``Z4J_CELERY_LOCAL_HOSTNAME`` env var (operator override).
        # 2. ``CELERY_HOSTNAME`` env var (some celery deployments set
        #    this).
        # 3. None - filter disabled, monitor accepts every event
        #    (legacy behaviour for "one z4j-bare watching N celery
        #    workers" deployments). Brain-side dedupe (Bug X-B) is
        #    the safety net here.
        local_hostname = (
            os.environ.get("Z4J_CELERY_LOCAL_HOSTNAME", "").strip()
            or os.environ.get("CELERY_HOSTNAME", "").strip()
        )
        hostname_filter: "Callable[[str], bool] | None" = None
        if local_hostname:
            _local = local_hostname

            def _match_local(host: str, _local: str = _local) -> bool:
                return host == _local

            hostname_filter = _match_local
            logger.info(
                "z4j celery: broker events monitor filtering by hostname=%s",
                local_hostname,
            )
        else:
            logger.info(
                "z4j celery: broker events monitor accepts ALL events "
                "(no Z4J_CELERY_LOCAL_HOSTNAME set; relying on brain-side dedupe)",
            )

        self._broker_monitor = CeleryBrokerEventsMonitor(
            celery_app=self.celery_app,
            sink=sink,
            redaction=self.redaction,
            hostname_filter=hostname_filter,
        )
        self._broker_monitor.start()

    def disconnect_signals(self) -> None:
        """Disconnect signals or stop broker events monitor. Idempotent."""
        if self._signal_hooks is not None:
            self._signal_hooks.disconnect()
            self._signal_hooks = None
        if self._broker_monitor is not None:
            self._broker_monitor.stop()
            self._broker_monitor = None

    def _detect_pool_type(self) -> str:
        """Return the Celery worker pool type as a string.

        Checks ``worker_pool`` from the app config. Defaults to
        ``"prefork"`` which is Celery's default. The detection is
        best-effort - if the config key is missing or unreadable,
        we assume prefork (the safer choice, since it needs broker
        events).
        """
        try:
            pool = self.celery_app.conf.get("worker_pool", "prefork")
            if pool is None:
                return "prefork"
            pool_str = str(pool).lower()
            if "solo" in pool_str:
                return "solo"
            return pool_str
        except Exception:  # noqa: BLE001
            return "prefork"

    def _enqueue_event(self, event: Event) -> None:
        """Internal helper - invoked on the runtime loop thread.

        If the queue is full, drop oldest entries until there is room.
        Logs every drop so operators have visibility into backpressure.
        """
        for _attempt in range(3):
            try:
                self._event_queue.put_nowait(event)
                return
            except asyncio.QueueFull:
                try:
                    dropped = self._event_queue.get_nowait()
                    logger.warning(
                        "z4j celery: event queue full, dropped event",
                        dropped_kind=getattr(dropped, "kind", "?"),
                    )
                except asyncio.QueueEmpty:
                    pass
        # All retries exhausted - log and drop the new event.
        logger.error(
            "z4j celery: failed to enqueue event after retries",
            event_kind=getattr(event, "kind", "?"),
        )

    # ------------------------------------------------------------------
    # QueueEngineAdapter - discovery
    # ------------------------------------------------------------------

    async def discover_tasks(
        self,
        hints: DiscoveryHints | None = None,
    ) -> list[TaskDefinition]:
        # Cache the hints so the dev-mode watcher knows which
        # paths to monitor without us threading them through the
        # subscribe API. Hints are stable across the agent's
        # lifetime (the framework adapter computes them once at
        # boot) so this single assignment is safe.
        if hints is not None:
            self._discovery_hints = hints
        runtime_defs = discover_runtime(self.celery_app)
        static_defs: list[TaskDefinition] = []
        if hints is not None and hints.app_paths:
            app_paths: list[Path] = [Path(p) for p in hints.app_paths]
            static_defs = discover_static(app_paths)
        return merge_discoveries(runtime_defs, static_defs)

    async def subscribe_registry_changes(
        self,
    ) -> AsyncIterator[TaskRegistryDelta]:
        """Yield registry deltas as the task surface changes.

        Two sources, both optional and additive:

        - **Filesystem watcher** (``Z4J_DEV_MODE=true`` AND
          ``z4j-bare[watcher]`` installed). On every saved
          ``tasks.py`` we re-run discovery, diff against the
          last-known set, and yield a single delta capturing the
          added/removed/updated tasks. Restart-free task
          development.
        - **Future**: a periodic reconciliation loop landing in
          Phase 2 will compare ``celery_app.tasks`` to the
          previous snapshot every N seconds and yield drift -
          covers cases where the watcher missed an event (NFS
          mount, container restart, …).

        In production with neither source enabled this method
        simply blocks forever on the queue, which is the correct
        no-op behaviour - the agent's task-group cancels it on
        shutdown.
        """
        import os

        if (
            os.environ.get("Z4J_DEV_MODE", "").lower() in ("1", "true", "yes", "on")
            and self._discovery_hints is not None
            and self._discovery_hints.app_paths
            and self._fs_watcher is None
        ):
            self._start_fs_watcher()

        while True:
            delta = await self._registry_delta_queue.get()
            yield delta

    def _start_fs_watcher(self) -> None:
        """Spin up the dev-mode filesystem watcher, if available."""
        from z4j_bare.watcher import TasksFileWatcher

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("z4j watcher: no running loop; skipping start")
            return
        self._registry_loop = loop
        watcher = TasksFileWatcher(
            app_paths=self._discovery_hints.app_paths if self._discovery_hints else [],
            on_change=self._on_tasks_file_changed,
        )
        if watcher.start():
            self._fs_watcher = watcher
        else:
            self._fs_watcher = None

    def _on_tasks_file_changed(self, path: Path) -> None:
        """Filesystem-watcher callback (runs on the watcher thread).

        Hops back to the agent's asyncio loop via
        ``call_soon_threadsafe`` and schedules a re-discovery.
        Diffing + delta construction happens on the loop so we
        don't fight cross-thread asyncio Queue semantics.
        """
        loop = self._registry_loop
        if loop is None or self._discovery_hints is None:
            return

        async def _rediscover() -> None:
            try:
                defs = await self.discover_tasks(self._discovery_hints)
            except Exception:  # noqa: BLE001
                logger.exception("z4j watcher: re-discovery raised")
                return
            delta = TaskRegistryDelta(
                engine=_ENGINE_NAME,
                added=defs,
                removed=[],
                updated=[],
            )
            try:
                self._registry_delta_queue.put_nowait(delta)
            except asyncio.QueueFull:
                logger.warning(
                    "z4j watcher: registry-delta queue full; dropping",
                )

        loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(_rediscover()),
        )
        logger.info("z4j watcher: tasks.py changed (%s); re-discovery queued", path)

    # ------------------------------------------------------------------
    # QueueEngineAdapter - observation
    # ------------------------------------------------------------------

    async def subscribe_events(self) -> AsyncIterator[Event]:
        """Drain the internal event queue as they arrive.

        The signal handlers push events onto ``self._event_queue``
        via ``call_soon_threadsafe``; this method yields them to the
        agent runtime's transport loop.
        """
        while True:
            event = await self._event_queue.get()
            yield event

    async def list_queues(self) -> list[Queue]:
        return []

    async def list_workers(self) -> list[Worker]:
        return []

    def get_health(self) -> dict[str, Any]:
        """Return broker health + queue depths + worker stats for the heartbeat.

        Called synchronously from the heartbeat loop. Uses the
        Celery app's existing broker connection - no extra Redis
        URL needed. Best-effort: if the broker is unreachable
        (unlikely - the worker wouldn't be running), returns
        degraded status without crashing.
        """
        health: dict[str, Any] = {
            "broker_type": self._detect_broker_type(),
            "broker_connected": False,
            "queue_depths": {},
        }
        try:
            with self.celery_app.connection_for_read() as conn:
                health["broker_connected"] = True
                transport = conn.transport
                # Redis: use LLEN directly for queue depths
                if hasattr(transport, "client"):
                    client = transport.client
                    for queue_name in self._known_queues():
                        try:
                            depth = client.llen(queue_name)
                            health["queue_depths"][queue_name] = depth
                        except Exception:  # noqa: BLE001
                            pass
                # AMQP: use passive queue_declare
                elif hasattr(conn, "channel"):
                    try:
                        channel = conn.channel()
                        for queue_name in self._known_queues():
                            try:
                                _, count, _ = channel.queue_declare(
                                    queue=queue_name, passive=True,
                                )
                                health["queue_depths"][queue_name] = count
                            except Exception:  # noqa: BLE001
                                pass
                    except Exception:  # noqa: BLE001
                        pass
        except Exception as exc:  # noqa: BLE001
            health["broker_error"] = str(exc)[:200]

        # Worker stats from control.inspect() - expensive (broker
        # round-trip), so cached for 60s. Heartbeats run every 10s
        # but the inspect only refreshes on cache expiry.
        import time as _time

        now = _time.monotonic()
        if now - self._last_worker_stats_at > 60:
            try:
                self._cached_worker_stats = self.get_worker_details()
                self._last_worker_stats_at = now
            except Exception:  # noqa: BLE001
                pass

        if self._cached_worker_stats:
            health["worker_details"] = self._cached_worker_stats

        return health

    def _detect_broker_type(self) -> str:
        """Return 'redis', 'amqp', or 'unknown' from the broker URL."""
        try:
            url = str(self.celery_app.conf.broker_url or "")
            if "redis" in url.lower():
                return "redis"
            if "amqp" in url.lower() or "rabbit" in url.lower():
                return "amqp"
            if "sqs" in url.lower():
                return "sqs"
            return "unknown"
        except Exception:  # noqa: BLE001
            return "unknown"

    def get_worker_details(self, hostname: str | None = None) -> dict[str, Any]:
        """Collect detailed worker stats via control.inspect().

        This is the data Flower shows in its worker detail tabs.
        Runs `inspect` broadcast commands against the Celery
        cluster. If `hostname` is provided, only inspects that
        worker (faster). Otherwise inspects all.

        Returns a dict keyed by worker hostname, each containing:
        - stats: system stats (rusage, pool info, broker info)
        - active: currently running tasks
        - active_queues: queues the worker consumes
        - registered: registered task names
        - conf: worker configuration

        Best-effort: if the worker is unreachable or the inspect
        times out, returns empty data for that worker.
        """
        result: dict[str, Any] = {}
        try:
            inspector = self.celery_app.control.inspect(
                destination=[hostname] if hostname else None,
                timeout=2.0,
            )

            # Stats: pool info, broker info, rusage, total tasks
            stats = inspector.stats() or {}
            for worker, data in stats.items():
                result.setdefault(worker, {})["stats"] = data

            # Active tasks
            active = inspector.active() or {}
            for worker, tasks in active.items():
                result.setdefault(worker, {})["active"] = tasks

            # Active queues
            queues = inspector.active_queues() or {}
            for worker, qs in queues.items():
                result.setdefault(worker, {})["active_queues"] = qs

            # Registered task names
            registered = inspector.registered() or {}
            for worker, names in registered.items():
                result.setdefault(worker, {})["registered"] = names

            # Configuration (may be large AND may include credentialed
            # keys; allowlist-filter via ``_redact_worker_conf`` before
            # exposing to the brain. See ``_CONF_ALLOWLIST`` docstring
            # for the round-7 audit context.)
            conf = inspector.conf() or {}
            for worker, cfg in conf.items():
                result.setdefault(worker, {})["conf"] = _redact_worker_conf(cfg)

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "z4j celery: worker inspect failed: %s",
                str(exc)[:200],
            )
        return result

    def _known_queues(self) -> list[str]:
        """Return queue names the worker is consuming from.

        Falls back to ["celery"] (the default queue) if we can't
        determine the active queues.
        """
        try:
            # Try to get queues from the app's active consumer
            queues = self.celery_app.conf.get("task_queues")
            if queues:
                return [
                    getattr(q, "name", str(q)) for q in queues
                ]
            default = self.celery_app.conf.get(
                "task_default_queue", "celery",
            )
            return [default]
        except Exception:  # noqa: BLE001
            return ["celery"]

    async def get_task(self, task_id: str) -> Task | None:
        # v1: the brain's own Postgres is the authoritative view
        # of task state. This method is here so the Protocol is
        # satisfied; adapters that want to query the Celery result
        # backend directly can override it.
        try:
            _ = self.celery_app.AsyncResult(task_id)
        except Exception:  # noqa: BLE001
            raise NotFoundError(f"task {task_id!r} not found") from None
        return None

    async def reconcile_task(self, task_id: str) -> CommandResult:
        """Query Celery's result backend for ground truth.

        Called by the brain's ReconciliationWorker. Maps Celery
        ``AsyncResult.state`` to the z4j-canonical engine-state
        taxonomy used by the brain's reconciliation projection.
        """
        # Celery's result-backend states: PENDING, STARTED, SUCCESS,
        # FAILURE, RETRY, REVOKED. Map to z4j's normalized taxonomy.
        state_map = {
            "PENDING": "pending",
            "RECEIVED": "pending",
            "STARTED": "started",
            "SUCCESS": "success",
            "FAILURE": "failure",
            "RETRY": "pending",
            "REVOKED": "failure",
        }
        try:
            res = self.celery_app.AsyncResult(task_id)
            state = state_map.get(
                _safe_str_attr(res, "state", "UNKNOWN"),
                "unknown",
            )
            exc_info: str | None = None
            if state == "failure":
                try:
                    traceback = res.traceback
                    if traceback:
                        exc_info = str(traceback)[:2000]
                except Exception:  # noqa: BLE001
                    pass
        except Exception as exc:  # noqa: BLE001
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": f"reconcile probe failed: {exc}",
                },
            )
        finished_at = None
        if state in ("success", "failure"):
            try:
                date_done = res.date_done
                if date_done is not None:
                    finished_at = date_done.isoformat()
            except Exception:  # noqa: BLE001
                pass
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "engine_state": state,
                "finished_at": finished_at,
                "exception": exc_info,
            },
        )

    # ------------------------------------------------------------------
    # QueueEngineAdapter - actions
    # ------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        eta: float | None = None,
        priority: int | None = None,
    ) -> CommandResult:
        """Universal enqueue via Celery's ``app.send_task``.

        Same primitive the brain calls for retries / bulk retries /
        DLQ requeues - Celery's adapter therefore needs no special
        polyfill code on the brain side.
        """
        from datetime import UTC, datetime, timedelta

        try:
            send_kwargs: dict[str, Any] = {"args": args, "kwargs": kwargs or {}}
            if queue:
                send_kwargs["queue"] = queue
            if eta is not None:
                send_kwargs["eta"] = datetime.now(UTC) + timedelta(seconds=eta)
            if priority is not None:
                send_kwargs["priority"] = priority

            # Prefer ``apply_async`` on a locally-registered task
            # over ``send_task``. Two reasons:
            #
            # 1. ``send_task`` bypasses the local task registry and
            #    goes straight to the broker, which means
            #    ``task_always_eager=True`` has no effect on it.
            #    Operators with eager-mode CI / dev setups expect
            #    the task to run synchronously; ``apply_async``
            #    honors that.
            # 2. ``apply_async`` knows the task's options (default
            #    queue, retry policy, time limit, etc.) from the
            #    decorator. ``send_task`` always sends to the
            #    default exchange unless the operator passes
            #    ``queue=`` explicitly.
            #
            # Fall back to ``send_task`` when the task is not in
            # the local registry (typical at-distance scheduling
            # where the worker process owns the registration but
            # this brain-side process does not).
            tasks = getattr(self.celery_app, "tasks", None)
            if tasks is not None and name in tasks:
                result = tasks[name].apply_async(**send_kwargs)
            else:
                result = self.celery_app.send_task(name, **send_kwargs)
            new_id = getattr(result, "id", None) or getattr(result, "task_id", None)
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": new_id, "engine": self.name},
        )

    async def retry_task(
        self,
        task_id: str,
        *,
        override_args: tuple[Any, ...] | None = None,
        override_kwargs: dict[str, Any] | None = None,
        eta: float | None = None,
        priority: object = None,
    ) -> CommandResult:
        return await retry_task_action(
            self.celery_app,
            task_id=task_id,
            override_args=override_args,
            override_kwargs=override_kwargs,
            eta=eta,
            priority=priority,
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        return await cancel_task_action(self.celery_app, task_id=task_id)

    async def bulk_retry(
        self,
        filter: dict[str, Any],
        *,
        max: int = 1000,
    ) -> CommandResult:
        task_ids_raw = filter.get("task_ids")
        task_ids: list[str] | None = None
        if isinstance(task_ids_raw, list):
            task_ids = [str(t) for t in task_ids_raw]
        # ``task_priorities`` is a {task_id: priority} mapping the
        # brain looks up before issuing a bulk-retry. Per-task so
        # we don't apply one priority to every retried task in a
        # mixed batch (`high` next to `low`).
        priorities_raw = filter.get("task_priorities")
        task_priorities: dict[str, object] | None = None
        if isinstance(priorities_raw, dict):
            task_priorities = {str(k): v for k, v in priorities_raw.items()}
        return await bulk_retry_action(
            self.celery_app,
            filter=filter,
            max=max,
            task_ids=task_ids,
            task_priorities=task_priorities,
        )

    async def purge_queue(
        self,
        queue_name: str,
        *,
        confirm_token: str | None = None,
        force: bool = False,
    ) -> CommandResult:
        return await purge_queue_action(
            self.celery_app,
            queue_name=queue_name,
            confirm_token=confirm_token,
            force=force,
        )

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:
        return await requeue_dead_letter_action(self.celery_app, task_id=task_id)

    async def restart_worker(self, worker_id: str) -> CommandResult:
        return await restart_worker_action(self.celery_app, worker_name=worker_id)

    async def pool_grow(self, worker_name: str, delta: int) -> CommandResult:
        """Grow the worker's process pool by ``delta``."""
        try:
            self.celery_app.control.pool_grow(delta, destination=[worker_name])
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=f"pool_grow failed: {exc}")
        return CommandResult(status="success", result={"worker": worker_name, "delta": delta})

    async def pool_shrink(self, worker_name: str, delta: int) -> CommandResult:
        """Shrink the worker's process pool by ``delta``."""
        try:
            self.celery_app.control.pool_shrink(delta, destination=[worker_name])
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=f"pool_shrink failed: {exc}")
        return CommandResult(status="success", result={"worker": worker_name, "delta": delta})

    async def add_consumer(self, worker_name: str, queue: str) -> CommandResult:
        """Start consuming from an additional queue."""
        try:
            self.celery_app.control.add_consumer(queue, destination=[worker_name])
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=f"add_consumer failed: {exc}")
        return CommandResult(status="success", result={"worker": worker_name, "queue": queue})

    async def cancel_consumer(self, worker_name: str, queue: str) -> CommandResult:
        """Stop consuming from a queue."""
        try:
            self.celery_app.control.cancel_consumer(queue, destination=[worker_name])
        except Exception as exc:  # noqa: BLE001
            return CommandResult(status="failed", error=f"cancel_consumer failed: {exc}")
        return CommandResult(status="success", result={"worker": worker_name, "queue": queue})

    async def rate_limit(
        self,
        task_name: str,
        rate: str,
        *,
        worker_name: str | None = None,
    ) -> CommandResult:
        """Set or clear a per-task rate limit on one (or every) worker."""
        return await rate_limit_action(
            self.celery_app,
            task_name=task_name,
            rate=rate,
            worker_name=worker_name,
        )

    # ------------------------------------------------------------------
    # QueueEngineAdapter - capabilities
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        return set(DEFAULT_CAPABILITIES)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _safe_str_attr(obj: Any, name: str, default: str) -> str:
    """Read an attribute as a string without ever raising.

    Celery result-backend proxies can raise on attribute access if
    the backend connection drops mid-call - we tolerate that by
    returning ``default`` instead of propagating.
    """
    try:
        v = getattr(obj, name, None)
        return str(v) if v is not None else default
    except Exception:  # noqa: BLE001
        return default


def _method_is_async(obj: Any, name: str) -> bool:
    """Utility used by tests to assert adapter methods are async.

    Accepts both ``async def`` coroutines AND ``async def ... yield``
    async-generator functions - the QueueEngineAdapter Protocol
    declares ``subscribe_events`` and ``subscribe_registry_changes``
    as async generators, so a strict ``iscoroutinefunction`` check
    misclassifies them as sync.
    """
    method = getattr(obj, name, None)
    return inspect.iscoroutinefunction(method) or inspect.isasyncgenfunction(
        method,
    )


__all__ = ["CeleryEngineAdapter"]
