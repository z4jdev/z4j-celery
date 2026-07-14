"""Celery broker events monitor - prefork-safe alternative to signals.

In ``--pool=prefork`` (Celery's default), task lifecycle signals
fire in the forked worker child process. The z4j agent runtime
(including the WebSocket transport) lives in the main process.
Signals in the child can't reach the transport in the parent -
events are silently lost.

The fix: use Celery's **events system** instead of signals. When
``worker_send_task_events = True``, the worker publishes lifecycle
events to the broker. This monitor opens a **dedicated consumer
connection** in the main process and receives those events via
``EventReceiver.itercapture()``. The events arrive in the same
process as the z4j runtime, so ``call_soon_threadsafe`` works.

This is the same approach Flower uses. It works with every pool
type (prefork, gevent, eventlet, solo) and does not require
``-E`` on the CLI - the adapter enables it programmatically.

Thread model: ``itercapture()`` blocks, so we run it in a
dedicated daemon thread. Each event is mapped to a z4j ``Event``
via the same mapper functions the signal-based path uses, then
enqueued onto the engine's async queue via ``call_soon_threadsafe``.
"""

from __future__ import annotations

import ast
import contextlib
import logging
import threading
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from z4j_bare.safety import safe_boundary
from z4j_core.models import Event
from z4j_core.redaction.engine import RedactionEngine

from z4j_celery.events.mapper import (
    build_task_failed_event,
    build_task_received_event,
    build_task_retried_event,
    build_task_revoked_event,
    build_task_started_event,
    build_task_succeeded_event,
)

logger = logging.getLogger("z4j.adapter.celery.broker_events")


EventSink = Callable[[Event], None]


class CeleryBrokerEventsMonitor:
    """Receives task lifecycle events from the Celery broker.

    Designed to run in the z4j agent's main process alongside the
    WebSocket transport. The sink callback bridges events into the
    engine's asyncio queue via ``call_soon_threadsafe``.
    """

    def __init__(
        self,
        *,
        celery_app: Any,
        sink: EventSink,
        redaction: RedactionEngine,
        hostname_filter: Callable[[str], bool] | None = None,
    ) -> None:
        self._app = celery_app
        self._sink = sink
        self._redaction = redaction
        # Per-event hostname filter. When set, the monitor skips
        # events whose ``hostname`` field does not match the
        # predicate. Used to bound the celery-events fanout
        # amplification when multiple z4j agents share a broker
        # (each agent's monitor otherwise receives EVERY task event
        # from EVERY worker because the celery events exchange is
        # fanout, so N agents on one broker = N times event volume =
        # N times brain ingest contention, which can drive heavy
        # delivery loss via deadlock storms).
        #
        # When ``None`` (default) the monitor accepts every event,
        # preserving the "see all cluster activity" semantics for
        # single-agent deployments. The agent runtime wires the
        # filter to the LOCAL celery worker's hostname when
        # auto-detection succeeds (see engine.py).
        self._hostname_filter = hostname_filter
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        # Cache uuid -> task name from task-sent/task-received events.
        # task-started/succeeded/failed events don't carry the name
        # field, so we look it up from the cache. OrderedDict gives
        # LRU eviction (move_to_end on access, popitem(last=False)
        # to evict oldest).
        self._name_cache: OrderedDict[str, str] = OrderedDict()
        self._NAME_CACHE_MAX = 50_000
        # Poison-message
        # drop counter. Operators can probe this via a future
        # observability endpoint to detect malformed events that
        # are silently being dropped from the broker stream.
        self._poison_drop_count: int = 0

    @property
    def poison_drop_count(self) -> int:
        """Number of poisoned messages dropped since process start.

        Operators read this via the agent's debug endpoints /
        metrics.
        """
        return self._poison_drop_count

    def start(self) -> None:
        """Enable task events on the worker + start the receiver thread."""
        if self._thread is not None:
            return

        # Enable task event publishing programmatically so operators
        # don't have to pass ``-E`` on the CLI. Setting it on conf
        # alone isn't enough for prefork pools (children are already
        # forked). Use control.enable_events() to broadcast the
        # change to all running worker processes.
        self._app.conf.worker_send_task_events = True
        self._app.conf.task_send_sent_event = True
        try:
            self._app.control.enable_events()
        except Exception:
            logger.debug("z4j broker events: enable_events broadcast failed (non-fatal)")

        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="z4j-celery-broker-events",
        )
        self._thread.start()
        logger.info("z4j celery broker events monitor started")

    def stop(self) -> None:
        """Stop the receiver thread. Idempotent."""
        if self._thread is None:
            return
        self._stop.set()
        self._thread.join(timeout=5)
        self._thread = None
        logger.info("z4j celery broker events monitor stopped")

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Event receiver loop - runs in a dedicated thread.

        Uses a raw Kombu consumer with our own dispatch callback
        instead of relying on ``EventReceiver.capture()`` /
        ``itercapture()``. The Receiver's built-in dispatch path
        proved unreliable for ``task-*`` events on Redis transport
        (see comment block in handler dispatch below).
        """
        # Mapping of Celery event type -> our handler. ``task-sent``
        # is treated as ``received`` because Celery emits it from
        # the producer side - we want to record the lifecycle the
        # moment the task hits the broker.
        # Note: ``task-sent`` is intentionally NOT mapped. The Celery
        # ``task-sent`` event encodes args/kwargs differently from the
        # ``task-received`` event (base64/JSON strings vs lists/dicts),
        # which breaks our redaction-aware mapper.
        #
        # ``task-received`` is also NOT mapped here as of 1.5.
        # ``CelerySignalHooks._on_received`` (signals.py) fires
        # synchronously in the parent worker process for every task
        # in every pool (solo, prefork, gevent, eventlet, threads),
        # so the broker-events monitor would emit a redundant
        # TASK_RECEIVED for every task. The duplicate showed up in
        # the 1.5 e2e matrix as F-7. signals own ``task-received``,
        # broker-events owns ``task-started`` / ``task-succeeded``
        # / ``task-failed`` / ``task-retried`` / ``task-revoked``
        # (which fire in CHILD processes in prefork pools and are
        # not visible to signals there).
        type_handlers: dict[str, Callable[[dict[str, Any]], None]] = {
            "task-started": self._on_started,
            "task-succeeded": self._on_succeeded,
            "task-failed": self._on_failed,
            "task-retried": self._on_retried,
            "task-revoked": self._on_revoked,
        }

        def _dispatch(body: Any, message: Any) -> None:
            """Kombu consumer callback - dispatches to type_handlers.

            Celery 4.0+ may batch multiple events into one message
            with a list body (``task-multi``). Handle both shapes.
            The Receiver's own ``_receive`` does this too; we
            replicate it here so we don't depend on its dispatch.
            """
            # Ack ONLY after the handler succeeded. The
            # prior ``no_ack=True`` consumer dropped any event
            # whose handler raised, so a transient sink failure
            # silently lost a window of task lifecycle events.
            # We now use explicit ack-after-success: at-least-once
            # delivery, with the brain's ``event_id`` UNIQUE
            # constraint dedup'ing any retries.
            # On handler failure, ACK + drop + counter (poison-
            # message dead-letter pattern); do NOT requeue + re-raise.
            # A requeue-then-raise path on a malformed event would
            # cause a tight 2s reconnect loop, pegging CPU on the
            # host whenever a poisoned event hit a handler that the
            # safe_boundary wrapper didn't cover.
            #
            # Brain-side dedup on event_id makes the at-least-once
            # property already weaker than ideal in the presence of
            # safe_boundary swallow on each handler, so requeue-and-
            # raise would only buy us a forever-loop on poisoned
            # data. Instead: ack the message (drops it from the
            # broker), increment a poison counter, log loudly.
            # Operators see "N poisoned events dropped" instead of
            # CPU pegged.
            try:
                events = body if isinstance(body, list) else [body]
                for ev in events:
                    # Hostname filter. ``hostname`` is the celery
                    # worker that emitted the event. When the filter
                    # is active and the event came from a worker we
                    # don't represent, drop it - the agent paired
                    # with that worker will (or already has) emit it.
                    if self._hostname_filter is not None:
                        ev_host = ev.get("hostname", "")
                        if not self._hostname_filter(ev_host):
                            continue
                    handler = type_handlers.get(ev.get("type", ""))
                    if handler is not None:
                        handler(ev)
            except Exception:
                logger.exception(
                    "z4j broker events: handler raised on poisoned "
                    "message; dropping and continuing",
                )
                self._poison_drop_count += 1
                with contextlib.suppress(Exception):
                    message.ack()
                return
            # Ack failure (broker disconnect mid-handler) is tolerated:
            # the broker will re-deliver on reconnect; the brain's
            # event_id dedup absorbs the duplicate.
            with contextlib.suppress(Exception):
                message.ack()

        while not self._stop.is_set():
            try:
                with self._app.connection_for_read() as conn:
                    # Build the Receiver only to get its bound queue
                    # (correct exchange, routing key, queue name).
                    recv = self._app.events.Receiver(conn)
                    consumer = conn.Consumer(
                        queues=[recv.queue],
                        callbacks=[_dispatch],
                        # Was ``no_ack=True`` (lost
                        # events on any handler exception). Now
                        # ``no_ack=False`` so the broker holds the
                        # message until ``_dispatch`` ack's it
                        # explicitly. Brain dedup on ``event_id``
                        # absorbs any redeliveries.
                        no_ack=False,
                    )
                    logger.info(
                        "z4j broker events monitor connected to broker",
                    )
                    with consumer:
                        while not self._stop.is_set():
                            try:
                                conn.drain_events(timeout=1.0)
                            except TimeoutError:
                                # No events in 1s - loop back so we
                                # can re-check the stop flag. The
                                # consumer stays alive across these
                                # timeouts (no reconnect storm).
                                continue
                            except OSError:
                                # Connection broke - exit inner loop
                                # so the outer ``while`` rebuilds it.
                                break
            except Exception:
                if self._stop.is_set():
                    return
                logger.exception(
                    "z4j broker events monitor: receiver error, reconnecting",
                )
                self._stop.wait(timeout=2.0)

    # ------------------------------------------------------------------
    # Event handlers - each wrapped in safe_boundary
    # ------------------------------------------------------------------

    def _cache_name(self, event: dict[str, Any]) -> None:
        """Cache uuid -> task name from events that carry it (LRU)."""
        uuid = event.get("uuid")
        name = event.get("name")
        if uuid and name:
            # Move to end if already present (LRU touch).
            if uuid in self._name_cache:
                self._name_cache.move_to_end(uuid)
            self._name_cache[uuid] = name
            # Evict oldest entries when over capacity.
            while len(self._name_cache) > self._NAME_CACHE_MAX:
                self._name_cache.popitem(last=False)

    def _resolve_name(self, event: dict[str, Any]) -> str | None:
        """Get task name from event, falling back to LRU cache."""
        name = event.get("name")
        if name:
            return name
        uuid = event.get("uuid", "")
        cached = self._name_cache.get(uuid)
        if cached is not None:
            # Touch for LRU.
            self._name_cache.move_to_end(uuid)
        return cached

    @safe_boundary
    def _on_received(self, event: dict[str, Any]) -> None:
        self._cache_name(event)
        task_id = event.get("uuid", "")
        # Celery's broker transport (kombu) emits ``args`` and
        # ``kwargs`` on ``task-received`` as the **repr'd strings**
        # ``str(task.args)`` / ``str(task.kwargs)`` - not as native
        # Python objects. The signal-based path delivers real
        # tuples / dicts, so only the broker path needs coercion.
        # We parse with ``ast.literal_eval`` (literals only - no
        # eval, no name lookup, attacker-controlled task payloads
        # cannot execute code). Anything that doesn't parse to the
        # expected shape becomes ``None`` so the redaction layer
        # treats it as "no payload" instead of crashing on
        # ``dict("{'k': 'v'}")``.
        ev = build_task_received_event(
            redaction=self._redaction,
            task_id=task_id,
            task=self._resolve_name(event),
            args=_coerce_args(event.get("args")),
            kwargs=_coerce_kwargs(event.get("kwargs")),
            queue=event.get("queue"),
        )
        self._sink(ev)

    @safe_boundary
    def _on_started(self, event: dict[str, Any]) -> None:
        self._cache_name(event)
        task_id = event.get("uuid", "")
        ev = build_task_started_event(
            redaction=self._redaction,
            task_id=task_id,
            task=self._resolve_name(event),
            worker=event.get("hostname"),
        )
        self._sink(ev)

    @safe_boundary
    def _on_succeeded(self, event: dict[str, Any]) -> None:
        task_id = event.get("uuid", "")
        runtime_raw = event.get("runtime")
        runtime_ms = int(float(runtime_raw) * 1000) if runtime_raw is not None else None
        ev = build_task_succeeded_event(
            redaction=self._redaction,
            task_id=task_id,
            task=self._resolve_name(event),
            result=event.get("result"),
            runtime_ms=runtime_ms,
            worker=event.get("hostname"),
        )
        self._sink(ev)
        self._name_cache.pop(task_id, None)

    @safe_boundary
    def _on_failed(self, event: dict[str, Any]) -> None:
        task_id = event.get("uuid", "")
        exc_str = event.get("exception", "Unknown error")
        ev = build_task_failed_event(
            redaction=self._redaction,
            task_id=task_id,
            task=self._resolve_name(event),
            exception=RuntimeError(exc_str),
            traceback=event.get("traceback"),
            worker=event.get("hostname"),
        )
        self._sink(ev)
        self._name_cache.pop(task_id, None)

    @safe_boundary
    def _on_retried(self, event: dict[str, Any]) -> None:
        task_id = event.get("uuid", "")
        ev = build_task_retried_event(
            redaction=self._redaction,
            task_id=task_id,
            task=self._resolve_name(event),
            reason=event.get("exception"),
            worker=event.get("hostname"),
        )
        self._sink(ev)

    @safe_boundary
    def _on_revoked(self, event: dict[str, Any]) -> None:
        task_id = event.get("uuid", "")
        ev = build_task_revoked_event(
            task_id=task_id,
            task=self._resolve_name(event),
            terminated=event.get("terminated", False),
            signum=event.get("signum"),
            expired=event.get("expired", False),
            worker=event.get("hostname"),
        )
        self._sink(ev)
        self._name_cache.pop(task_id, None)


def _safe_literal_eval(value: str) -> Any | None:
    """Parse a Python-literal string. Return ``None`` on any failure.

    ``ast.literal_eval`` itself only handles literals (no eval, no
    attribute lookup, no function calls), so it is safe to call on
    attacker-controlled task payloads. We catch its narrow set of
    expected exceptions (``ValueError``, ``SyntaxError``,
    ``MemoryError``, ``TypeError``, ``RecursionError``) and turn
    them into ``None`` so the broker monitor never crashes on
    malformed input - the worst case is a missing payload, which
    redaction already handles.
    """
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError, MemoryError, TypeError, RecursionError):
        return None


def _coerce_args(value: Any) -> list[Any] | None:
    """Coerce a broker ``args`` payload into a list, or ``None``.

    Accepts:
      * ``None`` / ``""`` → ``None``
      * ``list`` / ``tuple`` → list
      * Python-literal string (``"['a', 1]"``) → list, else ``None``
      * Anything else → ``None`` + warn-once
    """
    if value is None or value == "":
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        parsed = _safe_literal_eval(value)
        if isinstance(parsed, (list, tuple)):
            return list(parsed)
        return None
    logger.warning(
        "z4j broker: dropping unexpected args type %s",
        type(value).__name__,
    )
    return None


def _coerce_kwargs(value: Any) -> dict[str, Any] | None:
    """Coerce a broker ``kwargs`` payload into a dict, or ``None``.

    Accepts:
      * ``None`` / ``""`` → ``None`` (no payload at all)
      * ``dict`` (incl. ``{}``) → pass through
      * Python-literal string (``"{'k': 'v'}"`` / ``"{}"``) → dict
      * Anything else (incl. malformed strings) → ``None`` + warn-once

    ``{}`` and ``"{}"`` both pass through as ``{}`` rather than
    collapsing to ``None`` so the downstream event accurately
    distinguishes "task was called with no kwargs" from "broker
    sent no kwargs field at all".
    """
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = _safe_literal_eval(value)
        if isinstance(parsed, dict):
            return parsed
        return None
    logger.warning(
        "z4j broker: dropping unexpected kwargs type %s",
        type(value).__name__,
    )
    return None


__all__ = ["CeleryBrokerEventsMonitor"]
