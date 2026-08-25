"""Celery signal handlers.

Connects ``celery.signals`` to the z4j agent's event pipeline. Every
handler is wrapped in :func:`z4j_bare.safety.safe_boundary` so a
regular exception inside z4j is logged and suppressed. Process-lifecycle
exceptions still propagate by design.

Signals hooked (v1):

- ``task_received`` - worker picked up a task
- ``task_prerun`` - task is about to execute
- ``task_postrun`` - task finished (success or failure - Celery dispatches this regardless)
- ``task_success`` - explicit success signal
- ``task_failure`` - task raised
- ``task_retry`` - task scheduled for retry
- ``task_revoked`` - task cancelled or expired
- ``worker_ready`` - worker came online (for worker-state tracking)
- ``worker_shutdown`` - worker going offline

Signal handlers build and redact each event synchronously, then hand it to the
engine's bounded queue. Queue draining and network delivery happen in the
agent runtime's background thread; queue pressure or a mapper/sink failure can
drop an event.
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger("z4j.adapter.celery.signals")


EventSink = Callable[[Event], None]
"""Type of the callback the signal hooks call when they produce an event.

In production this is ``AgentRuntime.record_event``. In tests it is a
list append.
"""


class CelerySignalHooks:
    """Owns the set of Celery signal handlers for one agent.

    Construct once per :class:`z4j_celery.engine.CeleryEngineAdapter`
    and call :meth:`connect` to subscribe. :meth:`disconnect` tears
    the subscriptions down - used during runtime shutdown or in
    tests.

    We defer the ``celery.signals`` import until :meth:`connect` so
    that importing ``z4j_celery`` does not force Celery to be
    installed. This matters for unit tests of the mapper that mock
    out Celery entirely.

    Attributes:
        sink: Function to call with every :class:`Event` produced.
        redaction: Redaction engine used by the mapper.
        sender: Optional Celery ``Task`` sender filter. If ``None``,
                the hooks match every task.
    """

    def __init__(
        self,
        *,
        sink: EventSink,
        redaction: RedactionEngine,
        sender: Any = None,
    ) -> None:
        self.sink = sink
        self.redaction = redaction
        self.sender = sender
        self._connected = False
        self._handlers: list[tuple[Any, Callable[..., Any]]] = []

    def connect(self) -> None:
        """Subscribe every handler to its corresponding Celery signal."""
        if self._connected:
            return

        from celery import signals  # local import; see class docstring

        self._subscribe(signals.task_received, self._on_received)
        self._subscribe(signals.task_prerun, self._on_prerun)
        # task_postrun is dispatched by Celery for both success and
        # failure outcomes, AND it always carries the task_id (which
        # task_success does not). We use the ``state`` kwarg to tell
        # success from failure inside the handler.
        self._subscribe(signals.task_postrun, self._on_postrun)
        self._subscribe(signals.task_failure, self._on_failure)
        self._subscribe(signals.task_retry, self._on_retry)
        self._subscribe(signals.task_revoked, self._on_revoked)

        self._connected = True
        logger.info("z4j celery signal hooks connected")

    def disconnect(self) -> None:
        """Unsubscribe all handlers from their signals."""
        if not self._connected:
            return
        for signal, handler in self._handlers:
            try:
                signal.disconnect(handler)
            except Exception:
                logger.exception("error disconnecting celery signal handler")
        self._handlers.clear()
        self._connected = False
        logger.info("z4j celery signal hooks disconnected")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _subscribe(self, signal: Any, handler: Callable[..., Any]) -> None:
        signal.connect(handler, sender=self.sender, weak=False)
        self._handlers.append((signal, handler))

    # ------------------------------------------------------------------
    # Handlers - every one wrapped in safe_boundary
    # ------------------------------------------------------------------

    @safe_boundary
    def _on_received(
        self,
        sender: Any = None,
        request: Any = None,
        **_: Any,
    ) -> None:
        if request is None:
            return
        task_id = _get(request, "id", _get(request, "task_id", ""))
        task = _get(request, "task", None)
        args = _get(request, "args", None)
        kwargs = _get(request, "kwargs", None)
        queue = (
            _get(request, "delivery_info", {}).get("routing_key")
            if isinstance(
                _get(request, "delivery_info", {}),
                dict,
            )
            else None
        )
        # Celery's request carries the canvas-graph linkage on
        # every spawned task: ``parent_id`` is the task that
        # called ``apply_async`` for me; ``root_id`` is the
        # original entry point of the chain / group / chord.
        # Both are set even on a plain non-canvas task (parent =
        # None, root = self), which keeps the projection uniform.
        parent_id = _get(request, "parent_id", None)
        root_id = _get(request, "root_id", None)
        event = build_task_received_event(
            redaction=self.redaction,
            task_id=str(task_id),
            task=task,
            args=args,
            kwargs=kwargs,
            queue=queue,
            parent_task_id=str(parent_id) if parent_id else None,
            root_task_id=str(root_id) if root_id else None,
        )
        self.sink(event)

    @safe_boundary
    def _on_prerun(
        self,
        sender: Any = None,
        task_id: str | None = None,
        task: Any = None,
        args: Any = None,
        kwargs: dict[str, Any] | None = None,
        **_: Any,
    ) -> None:
        event = build_task_started_event(
            redaction=self.redaction,
            task_id=str(task_id or ""),
            task=task or sender,
            args=args,
            kwargs=kwargs,
        )
        self.sink(event)

    @safe_boundary
    def _on_postrun(
        self,
        sender: Any = None,
        task_id: str | None = None,
        task: Any = None,
        retval: Any = None,
        state: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Handle ``task_postrun``.

        Celery dispatches ``task_postrun`` for every terminal outcome
        (SUCCESS, FAILURE, REVOKED, ...) and *always* includes the
        task_id - unlike ``task_success`` which only fires on success
        and historically did not pass the id reliably across Celery
        versions. ``task_failure`` still produces the failed event
        with the exception detail; we only emit a *succeeded* event
        from here when ``state == "SUCCESS"``.
        """
        if state != "SUCCESS":
            return
        event = build_task_succeeded_event(
            redaction=self.redaction,
            task_id=str(task_id or ""),
            task=task or sender,
            result=retval,
            runtime_ms=None,
        )
        self.sink(event)

    @safe_boundary
    def _on_failure(
        self,
        sender: Any = None,
        task_id: str | None = None,
        exception: BaseException | None = None,
        traceback: Any = None,
        einfo: Any = None,
        **kwargs: Any,
    ) -> None:
        if exception is None:
            return
        tb_str = None
        if traceback is not None:
            tb_str = str(traceback)
        elif einfo is not None:
            tb_str = getattr(einfo, "traceback", None) or str(einfo)
        event = build_task_failed_event(
            redaction=self.redaction,
            task_id=str(task_id or ""),
            task=sender,
            exception=exception,
            traceback=tb_str,
        )
        self.sink(event)

    @safe_boundary
    def _on_retry(
        self,
        sender: Any = None,
        request: Any = None,
        reason: str | None = None,
        einfo: Any = None,
        **kwargs: Any,
    ) -> None:
        # B18: the ``task_retry`` signal carries the id on ``request.id``,
        # NOT in kwargs. Reading kwargs.get("task_id") yielded "" so the
        # TASK_RETRIED event was uncorrelatable on solo/gevent/eventlet
        # pools (prefork is covered by the broker-events path). Mirror the
        # sibling _on_revoked handler.
        task_id = _get(request, "id", _get(request, "task_id", "")) or kwargs.get("task_id") or ""
        event = build_task_retried_event(
            redaction=self.redaction,
            task_id=str(task_id),
            task=sender,
            reason=reason,
            einfo=einfo,
        )
        self.sink(event)

    @safe_boundary
    def _on_revoked(
        self,
        sender: Any = None,
        request: Any = None,
        terminated: bool = False,
        signum: int | None = None,
        expired: bool = False,
        **_: Any,
    ) -> None:
        task_id = _get(request, "id", _get(request, "task_id", ""))
        event = build_task_revoked_event(
            task_id=str(task_id),
            task=sender,
            terminated=terminated,
            signum=signum,
            expired=expired,
        )
        self.sink(event)


def _get(obj: Any, name: str, default: Any) -> Any:
    """``obj.name`` if present, else ``obj[name]`` if present, else default."""
    if obj is None:
        return default
    if hasattr(obj, name):
        return getattr(obj, name, default)
    if isinstance(obj, dict):
        return obj.get(name, default)
    return default


__all__ = ["CelerySignalHooks", "EventSink"]
