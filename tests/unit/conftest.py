"""Shared fixtures for z4j-celery unit tests.

The fakes here stand in for real Celery objects so the z4j-celery
tests can run without Celery installed. When integration tests land
in a later turn, they use a real Celery app via ``pytest-celery``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest


class FakeAsyncResult:
    """Minimal stand-in for ``celery.result.AsyncResult``."""

    def __init__(
        self,
        *,
        task_id: str,
        name: str | None = None,
        args: tuple[Any, ...] | list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.id = task_id
        self.name = name
        self.args = list(args or [])
        self.kwargs = dict(kwargs or {})
        self.info: dict[str, Any] = {}


@dataclass
class FakeControl:
    """Stand-in for ``celery_app.control``."""

    revoked: list[tuple[str, bool, str]] = field(default_factory=list)
    broadcasts: list[tuple[str, list[str], dict[str, Any]]] = field(default_factory=list)

    def revoke(self, task_id: str, *, terminate: bool = False, signal: str = "SIGTERM") -> None:
        self.revoked.append((task_id, terminate, signal))

    def broadcast(
        self,
        name: str,
        *,
        destination: list[str] | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        self.broadcasts.append((name, list(destination or []), dict(arguments or {})))


class FakeChannel:
    def __init__(self) -> None:
        self.purged_queues: list[str] = []
        self.purge_returns: dict[str, int] = {}

    def queue_purge(self, queue_name: str) -> int:
        self.purged_queues.append(queue_name)
        return self.purge_returns.get(queue_name, 0)

    def queue_delete(self, queue_name: str) -> None:
        self.purged_queues.append(queue_name)


class FakeConnection:
    """Supports ``with celery_app.connection_for_write() as conn:``."""

    def __init__(self, channel: FakeChannel) -> None:
        self.default_channel = channel

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


@dataclass
class FakeTask:
    """Stand-in for a Celery Task object."""

    name: str
    queue: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        def _fn() -> None:
            """Placeholder body."""

        _fn.__module__ = "myapp.tasks"
        self.run = _fn


class FakeCeleryApp:
    """Minimal Celery app for unit tests.

    Duck-types everything the adapter and action helpers need:

    - ``.tasks`` - dict of task name to task object
    - ``.control`` - FakeControl
    - ``.connection_for_write()`` - returns FakeConnection
    - ``.AsyncResult(task_id)`` - returns FakeAsyncResult
    - ``.send_task(name, args, kwargs, eta)`` - records the call
    """

    def __init__(self) -> None:
        self.tasks: dict[str, Any] = {}
        self.control = FakeControl()
        self._channel = FakeChannel()
        self._async_results: dict[str, FakeAsyncResult] = {}
        self.sent_tasks: list[dict[str, Any]] = []
        self._next_id = 0

    def connection_for_write(self) -> FakeConnection:
        return FakeConnection(self._channel)

    def AsyncResult(self, task_id: str) -> FakeAsyncResult:  # noqa: N802
        if task_id in self._async_results:
            return self._async_results[task_id]
        return FakeAsyncResult(task_id=task_id)

    def register_result(
        self,
        task_id: str,
        *,
        name: str,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._async_results[task_id] = FakeAsyncResult(
            task_id=task_id,
            name=name,
            args=args,
            kwargs=kwargs,
        )

    def send_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
        eta: Any = None,
    ) -> FakeAsyncResult:
        self._next_id += 1
        new_id = f"new-task-{self._next_id}"
        self.sent_tasks.append(
            {"name": name, "args": list(args), "kwargs": dict(kwargs or {}), "eta": eta},
        )
        return FakeAsyncResult(task_id=new_id, name=name, args=args, kwargs=kwargs)

    def register_task(self, task: FakeTask) -> None:
        self.tasks[task.name] = task


@pytest.fixture
def fake_app() -> FakeCeleryApp:
    return FakeCeleryApp()
