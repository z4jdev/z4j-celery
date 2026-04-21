"""Unit tests for ``z4j_celery.engine.CeleryEngineAdapter``.

Exercises the adapter's implementation of the ``QueueEngineAdapter``
Protocol against the FakeCeleryApp. Real-Celery integration tests
live in ``packages/z4j-celery/tests/integration/`` and land in the
next turn alongside the brain backend.
"""

from __future__ import annotations

import inspect

import pytest

from z4j_core.models import DiscoveryHints
from z4j_core.protocols import QueueEngineAdapter

from z4j_celery.capabilities import DEFAULT_CAPABILITIES
from z4j_celery.engine import CeleryEngineAdapter, _method_is_async


@pytest.fixture
def adapter(fake_app) -> CeleryEngineAdapter:
    return CeleryEngineAdapter(celery_app=fake_app)


class TestProtocolConformance:
    def test_satisfies_protocol(self, adapter: CeleryEngineAdapter) -> None:
        assert isinstance(adapter, QueueEngineAdapter)

    def test_name_is_celery(self, adapter: CeleryEngineAdapter) -> None:
        assert adapter.name == "celery"

    def test_protocol_version_is_set(self, adapter: CeleryEngineAdapter) -> None:
        assert adapter.protocol_version

    @pytest.mark.parametrize(
        "method",
        [
            "discover_tasks",
            "subscribe_events",
            "list_queues",
            "list_workers",
            "get_task",
            "retry_task",
            "cancel_task",
            "bulk_retry",
            "purge_queue",
            "requeue_dead_letter",
            "restart_worker",
        ],
    )
    def test_async_methods(
        self, adapter: CeleryEngineAdapter, method: str,
    ) -> None:
        assert _method_is_async(adapter, method)


class TestCapabilities:
    def test_default_capabilities(self, adapter: CeleryEngineAdapter) -> None:
        caps = adapter.capabilities()
        assert caps == set(DEFAULT_CAPABILITIES)

    def test_includes_all_expected_actions(
        self, adapter: CeleryEngineAdapter,
    ) -> None:
        caps = adapter.capabilities()
        assert "retry_task" in caps
        assert "cancel_task" in caps
        assert "bulk_retry" in caps
        assert "purge_queue" in caps
        assert "requeue_dead_letter" in caps
        assert "restart_worker" in caps


class TestDiscovery:
    async def test_discover_tasks_returns_runtime_tasks(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        from tests.unit.conftest import FakeTask  # type: ignore[import-untyped]
        fake_app.register_task(FakeTask(name="myapp.tasks.send_email"))
        fake_app.register_task(FakeTask(name="myapp.tasks.ship", queue="shipping"))

        defs = await adapter.discover_tasks()
        names = {d.name for d in defs}
        assert "myapp.tasks.send_email" in names
        assert "myapp.tasks.ship" in names

    async def test_discover_merges_static_hints(
        self, adapter: CeleryEngineAdapter, fake_app, tmp_path,
    ) -> None:
        from tests.unit.conftest import FakeTask  # type: ignore[import-untyped]

        # Register one runtime task.
        fake_app.register_task(FakeTask(name="myapp.tasks.runtime_only"))

        # Create a static tasks.py with another task.
        app_dir = tmp_path / "staticapp"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        (app_dir / "tasks.py").write_text(
            """
from celery import shared_task

@shared_task
def static_only():
    pass
""",
        )

        hints = DiscoveryHints(app_paths=[app_dir], framework_name="bare")
        defs = await adapter.discover_tasks(hints)
        names = {d.name.rsplit(".", 1)[-1] for d in defs}
        assert "runtime_only" in names
        assert "static_only" in names


class TestActions:
    async def test_retry_task(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        fake_app.register_result("orig", name="myapp.tasks.foo")
        result = await adapter.retry_task("orig")
        assert result.status == "success"

    async def test_cancel_task(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        result = await adapter.cancel_task("abc")
        assert result.status == "success"
        assert fake_app.control.revoked[0][0] == "abc"

    async def test_bulk_retry_extracts_task_ids_from_filter(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        fake_app.register_result("a", name="t.a")
        fake_app.register_result("b", name="t.b")
        result = await adapter.bulk_retry(
            {"task_ids": ["a", "b"]},
            max=10,
        )
        assert result.status == "success"
        assert result.result is not None
        assert result.result["succeeded"] == 2

    async def test_purge_queue(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        # Audit H13: purge_queue now requires either a valid
        # ``confirm_token`` (HMAC of queue_name + current depth) or
        # ``force=True``. The fake broker cannot report depth, so
        # force is the only path available to the engine test.
        result = await adapter.purge_queue("emails", force=True)
        assert result.status == "success"

    async def test_requeue_dead_letter(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        fake_app.register_result("dead", name="t.failing")
        result = await adapter.requeue_dead_letter("dead")
        assert result.status == "success"

    async def test_restart_worker(
        self, adapter: CeleryEngineAdapter, fake_app,
    ) -> None:
        result = await adapter.restart_worker("celery@w1")
        assert result.status == "success"


class TestEventQueue:
    async def test_event_queue_starts_empty(self, adapter: CeleryEngineAdapter) -> None:
        # We don't connect signals here, just assert the queue exists.
        assert adapter._event_queue.qsize() == 0  # noqa: SLF001

    async def test_enqueue_via_helper(self, adapter: CeleryEngineAdapter) -> None:
        from z4j_core.models import Event, EventKind
        from uuid import uuid4
        from datetime import UTC, datetime

        event = Event(
            id=uuid4(),
            project_id=uuid4(),
            agent_id=uuid4(),
            engine="celery",
            task_id="abc",
            kind=EventKind.TASK_STARTED,
            occurred_at=datetime.now(UTC),
            data={},
        )
        adapter._enqueue_event(event)  # noqa: SLF001
        assert adapter._event_queue.qsize() == 1  # noqa: SLF001
