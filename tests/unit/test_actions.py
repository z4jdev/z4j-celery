"""Unit tests for ``z4j_celery.actions.*`` using the FakeCeleryApp."""

from __future__ import annotations

import pytest

from z4j_celery.actions import (
    bulk_retry_action,
    cancel_task_action,
    purge_queue_action,
    requeue_dead_letter_action,
    restart_worker_action,
    retry_task_action,
)


class TestRetryAction:
    async def test_retry_uses_original_args_from_result_backend(self, fake_app) -> None:
        fake_app.register_result(
            "orig-1",
            name="myapp.tasks.send_email",
            args=("user42",),
            kwargs={"template": "welcome"},
        )
        result = await retry_task_action(fake_app, task_id="orig-1")
        assert result.status == "success"
        assert result.result is not None
        assert "new_task_id" in result.result
        assert result.result["task_name"] == "myapp.tasks.send_email"
        assert fake_app.sent_tasks[0]["args"] == ["user42"]
        assert fake_app.sent_tasks[0]["kwargs"] == {"template": "welcome"}

    async def test_retry_with_overrides(self, fake_app) -> None:
        fake_app.register_result(
            "orig-2",
            name="myapp.tasks.ship",
            args=("abc",),
            kwargs={"priority": 1},
        )
        result = await retry_task_action(
            fake_app,
            task_id="orig-2",
            override_args=("xyz",),
            override_kwargs={"priority": 9},
        )
        assert result.status == "success"
        assert fake_app.sent_tasks[0]["args"] == ["xyz"]
        assert fake_app.sent_tasks[0]["kwargs"] == {"priority": 9}

    async def test_retry_with_eta(self, fake_app) -> None:
        import time

        fake_app.register_result("orig-3", name="myapp.tasks.do_it")
        # Eta must land inside the audit-H14 window [-60s, +365d] -
        # a naked 2023 timestamp now gets rejected before send_task.
        eta_in_window = time.time() + 60.0
        await retry_task_action(fake_app, task_id="orig-3", eta=eta_in_window)
        assert fake_app.sent_tasks[0]["eta"] is not None

    async def test_retry_rejects_eta_too_far_in_past(self, fake_app) -> None:
        fake_app.register_result("orig-past", name="myapp.tasks.do_it")
        result = await retry_task_action(
            fake_app, task_id="orig-past", eta=1_700_000_000.0,
        )
        assert result.status == "failed"
        assert "eta" in (result.error or "")

    async def test_retry_fails_without_task_name(self, fake_app) -> None:
        # No result registered → FakeAsyncResult.name is None
        result = await retry_task_action(fake_app, task_id="orphan")
        assert result.status == "failed"
        assert "task name" in (result.error or "")

    async def test_retry_propagates_send_task_error(self, fake_app) -> None:
        fake_app.register_result("orig-4", name="myapp.tasks.oops")

        def boom(*_, **__):
            raise RuntimeError("broker down")

        fake_app.send_task = boom  # type: ignore[method-assign]
        result = await retry_task_action(fake_app, task_id="orig-4")
        assert result.status == "failed"
        assert "broker down" in (result.error or "")


class TestBulkRetryAction:
    async def test_requires_task_ids_in_v1(self, fake_app) -> None:
        result = await bulk_retry_action(fake_app, filter={"state": "failure"})
        assert result.status == "failed"
        assert "task_ids" in (result.error or "")

    async def test_bulk_retry_happy_path(self, fake_app) -> None:
        for i in range(3):
            fake_app.register_result(f"t-{i}", name=f"myapp.tasks.task_{i}")
        result = await bulk_retry_action(
            fake_app,
            filter={"state": "failure"},
            task_ids=["t-0", "t-1", "t-2"],
        )
        assert result.status == "success"
        assert result.result is not None
        assert result.result["requested"] == 3
        assert result.result["succeeded"] == 3
        assert result.result["failed"] == 0
        assert len(result.result["new_task_ids"]) == 3

    async def test_bulk_retry_applies_max_cap(self, fake_app) -> None:
        for i in range(10):
            fake_app.register_result(f"t-{i}", name=f"myapp.tasks.task_{i}")
        result = await bulk_retry_action(
            fake_app,
            filter={},
            task_ids=[f"t-{i}" for i in range(10)],
            max=3,
        )
        assert result.status == "success"
        assert result.result is not None
        assert result.result["requested"] == 3


class TestCancelAction:
    async def test_cancel_calls_revoke(self, fake_app) -> None:
        result = await cancel_task_action(fake_app, task_id="abc")
        assert result.status == "success"
        assert fake_app.control.revoked == [("abc", True, "SIGTERM")]

    async def test_cancel_returns_failed_on_exception(self, fake_app) -> None:
        def boom(*_, **__):
            raise RuntimeError("can't")
        fake_app.control.revoke = boom  # type: ignore[method-assign]
        result = await cancel_task_action(fake_app, task_id="abc")
        assert result.status == "failed"


class TestPurgeAction:
    async def test_purge_happy_path(self, fake_app) -> None:
        # Audit H13: purge_queue now requires ``force=True`` or a
        # depth-bound confirm_token. The fake broker has no
        # ``queue_declare`` hook so depth comes back as None and the
        # only way through for a happy-path test is force.
        fake_app._channel.purge_returns = {"emails": 42}  # noqa: SLF001
        result = await purge_queue_action(
            fake_app, queue_name="emails", force=True,
        )
        assert result.status == "success"
        assert result.result is not None
        assert result.result["queue"] == "emails"
        assert result.result["purged"] == 42

    async def test_purge_without_confirm_token_is_rejected(self, fake_app) -> None:
        fake_app._channel.purge_returns = {"emails": 42}  # noqa: SLF001
        result = await purge_queue_action(fake_app, queue_name="emails")
        assert result.status == "failed"
        assert "confirm_token" in (result.error or "")

    async def test_purge_handles_exception(self, fake_app) -> None:
        def boom(*_, **__):
            raise RuntimeError("broker down")
        fake_app._channel.queue_purge = boom  # type: ignore[method-assign]  # noqa: SLF001
        result = await purge_queue_action(
            fake_app, queue_name="emails", force=True,
        )
        assert result.status == "failed"


class TestDeadLetterAction:
    async def test_dlq_delegates_to_retry(self, fake_app) -> None:
        fake_app.register_result("orig", name="myapp.tasks.failing")
        result = await requeue_dead_letter_action(fake_app, task_id="orig")
        assert result.status == "success"
        assert result.result is not None
        assert result.result["source"] == "dlq"
        assert "new_task_id" in result.result


class TestRestartWorkerAction:
    async def test_restart_broadcasts_pool_restart(self, fake_app) -> None:
        result = await restart_worker_action(fake_app, worker_name="celery@w1")
        assert result.status == "success"
        assert fake_app.control.broadcasts[0][0] == "pool_restart"
        assert fake_app.control.broadcasts[0][1] == ["celery@w1"]

    async def test_restart_failure(self, fake_app) -> None:
        def boom(*_, **__):
            raise RuntimeError("network")
        fake_app.control.broadcast = boom  # type: ignore[method-assign]
        result = await restart_worker_action(fake_app, worker_name="celery@w1")
        assert result.status == "failed"
