"""Unit tests for ``z4j_celery.actions.*`` using the FakeCeleryApp."""

from __future__ import annotations

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
            fake_app,
            task_id="orig-past",
            eta=1_700_000_000.0,
        )
        assert result.status == "failed"
        assert "eta" in (result.error or "")

    async def test_retry_fails_without_task_name(self, fake_app) -> None:
        # No result registered → FakeAsyncResult.name is None
        result = await retry_task_action(fake_app, task_id="orphan")
        assert result.status == "failed"
        assert "task name" in (result.error or "")

    async def test_retry_fails_closed_without_stored_args_h3(self, fake_app) -> None:
        # H3/M7: with result_extended OFF the result backend stores no
        # arguments (AsyncResult.args is None). With no operator override
        # and no failed-job registry to requeue by reference, retry must
        # FAIL CLOSED, not send_task with an empty payload.
        class _NoArgsResult:
            def __init__(self, tid: str) -> None:
                self.id = tid
                self.name = "myapp.tasks.work"
                self.args = None  # result_extended off -> not stored
                self.kwargs = None

        fake_app.AsyncResult = _NoArgsResult  # type: ignore[method-assign]
        before = len(fake_app.sent_tasks)
        result = await retry_task_action(
            fake_app,
            task_id="t-noargs",
            task_name="myapp.tasks.work",
        )
        assert result.status == "failed"
        assert "result_extended" in (result.error or "")
        # Nothing was enqueued (no empty-args send).
        assert len(fake_app.sent_tasks) == before

    async def test_retry_with_overrides_succeeds_even_without_stored_args_h3(
        self,
        fake_app,
    ) -> None:
        # The operator "retry with different inputs" path stays functional
        # even when the backend stored nothing: overrides are authoritative.
        class _NoArgsResult:
            def __init__(self, tid: str) -> None:
                self.id = tid
                self.name = "myapp.tasks.work"
                self.args = None
                self.kwargs = None

        fake_app.AsyncResult = _NoArgsResult  # type: ignore[method-assign]
        result = await retry_task_action(
            fake_app,
            task_id="t-noargs",
            task_name="myapp.tasks.work",
            override_args=("explicit",),
            override_kwargs={"k": 1},
        )
        assert result.status == "success"
        assert fake_app.sent_tasks[-1]["args"] == ["explicit"]

    async def test_retry_fails_closed_on_partial_override_h3(self, fake_app) -> None:
        # H3: with result_extended OFF and only ONE override half supplied,
        # the OTHER half is unresolvable. Retry must fail closed rather than
        # silently emptying it (was: args-only -> (("x",), {}) erasing the
        # original kwargs; kwargs-only -> ((), {...}) erasing the args).
        class _NoArgsResult:
            def __init__(self, tid: str) -> None:
                self.id = tid
                self.name = "myapp.tasks.work"
                self.args = None
                self.kwargs = None

        fake_app.AsyncResult = _NoArgsResult  # type: ignore[method-assign]
        before = len(fake_app.sent_tasks)
        r_args_only = await retry_task_action(
            fake_app,
            task_id="t-args-only",
            task_name="myapp.tasks.work",
            override_args=("x",),
        )
        assert r_args_only.status == "failed"
        r_kwargs_only = await retry_task_action(
            fake_app,
            task_id="t-kwargs-only",
            task_name="myapp.tasks.work",
            override_kwargs={"k": 1},
        )
        assert r_kwargs_only.status == "failed"
        # Neither partial override enqueued anything.
        assert len(fake_app.sent_tasks) == before

    async def test_retry_propagates_send_task_error(self, fake_app) -> None:
        fake_app.register_result("orig-4", name="myapp.tasks.oops")

        def boom(*_, **__):
            raise RuntimeError("broker down")

        fake_app.send_task = boom  # type: ignore[method-assign]
        result = await retry_task_action(fake_app, task_id="orig-4")
        assert result.status == "failed"
        assert "broker down" in (result.error or "")

    async def test_named_priority_uses_amqp_higher_first_order(self, fake_app) -> None:
        fake_app.broker_driver_type = "amqp"
        fake_app.register_result("amqp-critical", name="myapp.tasks.work")
        fake_app.register_result("amqp-low", name="myapp.tasks.work")

        await retry_task_action(fake_app, task_id="amqp-critical", priority="critical")
        await retry_task_action(fake_app, task_id="amqp-low", priority="low")

        assert [item["priority"] for item in fake_app.sent_tasks] == [9, 0]

    async def test_named_priority_uses_redis_ascending_bucket_order(self, fake_app) -> None:
        fake_app.broker_driver_type = "redis"
        fake_app.register_result("redis-critical", name="myapp.tasks.work")
        fake_app.register_result("redis-low", name="myapp.tasks.work")

        await retry_task_action(fake_app, task_id="redis-critical", priority="critical")
        await retry_task_action(fake_app, task_id="redis-low", priority="low")

        assert [item["priority"] for item in fake_app.sent_tasks] == [0, 9]

    async def test_raw_integer_priority_is_preserved_on_redis(self, fake_app) -> None:
        fake_app.broker_driver_type = "redis"
        fake_app.register_result("redis-raw", name="myapp.tasks.work")

        await retry_task_action(fake_app, task_id="redis-raw", priority=7)

        assert fake_app.sent_tasks[-1]["priority"] == 7

    def test_mapping_matches_kombu_redis_consumption_order(self) -> None:
        import inspect

        from kombu.transport.redis import Channel
        from z4j_celery.actions.retry import _coerce_priority

        # Kombu's Redis Channel checks these buckets in list order. This guard
        # bites if upstream changes that ordering and our named mapping needs
        # to move with it.
        assert list(Channel.priority_steps) == [0, 3, 6, 9]
        assert "for pri in self.priority_steps" in inspect.getsource(Channel._get)
        assert _coerce_priority("critical", driver_type="redis") == Channel.priority_steps[0]
        assert _coerce_priority("low", driver_type="redis") == Channel.priority_steps[-1]


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
        fake_app._channel.purge_returns = {"emails": 42}
        result = await purge_queue_action(
            fake_app,
            queue_name="emails",
            force=True,
        )
        assert result.status == "success"
        assert result.result is not None
        assert result.result["queue"] == "emails"
        assert result.result["purged"] == 42

    async def test_purge_without_confirm_token_is_rejected(self, fake_app) -> None:
        fake_app._channel.purge_returns = {"emails": 42}
        result = await purge_queue_action(fake_app, queue_name="emails")
        assert result.status == "failed"
        assert "confirm_token" in (result.error or "")

    async def test_purge_handles_exception(self, fake_app) -> None:
        def boom(*_, **__):
            raise RuntimeError("broker down")

        fake_app._channel.queue_purge = boom  # type: ignore[method-assign]
        result = await purge_queue_action(
            fake_app,
            queue_name="emails",
            force=True,
        )
        assert result.status == "failed"


class TestResolveAgentSecret:
    """M-7: the purge action keys the confirm token on the agent's
    Z4J_HMAC_SECRET, decoded the same way frame signing decodes it."""

    def test_decodes_env_secret(self, monkeypatch) -> None:
        import base64

        from z4j_celery.actions.purge import _resolve_agent_secret

        raw = b"k" * 32
        monkeypatch.setenv(
            "Z4J_HMAC_SECRET",
            base64.urlsafe_b64encode(raw).decode("ascii"),
        )
        assert _resolve_agent_secret() == raw

    def test_absent_secret_is_none(self, monkeypatch) -> None:
        from z4j_celery.actions.purge import _resolve_agent_secret

        monkeypatch.delenv("Z4J_HMAC_SECRET", raising=False)
        assert _resolve_agent_secret() is None

    def test_undecodable_secret_is_none(self, monkeypatch) -> None:
        from z4j_celery.actions.purge import _resolve_agent_secret

        monkeypatch.setenv("Z4J_HMAC_SECRET", "not!valid!base64!")
        assert _resolve_agent_secret() is None


class TestDeadLetterAction:
    async def test_dlq_fails_closed_without_publishing(self, fake_app) -> None:
        before = list(fake_app.sent_tasks)
        result = await requeue_dead_letter_action(fake_app, task_id="orig")
        assert result.status == "failed"
        assert "cannot safely requeue" in result.error
        assert fake_app.sent_tasks == before


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
