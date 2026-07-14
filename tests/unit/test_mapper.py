"""Unit tests for ``z4j_celery.events.mapper``."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from z4j_celery.events.mapper import (
    build_task_failed_event,
    build_task_received_event,
    build_task_retried_event,
    build_task_revoked_event,
    build_task_started_event,
    build_task_succeeded_event,
)
from z4j_celery.meta import z4j_meta
from z4j_core.models import EventKind
from z4j_core.redaction.engine import RedactionEngine


@pytest.fixture
def redaction() -> RedactionEngine:
    return RedactionEngine()


def _fake_task(name: str, *, meta_decorator=None):
    def body(*_, **__) -> None:
        pass

    if meta_decorator is not None:
        body = meta_decorator(body)

    return SimpleNamespace(name=name, run=body)


class TestBasicBuilders:
    def test_started_event_shape(self, redaction: RedactionEngine) -> None:
        task = _fake_task("myapp.tasks.send_email")
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc123",
            task=task,
            args=["user"],
            kwargs={"template": "welcome"},
            worker="celery@w1",
            queue="emails",
        )
        assert event.kind == EventKind.TASK_STARTED
        assert event.task_id == "abc123"
        assert event.engine == "celery"
        assert event.data["task_name"] == "myapp.tasks.send_email"
        assert event.data["worker"] == "celery@w1"
        assert event.data["queue"] == "emails"

    def test_received_event_shape(self, redaction: RedactionEngine) -> None:
        task = _fake_task("myapp.tasks.send_email")
        event = build_task_received_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            args=[1, 2],
            kwargs={"x": "y"},
        )
        assert event.kind == EventKind.TASK_RECEIVED
        assert event.data["args"] == [1, 2]

    def test_succeeded_event_shape(self, redaction: RedactionEngine) -> None:
        task = _fake_task("myapp.tasks.send_email")
        event = build_task_succeeded_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            result={"sent": True},
            runtime_ms=42,
        )
        assert event.kind == EventKind.TASK_SUCCEEDED
        assert event.data["result"] == {"sent": True}
        assert event.data["runtime_ms"] == 42

    def test_failed_event_shape(self, redaction: RedactionEngine) -> None:
        task = _fake_task("myapp.tasks.send_email")
        event = build_task_failed_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            exception=ValueError("bad input"),
            traceback="Traceback (...): ValueError: bad input\n",
        )
        assert event.kind == EventKind.TASK_FAILED
        assert event.data["exception"] == "ValueError"
        assert "bad input" in event.data["exception_message"]

    def test_retried_event_shape(self, redaction: RedactionEngine) -> None:
        task = _fake_task("myapp.tasks.send_email")
        event = build_task_retried_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            reason="connection refused",
        )
        assert event.kind == EventKind.TASK_RETRIED
        assert event.data["reason"] == "connection refused"

    def test_revoked_event_shape(self) -> None:
        task = _fake_task("myapp.tasks.send_email")
        event = build_task_revoked_event(
            task_id="abc",
            task=task,
            terminated=True,
            signum=15,
        )
        assert event.kind == EventKind.TASK_REVOKED
        assert event.data["terminated"] is True
        assert event.data["signum"] == 15


class TestRedaction:
    def test_kwargs_with_sensitive_keys_are_redacted(
        self,
        redaction: RedactionEngine,
    ) -> None:
        task = _fake_task("myapp.tasks.login")
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            kwargs={"user_id": 42, "password": "hunter2"},
        )
        kwargs = event.data["kwargs"]
        assert kwargs["user_id"] == 42
        assert kwargs["password"] == "[REDACTED]"

    def test_meta_redact_kwargs_forces_redaction(
        self,
        redaction: RedactionEngine,
    ) -> None:
        task = _fake_task(
            "myapp.tasks.charge",
            meta_decorator=z4j_meta(redact_kwargs=["custom_field"]),
        )
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            kwargs={"user_id": 1, "custom_field": "secret"},
        )
        assert event.data["kwargs"]["custom_field"] == "[REDACTED]"
        assert event.data["kwargs"]["user_id"] == 1

    def test_meta_keep_kwargs_is_whitelist(
        self,
        redaction: RedactionEngine,
    ) -> None:
        task = _fake_task(
            "myapp.tasks.charge",
            meta_decorator=z4j_meta(keep_kwargs=["user_id"]),
        )
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            kwargs={"user_id": 1, "amount": 100, "note": "test"},
        )
        kwargs = event.data["kwargs"]
        assert kwargs == {"user_id": 1}

    def test_meta_keep_kwargs_redacts_positional_args(
        self,
        redaction: RedactionEngine,
    ) -> None:
        task = _fake_task(
            "myapp.tasks.charge",
            meta_decorator=z4j_meta(keep_kwargs=["user_id"]),
        )
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            args=[1, 2, 3],
            kwargs={"user_id": 1},
        )
        assert event.data["args"] == "[REDACTED]"

    def test_meta_redact_result(self, redaction: RedactionEngine) -> None:
        task = _fake_task(
            "myapp.tasks.charge",
            meta_decorator=z4j_meta(redact_result=True),
        )
        event = build_task_succeeded_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            result={"charge_id": "ch_123"},
        )
        assert event.data["result"] == "[REDACTED]"


class TestMetaPropagation:
    def test_tags_appear_in_event_data(self, redaction: RedactionEngine) -> None:
        task = _fake_task(
            "myapp.tasks.charge",
            meta_decorator=z4j_meta(tags=["billing", "critical"]),
        )
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc",
            task=task,
        )
        assert event.data["tags"] == ["billing", "critical"]

    def test_expected_duration_and_deadline(
        self,
        redaction: RedactionEngine,
    ) -> None:
        task = _fake_task(
            "myapp.tasks.charge",
            meta_decorator=z4j_meta(expected_duration_ms=200, deadline_ms=5000),
        )
        event = build_task_started_event(
            redaction=redaction,
            task_id="abc",
            task=task,
        )
        assert event.data["expected_duration_ms"] == 200
        assert event.data["deadline_ms"] == 5000


class TestTracebackTruncation:
    def test_long_traceback_is_truncated(self, redaction: RedactionEngine) -> None:
        task = _fake_task("myapp.tasks.fail")
        huge_tb = "stack frame\n" * 1000  # well over 4096 chars
        event = build_task_failed_event(
            redaction=redaction,
            task_id="abc",
            task=task,
            exception=RuntimeError("boom"),
            traceback=huge_tb,
        )
        assert "[... traceback truncated ...]" in event.data["traceback"]
