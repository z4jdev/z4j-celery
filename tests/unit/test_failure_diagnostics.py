"""Failure details retain their meaning through signal and broker capture."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from z4j_celery.events.broker import CeleryBrokerEventsMonitor
from z4j_celery.events.signals import CelerySignalHooks
from z4j_core.models import Event
from z4j_core.redaction.engine import RedactionEngine


def test_signal_formats_a_real_traceback_object() -> None:
    events: list[Event] = []
    hooks = CelerySignalHooks(sink=events.append, redaction=RedactionEngine())

    def fail() -> None:
        raise ValueError("diagnostic failure")

    try:
        fail()
    except ValueError as exc:
        hooks._on_failure(task_id="failed-job", exception=exc, traceback=exc.__traceback__)
    assert len(events) == 1
    assert events[0].data["exception"] == "ValueError"
    assert "test_signal_formats_a_real_traceback_object" in events[0].data["traceback"]
    assert "ValueError: diagnostic failure" in events[0].data["traceback"]
    assert "<traceback object" not in events[0].data["traceback"]


def test_signal_uses_celery_formatted_exception_info() -> None:
    events: list[Event] = []
    hooks = CelerySignalHooks(sink=events.append, redaction=RedactionEngine())
    hooks._on_failure(
        task_id="failed-job",
        exception=ValueError("diagnostic failure"),
        traceback=object(),
        einfo=SimpleNamespace(traceback="ValueError: formatted by Celery"),
    )
    assert events[0].data["traceback"] == "ValueError: formatted by Celery"


@pytest.mark.parametrize(
    ("representation", "expected"),
    [
        ("ValueError('diagnostic failure')", "ValueError"),
        ("custom.errors.TaskError('failure')", "TaskError"),
        ("Unstructured failure", "Exception"),
    ],
)
def test_broker_preserves_reported_exception_class(representation: str, expected: str) -> None:
    events: list[Event] = []
    monitor = CeleryBrokerEventsMonitor(
        celery_app=None, sink=events.append, redaction=RedactionEngine()
    )
    monitor._on_failed(
        {"uuid": "failed-job", "exception": representation, "traceback": "worker stack"}
    )
    assert len(events) == 1
    assert events[0].data["exception"] == expected
    assert events[0].data["exception_message"] == representation
    assert events[0].data["traceback"] == "worker stack"
