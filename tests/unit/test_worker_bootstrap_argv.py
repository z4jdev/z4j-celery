"""Celery worker auto-bootstrap command-line detection tests."""

from __future__ import annotations

import sys

import pytest
from z4j_celery.worker_bootstrap import _is_celery_worker_invocation


@pytest.mark.parametrize(
    "option,value",
    [
        ("-A", "proj.celery"),
        ("--app", "proj.celery"),
        ("-b", "redis://localhost/0"),
        ("--broker", "redis://localhost/0"),
        ("--result-backend", "redis://localhost/1"),
        ("--loader", "celery.loaders.app.AppLoader"),
        ("--config", "proj.celeryconfig"),
        ("--workdir", "/srv/app"),
    ],
)
def test_worker_detected_after_global_option_value(
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    value: str,
) -> None:
    monkeypatch.setattr(sys, "argv", ["celery", option, value, "worker", "-l", "info"])

    assert _is_celery_worker_invocation() is True


@pytest.mark.parametrize(
    "argv",
    [
        ["celery", "--broker=redis://localhost/0", "worker"],
        ["celery", "-q", "--skip-checks", "worker"],
        ["python", "-m", "celery", "--workdir", "/srv/app", "worker"],
        ["uv", "run", "celery", "-b", "redis://localhost/0", "worker"],
        ["/usr/local/bin/celery", "--config", "proj.settings", "worker"],
    ],
)
def test_worker_detected_across_supported_launchers(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> None:
    monkeypatch.setattr(sys, "argv", argv)

    assert _is_celery_worker_invocation() is True


@pytest.mark.parametrize(
    "argv",
    [
        ["celery", "-b", "worker", "beat"],
        ["celery", "--workdir", "worker", "inspect"],
        ["celery", "--result-backend", "worker", "purge"],
        ["python", "-m", "celery", "--loader", "worker", "shell"],
    ],
)
def test_option_value_named_worker_does_not_create_false_positive(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> None:
    monkeypatch.setattr(sys, "argv", argv)

    assert _is_celery_worker_invocation() is False
