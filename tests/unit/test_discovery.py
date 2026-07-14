"""Unit tests for ``z4j_celery.discovery``."""

from __future__ import annotations

from pathlib import Path

from z4j_celery.discovery import (
    discover_runtime,
    discover_static,
    merge_discoveries,
)
from z4j_core.models import TaskDefinition


class TestRuntimeDiscovery:
    def test_empty_app(self, fake_app) -> None:
        defs = discover_runtime(fake_app)
        assert defs == []

    def test_single_task(self, fake_app) -> None:
        from tests.unit.conftest import FakeTask  # type: ignore[import-untyped]

        fake_app.register_task(FakeTask(name="myapp.tasks.send_email"))
        defs = discover_runtime(fake_app)
        assert len(defs) == 1
        assert defs[0].name == "myapp.tasks.send_email"
        assert defs[0].engine == "celery"
        assert defs[0].loaded is True

    def test_internal_celery_tasks_excluded(self, fake_app) -> None:
        from tests.unit.conftest import FakeTask  # type: ignore[import-untyped]

        fake_app.register_task(FakeTask(name="celery.backend_cleanup"))
        fake_app.register_task(FakeTask(name="myapp.tasks.send_email"))
        defs = discover_runtime(fake_app)
        assert len(defs) == 1
        assert defs[0].name == "myapp.tasks.send_email"

    def test_task_with_queue(self, fake_app) -> None:
        from tests.unit.conftest import FakeTask  # type: ignore[import-untyped]

        fake_app.register_task(FakeTask(name="myapp.tasks.ship", queue="shipping"))
        defs = discover_runtime(fake_app)
        assert defs[0].queue == "shipping"

    def test_bad_app_returns_empty(self) -> None:
        class NoTasks:
            pass

        assert discover_runtime(NoTasks()) == []


class TestStaticDiscovery:
    def test_finds_shared_task(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        (app_dir / "tasks.py").write_text(
            """
from celery import shared_task

@shared_task
def send_email(user_id: int, template: str) -> None:
    pass

@shared_task(queue="priority")
def urgent(user_id):
    pass
""",
        )
        defs = discover_static([app_dir])
        names = {d.name.rsplit(".", 1)[-1] for d in defs}
        assert "send_email" in names
        assert "urgent" in names
        for d in defs:
            assert d.loaded is False
            assert d.engine == "celery"

    def test_extracts_queue_from_decorator(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        (app_dir / "tasks.py").write_text(
            """
from celery import shared_task

@shared_task(queue="emails")
def send(user_id):
    pass
""",
        )
        defs = discover_static([app_dir])
        assert len(defs) == 1
        assert defs[0].queue == "emails"

    def test_extracts_signature(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        (app_dir / "tasks.py").write_text(
            """
from celery import shared_task

@shared_task
def do_work(user_id: int, template: str = "default") -> None:
    pass
""",
        )
        defs = discover_static([app_dir])
        assert len(defs) == 1
        assert "user_id" in (defs[0].signature or "")
        assert "template" in (defs[0].signature or "")

    def test_skips_file_with_syntax_error(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "broken"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        (app_dir / "tasks.py").write_text("def oops(\n")  # broken
        defs = discover_static([app_dir])
        assert defs == []

    def test_nonexistent_path_returns_empty(self, tmp_path: Path) -> None:
        defs = discover_static([tmp_path / "missing"])
        assert defs == []

    def test_nested_tasks_package(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        tasks_pkg = app_dir / "tasks"
        tasks_pkg.mkdir()
        (tasks_pkg / "__init__.py").write_text(
            """
from celery import shared_task

@shared_task
def from_init():
    pass
""",
        )
        (tasks_pkg / "emails.py").write_text(
            """
from celery import shared_task

@shared_task
def send_welcome():
    pass
""",
        )
        defs = discover_static([app_dir])
        names = {d.name.rsplit(".", 1)[-1] for d in defs}
        assert "from_init" in names
        assert "send_welcome" in names

    def test_decorator_chain_matches_full_name(self, tmp_path: Path) -> None:
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "__init__.py").write_text("")
        (app_dir / "tasks.py").write_text(
            """
from celery import app

@app.task
def my_task():
    pass
""",
        )
        defs = discover_static([app_dir])
        assert len(defs) == 1


class TestMergeDiscoveries:
    def _td(self, name: str, *, loaded: bool) -> TaskDefinition:
        return TaskDefinition(name=name, engine="celery", loaded=loaded)

    def test_runtime_wins_on_conflict(self) -> None:
        runtime = [self._td("myapp.tasks.foo", loaded=True)]
        static = [self._td("myapp.tasks.foo", loaded=False)]
        merged = merge_discoveries(runtime, static)
        assert len(merged) == 1
        assert merged[0].loaded is True

    def test_static_only_entries_survive(self) -> None:
        runtime = [self._td("myapp.tasks.foo", loaded=True)]
        static = [self._td("myapp.tasks.bar", loaded=False)]
        merged = merge_discoveries(runtime, static)
        assert len(merged) == 2
        names = [d.name for d in merged]
        assert names == sorted(names)

    def test_empty_lists(self) -> None:
        assert merge_discoveries([], []) == []
