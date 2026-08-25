"""Unit tests for ``z4j_celery.engine.CeleryEngineAdapter``.

Exercises the adapter's implementation of the ``QueueEngineAdapter``
Protocol against the FakeCeleryApp. Real-Celery integration tests
live in ``packages/z4j-celery/tests/integration/`` and land in the
next turn alongside the brain backend.
"""

from __future__ import annotations

import pytest
from z4j_celery.capabilities import DEFAULT_CAPABILITIES
from z4j_celery.engine import CeleryEngineAdapter, _method_is_async
from z4j_core.models import DiscoveryHints
from z4j_core.protocols import QueueEngineAdapter


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
        self,
        adapter: CeleryEngineAdapter,
        method: str,
    ) -> None:
        assert _method_is_async(adapter, method)


class TestCapabilities:
    def test_default_capabilities(self, adapter: CeleryEngineAdapter) -> None:
        caps = adapter.capabilities()
        assert caps == set(DEFAULT_CAPABILITIES)

    def test_includes_all_expected_actions(
        self,
        adapter: CeleryEngineAdapter,
    ) -> None:
        caps = adapter.capabilities()
        assert "retry_task" in caps
        assert "cancel_task" in caps
        assert "bulk_retry" in caps
        assert "purge_queue" in caps
        assert "requeue_dead_letter" not in caps
        assert "restart_worker" in caps


class TestDiscovery:
    async def test_discover_tasks_returns_runtime_tasks(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        from tests.unit.conftest import FakeTask  # type: ignore[import-untyped]

        fake_app.register_task(FakeTask(name="myapp.tasks.send_email"))
        fake_app.register_task(FakeTask(name="myapp.tasks.ship", queue="shipping"))

        defs = await adapter.discover_tasks()
        names = {d.name for d in defs}
        assert "myapp.tasks.send_email" in names
        assert "myapp.tasks.ship" in names

    async def test_discover_merges_static_hints(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
        tmp_path,
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
    async def test_submit_task_treats_eta_as_absolute_timestamp(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        from datetime import UTC, datetime, timedelta

        target = datetime.now(UTC) + timedelta(minutes=5)
        result = await adapter.submit_task(
            "myapp.tasks.delayed",
            eta=target.timestamp(),
        )
        assert result.status == "success"
        assert fake_app.sent_tasks[-1]["eta"] == target

    async def test_retry_task(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        fake_app.register_result("orig", name="myapp.tasks.foo")
        result = await adapter.retry_task("orig")
        assert result.status == "success"

    async def test_cancel_task(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        result = await adapter.cancel_task("abc")
        assert result.status == "success"
        assert fake_app.control.revoked[0][0] == "abc"

    async def test_bulk_retry_extracts_task_ids_from_filter(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
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
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        # Audit H13: purge_queue now requires either a valid
        # ``confirm_token`` (HMAC of queue_name + current depth) or
        # ``force=True``. The fake broker cannot report depth, so
        # force is the only path available to the engine test.
        result = await adapter.purge_queue("emails", force=True)
        assert result.status == "success"

    async def test_requeue_dead_letter(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        before = list(fake_app.sent_tasks)
        result = await adapter.requeue_dead_letter("dead")
        assert result.status == "failed"
        assert "cannot safely requeue" in result.error
        assert fake_app.sent_tasks == before

    async def test_restart_worker(
        self,
        adapter: CeleryEngineAdapter,
        fake_app,
    ) -> None:
        result = await adapter.restart_worker("celery@w1")
        assert result.status == "success"


class TestEventQueue:
    async def test_event_queue_starts_empty(self, adapter: CeleryEngineAdapter) -> None:
        # We don't connect signals here, just assert the queue exists.
        assert adapter._event_queue.qsize() == 0

    async def test_enqueue_via_helper(self, adapter: CeleryEngineAdapter) -> None:
        from datetime import UTC, datetime
        from uuid import uuid4

        from z4j_core.models import Event, EventKind

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
        adapter._enqueue_event(event)
        assert adapter._event_queue.qsize() == 1


# ---------------------------------------------------------------------------
# Round-7 audit: worker_conf credential leak via inspector.conf()
# ---------------------------------------------------------------------------


class _FakeInspector:
    """Stand-in for ``celery_app.control.inspect()``.

    Records arguments + returns canned data per call. Used only by the
    Regression tests; the broader engine tests do not exercise
    ``get_worker_details`` because the rest of the FakeCeleryApp does
    not implement ``inspect``.
    """

    def __init__(self, conf_payload: dict[str, dict[str, object]]) -> None:
        self._conf_payload = conf_payload

    def stats(self) -> dict[str, object]:
        return {}

    def active(self) -> dict[str, object]:
        return {}

    def active_queues(self) -> dict[str, object]:
        return {}

    def registered(self) -> dict[str, object]:
        return {}

    def conf(self, with_defaults: bool = False) -> dict[str, dict[str, object]]:
        # The adapter asks for defaults on purpose: the settings worth
        # warning about are the ones nobody set. Recording the argument
        # keeps this double honest about the call it received.
        self.conf_called_with_defaults = with_defaults
        return self._conf_payload


class _ControlWithInspect:
    """Wraps the existing FakeControl-style stub with an ``inspect``
    constructor that returns a canned ``_FakeInspector``."""

    def __init__(self, inspector: _FakeInspector) -> None:
        self._inspector = inspector

    def inspect(
        self,
        *,
        destination: list[str] | None = None,
        timeout: float | None = None,
    ) -> _FakeInspector:
        return self._inspector


class TestGetWorkerDetailsR7H1:
    """``get_worker_details`` must not expose credentialed
    Celery conf keys to the brain (and thence to ProjectRole.VIEWER).

    The adapter's ``inspector.conf()`` call returns the FULL Celery
    ``app.conf`` dict on a real cluster, including ``broker_url`` /
    ``result_backend`` / ``broker_transport_options`` / ``beat_schedule``.
    Pre-1.6.6 the adapter shipped the dict verbatim. 1.6.6 introduces
    an allowlist so only operationally-useful tuning knobs are sent
    upstream.
    """

    def test_get_worker_details_strips_credentialed_conf_keys_r7_h1(
        self,
        fake_app,
    ) -> None:
        # Fabricate a worker conf that mixes the dangerous keys an
        # operator may have set in their Celery app (broker / backend
        # URLs with secrets, transport options carrying TLS keys, a
        # beat schedule that may embed PII) with the benign tuning
        # knobs the dashboard legitimately surfaces.
        dangerous_conf = {
            # These MUST be stripped.
            "broker_url": "redis://:supersecret@redis.internal:6379/0",
            "result_backend": "db+postgresql://celery:pgpass@db.internal/celery",
            "broker_transport_options": {
                "region": "us-east-1",
                "aws_access_key_id": "AKIA...",
                "aws_secret_access_key": "AAAAAAAAAAAAAAAAAAAA",
            },
            "beat_schedule": {
                "send-weekly-report": {
                    "task": "myapp.tasks.report",
                    "schedule": 60.0,
                    "kwargs": {"recipient": "ceo@example.com"},
                },
            },
            # And these MUST be kept (operationally useful).
            "task_serializer": "json",
            "result_serializer": "json",
            "accept_content": ["json"],
            "task_default_queue": "celery",
            "worker_concurrency": 4,
            "worker_prefetch_multiplier": 1,
            "task_acks_late": True,
            "task_reject_on_worker_lost": True,
            "broker_pool_limit": 10,
            "broker_heartbeat": 120,
            "task_time_limit": 600,
            "task_soft_time_limit": 300,
            "worker_max_tasks_per_child": 1000,
            "worker_max_memory_per_child": 200_000,
            "timezone": "UTC",
            "enable_utc": True,
        }
        inspector = _FakeInspector(
            conf_payload={"celery@worker-1": dangerous_conf},
        )
        fake_app.control = _ControlWithInspect(inspector)
        adapter = CeleryEngineAdapter(celery_app=fake_app)

        details = adapter.get_worker_details()

        assert "celery@worker-1" in details, (
            "expected the inspector's conf payload to land under the "
            f"worker hostname key; got {details!r}"
        )
        conf = details["celery@worker-1"]["conf"]
        assert isinstance(conf, dict)

        # 1. ALL credentialed keys must be stripped.
        for forbidden in (
            "broker_url",
            "result_backend",
            "broker_transport_options",
            "beat_schedule",
        ):
            assert forbidden not in conf, (
                f": {forbidden!r} leaked through the allowlist; the brain "
                "would persist it into workers.metadata.conf and "
                "expose it to ProjectRole.VIEWER"
            )

        # 2. ALL benign tuning knobs MUST survive (the dashboard relies
        # on them; an over-aggressive denylist would regress the
        # worker-detail page).
        for expected_key, expected_value in (
            ("task_serializer", "json"),
            ("result_serializer", "json"),
            ("accept_content", ["json"]),
            ("task_default_queue", "celery"),
            ("worker_concurrency", 4),
            ("worker_prefetch_multiplier", 1),
            ("task_acks_late", True),
            ("task_reject_on_worker_lost", True),
            ("broker_pool_limit", 10),
            ("broker_heartbeat", 120),
            ("task_time_limit", 600),
            ("task_soft_time_limit", 300),
            ("worker_max_tasks_per_child", 1000),
            ("worker_max_memory_per_child", 200_000),
            ("timezone", "UTC"),
            ("enable_utc", True),
        ):
            assert conf.get(expected_key) == expected_value, (
                f"expected allowlisted key {expected_key!r}={expected_value!r} "
                f"in conf, got {conf.get(expected_key)!r}"
            )

        # 3. No unknown extras smuggled in (the allowlist is closed,
        # not best-effort).
        assert set(conf.keys()).issubset(
            {
                "task_serializer",
                "result_serializer",
                "accept_content",
                "task_default_queue",
                "worker_concurrency",
                "worker_prefetch_multiplier",
                "worker_max_tasks_per_child",
                "worker_max_memory_per_child",
                "task_acks_late",
                "task_reject_on_worker_lost",
                "task_time_limit",
                "task_soft_time_limit",
                "broker_pool_limit",
                "broker_heartbeat",
                "timezone",
                "enable_utc",
            }
        ), f"allowlist accepted a key it should not have: {sorted(conf.keys())!r}"

    def test_get_worker_details_handles_non_dict_conf_r7_h1(
        self,
        fake_app,
    ) -> None:
        """If ``inspector.conf()`` returns a non-dict value for a worker
        (older Celery, broken plugin), the redactor must return an
        empty dict instead of leaking the raw bytes upstream."""
        inspector = _FakeInspector(
            conf_payload={"celery@weird": "raw-string-not-a-dict"},  # type: ignore[dict-item]
        )
        fake_app.control = _ControlWithInspect(inspector)
        adapter = CeleryEngineAdapter(celery_app=fake_app)

        details = adapter.get_worker_details()

        assert details["celery@weird"]["conf"] == {}

    def test_redact_helper_is_exported_for_brain_reuse_r7_h1(self) -> None:
        """The brain re-applies the same filter defense-in-depth. The
        helper + allowlist must remain module-level importable so the
        round-7 patch set stays auditable across packages."""
        from z4j_celery.engine import _CONF_ALLOWLIST, _redact_worker_conf

        assert "broker_url" not in _CONF_ALLOWLIST
        assert "result_backend" not in _CONF_ALLOWLIST
        assert "broker_transport_options" not in _CONF_ALLOWLIST
        assert "beat_schedule" not in _CONF_ALLOWLIST
        assert "task_serializer" in _CONF_ALLOWLIST

        # Helper accepts anything dict-like, returns plain dict.
        assert _redact_worker_conf({"broker_url": "x", "timezone": "UTC"}) == {
            "timezone": "UTC",
        }
        assert _redact_worker_conf(None) == {}
        assert _redact_worker_conf("not-a-dict") == {}
        assert _redact_worker_conf(42) == {}


# ---------------------------------------------------------------------------
# Round-8 audit: CeleryEngineAdapter worker-stats cache must be
# instance-scoped (not class-scoped) to prevent cross-tenant bleed in
# multi-brain test rigs and the 1.7 soak harness.
# ---------------------------------------------------------------------------


class TestCacheInstanceScopingR8L2:
    """``_last_worker_stats_at`` and ``_cached_worker_stats`` must
    live on the instance, never the class.

    Pre-1.6.7 these two attributes were declared at class scope
    (``engine.py:463-464``) as mutable defaults, so two adapters
    instantiated in the same Python process shared the same dict and
    saw each other's last cached ``get_worker_details()`` payload.
    In single-tenant prod that was harmless (one adapter per agent
    process); in multi-tenant test rigs and the 1.7 soak harness it
    would let project A's worker conf land in project B's heartbeat.
    """

    def test_cache_attrs_live_on_instance_not_class_r8_l2(self) -> None:
        """Structural invariant: the class object itself must NOT carry
        the cache attributes; they're created per-instance in __init__.

        A future refactor that re-declares them at class scope (the
        original bug shape) trips this guard immediately.
        """
        assert "_cached_worker_stats" not in CeleryEngineAdapter.__dict__, (
            " regression: _cached_worker_stats reappeared at class "
            "scope. Move it back into __init__ as `self._cached_worker_stats = {}` "
            "so two adapter instances in the same process don't share a dict."
        )
        assert "_last_worker_stats_at" not in CeleryEngineAdapter.__dict__, (
            " regression: _last_worker_stats_at reappeared at class "
            "scope. Move it back into __init__."
        )

    def test_two_adapters_do_not_share_worker_stats_cache_r8_l2(
        self,
        fake_app,
    ) -> None:
        """Behavioral invariant: populating one adapter's cache must
        not populate the other's. Constructs two adapters, drives one
        through ``get_health()`` so its cache fills, and asserts the
        other's cache is still the empty-default state.
        """
        # Adapter A: backed by a fake_app whose control surface returns
        # a populated worker_details payload via the existing
        # inspector stub. Build its own inspector so the cached dict
        # carries a fingerprint we can detect later.
        from tests.unit.conftest import FakeCeleryApp

        app_a = FakeCeleryApp()
        app_a.control = _ControlWithInspect(
            _FakeInspector(
                conf_payload={"celery@a": {"task_serializer": "json"}},
            ),
        )
        # worker_hostname makes get_health() take the full-detail
        # refresh path (known destination); without it the refresh is a
        # single stats() probe and the conf-fingerprint below would
        # never land in the cache.
        adapter_a = CeleryEngineAdapter(
            celery_app=app_a,
            worker_hostname="celery@a",
        )

        # Adapter B: distinct fake_app, distinct inspector payload.
        # We never drive get_health() on adapter_b - it must stay in
        # the empty-default state if the cache is properly instance-
        # scoped.
        app_b = FakeCeleryApp()
        app_b.control = _ControlWithInspect(
            _FakeInspector(
                conf_payload={"celery@b": {"task_serializer": "pickle"}},
            ),
        )
        adapter_b = CeleryEngineAdapter(celery_app=app_b)

        # Belt-and-braces: assert the dicts are distinct objects even
        # before any get_health() call - if they shared a class-level
        # default they'd be the SAME dict object.
        assert adapter_a._cached_worker_stats is not adapter_b._cached_worker_stats
        assert adapter_a._cached_worker_stats == {}
        assert adapter_b._cached_worker_stats == {}

        # Drive adapter A through get_health() so its cache populates.
        # get_health() returns a dict; we don't care about the broker
        # health half (no real broker), we care about the worker_details
        # side effect.
        adapter_a.get_health()

        # Adapter A's cache should now hold the celery@a fingerprint.
        assert adapter_a._cached_worker_stats, (
            "fixture problem: adapter_a.get_health() did not populate "
            "the cache - inspect() returned empty?"
        )
        assert "celery@a" in adapter_a._cached_worker_stats

        # The pre-1.6.7 bug: adapter_b's cache would now also contain
        # 'celery@a' because both adapters point at the SAME class-
        # level dict. With the fix, adapter_b's cache is still
        # the per-instance empty default.
        assert adapter_b._cached_worker_stats == {}, (
            " regression: adapter_b's worker stats cache leaked "
            "from adapter_a. Both adapters are sharing the class-level "
            f"mutable default. Got: {adapter_b._cached_worker_stats!r}"
        )
        assert adapter_b._last_worker_stats_at == 0.0, (
            " regression: adapter_b's last-stats timestamp leaked "
            "from adapter_a (class-level mutable shared)."
        )


class TestWorkerConfAsksForDefaults:
    """The lint rules exist for settings nobody set, so defaults must arrive.

    Celery's ``Inspect.conf()`` defaults to ``with_defaults=False``, which
    returns only what an application explicitly overrode. A stock worker then
    reports an empty configuration, and every setting the worker-configuration
    lint exists to warn about (acks_late off, prefetch 4, reject-on-worker-lost
    disabled, no time limits) is invisible to it.
    """

    def test_the_adapter_requests_defaults(self, fake_app) -> None:
        inspector = _FakeInspector({"w1": {"task_serializer": "json"}})
        fake_app.control = _ControlWithInspect(inspector)
        adapter = CeleryEngineAdapter(celery_app=fake_app)

        adapter.get_worker_details(hostname="w1")

        assert inspector.conf_called_with_defaults is True, (
            "without defaults a stock worker reports {} and the lint sees nothing"
        )

    def test_a_default_valued_setting_survives_the_allowlist(self, fake_app) -> None:
        """The dangerous values are falsy, so a filter must not drop them.

        ``task_acks_late=False`` and ``task_reject_on_worker_lost=False`` are
        precisely the states worth flagging. A redaction step written with a
        truthiness test would silently discard them and report a clean worker.
        """
        inspector = _FakeInspector(
            {
                "w1": {
                    "task_acks_late": False,
                    "task_reject_on_worker_lost": False,
                    "worker_prefetch_multiplier": 4,
                    "task_time_limit": None,
                    "broker_url": "redis://:secret@host/0",
                },
            },
        )
        fake_app.control = _ControlWithInspect(inspector)
        adapter = CeleryEngineAdapter(celery_app=fake_app)

        conf = adapter.get_worker_details(hostname="w1")["w1"]["conf"]

        assert conf["task_acks_late"] is False
        assert conf["task_reject_on_worker_lost"] is False
        assert conf["worker_prefetch_multiplier"] == 4
        assert "task_time_limit" in conf
        assert "broker_url" not in conf
