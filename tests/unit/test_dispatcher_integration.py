"""End-to-end dispatcher integration: real Celery engine + bare dispatcher.

Final piece of the v1.1.0 schedule.fire verification matrix. The
unit tests in ``test_engine.py`` prove the engine method itself
works; the bare dispatcher's ``TestScheduleFire`` proves the
dispatch routing with a fake engine. This file proves the COMPOSITION:
a real ``schedule.fire`` CommandFrame, handed to a real
CommandDispatcher wired to a real ``CeleryEngineAdapter``, results
in a real ``celery_app.send_task`` call and a successful
``command_result`` frame.

z4j-celery was the LAST adapter to gain this test (round-4
audit Apr 2026): the e2e at
``packages/z4j-scheduler/tests/integration/test_brain_scheduler_e2e.py``
already covered the celery path through the brain side, but the
bare-dispatcher composition test (this file) had been written for
RQ / Dramatiq / Huey / arq / Taskiq and never backfilled to
celery. With this file in place, the engine matrix is complete
and a ``schedule.fire`` regression in the bare dispatcher would
trip across all six engines.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from z4j_bare.buffer import BufferStore
from z4j_bare.dispatcher import CommandDispatcher
from z4j_celery.engine import CeleryEngineAdapter
from z4j_core.transport.frames import CommandFrame, CommandPayload


@pytest.fixture
def buf(tmp_path: Path) -> BufferStore:
    store = BufferStore(path=tmp_path / "buf.sqlite")
    yield store
    store.close()


def _patch_send_task_with_queue(fake_app) -> None:
    """Extend FakeCeleryApp.send_task to accept queue + priority.

    The real celery ``send_task`` accepts ``queue=`` and ``priority=``
    kwargs but the fake in conftest only models ``args/kwargs/eta``
    (it predates the schedule.fire integration tests). Patch the
    method locally so this test's dispatcher payload (which carries
    the brain's ``queue`` field through to the adapter) lands cleanly.
    """

    original = fake_app.send_task

    def wrapped(name, *, args=(), kwargs=None, eta=None, queue=None, priority=None):
        # Record the queue/priority alongside the original call shape.
        result = original(name, args=args, kwargs=kwargs, eta=eta)
        if fake_app.sent_tasks:
            fake_app.sent_tasks[-1]["queue"] = queue
            fake_app.sent_tasks[-1]["priority"] = priority
        return result

    fake_app.send_task = wrapped


@pytest.mark.asyncio
async def test_schedule_fire_end_to_end_through_dispatcher(
    fake_app,
    buf: BufferStore,
) -> None:
    """A schedule.fire CommandFrame for the celery engine must:

    1. Survive the bare dispatcher's ``_dispatch_schedule_fire`` route.
    2. Reach ``CeleryEngineAdapter.submit_task`` with the right kwargs.
    3. Land on the actual celery ``send_task`` call.
    4. Produce a success ``command_result`` frame on the buffer.

    Round-4 audit fix (Apr 2026): pre-fix this composition was only
    proven for the celery path implicitly via the much bigger
    z4j-scheduler integration suite. A regression in the bare
    dispatcher's schedule.fire branch (the "audit log finding
    2026-04-28" comment in z4j-bare/dispatcher.py) would have been
    caught here in milliseconds; the e2e took seconds. Pinning the
    composition here closes the engine matrix.
    """
    _patch_send_task_with_queue(fake_app)
    engine = CeleryEngineAdapter(celery_app=fake_app)
    dispatcher = CommandDispatcher(
        engines={"celery": engine},
        schedulers={},  # no scheduler adapter, proves the v1.1.0
        # bare-dispatcher schedule.fire fix works
        # without one (a celery WORKER agent has no
        # celery-beat adapter registered).
        buffer=buf,
    )

    frame = CommandFrame(
        id="cmd_e2e_celery_01",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "schedule_id": "sched-1",
                "schedule_name": "nightly-cleanup",
                "task_name": "myapp.tasks.cleanup",
                "engine": "celery",
                "queue": "high-priority",
                "args": ["arg1"],
                "kwargs": {"verbose": True},
                "fire_id": "fire-1",
            },
        ),
        hmac="deadbeef" * 8,
    )

    await dispatcher.handle(frame)

    # The Celery app saw the send_task with the brain's payload values.
    assert len(fake_app.sent_tasks) == 1, (
        "schedule.fire must invoke celery_app.send_task exactly once"
    )
    sent = fake_app.sent_tasks[0]
    assert sent["name"] == "myapp.tasks.cleanup"
    assert sent["args"] == ["arg1"]
    assert sent["kwargs"] == {"verbose": True}
    # Brain's ``queue`` field threaded through to the adapter and on
    # to celery's send_task.
    assert sent["queue"] == "high-priority"

    # The dispatcher emitted a success command_result frame.
    entries = buf.drain(10)
    results = [e for e in entries if e.kind == "command_result"]
    assert len(results) == 1, "exactly one success command_result frame must be buffered"
    parsed = json.loads(results[0].payload.decode("utf-8"))
    assert parsed["payload"]["status"] == "success"
    assert parsed["payload"]["result"]["engine"] == "celery"


@pytest.mark.asyncio
async def test_schedule_fire_celery_routes_via_engine_field(
    fake_app,
    buf: BufferStore,
) -> None:
    """When the dispatcher has multiple engines registered, the
    ``engine`` field in the ``schedule.fire`` payload selects which
    one runs. Pre-1.1 this would have collapsed to "the only
    registered engine" and silently mis-fired in mixed-engine
    deployments.
    """
    from z4j_core.models.command import CommandResult

    class _RecordingFakeEngine:
        name = "rq"
        capabilities_value = ("submit_task",)
        last_call: dict | None = None

        def capabilities(self) -> tuple[str, ...]:
            return self.capabilities_value

        async def submit_task(
            self, name, *, args=(), kwargs=None, queue=None, eta=None, priority=None
        ):
            type(self).last_call = {"name": name, "args": args, "kwargs": kwargs}
            return CommandResult(status="success", result={"engine": "rq"})

    rq_fake = _RecordingFakeEngine()
    celery_engine = CeleryEngineAdapter(celery_app=fake_app)
    dispatcher = CommandDispatcher(
        engines={"celery": celery_engine, "rq": rq_fake},
        schedulers={},
        buffer=buf,
    )

    # Fire targeting celery - rq adapter must NOT see it.
    frame = CommandFrame(
        id="cmd_route_celery",
        payload=CommandPayload(
            action="schedule.fire",
            target={},
            parameters={
                "task_name": "myapp.tasks.t",
                "engine": "celery",
                "args": [],
                "kwargs": {},
                "fire_id": "f1",
            },
        ),
        hmac="deadbeef" * 8,
    )
    await dispatcher.handle(frame)

    assert len(fake_app.sent_tasks) == 1
    assert _RecordingFakeEngine.last_call is None, (
        "schedule.fire with engine='celery' must not leak to the "
        "rq adapter even when both are registered"
    )
