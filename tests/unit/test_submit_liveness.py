"""A stalled submit must not block agent liveness or report a definite failure."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest
from z4j_celery.engine import CeleryEngineAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize("registered", [False, True])
@pytest.mark.parametrize("times_out", [False, True])
async def test_submit_preserves_liveness_and_ambiguous_outcome(monkeypatch, registered, times_out):
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    finished = asyncio.Event()
    release = threading.Event()
    completed = threading.Event()
    calls = []

    def publish(*args, **kwargs):
        calls.append((args, kwargs))
        loop.call_soon_threadsafe(entered.set)
        # Bound the broken pre-fix path as well as test cleanup.
        release.wait(2)
        completed.set()
        loop.call_soon_threadsafe(finished.set)
        return SimpleNamespace(id="accepted-once")

    app = SimpleNamespace(
        tasks={"app.task": SimpleNamespace(apply_async=publish)} if registered else {},
        send_task=publish,
    )
    adapter = CeleryEngineAdapter(celery_app=app)
    monkeypatch.setattr(
        "z4j_celery.engine._SUBMIT_TIMEOUT", 0.1 if times_out else 10.0, raising=False
    )
    submit = asyncio.create_task(
        adapter.submit_task("app.task", args=(1,), kwargs={"value": 2}, queue="critical")
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        # This coroutine must regain control while the broker is still blocked.
        assert not completed.is_set(), "broker submission blocked the agent event loop"
        if times_out:
            result = await asyncio.wait_for(submit, 1)
            assert result.status == "failed"
            assert result.result is not None and result.result["indeterminate"] is True
            assert "INDETERMINATE" in result.error
            assert not completed.is_set()
        else:
            release.set()
            result = await asyncio.wait_for(submit, 1)
            assert result.status == "success"
            assert result.result["task_id"] == "accepted-once"
        release.set()
        await asyncio.wait_for(finished.wait(), 5)
        # Even a timed-out publish can later finish; the adapter must not repeat it.
        assert len(calls) == 1
    finally:
        release.set()
        await asyncio.gather(submit, return_exceptions=True)
