"""Queue overflow counters describe actual event removal, including exhausted retries."""

import asyncio
from types import SimpleNamespace

from z4j_celery.engine import CeleryEngineAdapter


def test_oldest_event_loss_is_exact_and_instance_scoped():
    adapter = CeleryEngineAdapter(celery_app=SimpleNamespace())
    adapter._event_queue = asyncio.Queue(maxsize=2)
    events = [SimpleNamespace(kind="task.completed", id=i) for i in range(5)]
    for event in events:
        adapter._enqueue_event(event)
    assert adapter.dropped_event_count == 3
    assert [adapter._event_queue.get_nowait().id for _ in range(2)] == [3, 4]
    assert adapter.dropped_event_count == 3
    assert CeleryEngineAdapter(celery_app=SimpleNamespace()).dropped_event_count == 0


def test_exhausted_put_counts_only_the_lost_new_event():
    class ContendedQueue:
        def put_nowait(self, event):
            raise asyncio.QueueFull

        def get_nowait(self):
            raise asyncio.QueueEmpty

    adapter = CeleryEngineAdapter(celery_app=SimpleNamespace())
    adapter._event_queue = ContendedQueue()
    adapter._enqueue_event(SimpleNamespace(kind="task.completed"))
    assert adapter.dropped_event_count == 1


def test_child_process_discards_parent_counter_and_inherited_lock():
    adapter = CeleryEngineAdapter(celery_app=SimpleNamespace())
    adapter._dropped_event_count = 12
    inherited_lock = adapter._event_loss_lock
    inherited_lock.acquire()
    try:
        adapter._event_loss_pid = -1
        assert adapter.dropped_event_count == 0
        assert adapter._event_loss_lock is not inherited_lock
        adapter._record_event_drop()
        assert adapter.dropped_event_count == 1
    finally:
        inherited_lock.release()
