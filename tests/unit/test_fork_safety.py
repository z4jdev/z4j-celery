"""Replacement-fork safety: the agent must not wedge recycled pool children.

A prefork pool that recycles children (``worker_max_tasks_per_child``,
``worker_max_memory_per_child``, a crash, a time-limit kill) forks
replacements for the life of the worker while the agent's threads run in the
MainProcess. Each replacement runs kombu's after-fork cleanup on Celery's
shared broker connection pool before it sends the pool its UP message, and
closing a Redis channel with a BRPOP outstanding reads from the socket the
child shares with the parent. The agent's health refresh used to wait for
``control.inspect`` replies on a pooled connection; a reply wait that timed
out (or whose reply a child read first) left such a channel in the pool and
every later child blocked in that read forever ("Timed out waiting for UP
message"; the worker took no tasks until restarted).

These tests pin the fix: reply waits run on an agent-owned connection.
"""

from __future__ import annotations

import ast
import contextlib
import os
import signal
import socket
import threading
import time
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from z4j_celery import engine as engine_module
from z4j_celery.engine import CeleryEngineAdapter, _agent_broker_connection
from z4j_celery.worker_bootstrap import _pool_recycle_limits

# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _RecordingConnection:
    def __init__(self, driver_type: str = "redis", transport_options: Any = None) -> None:
        self.transport = SimpleNamespace(driver_type=driver_type)
        self.transport_options = {} if transport_options is None else transport_options
        self.released = False
        self.release_error: Exception | None = None

    def release(self) -> None:
        self.released = True
        if self.release_error is not None:
            raise self.release_error


class _RecordingInspector:
    def __init__(self, stats: dict[str, Any] | None = None) -> None:
        self._stats = stats or {}

    def stats(self) -> dict[str, Any]:
        return self._stats

    def active(self) -> dict[str, Any]:
        return {}

    def active_queues(self) -> dict[str, Any]:
        return {}

    def registered(self) -> dict[str, Any]:
        return {}

    def conf(self, with_defaults: bool = False) -> dict[str, Any]:
        return {}


class _RecordingControl:
    def __init__(self, inspector: _RecordingInspector) -> None:
        self._inspector = inspector
        self.inspect_calls: list[dict[str, Any]] = []

    def inspect(self, **kwargs: Any) -> _RecordingInspector:
        self.inspect_calls.append(kwargs)
        return self._inspector


class _App:
    def __init__(self, conn: _RecordingConnection, stats: dict[str, Any] | None = None) -> None:
        self.conn = conn
        self.connections_opened = 0
        self.control = _RecordingControl(_RecordingInspector(stats))

    def connection_for_write(self) -> _RecordingConnection:
        self.connections_opened += 1
        return self.conn


# ---------------------------------------------------------------------------
# Reply waits run on an agent-owned connection
# ---------------------------------------------------------------------------


class TestInspectUsesAgentConnection:
    def test_worker_details_inspects_on_its_own_connection(self) -> None:
        conn = _RecordingConnection()
        app = _App(conn, stats={"celery@a": {"pid": 1}})
        adapter = CeleryEngineAdapter(celery_app=app, worker_hostname="celery@a")

        details = adapter.get_worker_details(hostname="celery@a")

        assert details["celery@a"]["stats"] == {"pid": 1}
        assert app.control.inspect_calls == [
            {"destination": ["celery@a"], "timeout": 2.0, "connection": conn},
        ]
        # One connection for the five calls, closed afterwards.
        assert app.connections_opened == 1
        assert conn.released is True

    def test_basic_stats_probe_inspects_on_its_own_connection(self) -> None:
        conn = _RecordingConnection()
        app = _App(conn, stats={"celery@b": {"pid": 2}})
        adapter = CeleryEngineAdapter(celery_app=app)

        assert adapter._worker_stats_basic() == {"celery@b": {"stats": {"pid": 2}}}
        assert app.control.inspect_calls == [{"timeout": 1.0, "connection": conn}]
        assert conn.released is True

    def test_health_refresh_takes_the_agent_connection_path(self) -> None:
        conn = _RecordingConnection()
        app = _App(conn)
        app.connection_for_read = lambda: contextlib.nullcontext(SimpleNamespace())
        adapter = CeleryEngineAdapter(celery_app=app, worker_hostname="celery@a")

        adapter.get_health()

        assert [call["connection"] for call in app.control.inspect_calls] == [conn]

    def test_close_error_keeps_the_replies_already_read(self) -> None:
        conn = _RecordingConnection()
        conn.release_error = OSError("broker went away")
        app = _App(conn, stats={"celery@a": {"pid": 1}})
        adapter = CeleryEngineAdapter(celery_app=app)

        assert adapter._worker_stats_basic() == {"celery@a": {"stats": {"pid": 1}}}
        assert conn.released is True

    def test_every_inspect_call_in_the_package_passes_a_connection(self) -> None:
        """Structural guard: a new ``control.inspect(...)`` without
        ``connection=`` would wait for replies on Celery's shared pool and
        bring the replacement-fork wedge back."""
        src = Path(engine_module.__file__).resolve().parent
        offenders: list[str] = []
        for path in sorted(src.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "inspect"
                    and isinstance(node.func.value, ast.Attribute)
                    and node.func.value.attr == "control"
                    and not any(kw.arg == "connection" for kw in node.keywords)
                ):
                    offenders.append(f"{path.relative_to(src)}:{node.lineno}")
        assert offenders == []


class TestAgentConnectionSocketTimeout:
    def test_redis_connection_gets_a_bounded_read_timeout(self) -> None:
        conn = _RecordingConnection(transport_options={"visibility_timeout": 3600})
        with _agent_broker_connection(_App(conn)) as got:
            assert got is conn
            assert conn.transport_options == {
                "visibility_timeout": 3600,
                "socket_timeout": engine_module._AGENT_SOCKET_TIMEOUT,
            }
        assert conn.released is True

    def test_operator_socket_timeout_wins(self) -> None:
        conn = _RecordingConnection(transport_options={"socket_timeout": 30})
        with _agent_broker_connection(_App(conn)):
            assert conn.transport_options == {"socket_timeout": 30}

    def test_non_redis_transport_options_are_untouched(self) -> None:
        conn = _RecordingConnection(driver_type="amqp")
        with _agent_broker_connection(_App(conn)):
            assert conn.transport_options == {}

    def test_timeout_stays_above_kombus_brpop_poll(self) -> None:
        # kombu's Redis transport polls BRPOP with a one-second server-side
        # timeout; a read timeout at or under it would fail ordinary waits.
        assert engine_module._AGENT_SOCKET_TIMEOUT > 1.0


# ---------------------------------------------------------------------------
# worker_ready breadcrumb for recycling pools
# ---------------------------------------------------------------------------


def _sender(pool_cls: Any, **controller: Any) -> SimpleNamespace:
    return SimpleNamespace(controller=SimpleNamespace(pool_cls=pool_cls, **controller))


class TestPoolRecycleLimits:
    def test_cli_limits_on_the_controller_are_reported(self) -> None:
        prefork = type("TaskPool", (), {"__module__": "celery.concurrency.prefork"})
        sender = _sender(prefork, max_tasks_per_child=200, max_memory_per_child=None)
        app = SimpleNamespace(conf={"worker_max_memory_per_child": 250_000})

        assert _pool_recycle_limits(sender, app) == {
            "worker_max_tasks_per_child": 200,
            "worker_max_memory_per_child": 250_000,
        }

    def test_unresolved_prefork_name_is_recognised(self) -> None:
        sender = _sender("prefork", max_tasks_per_child=5, max_memory_per_child=None)
        assert _pool_recycle_limits(sender, SimpleNamespace(conf={})) == {
            "worker_max_tasks_per_child": 5,
        }

    def test_no_limits_means_no_report(self) -> None:
        sender = _sender("prefork", max_tasks_per_child=None, max_memory_per_child=None)
        assert _pool_recycle_limits(sender, SimpleNamespace(conf={})) == {}

    @pytest.mark.parametrize("pool", ["solo", "threads", "gevent", "eventlet"])
    def test_pools_that_do_not_fork_are_not_reported(self, pool: str) -> None:
        sender = _sender(pool, max_tasks_per_child=10, max_memory_per_child=None)
        assert _pool_recycle_limits(sender, SimpleNamespace(conf={})) == {}

    def test_missing_controller_is_tolerated(self) -> None:
        assert _pool_recycle_limits(SimpleNamespace(), SimpleNamespace(conf={})) == {}
        assert _pool_recycle_limits(None, None) == {}


# ---------------------------------------------------------------------------
# Real Celery + kombu: the broadcast never runs on a pooled connection
# ---------------------------------------------------------------------------


def test_real_inspect_broadcasts_on_the_agent_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    celery = pytest.importorskip("celery")
    pytest.importorskip("redis")
    from kombu.connection import PooledConnection
    from kombu.pidbox import Mailbox

    seen: list[Any] = []

    def fake_broadcast(self: Any, *args: Any, **kwargs: Any) -> list[Any]:
        seen.append(self.connection)
        return []

    monkeypatch.setattr(Mailbox, "_broadcast", fake_broadcast)
    app = celery.Celery("fork_safety_probe", broker="redis://127.0.0.1:1/0", set_as_current=False)
    adapter = CeleryEngineAdapter(celery_app=app, worker_hostname="celery@probe")

    adapter.get_worker_details(hostname="celery@probe")
    adapter._worker_stats_basic()

    assert len(seen) == 6
    assert not any(isinstance(conn, PooledConnection) for conn in seen)
    assert all(
        conn.transport_options["socket_timeout"] == engine_module._AGENT_SOCKET_TIMEOUT
        for conn in seen
    )
    # The agent never instantiated Celery's shared connection pool.
    assert app._pool is None


# ---------------------------------------------------------------------------
# Real fork: a replacement child's after-fork cleanup must not block
# ---------------------------------------------------------------------------


class _BlackholeRedis:
    """Just enough RESP for kombu's control round trip; BRPOP never answers.

    A BRPOP that never answers is what a pooled channel looks like once a
    child has read the reply or the connection has gone half-open.
    """

    def __init__(self) -> None:
        self._sock = socket.create_server(("127.0.0.1", 0))
        self.port = self._sock.getsockname()[1]
        self._conns: list[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        for conn in [self._sock, *self._conns]:
            with contextlib.suppress(OSError):
                conn.close()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self._conns.append(conn)
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        reader = conn.makefile("rb")
        queued: int | None = None
        try:
            while True:
                command = _read_command(reader)
                if command is None:
                    return
                name = command[0].upper()
                if queued is not None and name not in {b"EXEC", b"DISCARD"}:
                    queued += 1
                    reply = b"+QUEUED\r\n"
                elif name == b"MULTI":
                    queued = 0
                    reply = b"+OK\r\n"
                elif name == b"EXEC":
                    reply = b"*%d\r\n" % (queued or 0) + b":0\r\n" * (queued or 0)
                    queued = None
                elif name == b"BRPOP":
                    continue
                elif name == b"HELLO":
                    # redis-py 8 opens every connection with HELLO 3 (RESP3)
                    # and checks the protocol the server says it agreed to.
                    proto = int(command[1]) if len(command) > 1 else 2
                    reply = b"%%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:%d\r\n" % proto
                elif name == b"PING":
                    reply = b"+PONG\r\n"
                elif name in _NIL_COMMANDS:
                    reply = b"$-1\r\n"
                elif name in _EMPTY_ARRAY_COMMANDS:
                    reply = b"*0\r\n"
                elif name in _INTEGER_COMMANDS:
                    reply = b":0\r\n"
                else:
                    reply = b"+OK\r\n"
                conn.sendall(reply)
        except OSError:
            return


#: Replies for the commands kombu's control round trip and its unacked-message
#: restore send; anything else (CLIENT SETINFO, WATCH, SET NX) gets ``+OK``.
_NIL_COMMANDS = frozenset({b"GET", b"HGET"})
_EMPTY_ARRAY_COMMANDS = frozenset({b"SMEMBERS", b"ZREVRANGEBYSCORE", b"ZRANGE", b"LRANGE"})
_INTEGER_COMMANDS = frozenset(
    {
        b"SADD",
        b"SREM",
        b"PUBLISH",
        b"DEL",
        b"LPUSH",
        b"RPUSH",
        b"EXISTS",
        b"HDEL",
        b"ZREM",
        b"ZADD",
    },
)


def _read_command(reader: Any) -> list[bytes] | None:
    header = reader.readline()
    if not header.startswith(b"*"):
        return None
    parts = []
    for _ in range(int(header[1:])):
        size = int(reader.readline()[1:])
        parts.append(reader.read(size + 2)[:-2])
    return parts


def _fork_quietly() -> int:
    with warnings.catch_warnings():
        # 3.12+ warns that forking a multi-threaded process may deadlock the
        # child. That is the very property under test.
        warnings.simplefilter("ignore", DeprecationWarning)
        return os.fork()


def _wait(pid: int, deadline: float) -> int | None:
    """Exit code of ``pid``, or None after killing it at the deadline."""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done:
            return os.waitstatus_to_exitcode(status)
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    return None


def _run_after_forkers_in_child(deadline: float) -> bool:
    """Fork, run what billiard runs in a new pool child before UP, report.

    True when the child finished its after-fork cleanup within ``deadline``.
    """
    import multiprocessing.util as mp_util

    runners = [mp_util._run_after_forkers]
    with contextlib.suppress(ImportError):
        import billiard.util as billiard_util

        if billiard_util._run_after_forkers is not mp_util._run_after_forkers:
            runners.append(billiard_util._run_after_forkers)

    pid = _fork_quietly()
    if pid == 0:  # pragma: no cover - runs in the child
        try:
            for run in runners:
                run()
        finally:
            os._exit(0)
    return _wait(pid, deadline) == 0


_CHILD_FINISHED, _CHILD_BLOCKED = 0, 1


def _replacement_child_outcome(make_app: Any, probe: Any) -> int | None:
    """Play a worker MainProcess, then fork a replacement pool child.

    In a fresh process (so no broker state leaks between scenarios: kombu's
    after-fork registry is process-global) build the app, run ``probe`` the
    way an agent thread would, then fork a child that runs the pool child's
    after-fork cleanup. Returns ``_CHILD_FINISHED``, ``_CHILD_BLOCKED``, or
    another value when the probe itself failed.
    """
    pid = _fork_quietly()
    if pid == 0:  # pragma: no cover - runs in the forked "MainProcess"
        code = 2
        try:
            probe(make_app())
            code = _CHILD_FINISHED if _run_after_forkers_in_child(3.0) else _CHILD_BLOCKED
        finally:
            os._exit(code)
    return _wait(pid, 30.0)


@pytest.mark.slow
@pytest.mark.skipif(not hasattr(os, "fork"), reason="prefork children need os.fork")
def test_replacement_child_after_fork_cleanup_does_not_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    celery = pytest.importorskip("celery")
    pytest.importorskip("redis")
    # Keep the agent's own close of its reply connection short; the server
    # never answers BRPOP, so that close waits for the read timeout.
    monkeypatch.setattr(engine_module, "_AGENT_SOCKET_TIMEOUT", 0.5)
    server = _BlackholeRedis()

    def make_app() -> Any:
        return celery.Celery(
            "fork_safety",
            broker=f"redis://127.0.0.1:{server.port}/0",
            set_as_current=False,
        )

    try:
        # Precondition: Celery's own inspect, which waits on a pooled
        # connection, leaves state that blocks a forked child. If kombu ever
        # stops reading inherited sockets after fork there is nothing left
        # for this test to guard.
        pooled = _replacement_child_outcome(
            make_app,
            lambda app: app.control.inspect(timeout=0.3).stats(),
        )
        if pooled == _CHILD_FINISHED:
            pytest.skip("kombu no longer blocks a forked child on a pooled BRPOP")
        assert pooled == _CHILD_BLOCKED, f"pooled inspect probe failed (exit {pooled})"

        agent = _replacement_child_outcome(
            make_app,
            lambda app: CeleryEngineAdapter(celery_app=app)._worker_stats_basic(),
        )
        assert agent == _CHILD_FINISHED, (
            "a forked child blocked in kombu's after-fork cleanup after the "
            f"agent's health inspect (exit {agent}): the reply wait is back "
            "on Celery's pool"
        )
    finally:
        server.close()
