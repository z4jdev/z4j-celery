"""Tests for the broker-event ``args`` / ``kwargs`` coercion helpers.

The Celery broker transport (kombu) emits ``args`` and ``kwargs``
on ``task-received`` events as the **repr'd strings**
``str(task.args)`` / ``str(task.kwargs)``, not as native Python
objects. The signal-based path delivers real tuples / dicts, so
without coercion the broker monitor crashed in
``_scrub_kwargs(...)`` with ``ValueError: dictionary update
sequence element #0 has length 1; 2 is required`` (every
``task-received`` event from a real Celery worker, observed in
the live Docker stack on 2026-04-21).

These tests pin the coercion contract so the regression cannot
recur. They exercise every shape we have observed or would expect
from kombu and from a malicious payload that survived past the
broker's own validation.
"""

from __future__ import annotations

from z4j_celery.events.broker import _coerce_args, _coerce_kwargs


class TestCoerceKwargs:
    def test_none_returns_none(self) -> None:
        assert _coerce_kwargs(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _coerce_kwargs("") is None

    def test_dict_passes_through(self) -> None:
        d = {"customer_id": 42, "email": "a@b.c"}
        assert _coerce_kwargs(d) == d

    def test_empty_dict_passes_through(self) -> None:
        # Distinguished from ``None`` so downstream events can
        # accurately say "task called with no kwargs" vs "no
        # kwargs field on the broker event".
        assert _coerce_kwargs({}) == {}

    def test_repr_dict_string_parsed(self) -> None:
        # The exact shape kombu emits on ``task-received``.
        out = _coerce_kwargs("{'customer_id': 42, 'email': 'a@b.c'}")
        assert out == {"customer_id": 42, "email": "a@b.c"}

    def test_empty_repr_dict_passes_through(self) -> None:
        assert _coerce_kwargs("{}") == {}

    def test_malformed_string_returns_none(self) -> None:
        # A broken repr (truncated, mismatched braces, etc.) must
        # NEVER raise - it returns ``None`` and the event is
        # accepted with no kwargs payload.
        assert _coerce_kwargs("{'k':") is None
        assert _coerce_kwargs("{'k': 'v'") is None
        assert _coerce_kwargs("not a dict") is None

    def test_list_string_returns_none(self) -> None:
        # Parses to a list, which is not a dict - coerce drops it.
        assert _coerce_kwargs("['a', 'b']") is None

    def test_attacker_payload_with_call_returns_none(self) -> None:
        # ``ast.literal_eval`` refuses anything that isn't a pure
        # literal - function calls, attribute lookups, names. This
        # confirms we cannot be tricked into evaluating code from
        # a crafted broker payload.
        assert _coerce_kwargs("__import__('os').system('rm -rf /')") is None
        assert _coerce_kwargs("{'a': open('/etc/passwd')}") is None

    def test_unexpected_type_returns_none(self) -> None:
        # int / bytes / object with no string protocol → drop.
        assert _coerce_kwargs(42) is None
        assert _coerce_kwargs(b"{'a': 1}") is None


class TestCoerceArgs:
    def test_none_returns_none(self) -> None:
        assert _coerce_args(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _coerce_args("") is None

    def test_list_passes_through(self) -> None:
        assert _coerce_args([1, "x"]) == [1, "x"]

    def test_tuple_becomes_list(self) -> None:
        # The mapper's typing wants a list-like iterable; tuples
        # become lists for downstream stability.
        assert _coerce_args((1, "x")) == [1, "x"]

    def test_repr_tuple_string_parsed(self) -> None:
        # Kombu's ``str(task.args)`` emits a tuple-repr for the
        # positional args of a regular Celery task.
        assert _coerce_args("(42, 'hello')") == [42, "hello"]

    def test_repr_list_string_parsed(self) -> None:
        assert _coerce_args("[1, 2, 3]") == [1, 2, 3]

    def test_malformed_string_returns_none(self) -> None:
        assert _coerce_args("(1,") is None
        assert _coerce_args("not args") is None

    def test_dict_string_returns_none(self) -> None:
        # Parses to a dict, which is not a list - drop.
        assert _coerce_args("{'k': 'v'}") is None
