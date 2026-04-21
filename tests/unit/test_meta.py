"""Unit tests for ``z4j_celery.meta``."""

from __future__ import annotations

import pytest

from z4j_celery.meta import META_ATTR, TaskMeta, get_meta, z4j_meta


class TestBasicBehavior:
    def test_decorator_returns_function_unchanged(self) -> None:
        @z4j_meta(redact_kwargs=["email"])
        def my_task(a: int) -> int:
            return a * 2

        assert my_task(3) == 6  # decorator is a no-op at call time

    def test_attribute_is_set(self) -> None:
        @z4j_meta(redact_kwargs=["email"])
        def my_task() -> None:
            pass

        assert hasattr(my_task, META_ATTR)
        meta = getattr(my_task, META_ATTR)
        assert isinstance(meta, TaskMeta)


class TestFields:
    def test_redact_kwargs_is_frozenset(self) -> None:
        @z4j_meta(redact_kwargs=["email", "phone"])
        def t() -> None:
            pass

        meta = get_meta(t)
        assert meta is not None
        assert meta.redact_kwargs == frozenset({"email", "phone"})

    def test_keep_kwargs_defaults_to_none(self) -> None:
        @z4j_meta()
        def t() -> None:
            pass

        assert get_meta(t).keep_kwargs is None  # type: ignore[union-attr]

    def test_keep_kwargs_is_frozenset_when_set(self) -> None:
        @z4j_meta(keep_kwargs=["user_id"])
        def t() -> None:
            pass

        meta = get_meta(t)
        assert meta is not None
        assert meta.keep_kwargs == frozenset({"user_id"})

    def test_tags_is_tuple(self) -> None:
        @z4j_meta(tags=["a", "b"])
        def t() -> None:
            pass

        assert get_meta(t).tags == ("a", "b")  # type: ignore[union-attr]

    def test_deadline_and_expected_duration(self) -> None:
        @z4j_meta(expected_duration_ms=500, deadline_ms=5000)
        def t() -> None:
            pass

        meta = get_meta(t)
        assert meta is not None
        assert meta.expected_duration_ms == 500
        assert meta.deadline_ms == 5000

    def test_skip_and_sample_rate(self) -> None:
        @z4j_meta(skip=True, sample_rate=0.25)
        def t() -> None:
            pass

        meta = get_meta(t)
        assert meta is not None
        assert meta.skip is True
        assert meta.sample_rate == pytest.approx(0.25)


class TestValidation:
    def test_sample_rate_below_zero_raises(self) -> None:
        with pytest.raises(ValueError, match=r"\[0.0, 1.0\]"):
            z4j_meta(sample_rate=-0.1)

    def test_sample_rate_above_one_raises(self) -> None:
        with pytest.raises(ValueError, match=r"\[0.0, 1.0\]"):
            z4j_meta(sample_rate=1.1)

    def test_sample_rate_exactly_zero_allowed(self) -> None:
        @z4j_meta(sample_rate=0.0)
        def t() -> None:
            pass

        assert get_meta(t).sample_rate == 0.0  # type: ignore[union-attr]

    def test_sample_rate_exactly_one_allowed(self) -> None:
        @z4j_meta(sample_rate=1.0)
        def t() -> None:
            pass

        assert get_meta(t).sample_rate == 1.0  # type: ignore[union-attr]


class TestGetMeta:
    def test_get_meta_returns_none_for_undecorated(self) -> None:
        def plain() -> None:
            pass

        assert get_meta(plain) is None

    def test_get_meta_returns_none_for_none(self) -> None:
        assert get_meta(None) is None

    def test_get_meta_reads_from_task_run_attribute(self) -> None:
        class FakeCeleryTask:
            pass

        task = FakeCeleryTask()

        @z4j_meta(tags=["a"])
        def body() -> None:
            pass

        task.run = body  # type: ignore[attr-defined]
        meta = get_meta(task)
        assert meta is not None
        assert meta.tags == ("a",)
