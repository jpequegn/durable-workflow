"""Unit tests for workflow.step — the core step() primitive.

Acceptance criteria (issue #2):
- Same inputs twice → function body executes exactly once (cache hit).
- Different inputs   → function executes both times (different hash).
- Exception in step  → stored as FAILED, exception propagates to caller.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import pytest

from workflow.step import reset_current_run, set_current_run, step
from workflow.store import WorkflowStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path) -> WorkflowStore:
    s = WorkflowStore(db_path=tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def run_ctx(store: WorkflowStore):
    """Create a run and install it as the active context; clean up after."""
    run_id = store.create_run("test_workflow")
    token = set_current_run(store, run_id)
    yield store, run_id
    reset_current_run(token)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_counter_func():
    """Return a function that records every call and its argument."""
    calls: list[Any] = []

    def fn(value: Any) -> Any:
        calls.append(value)
        return value

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


class BoomError(Exception):
    pass


def boom(msg: str = "boom") -> str:
    raise BoomError(msg)


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------


class TestStepBasicExecution:
    def test_executes_func_and_returns_result(self, run_ctx) -> None:
        _, run_id = run_ctx
        result = step("add", lambda a, b: a + b, 2, 3)
        assert result == 5

    def test_result_is_persisted_as_completed(self, run_ctx) -> None:
        store, run_id = run_ctx
        step("compute", lambda: 42)
        record = store.get_step(run_id, "compute")
        assert record is not None
        assert record.status == "completed"
        assert pickle.loads(record.output) == 42

    def test_input_hash_is_stored(self, run_ctx) -> None:
        store, run_id = run_ctx
        step("compute", lambda x: x, 99)
        record = store.get_step(run_id, "compute")
        assert record is not None
        assert record.input_hash is not None
        assert len(record.input_hash) == 64  # sha256 hex


# ---------------------------------------------------------------------------
# Idempotency — same inputs
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_same_inputs_executes_once(self, run_ctx) -> None:
        """Calling step() twice with the same inputs must run func only once."""
        fn = make_counter_func()
        step("s", fn, "hello")
        step("s", fn, "hello")
        assert len(fn.calls) == 1

    def test_same_inputs_returns_same_result(self, run_ctx) -> None:
        fn = make_counter_func()
        r1 = step("s", fn, "hello")
        r2 = step("s", fn, "hello")
        assert r1 == r2 == "hello"

    def test_cache_hit_preserves_stored_record(self, run_ctx) -> None:
        store, run_id = run_ctx
        step("s", lambda x: x * 2, 7)
        step("s", lambda x: x * 2, 7)  # cache hit
        # Still only one record in the store.
        records = store.get_steps(run_id)
        assert len(records) == 1

    def test_complex_return_value_survives_round_trip(self, run_ctx) -> None:
        payload = {"key": [1, 2, 3], "nested": {"a": True}}
        result = step("s", lambda d: d, payload)
        cached = step("s", lambda d: d, payload)
        assert result == cached == payload


# ---------------------------------------------------------------------------
# Different inputs → re-execute
# ---------------------------------------------------------------------------


class TestDifferentInputs:
    def test_different_inputs_executes_again(self, run_ctx) -> None:
        fn = make_counter_func()
        step("s", fn, "first")
        # Different step name — simulates a new independent step.
        step("s2", fn, "second")
        assert len(fn.calls) == 2

    def test_same_name_different_inputs_creates_new_attempt(self, run_ctx) -> None:
        """If inputs change (hash mismatch), the step runs again as a new attempt."""
        store, run_id = run_ctx
        fn = make_counter_func()
        step("s", fn, "first")
        step("s", fn, "changed")  # different hash → new attempt
        assert len(fn.calls) == 2
        record = store.get_step(run_id, "s")
        assert record is not None
        assert record.attempt == 1  # second attempt

    def test_kwargs_affect_hash(self, run_ctx) -> None:
        fn = make_counter_func()
        step("s", fn, value="a")
        step("s", fn, value="b")  # different kwargs → different hash
        assert len(fn.calls) == 2


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


class TestFailureHandling:
    def test_exception_propagates(self, run_ctx) -> None:
        with pytest.raises(BoomError):
            step("boom_step", boom)

    def test_failed_status_persisted(self, run_ctx) -> None:
        store, run_id = run_ctx
        with pytest.raises(BoomError):
            step("boom_step", boom, "kaboom")
        record = store.get_step(run_id, "boom_step")
        assert record is not None
        assert record.status == "failed"

    def test_error_message_stored(self, run_ctx) -> None:
        store, run_id = run_ctx
        with pytest.raises(BoomError):
            step("boom_step", boom, "kaboom")
        record = store.get_step(run_id, "boom_step")
        assert record is not None
        assert "BoomError" in record.error
        assert "kaboom" in record.error

    def test_no_output_on_failure(self, run_ctx) -> None:
        store, run_id = run_ctx
        with pytest.raises(BoomError):
            step("boom_step", boom)
        record = store.get_step(run_id, "boom_step")
        assert record is not None
        assert record.output is None


# ---------------------------------------------------------------------------
# Context guard
# ---------------------------------------------------------------------------


class TestContextGuard:
    def test_step_outside_context_raises_runtime_error(self) -> None:
        """step() must raise RuntimeError when called without an active run."""
        # No set_current_run() → ContextVar default is None.
        with pytest.raises(RuntimeError, match="outside of a workflow execution context"):
            step("orphan", lambda: None)


# ---------------------------------------------------------------------------
# Multiple steps in a single run
# ---------------------------------------------------------------------------


class TestMultipleSteps:
    def test_three_steps_all_persisted(self, run_ctx) -> None:
        store, run_id = run_ctx
        step("a", lambda: 1)
        step("b", lambda: 2)
        step("c", lambda: 3)
        records = store.get_steps(run_id)
        names = {r.step_name for r in records}
        assert names == {"a", "b", "c"}
        assert all(r.status == "completed" for r in records)

    def test_chained_steps_pass_values(self, run_ctx) -> None:
        r1 = step("fetch", lambda: "raw_data")
        r2 = step("transform", lambda x: x.upper(), r1)
        r3 = step("load", lambda x: f"loaded:{x}", r2)
        assert r3 == "loaded:RAW_DATA"
