"""Tests for retry logic in step() — max_retries and exponential backoff.

Acceptance criteria (issue #5):
- A step that fails twice then succeeds: 3 rows in step_records, run completes.
- Backoff timing is respected (tested with a mock sleep).
- All retries exhausted: original exception propagates.
- status() shows all attempt rows for a retried step.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch

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
    run_id = store.create_run("test_workflow")
    token = set_current_run(store, run_id)
    yield store, run_id
    reset_current_run(token)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_flaky(fail_times: int):
    """Return a function that raises ValueError the first *fail_times* calls,
    then returns 'ok'."""
    state = {"calls": 0}

    def fn() -> str:
        state["calls"] += 1
        if state["calls"] <= fail_times:
            raise ValueError(f"transient failure #{state['calls']}")
        return "ok"

    fn.state = state  # type: ignore[attr-defined]
    return fn


# ---------------------------------------------------------------------------
# Basic retry behaviour
# ---------------------------------------------------------------------------


class TestRetrySuccess:
    def test_fail_twice_succeed_third_creates_three_rows(self, run_ctx, tmp_path) -> None:
        """Acceptance criterion: step fails twice then succeeds → 3 step_records rows."""
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=2)

        with patch("workflow.step.time.sleep"):  # skip actual delays
            result = step("s", flaky, max_retries=2, base_delay=0.0)

        assert result == "ok"
        assert flaky.state["calls"] == 3

        rows = store.get_steps(run_id)
        assert len(rows) == 3
        assert rows[0].status == "failed"
        assert rows[1].status == "failed"
        assert rows[2].status == "completed"
        assert rows[0].attempt == 0
        assert rows[1].attempt == 1
        assert rows[2].attempt == 2

    def test_fail_once_succeed_second(self, run_ctx) -> None:
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=1)

        with patch("workflow.step.time.sleep"):
            result = step("s", flaky, max_retries=1, base_delay=0.0)

        assert result == "ok"
        rows = store.get_steps(run_id)
        assert len(rows) == 2
        assert rows[0].status == "failed"
        assert rows[1].status == "completed"

    def test_no_retries_needed_single_row(self, run_ctx) -> None:
        store, run_id = run_ctx
        result = step("s", lambda: 42, max_retries=3)
        rows = store.get_steps(run_id)
        assert len(rows) == 1
        assert rows[0].status == "completed"
        assert result == 42

    def test_run_completes_after_retries(self, run_ctx) -> None:
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=1)

        with patch("workflow.step.time.sleep"):
            step("s", flaky, max_retries=2, base_delay=0.0)

        # The run itself is marked completed by the engine; but the step record
        # should show "completed" on the last attempt.
        rows = store.get_steps(run_id)
        assert rows[-1].status == "completed"


# ---------------------------------------------------------------------------
# Exhausted retries
# ---------------------------------------------------------------------------


class TestRetryExhausted:
    def test_all_retries_exhausted_raises(self, run_ctx) -> None:
        flaky = make_flaky(fail_times=99)  # never succeeds

        with patch("workflow.step.time.sleep"):
            with pytest.raises(ValueError, match="transient failure"):
                step("s", flaky, max_retries=2, base_delay=0.0)

    def test_all_retries_exhausted_creates_correct_rows(self, run_ctx) -> None:
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=99)

        with patch("workflow.step.time.sleep"):
            with pytest.raises(ValueError):
                step("s", flaky, max_retries=2, base_delay=0.0)

        rows = store.get_steps(run_id)
        assert len(rows) == 3  # attempt 0, 1, 2 — all failed
        assert all(r.status == "failed" for r in rows)
        assert rows[-1].attempt == 2

    def test_zero_retries_raises_immediately(self, run_ctx) -> None:
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=99)

        with pytest.raises(ValueError):
            step("s", flaky, max_retries=0)

        rows = store.get_steps(run_id)
        assert len(rows) == 1
        assert rows[0].status == "failed"
        assert rows[0].attempt == 0

    def test_error_message_stored_on_each_attempt(self, run_ctx) -> None:
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=99)

        with patch("workflow.step.time.sleep"):
            with pytest.raises(ValueError):
                step("s", flaky, max_retries=1, base_delay=0.0)

        rows = store.get_steps(run_id)
        for row in rows:
            assert row.error is not None
            assert "ValueError" in row.error


# ---------------------------------------------------------------------------
# Backoff timing
# ---------------------------------------------------------------------------


class TestBackoffTiming:
    def test_backoff_calls_sleep_with_correct_delays(self, run_ctx) -> None:
        """sleep is called with base_delay * 2**attempt for each retry."""
        flaky = make_flaky(fail_times=2)
        sleep_calls: list[float] = []

        def record_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)

        with patch("workflow.step.time.sleep", side_effect=record_sleep):
            step("s", flaky, max_retries=2, base_delay=1.0)

        # attempt 0 fails → sleep 1.0 * 2^0 = 1.0
        # attempt 1 fails → sleep 1.0 * 2^1 = 2.0
        # attempt 2 succeeds → no sleep
        assert sleep_calls == [1.0, 2.0]

    def test_custom_base_delay(self, run_ctx) -> None:
        flaky = make_flaky(fail_times=1)
        sleep_calls: list[float] = []

        with patch("workflow.step.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            step("s", flaky, max_retries=1, base_delay=0.5)

        assert sleep_calls == [0.5]  # 0.5 * 2^0

    def test_no_sleep_when_no_retries(self, run_ctx) -> None:
        with patch("workflow.step.time.sleep") as mock_sleep:
            step("s", lambda: "ok", max_retries=0)
        mock_sleep.assert_not_called()

    def test_no_sleep_on_success_first_try(self, run_ctx) -> None:
        with patch("workflow.step.time.sleep") as mock_sleep:
            step("s", lambda: "ok", max_retries=3, base_delay=1.0)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Idempotency still works with retries
# ---------------------------------------------------------------------------


class TestRetryIdempotency:
    def test_completed_step_not_retried(self, run_ctx) -> None:
        """A COMPLETED step is still a cache hit even when max_retries > 0."""
        store, run_id = run_ctx
        call_count = {"n": 0}

        def fn() -> str:
            call_count["n"] += 1
            return "done"

        step("s", fn, max_retries=3)
        step("s", fn, max_retries=3)  # should be a cache hit

        assert call_count["n"] == 1
        assert len(store.get_steps(run_id)) == 1

    def test_input_hash_consistent_across_retries(self, run_ctx) -> None:
        """All retry attempts share the same input_hash."""
        store, run_id = run_ctx
        flaky = make_flaky(fail_times=1)

        with patch("workflow.step.time.sleep"):
            step("s", flaky, max_retries=1, base_delay=0.0)

        rows = store.get_steps(run_id)
        hashes = {r.input_hash for r in rows}
        assert len(hashes) == 1  # same hash on all attempts


# ---------------------------------------------------------------------------
# status() shows all attempt rows
# ---------------------------------------------------------------------------


class TestStatusShowsAttempts:
    def test_status_all_attempts_visible(self, tmp_path: Path) -> None:
        from workflow.engine import WorkflowEngine

        flaky = make_flaky(fail_times=2)

        with WorkflowEngine(db_path=tmp_path / "e.db") as engine:
            def wf() -> None:
                with patch("workflow.step.time.sleep"):
                    step("flaky_step", flaky, max_retries=2, base_delay=0.0)

            run_id = engine.run(wf)
            status = engine.status(run_id)

        step_rows = [s for s in status.steps if s.step_name == "flaky_step"]
        assert len(step_rows) == 3
        statuses = [r.status for r in step_rows]
        assert statuses == ["failed", "failed", "completed"]
