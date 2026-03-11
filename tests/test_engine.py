"""Unit tests for workflow.engine — WorkflowEngine.run(), resume(), status().

Acceptance criteria (issue #6):
- run() returns a run_id, workflow executes all steps.
- resume() on a completed run: all steps return cached output, func not re-executed.
- status() returns correct per-step timing after completion.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from workflow.engine import RunStatus, WorkflowEngine, WorkflowError
from workflow.step import step


# ---------------------------------------------------------------------------
# Helpers / shared workflow functions
# ---------------------------------------------------------------------------


def make_call_tracker():
    """Return a dict tracking how many times each key was called."""
    return {"counts": {}}


def counting_step(tracker: dict, name: str, value: Any) -> Any:
    tracker["counts"][name] = tracker["counts"].get(name, 0) + 1
    return value


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine(tmp_path: Path) -> WorkflowEngine:
    e = WorkflowEngine(db_path=tmp_path / "test.db")
    yield e
    e.close()


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


class TestRun:
    def test_returns_run_id_string(self, engine: WorkflowEngine) -> None:
        def wf() -> str:
            return step("s", lambda: "ok")

        run_id = engine.run(wf)
        assert isinstance(run_id, str)
        assert len(run_id) == 36

    def test_run_status_completed(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("s", lambda: None)

        run_id = engine.run(wf)
        assert engine.store.get_run(run_id).status == "completed"

    def test_workflow_receives_inputs(self, engine: WorkflowEngine) -> None:
        results = {}

        def wf(x: int, y: int) -> None:
            results["sum"] = step("add", lambda a, b: a + b, x, y)

        engine.run(wf, x=3, y=4)
        assert results["sum"] == 7

    def test_all_steps_persisted(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("a", lambda: 1)
            step("b", lambda: 2)
            step("c", lambda: 3)

        run_id = engine.run(wf)
        steps = engine.store.get_steps(run_id)
        assert {s.step_name for s in steps} == {"a", "b", "c"}
        assert all(s.status == "completed" for s in steps)

    def test_failed_workflow_marks_run_failed(self, engine: WorkflowEngine) -> None:
        """Exceptions propagate to the caller AND the run is marked failed."""
        def wf() -> None:
            raise ValueError("oops")

        with pytest.raises(ValueError, match="oops"):
            run_id = engine.run(wf)

    def test_failed_workflow_persists_failed_status(self, engine: WorkflowEngine) -> None:
        """Even though the exception propagates, the run record shows 'failed'."""
        def wf() -> None:
            raise RuntimeError("oops")

        try:
            engine.run(wf)
        except RuntimeError:
            pass

        runs = engine.store.list_runs(limit=1)
        assert runs[0].status == "failed"

    def test_run_raises_on_workflow_exception(self, engine: WorkflowEngine) -> None:
        """engine.run() re-raises exceptions from the workflow function."""
        def wf() -> None:
            raise RuntimeError("propagated")

        with pytest.raises(RuntimeError, match="propagated"):
            engine.run(wf)

    def test_multiple_runs_are_independent(self, engine: WorkflowEngine) -> None:
        def wf(label: str) -> str:
            return step("s", lambda l: l, label)

        id1 = engine.run(wf, label="first")
        id2 = engine.run(wf, label="second")
        assert id1 != id2
        assert engine.store.get_run(id1).status == "completed"
        assert engine.store.get_run(id2).status == "completed"


# ---------------------------------------------------------------------------
# resume()
# ---------------------------------------------------------------------------


class TestResume:
    def test_resume_skips_completed_steps(self, engine: WorkflowEngine) -> None:
        """After a successful run, resuming must not re-execute any step."""
        call_counts: dict[str, int] = {}

        def wf(episode_id: str) -> None:
            def track(name: str, val: Any) -> Any:
                call_counts[name] = call_counts.get(name, 0) + 1
                return val

            step("download",   track, "download",   episode_id)
            step("transcribe", track, "transcribe", "audio.mp3")
            step("summarize",  track, "summarize",  "transcript")

        run_id = engine.run(wf, episode_id="ep-1")
        # All three steps ran once.
        assert call_counts == {"download": 1, "transcribe": 1, "summarize": 1}

        engine.resume(run_id)
        # After resume, still only once — all cache hits.
        assert call_counts == {"download": 1, "transcribe": 1, "summarize": 1}

    def test_resume_returns_same_results(self, engine: WorkflowEngine) -> None:
        collected: list[str] = []

        def wf(val: str) -> None:
            collected.append(step("s", lambda v: v.upper(), val))

        run_id = engine.run(wf, val="hello")
        engine.resume(run_id)
        # Both invocations must produce the same result.
        assert collected == ["HELLO", "HELLO"]

    def test_resume_unknown_run_id_raises(self, engine: WorkflowEngine) -> None:
        with pytest.raises(WorkflowError, match="No workflow run found"):
            engine.resume("does-not-exist")

    def test_resume_unregistered_function_raises(self, engine: WorkflowEngine, tmp_path: Path) -> None:
        """If the function was run on a different engine instance, resume() raises."""
        def wf() -> None:
            step("s", lambda: None)

        run_id = engine.run(wf)

        fresh_engine = WorkflowEngine(db_path=tmp_path / "test.db")
        try:
            with pytest.raises(WorkflowError, match="not registered"):
                fresh_engine.resume(run_id)
        finally:
            fresh_engine.close()

    def test_resume_after_partial_failure(self, engine: WorkflowEngine) -> None:
        """Steps before the failure are cached; the failed step re-runs on resume."""
        call_counts: dict[str, int] = {}
        should_fail = {"flag": True}

        def wf() -> None:
            def track(name: str) -> str:
                call_counts[name] = call_counts.get(name, 0) + 1
                return name

            step("step_a", track, "step_a")

            def maybe_fail(name: str) -> str:
                call_counts[name] = call_counts.get(name, 0) + 1
                if should_fail["flag"]:
                    raise RuntimeError("transient failure")
                return name

            step("step_b", maybe_fail, "step_b")

        # First run — step_b fails; exception propagates.
        with pytest.raises(RuntimeError, match="transient failure"):
            run_id = engine.run(wf)

        # Grab the run_id from the store (most recent run).
        runs = engine.store.list_runs(limit=1)
        run_id = runs[0].id
        assert call_counts == {"step_a": 1, "step_b": 1}
        assert engine.store.get_run(run_id).status == "failed"

        # Fix the transient failure and resume.
        should_fail["flag"] = False
        engine.resume(run_id)

        # step_a: cached (still 1); step_b: re-ran once more (now 2).
        assert call_counts["step_a"] == 1
        assert call_counts["step_b"] == 2
        assert engine.store.get_run(run_id).status == "completed"


# ---------------------------------------------------------------------------
# status()
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_returns_run_status_object(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("s", lambda: 42)

        run_id = engine.run(wf)
        s = engine.status(run_id)
        assert isinstance(s, RunStatus)

    def test_status_run_is_completed(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("s", lambda: 1)

        run_id = engine.run(wf)
        assert engine.status(run_id).is_completed

    def test_status_contains_all_steps(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("alpha", lambda: 1)
            step("beta", lambda: 2)

        run_id = engine.run(wf)
        s = engine.status(run_id)
        assert {st.step_name for st in s.steps} == {"alpha", "beta"}

    def test_status_step_timing_present(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("s", lambda: None)

        run_id = engine.run(wf)
        s = engine.status(run_id)
        for st in s.steps:
            assert st.started_at is not None
            assert st.finished_at is not None

    def test_status_unknown_run_raises(self, engine: WorkflowEngine) -> None:
        with pytest.raises(WorkflowError, match="No workflow run found"):
            engine.status("does-not-exist")

    def test_status_properties(self, engine: WorkflowEngine) -> None:
        def wf() -> None:
            step("s", lambda: None)

        run_id = engine.run(wf)
        s = engine.status(run_id)
        assert s.is_completed is True
        assert s.is_failed is False
        assert s.is_running is False


# ---------------------------------------------------------------------------
# register() decorator
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_allows_resume(self, engine: WorkflowEngine, tmp_path: Path) -> None:
        """Functions registered on a second engine instance can be resumed."""
        call_counts: dict[str, int] = {}

        def wf(val: str) -> None:
            def track(v: str) -> str:
                call_counts[v] = call_counts.get(v, 0) + 1
                return v

            step("s", track, val)

        run_id = engine.run(wf, val="x")

        second = WorkflowEngine(db_path=tmp_path / "test.db")
        second.register(wf)
        try:
            second.resume(run_id)
            # Cache hit — still only 1 execution.
            assert call_counts["x"] == 1
        finally:
            second.close()

    def test_register_as_decorator(self, engine: WorkflowEngine) -> None:
        @engine.register
        def my_wf() -> str:
            return step("s", lambda: "decorated")

        run_id = engine.run(my_wf)
        assert engine.status(run_id).is_completed


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_context_manager(self, tmp_path: Path) -> None:
        with WorkflowEngine(db_path=tmp_path / "cm.db") as eng:
            def wf() -> None:
                step("s", lambda: None)

            run_id = eng.run(wf)
            assert eng.status(run_id).is_completed
