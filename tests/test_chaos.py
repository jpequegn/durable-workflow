"""Chaos test — 10 random crash/resume cycles, 0 double-executions.

This is the acceptance test for the whole engine.

The engine's durability guarantee
-----------------------------------
- Steps that **completed** before a crash are **never re-executed** on resume.
- Only the failed step (and any steps after it that never ran) execute on resume.

So "0 double-executions" means:
- For every step that was COMPLETED before the crash, execution_count == 1.
- The crashed step runs once (fails) and once more (succeeds on resume) → count == 2.
- Steps after the crash run once (on resume) → count == 1.

Design
------
For each of 10 iterations:

1. Pick a random crash point (step 1–5).
2. Run a 5-step workflow that raises ``SimulatedCrash`` at that step.
3. Confirm the run is marked ``failed``.
4. Resume the run.
5. Confirm the run is now ``completed``.
6. Assert:
   - Steps 1 … crash_at-1 (completed before crash): executed exactly **once**.
   - Step crash_at: executed exactly **twice** (failed + resumed).
   - Steps crash_at+1 … 5: executed exactly **once** (only on resume).

Acceptance criteria (issue #1)
-------------------------------
- 10/10 iterations pass.
- 0 steps that were already COMPLETED are re-executed on resume.
"""

from __future__ import annotations

import random
from collections import defaultdict
from pathlib import Path

import pytest

from workflow.engine import WorkflowEngine
from workflow.step import step


# ---------------------------------------------------------------------------
# SimulatedCrash
# ---------------------------------------------------------------------------


class SimulatedCrash(Exception):
    """Raised intentionally to simulate a mid-workflow process crash."""


# ---------------------------------------------------------------------------
# Five-step workflow factory
# ---------------------------------------------------------------------------


def make_five_step_workflow(
    execution_counts: dict[str, int],
    crash_at: int,
    crashed: dict[str, bool],
):
    """Return a workflow function that:

    - Counts every execution of each step in *execution_counts*.
    - Raises SimulatedCrash at step *crash_at* on the **first** call to that
      step only (so resume succeeds after the crash is "fixed").
    """

    def five_step_workflow() -> str:
        for i in range(1, 6):
            name = f"step_{i}"

            def make_fn(n: str, idx: int):
                def fn() -> str:
                    execution_counts[n] += 1
                    if idx == crash_at and not crashed["done"]:
                        crashed["done"] = True
                        raise SimulatedCrash(f"Crash injected at {n}")
                    return f"result_{n}"
                return fn

            step(name, make_fn(name, i))

        return "done"

    return five_step_workflow


# ---------------------------------------------------------------------------
# Chaos test
# ---------------------------------------------------------------------------


class TestChaos:
    def test_10_random_crash_resume_cycles_zero_double_executions(
        self, tmp_path: Path
    ) -> None:
        """10/10 iterations, 0 already-completed steps re-executed on resume."""
        rng = random.Random(2026)  # fixed seed for reproducibility
        completed_steps_re_executed = 0

        for iteration in range(10):
            crash_at = rng.randint(1, 5)
            execution_counts: dict[str, int] = defaultdict(int)
            crashed = {"done": False}

            wf = make_five_step_workflow(execution_counts, crash_at, crashed)
            wf.__name__ = f"five_step_workflow_iter_{iteration}"

            db_path = tmp_path / f"chaos_{iteration}.db"

            with WorkflowEngine(db_path=db_path) as engine:
                # --- First run: crashes at step crash_at -------------------
                with pytest.raises(SimulatedCrash):
                    engine.run(wf)

                runs = engine.store.list_runs(limit=1)
                run_id = runs[0].id
                assert engine.store.get_run(run_id).status == "failed", (
                    f"iter={iteration} crash_at={crash_at}: expected failed"
                )

                # Steps before crash_at must be completed in the store.
                steps_before = engine.store.get_steps(run_id)
                completed_names = {s.step_name for s in steps_before if s.status == "completed"}
                for i in range(1, crash_at):
                    assert f"step_{i}" in completed_names, (
                        f"iter={iteration}: step_{i} should be completed before crash"
                    )

                # --- Resume: only failed/missing steps re-execute ----------
                engine.resume(run_id)

                assert engine.store.get_run(run_id).status == "completed", (
                    f"iter={iteration} crash_at={crash_at}: expected completed after resume"
                )

                # --- Assert durability guarantee ---------------------------
                # Steps BEFORE crash_at: COMPLETED before crash → must run exactly once.
                for i in range(1, crash_at):
                    name = f"step_{i}"
                    count = execution_counts[name]
                    if count != 1:
                        completed_steps_re_executed += 1
                    assert count == 1, (
                        f"iter={iteration} crash_at={crash_at}: "
                        f"{name} was completed before crash but ran {count} times "
                        f"(expected 1 — cache should have prevented re-execution)"
                    )

                # The crashed step: runs once (fails) + once (succeeds on resume) = 2.
                crashed_step = f"step_{crash_at}"
                assert execution_counts[crashed_step] == 2, (
                    f"iter={iteration} crash_at={crash_at}: "
                    f"{crashed_step} ran {execution_counts[crashed_step]} times "
                    f"(expected 2: fail + resume)"
                )

                # Steps AFTER crash_at: never ran before crash → run once on resume.
                for i in range(crash_at + 1, 6):
                    name = f"step_{i}"
                    count = execution_counts[name]
                    assert count == 1, (
                        f"iter={iteration} crash_at={crash_at}: "
                        f"{name} ran {count} times (expected 1)"
                    )

        assert completed_steps_re_executed == 0, (
            f"{completed_steps_re_executed} already-completed steps were "
            f"re-executed across 10 iterations"
        )

    def test_crash_at_every_step_position(self, tmp_path: Path) -> None:
        """Deterministic: crash at each of steps 1–5, verify no completed-step double-exec."""
        for crash_at in range(1, 6):
            execution_counts: dict[str, int] = defaultdict(int)
            crashed = {"done": False}

            wf = make_five_step_workflow(execution_counts, crash_at, crashed)
            wf.__name__ = f"five_step_wf_crash_{crash_at}"

            db_path = tmp_path / f"det_{crash_at}.db"

            with WorkflowEngine(db_path=db_path) as engine:
                with pytest.raises(SimulatedCrash):
                    engine.run(wf)

                runs = engine.store.list_runs(limit=1)
                run_id = runs[0].id
                engine.resume(run_id)

                assert engine.store.get_run(run_id).status == "completed", (
                    f"crash_at={crash_at}: run not completed after resume"
                )

                # Steps before crash: must be exactly 1 execution each (cache hit on resume).
                for i in range(1, crash_at):
                    name = f"step_{i}"
                    assert execution_counts[name] == 1, (
                        f"crash_at={crash_at}: {name} (completed before crash) "
                        f"executed {execution_counts[name]} times"
                    )

                # Crashed step: 2 executions (fail + resume success).
                crashed_step = f"step_{crash_at}"
                assert execution_counts[crashed_step] == 2, (
                    f"crash_at={crash_at}: {crashed_step} ran "
                    f"{execution_counts[crashed_step]} times (expected 2)"
                )

                # Steps after crash: 1 execution (only on resume).
                for i in range(crash_at + 1, 6):
                    name = f"step_{i}"
                    assert execution_counts[name] == 1, (
                        f"crash_at={crash_at}: {name} ran "
                        f"{execution_counts[name]} times (expected 1)"
                    )

    def test_double_resume_is_idempotent(self, tmp_path: Path) -> None:
        """Resuming an already-completed run must not re-execute any step."""
        execution_counts: dict[str, int] = defaultdict(int)
        crashed = {"done": False}

        wf = make_five_step_workflow(execution_counts, crash_at=3, crashed=crashed)
        wf.__name__ = "idempotent_wf"

        with WorkflowEngine(db_path=tmp_path / "idem.db") as engine:
            with pytest.raises(SimulatedCrash):
                engine.run(wf)

            runs = engine.store.list_runs(limit=1)
            run_id = runs[0].id

            engine.resume(run_id)   # completes the run
            counts_after_first_resume = dict(execution_counts)

            engine.resume(run_id)   # second resume — all steps are COMPLETED cache hits

            # Counts must not change after the second resume.
            for name, count in execution_counts.items():
                assert count == counts_after_first_resume[name], (
                    f"double-resume: {name} went from "
                    f"{counts_after_first_resume[name]} to {count} executions"
                )

    def test_no_completed_step_reruns_across_all_positions(self, tmp_path: Path) -> None:
        """Aggregate: sum of completed-before-crash re-executions == 0."""
        total_unwanted_reruns = 0

        for crash_at in range(1, 6):
            execution_counts: dict[str, int] = defaultdict(int)
            crashed = {"done": False}
            wf = make_five_step_workflow(execution_counts, crash_at, crashed)
            wf.__name__ = f"agg_wf_{crash_at}"

            with WorkflowEngine(db_path=tmp_path / f"agg_{crash_at}.db") as engine:
                with pytest.raises(SimulatedCrash):
                    engine.run(wf)
                runs = engine.store.list_runs(limit=1)
                engine.resume(runs[0].id)

            for i in range(1, crash_at):
                if execution_counts[f"step_{i}"] != 1:
                    total_unwanted_reruns += 1

        assert total_unwanted_reruns == 0


# ---------------------------------------------------------------------------
# Results summary (written to CHAOS_RESULTS.md)
# ---------------------------------------------------------------------------


def test_write_chaos_results(tmp_path: Path) -> None:
    """Run the chaos scenario and write a results summary to CHAOS_RESULTS.md."""
    import time

    results: list[dict] = []
    rng = random.Random(2026)

    for iteration in range(10):
        crash_at = rng.randint(1, 5)
        execution_counts: dict[str, int] = defaultdict(int)
        crashed = {"done": False}

        wf = make_five_step_workflow(execution_counts, crash_at, crashed)
        wf.__name__ = f"chaos_summary_wf_{iteration}"

        db_path = tmp_path / f"chaos_summary_{iteration}.db"
        t0 = time.perf_counter()

        with WorkflowEngine(db_path=db_path) as engine:
            try:
                engine.run(wf)
            except SimulatedCrash:
                pass

            runs = engine.store.list_runs(limit=1)
            run_id = runs[0].id
            engine.resume(run_id)

        elapsed = time.perf_counter() - t0

        # Count completed-before-crash steps that were re-executed (should be 0).
        unwanted_reruns = sum(
            1 for i in range(1, crash_at) if execution_counts[f"step_{i}"] != 1
        )
        results.append(
            {
                "iteration": iteration + 1,
                "crash_at": f"step_{crash_at}",
                "unwanted_reruns": unwanted_reruns,
                "elapsed_ms": round(elapsed * 1000, 1),
                "passed": unwanted_reruns == 0,
            }
        )

    # Write results file to the project root.
    project_root = Path(__file__).parent.parent
    out = project_root / "CHAOS_RESULTS.md"
    lines = [
        "# Chaos Test Results",
        "",
        "10 random crash/resume cycles — acceptance test for the durable workflow engine.",
        "",
        "**Property tested**: Steps that were COMPLETED before a crash are never",
        "re-executed on resume (0 unwanted re-executions).",
        "",
        "| # | Crash at | Unwanted re-execs | Time (ms) | Pass |",
        "|---|----------|-------------------|-----------|------|",
    ]
    total_unwanted = 0
    for r in results:
        icon = "✅" if r["passed"] else "❌"
        lines.append(
            f"| {r['iteration']} | {r['crash_at']} | {r['unwanted_reruns']} "
            f"| {r['elapsed_ms']} | {icon} |"
        )
        total_unwanted += r["unwanted_reruns"]

    lines += [
        "",
        f"**Result: {sum(1 for r in results if r['passed'])}/10 passed, "
        f"{total_unwanted} unwanted re-executions.**",
        "",
    ]
    out.write_text("\n".join(lines))

    assert total_unwanted == 0, f"{total_unwanted} unwanted re-executions detected"
