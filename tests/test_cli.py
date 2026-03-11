"""Tests for the wf CLI — all commands via Click's test runner.

Acceptance criteria (issue #7):
- wf run executes a workflow and prints a run_id.
- wf status shows per-step table with timing.
- wf resume resumes a failed run and marks it completed.
- wf runs list shows recent runs colour-coded by status.
- wf runs inspect shows the full step-by-step trace.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from workflow.cli import cli


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def db(tmp_path: Path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture
def simple_wf_file(tmp_path: Path) -> str:
    """A .py file with a WORKFLOW that runs 3 steps and succeeds."""
    p = tmp_path / "simple_wf.py"
    p.write_text(textwrap.dedent("""\
        from workflow.step import step

        def simple_workflow(label: str) -> str:
            a = step("step_a", lambda l: f"a_{l}", label)
            b = step("step_b", lambda v: v.upper(), a)
            return step("step_c", lambda v: f"done:{v}", b)

        WORKFLOW = simple_workflow
        INPUT_SCHEMA = {"label": str}
    """))
    return str(p)


@pytest.fixture
def crashing_wf_file(tmp_path: Path) -> str:
    """A .py file whose step_b always crashes."""
    p = tmp_path / "crash_wf.py"
    p.write_text(textwrap.dedent("""\
        from workflow.step import step

        def crash_workflow(label: str) -> str:
            a = step("step_a", lambda l: f"a_{l}", label)
            def boom(v):
                raise RuntimeError("boom!")
            b = step("step_b", boom, a)
            return step("step_c", lambda v: v, b)

        WORKFLOW = crash_workflow
        INPUT_SCHEMA = {"label": str}
    """))
    return str(p)


@pytest.fixture
def fixed_wf_file(tmp_path: Path) -> str:
    """Same workflow as crashing_wf_file but step_b is fixed."""
    p = tmp_path / "fixed_wf.py"
    p.write_text(textwrap.dedent("""\
        from workflow.step import step

        def crash_workflow(label: str) -> str:
            a = step("step_a", lambda l: f"a_{l}", label)
            b = step("step_b", lambda v: f"fixed_{v}", a)
            return step("step_c", lambda v: v, b)

        WORKFLOW = crash_workflow
        INPUT_SCHEMA = {"label": str}
    """))
    return str(p)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run_id_from_output(output: str) -> str:
    """Extract the uuid4 run_id from wf run output."""
    for token in output.split():
        token = token.strip(".")
        if len(token) == 36 and token.count("-") == 4:
            return token
    raise ValueError(f"No run_id found in:\n{output}")


# ---------------------------------------------------------------------------
# wf run
# ---------------------------------------------------------------------------


class TestCliRun:
    def test_run_succeeds(self, runner, db, simple_wf_file) -> None:
        result = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        assert result.exit_code == 0, result.output
        assert "Completed" in result.output

    def test_run_prints_run_id(self, runner, db, simple_wf_file) -> None:
        result = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        assert result.exit_code == 0
        run_id = _run_id_from_output(result.output)
        assert len(run_id) == 36

    def test_run_missing_workflow_var(self, runner, db, tmp_path) -> None:
        p = tmp_path / "no_wf.py"
        p.write_text("x = 1\n")
        result = runner.invoke(cli, ["run", str(p), "--db", db])
        assert result.exit_code != 0
        assert "WORKFLOW" in result.output

    def test_run_nonexistent_file(self, runner, db) -> None:
        result = runner.invoke(cli, ["run", "/does/not/exist.py", "--db", db])
        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_run_failed_workflow_exits_nonzero(self, runner, db, crashing_wf_file) -> None:
        result = runner.invoke(cli, ["run", crashing_wf_file, "--label", "x", "--db", db])
        assert result.exit_code != 0
        assert "failed" in result.output.lower()

    def test_run_kebab_flag_converted_to_underscore(self, runner, db, tmp_path) -> None:
        p = tmp_path / "kw_wf.py"
        p.write_text(textwrap.dedent("""\
            from workflow.step import step
            def kw_workflow(my_key: str) -> str:
                return step("s", lambda v: v, my_key)
            WORKFLOW = kw_workflow
            INPUT_SCHEMA = {"my_key": str}
        """))
        result = runner.invoke(cli, ["run", str(p), "--my-key", "val", "--db", db])
        assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# wf status
# ---------------------------------------------------------------------------


class TestCliStatus:
    def test_status_shows_completed(self, runner, db, simple_wf_file) -> None:
        r = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        run_id = _run_id_from_output(r.output)
        result = runner.invoke(cli, ["status", run_id, "--db", db])
        assert result.exit_code == 0
        assert "completed" in result.output

    def test_status_shows_all_step_names(self, runner, db, simple_wf_file) -> None:
        r = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        run_id = _run_id_from_output(r.output)
        result = runner.invoke(cli, ["status", run_id, "--db", db])
        assert "step_a" in result.output
        assert "step_b" in result.output
        assert "step_c" in result.output

    def test_status_shows_timing(self, runner, db, simple_wf_file) -> None:
        r = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        run_id = _run_id_from_output(r.output)
        result = runner.invoke(cli, ["status", run_id, "--db", db])
        # Duration column header present
        assert "DURATION" in result.output

    def test_status_unknown_run_id(self, runner, db) -> None:
        result = runner.invoke(cli, ["status", "no-such-id", "--db", db])
        assert result.exit_code != 0

    def test_status_failed_step_shows_error(self, runner, db, crashing_wf_file) -> None:
        runner.invoke(cli, ["run", crashing_wf_file, "--label", "x", "--db", db])
        from workflow.engine import WorkflowEngine
        with WorkflowEngine(db_path=db) as eng:
            runs = eng.store.list_runs(limit=1)
        run_id = runs[0].id
        result = runner.invoke(cli, ["status", run_id, "--db", db])
        assert "failed" in result.output
        assert "boom" in result.output


# ---------------------------------------------------------------------------
# wf resume
# ---------------------------------------------------------------------------


class TestCliResume:
    def _get_failed_run_id(self, runner, db, crashing_wf_file) -> str:
        runner.invoke(cli, ["run", crashing_wf_file, "--label", "x", "--db", db])
        from workflow.engine import WorkflowEngine
        with WorkflowEngine(db_path=db) as eng:
            runs = eng.store.list_runs(limit=1)
        return runs[0].id

    def test_resume_completes_run(self, runner, db, crashing_wf_file, fixed_wf_file) -> None:
        run_id = self._get_failed_run_id(runner, db, crashing_wf_file)
        result = runner.invoke(
            cli, ["resume", run_id, "--db", db, "--workflow-file", fixed_wf_file]
        )
        assert result.exit_code == 0, result.output
        assert "Completed" in result.output

    def test_resume_without_file_raises(self, runner, db, crashing_wf_file) -> None:
        run_id = self._get_failed_run_id(runner, db, crashing_wf_file)
        # No --workflow-file → engine has no registered function.
        result = runner.invoke(cli, ["resume", run_id, "--db", db])
        assert result.exit_code != 0
        assert "registered" in result.output

    def test_resume_unknown_run_id(self, runner, db) -> None:
        result = runner.invoke(cli, ["resume", "no-such-id", "--db", db])
        assert result.exit_code != 0

    def test_resume_skips_completed_steps(self, runner, db, crashing_wf_file, fixed_wf_file) -> None:
        run_id = self._get_failed_run_id(runner, db, crashing_wf_file)
        runner.invoke(cli, ["resume", run_id, "--db", db, "--workflow-file", fixed_wf_file])
        from workflow.engine import WorkflowEngine
        with WorkflowEngine(db_path=db) as eng:
            steps = eng.store.get_steps(run_id)
        # step_a was completed before crash — still only 1 row for it.
        step_a_rows = [s for s in steps if s.step_name == "step_a"]
        assert len(step_a_rows) == 1
        assert step_a_rows[0].status == "completed"


# ---------------------------------------------------------------------------
# wf runs list
# ---------------------------------------------------------------------------


class TestCliRunsList:
    def test_runs_list_shows_run(self, runner, db, simple_wf_file) -> None:
        runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        result = runner.invoke(cli, ["runs", "list", "--db", db])
        assert result.exit_code == 0
        assert "simple_workflow" in result.output

    def test_runs_list_empty(self, runner, db) -> None:
        result = runner.invoke(cli, ["runs", "list", "--db", db])
        assert result.exit_code == 0
        assert "No workflow runs found" in result.output

    def test_runs_list_limit(self, runner, db, simple_wf_file) -> None:
        for i in range(5):
            runner.invoke(cli, ["run", simple_wf_file, f"--label", f"l{i}", "--db", db])
        result = runner.invoke(cli, ["runs", "list", "--db", db, "-n", "2"])
        assert result.exit_code == 0
        assert "2 run(s)" in result.output

    def test_runs_list_shows_status(self, runner, db, simple_wf_file, crashing_wf_file) -> None:
        runner.invoke(cli, ["run", simple_wf_file, "--label", "ok", "--db", db])
        runner.invoke(cli, ["run", crashing_wf_file, "--label", "x", "--db", db])
        result = runner.invoke(cli, ["runs", "list", "--db", db])
        assert "completed" in result.output
        assert "failed" in result.output


# ---------------------------------------------------------------------------
# wf runs inspect
# ---------------------------------------------------------------------------


class TestCliRunsInspect:
    def test_inspect_shows_all_steps(self, runner, db, simple_wf_file) -> None:
        r = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        run_id = _run_id_from_output(r.output)
        result = runner.invoke(cli, ["runs", "inspect", run_id, "--db", db])
        assert result.exit_code == 0
        assert "step_a" in result.output
        assert "step_b" in result.output
        assert "step_c" in result.output

    def test_inspect_shows_error_on_failed_step(self, runner, db, crashing_wf_file) -> None:
        runner.invoke(cli, ["run", crashing_wf_file, "--label", "x", "--db", db])
        from workflow.engine import WorkflowEngine
        with WorkflowEngine(db_path=db) as eng:
            runs = eng.store.list_runs(limit=1)
        run_id = runs[0].id
        result = runner.invoke(cli, ["runs", "inspect", run_id, "--db", db])
        assert "boom" in result.output
        assert "RuntimeError" in result.output

    def test_inspect_show_output_flag(self, runner, db, simple_wf_file) -> None:
        r = runner.invoke(cli, ["run", simple_wf_file, "--label", "hi", "--db", db])
        run_id = _run_id_from_output(r.output)
        result = runner.invoke(
            cli, ["runs", "inspect", run_id, "--db", db, "--show-output"]
        )
        assert result.exit_code == 0
        assert "output" in result.output

    def test_inspect_unknown_run_id(self, runner, db) -> None:
        result = runner.invoke(cli, ["runs", "inspect", "no-such-id", "--db", db])
        assert result.exit_code != 0

    def test_inspect_all_steps_visible_after_resume(
        self, runner, db, crashing_wf_file, fixed_wf_file
    ) -> None:
        """Acceptance criterion: crash, resume, inspect — all steps visible."""
        # Crash
        runner.invoke(cli, ["run", crashing_wf_file, "--label", "x", "--db", db])
        from workflow.engine import WorkflowEngine
        with WorkflowEngine(db_path=db) as eng:
            runs = eng.store.list_runs(limit=1)
        run_id = runs[0].id

        # Resume
        runner.invoke(cli, ["resume", run_id, "--db", db, "--workflow-file", fixed_wf_file])

        # Inspect
        result = runner.invoke(cli, ["runs", "inspect", run_id, "--db", db])
        assert result.exit_code == 0
        assert "step_a" in result.output
        assert "step_b" in result.output
        assert "step_c" in result.output
        # Both the failed attempt and the successful retry are shown
        assert "failed" in result.output
        assert "completed" in result.output
