"""Unit tests for workflow.store — SQLite persistence layer.

Acceptance criteria (issue #3):
- create run, write 3 steps, read them back
- UNIQUE constraint raises on exact duplicate (same run_id, step_name, attempt)
"""

import pickle
import sqlite3
import tempfile
from pathlib import Path

import pytest

from workflow.store import WorkflowStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path) -> WorkflowStore:
    """A fresh WorkflowStore backed by a temporary SQLite file."""
    s = WorkflowStore(db_path=tmp_path / "test_runs.db")
    yield s
    s.close()


# ---------------------------------------------------------------------------
# Workflow run tests
# ---------------------------------------------------------------------------


class TestCreateRun:
    def test_returns_string_uuid(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        assert isinstance(run_id, str)
        assert len(run_id) == 36  # uuid4 canonical form

    def test_default_status_is_running(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        run = store.get_run(run_id)
        assert run.status == "running"

    def test_stores_workflow_name(self, store: WorkflowStore) -> None:
        run_id = store.create_run("podcast_pipeline")
        run = store.get_run(run_id)
        assert run.workflow_name == "podcast_pipeline"

    def test_stores_input_json(self, store: WorkflowStore) -> None:
        import json

        payload = json.dumps({"episode_id": "ep-42"})
        run_id = store.create_run("podcast_pipeline", input_json=payload)
        run = store.get_run(run_id)
        assert run.input_json == payload

    def test_finished_at_is_none_initially(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        run = store.get_run(run_id)
        assert run.finished_at is None


class TestUpdateRunStatus:
    def test_marks_completed(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        store.update_run_status(run_id, "completed")
        assert store.get_run(run_id).status == "completed"

    def test_marks_failed(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        store.update_run_status(run_id, "failed")
        assert store.get_run(run_id).status == "failed"

    def test_sets_finished_at_on_terminal_status(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        store.update_run_status(run_id, "completed")
        assert store.get_run(run_id).finished_at is not None

    def test_finished_at_remains_none_for_non_terminal(self, store: WorkflowStore) -> None:
        run_id = store.create_run("my_workflow")
        store.update_run_status(run_id, "running")
        assert store.get_run(run_id).finished_at is None


class TestGetRun:
    def test_raises_keyerror_for_unknown_id(self, store: WorkflowStore) -> None:
        with pytest.raises(KeyError):
            store.get_run("does-not-exist")


class TestListRuns:
    def test_returns_all_runs(self, store: WorkflowStore) -> None:
        for name in ("wf_a", "wf_b", "wf_c"):
            store.create_run(name)
        runs = store.list_runs()
        assert len(runs) == 3

    def test_respects_limit(self, store: WorkflowStore) -> None:
        for _ in range(5):
            store.create_run("wf")
        runs = store.list_runs(limit=2)
        assert len(runs) == 2


# ---------------------------------------------------------------------------
# Step record tests
# ---------------------------------------------------------------------------


class TestWriteStep:
    def test_write_and_read_back(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        store.write_step(run_id, "download", attempt=0, status="completed")
        step = store.get_step(run_id, "download")
        assert step is not None
        assert step.status == "completed"

    def test_write_three_steps_read_all_back(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        for name in ("download", "transcribe", "summarize"):
            store.write_step(run_id, name, attempt=0, status="completed")
        steps = store.get_steps(run_id)
        assert len(steps) == 3
        assert {s.step_name for s in steps} == {"download", "transcribe", "summarize"}

    def test_stores_pickled_output(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        payload = pickle.dumps({"result": 42})
        store.write_step(run_id, "compute", attempt=0, status="completed", output=payload)
        step = store.get_step(run_id, "compute")
        assert step is not None
        assert pickle.loads(step.output) == {"result": 42}

    def test_stores_input_hash(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        store.write_step(run_id, "compute", attempt=0, status="completed", input_hash="abc123")
        step = store.get_step(run_id, "compute")
        assert step is not None
        assert step.input_hash == "abc123"

    def test_stores_error_message(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        store.write_step(run_id, "download", attempt=0, status="failed", error="ConnectionError")
        step = store.get_step(run_id, "download")
        assert step is not None
        assert step.error == "ConnectionError"

    def test_update_status_via_upsert(self, store: WorkflowStore) -> None:
        """write_step is idempotent: calling it again updates the record."""
        run_id = store.create_run("wf")
        store.write_step(run_id, "download", attempt=0, status="running")
        store.write_step(run_id, "download", attempt=0, status="completed")
        step = store.get_step(run_id, "download")
        assert step is not None
        assert step.status == "completed"
        # Only one record should exist (upsert, not duplicate insert)
        assert len(store.get_steps(run_id)) == 1

    def test_finished_at_set_for_completed(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        store.write_step(run_id, "s", attempt=0, status="completed")
        step = store.get_step(run_id, "s")
        assert step is not None
        assert step.finished_at is not None

    def test_finished_at_none_for_running(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        store.write_step(run_id, "s", attempt=0, status="running")
        step = store.get_step(run_id, "s")
        assert step is not None
        assert step.finished_at is None


class TestUniqueConstraint:
    def test_duplicate_raises(self, store: WorkflowStore) -> None:
        """Directly inserting duplicate (run_id, step_name, attempt) should raise.

        write_step uses ON CONFLICT DO UPDATE, so we bypass it here and insert
        twice via the raw connection to prove the UNIQUE constraint exists.
        """
        run_id = store.create_run("wf")
        now = "2026-01-01 00:00:00.000000"
        store._conn.execute(
            """
            INSERT INTO step_records
                (run_id, step_name, attempt, status, started_at)
            VALUES (?, 'download', 0, 'running', ?)
            """,
            (run_id, now),
        )
        store._conn.commit()

        with pytest.raises(sqlite3.IntegrityError):
            store._conn.execute(
                """
                INSERT INTO step_records
                    (run_id, step_name, attempt, status, started_at)
                VALUES (?, 'download', 0, 'completed', ?)
                """,
                (run_id, now),
            )
            store._conn.commit()


class TestGetStep:
    def test_returns_none_for_unknown_step(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        assert store.get_step(run_id, "nonexistent") is None

    def test_returns_latest_attempt(self, store: WorkflowStore) -> None:
        run_id = store.create_run("wf")
        store.write_step(run_id, "download", attempt=0, status="failed")
        store.write_step(run_id, "download", attempt=1, status="completed")
        step = store.get_step(run_id, "download")
        assert step is not None
        assert step.attempt == 1
        assert step.status == "completed"


class TestContextManager:
    def test_context_manager(self, tmp_path: Path) -> None:
        with WorkflowStore(db_path=tmp_path / "cm.db") as store:
            run_id = store.create_run("wf")
            assert store.get_run(run_id).status == "running"
