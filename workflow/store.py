"""SQLite-backed persistence layer for workflow runs and step records.

All durability guarantees flow through this module.
No business logic lives here — only CRUD and schema management.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Dataclasses (plain data — no DB logic)
# ---------------------------------------------------------------------------


@dataclass
class RunRecord:
    id: str
    workflow_name: str
    input_json: str | None
    status: str  # running | completed | failed
    created_at: datetime
    finished_at: datetime | None


@dataclass
class StepRecord:
    id: int | None
    run_id: str
    step_name: str
    attempt: int
    status: str  # pending | running | completed | failed
    input_hash: str | None
    output: bytes | None  # pickled return value
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_runs (
    id            TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    input_json    TEXT,
    status        TEXT NOT NULL DEFAULT 'running',
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS step_records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT    NOT NULL,
    step_name   TEXT    NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 0,
    status      TEXT    NOT NULL DEFAULT 'pending',
    input_hash  TEXT,
    output      BLOB,
    error       TEXT,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    UNIQUE (run_id, step_name, attempt),
    FOREIGN KEY (run_id) REFERENCES workflow_runs (id)
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    # SQLite stores timestamps as strings; handle both formats.
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised timestamp format: {value!r}")


def _row_to_run(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        id=row["id"],
        workflow_name=row["workflow_name"],
        input_json=row["input_json"],
        status=row["status"],
        created_at=_parse_ts(row["created_at"]),  # type: ignore[arg-type]
        finished_at=_parse_ts(row["finished_at"]),
    )


def _row_to_step(row: sqlite3.Row) -> StepRecord:
    return StepRecord(
        id=row["id"],
        run_id=row["run_id"],
        step_name=row["step_name"],
        attempt=row["attempt"],
        status=row["status"],
        input_hash=row["input_hash"],
        output=row["output"],
        error=row["error"],
        started_at=_parse_ts(row["started_at"]),
        finished_at=_parse_ts(row["finished_at"]),
    )


# ---------------------------------------------------------------------------
# WorkflowStore
# ---------------------------------------------------------------------------


class WorkflowStore:
    """SQLite-backed store for workflow runs and step records.

    Creates the database file (and any parent directories) on first use.

    Args:
        db_path: Path to the SQLite database file.
                 Defaults to ``~/.wf/runs.db``.
    """

    def __init__(self, db_path: str | Path = "~/.wf/runs.db") -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # Enable WAL for better concurrency; enforce FK constraints.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Workflow run operations
    # ------------------------------------------------------------------

    def create_run(self, workflow_name: str, input_json: str | None = None) -> str:
        """Insert a new workflow run record and return its run_id (uuid4)."""
        import uuid

        run_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO workflow_runs (id, workflow_name, input_json, status)
            VALUES (?, ?, ?, 'running')
            """,
            (run_id, workflow_name, input_json),
        )
        self._conn.commit()
        return run_id

    def update_run_status(self, run_id: str, status: str) -> None:
        """Update the status (and finished_at when terminal) for a run."""
        finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") if status in ("completed", "failed") else None
        self._conn.execute(
            "UPDATE workflow_runs SET status = ?, finished_at = ? WHERE id = ?",
            (status, finished_at, run_id),
        )
        self._conn.commit()

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for *run_id*.

        Raises:
            KeyError: if no run with that id exists.
        """
        row = self._conn.execute(
            "SELECT * FROM workflow_runs WHERE id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"No workflow run with id {run_id!r}")
        return _row_to_run(row)

    def list_runs(self, limit: int = 50) -> list[RunRecord]:
        """Return the most recent *limit* runs, newest first."""
        rows = self._conn.execute(
            "SELECT * FROM workflow_runs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [_row_to_run(r) for r in rows]

    # ------------------------------------------------------------------
    # Step record operations
    # ------------------------------------------------------------------

    def write_step(
        self,
        run_id: str,
        step_name: str,
        attempt: int,
        status: str,
        input_hash: str | None = None,
        output: bytes | None = None,
        error: str | None = None,
    ) -> None:
        """Insert or replace a step record.

        Uses INSERT OR REPLACE so callers can call this both to create a
        RUNNING record and later to update it to COMPLETED / FAILED.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        finished_at = now if status in ("completed", "failed") else None
        self._conn.execute(
            """
            INSERT INTO step_records
                (run_id, step_name, attempt, status, input_hash, output, error,
                 started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, step_name, attempt) DO UPDATE SET
                status      = excluded.status,
                input_hash  = excluded.input_hash,
                output      = excluded.output,
                error       = excluded.error,
                finished_at = excluded.finished_at
            """,
            (run_id, step_name, attempt, status, input_hash, output, error, now, finished_at),
        )
        self._conn.commit()

    def get_step(self, run_id: str, step_name: str) -> StepRecord | None:
        """Return the latest attempt for *step_name* in *run_id*, or None."""
        row = self._conn.execute(
            """
            SELECT * FROM step_records
            WHERE run_id = ? AND step_name = ?
            ORDER BY attempt DESC
            LIMIT 1
            """,
            (run_id, step_name),
        ).fetchone()
        return _row_to_step(row) if row is not None else None

    def get_steps(self, run_id: str) -> list[StepRecord]:
        """Return all step records for *run_id*, ordered by started_at."""
        rows = self._conn.execute(
            "SELECT * FROM step_records WHERE run_id = ? ORDER BY started_at",
            (run_id,),
        ).fetchall()
        return [_row_to_step(r) for r in rows]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()

    def __enter__(self) -> "WorkflowStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
