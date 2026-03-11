"""WorkflowEngine — public API for running and resuming durable workflows.

Usage
-----
    from workflow.engine import WorkflowEngine

    engine = WorkflowEngine()           # defaults to ~/.wf/runs.db

    # First run — returns a run_id
    run_id = engine.run(process_podcast, episode_id="ep-123")

    # Resume after a crash — completed steps are skipped automatically
    engine.resume(run_id)

    # Inspect run status
    status = engine.status(run_id)
    print(status.run.status)            # "completed"
    for step in status.steps:
        print(step.step_name, step.status, step.finished_at)
"""

from __future__ import annotations

import json
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from workflow.step import reset_current_run, set_current_run
from workflow.store import RunRecord, StepRecord, WorkflowStore


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class WorkflowError(Exception):
    """Raised for engine-level errors (unknown run_id, bad state, etc.)."""


# ---------------------------------------------------------------------------
# Status snapshot
# ---------------------------------------------------------------------------


@dataclass
class RunStatus:
    """A point-in-time snapshot of a workflow run and all its steps."""

    run: RunRecord
    steps: list[StepRecord]

    # Convenience properties
    @property
    def is_completed(self) -> bool:
        return self.run.status == "completed"

    @property
    def is_failed(self) -> bool:
        return self.run.status == "failed"

    @property
    def is_running(self) -> bool:
        return self.run.status == "running"


# ---------------------------------------------------------------------------
# WorkflowEngine
# ---------------------------------------------------------------------------


class WorkflowEngine:
    """Runs and resumes durable workflow functions.

    Args:
        db_path: Path to the SQLite database.  Defaults to ``~/.wf/runs.db``.
    """

    def __init__(self, db_path: str | Path = "~/.wf/runs.db") -> None:
        self.store = WorkflowStore(db_path=db_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, func: Callable[..., Any], **inputs: Any) -> str:
        """Execute *func* as a new durable workflow run.

        1. Creates a run record in the store (status=running).
        2. Installs the run context so that ``step()`` calls inside *func*
           persist their results transparently.
        3. Calls ``func(**inputs)``.
        4. Marks the run completed (or failed).

        Args:
            func:     A plain Python callable — no decorator required.
            **inputs: Keyword arguments forwarded to *func*.

        Returns:
            The run_id (uuid4 string) that can be used with :meth:`resume`
            and :meth:`status`.
        """
        input_json = json.dumps(inputs, default=str)
        run_id = self.store.create_run(func.__name__, input_json=input_json)
        self._execute(run_id, func, inputs)
        return run_id

    def resume(self, run_id: str) -> None:
        """Resume a previously interrupted workflow run.

        Re-invokes the workflow function.  All steps that already completed
        with the same inputs are served from the store cache — only failed
        or not-yet-started steps actually execute.

        Args:
            run_id: The id returned by :meth:`run`.

        Raises:
            WorkflowError: if *run_id* is not found in the store.
        """
        try:
            run = self.store.get_run(run_id)
        except KeyError:
            raise WorkflowError(f"No workflow run found with id {run_id!r}")

        # Resolve the original function by name from the caller's registered
        # functions.  The engine stores func.__name__ and inputs; we need the
        # actual callable to re-invoke it.  The registry is populated by
        # engine.run() and engine.register().
        func = self._registry.get(run.workflow_name)
        if func is None:
            raise WorkflowError(
                f"Workflow function {run.workflow_name!r} is not registered with this engine. "
                "Call engine.register(func) before resuming."
            )

        inputs: dict[str, Any] = json.loads(run.input_json or "{}")
        self._execute(run_id, func, inputs)

    def status(self, run_id: str) -> RunStatus:
        """Return a :class:`RunStatus` snapshot for *run_id*.

        Raises:
            WorkflowError: if *run_id* is not found in the store.
        """
        try:
            run = self.store.get_run(run_id)
        except KeyError:
            raise WorkflowError(f"No workflow run found with id {run_id!r}")
        steps = self.store.get_steps(run_id)
        return RunStatus(run=run, steps=steps)

    def register(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Register *func* so it can be looked up by name during :meth:`resume`.

        Can be used as a decorator::

            @engine.register
            def my_workflow(x: int) -> int:
                ...
        """
        self._registry[func.__name__] = func
        return func

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    #: Maps workflow_name → callable; populated by run() and register().
    _registry: dict[str, Callable[..., Any]]

    def __init_subclass__(cls, **kwargs: Any) -> None:  # pragma: no cover
        super().__init_subclass__(**kwargs)

    def __new__(cls, *args: Any, **kwargs: Any) -> "WorkflowEngine":
        instance = super().__new__(cls)
        instance._registry = {}
        return instance

    def _execute(self, run_id: str, func: Callable[..., Any], inputs: dict[str, Any]) -> Any:
        """Internal: set context, call func, update run status, reset context."""
        # Register the function so resume() can find it later.
        self._registry[func.__name__] = func

        token = set_current_run(self.store, run_id)
        try:
            result = func(**inputs)
            self.store.update_run_status(run_id, "completed")
            return result
        except Exception:
            self.store.update_run_status(run_id, "failed")
            raise
        finally:
            reset_current_run(token)

    def close(self) -> None:
        """Close the underlying store connection."""
        self.store.close()

    def __enter__(self) -> "WorkflowEngine":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
