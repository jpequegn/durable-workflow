"""Core step() primitive — the heart of the durable workflow engine.

How it works
------------
1. Compute ``input_hash = sha256(pickle(args, kwargs))``.
2. Check store: is there a COMPLETED record for ``(run_id, step_name)``
   with the **same** ``input_hash``?
   - YES → return ``pickle.loads(record.output)``  (cache hit, skip execution)
   - NO  → continue
3. Write a RUNNING record to the store (durability checkpoint *before* work).
4. Call ``func(*args, **kwargs)``.
5. On success  → write COMPLETED + ``pickle.dumps(result)`` → return result.
6. On exception → write FAILED + error message.
   - If ``attempt < max_retries``: sleep ``base_delay * 2 ** attempt`` seconds,
     increment attempt, go to step 3.
   - Otherwise: re-raise the original exception.

Each retry is a **new row** in ``step_records`` with an incremented ``attempt``
number so the full history is always inspectable via ``status()``.

The current ``run_id`` is picked up transparently from a ``ContextVar`` so
callers never have to thread it through manually.
"""

from __future__ import annotations

import hashlib
import pickle
import time
import traceback
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any, TypeVar

from workflow.store import WorkflowStore

# ---------------------------------------------------------------------------
# Context variable — set by the engine before calling a workflow function
# ---------------------------------------------------------------------------

#: Holds the (store, run_id) pair for the currently-executing workflow run.
_current_run: ContextVar[tuple[WorkflowStore, str] | None] = ContextVar(
    "_current_run", default=None
)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def get_current_run() -> tuple[WorkflowStore, str]:
    """Return the ``(store, run_id)`` for the active workflow run.

    Raises:
        RuntimeError: if called outside of a workflow execution context.
    """
    value = _current_run.get()
    if value is None:
        raise RuntimeError(
            "step() called outside of a workflow execution context. "
            "Make sure you are calling step() from inside a @workflow-decorated "
            "function (or from code invoked by the WorkflowEngine)."
        )
    return value


def set_current_run(store: WorkflowStore, run_id: str) -> Any:
    """Set the active (store, run_id) and return the ContextVar Token.

    The token can be passed to ``reset_current_run()`` to restore the
    previous value (important for nested / concurrent runs).
    """
    return _current_run.set((store, run_id))


def reset_current_run(token: Any) -> None:
    """Restore the previous ContextVar state using *token*."""
    _current_run.reset(token)


# ---------------------------------------------------------------------------
# Input hashing
# ---------------------------------------------------------------------------


def _compute_input_hash(*args: Any, **kwargs: Any) -> str:
    """Return a sha256 hex digest of the pickled ``(args, kwargs)`` tuple.

    Steps are treated as pure functions of their inputs: same inputs →
    same hash → cached output can be reused safely.
    """
    payload = pickle.dumps((args, kwargs))
    return hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------
# step()
# ---------------------------------------------------------------------------


def step(
    name: str,
    func: Callable[..., T],
    *args: Any,
    max_retries: int = 0,
    base_delay: float = 1.0,
    **kwargs: Any,
) -> T:
    """Execute *func* as a durable, idempotent workflow step.

    Args:
        name:        Logical name for this step (must be unique within a workflow).
        func:        The callable to execute.
        *args:       Positional arguments forwarded to *func*.
        max_retries: Number of additional attempts after the first failure.
                     ``0`` means no retries (default).  Total attempts = ``max_retries + 1``.
        base_delay:  Base sleep time in seconds for exponential backoff.
                     Attempt *k* sleeps ``base_delay * 2 ** k`` seconds before
                     retrying (k=0 on first retry, k=1 on second, …).
        **kwargs:    Keyword arguments forwarded to *func*.

    Returns:
        The return value of ``func(*args, **kwargs)`` — either freshly computed
        or deserialized from the store on a cache hit.

    Raises:
        RuntimeError: if called outside a workflow execution context.
        Exception:    Whatever *func* raises on the final attempt, after all
                      retries are exhausted.
    """
    store, run_id = get_current_run()
    input_hash = _compute_input_hash(*args, **kwargs)

    # --- 1. Cache check -------------------------------------------------------
    existing = store.get_step(run_id, name)
    if existing is not None and existing.status == "completed" and existing.input_hash == input_hash:
        # Cache hit: return the previously stored result without re-executing.
        return pickle.loads(existing.output)  # type: ignore[return-value]

    # --- 2. Determine starting attempt number ---------------------------------
    # Resume after a prior failure: pick up where we left off.
    attempt = 0 if existing is None else existing.attempt + 1

    # --- 3-6. Execute with retry loop -----------------------------------------
    while True:
        # Checkpoint RUNNING before doing any work.
        store.write_step(run_id, name, attempt=attempt, status="running", input_hash=input_hash)

        try:
            result: T = func(*args, **kwargs)
        except Exception:
            error_text = traceback.format_exc()
            store.write_step(
                run_id,
                name,
                attempt=attempt,
                status="failed",
                input_hash=input_hash,
                error=error_text,
            )

            retries_remaining = max_retries - attempt
            if retries_remaining > 0:
                # Exponential backoff before next attempt.
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
                attempt += 1
                continue  # retry
            else:
                raise  # all attempts exhausted — propagate

        # Success
        store.write_step(
            run_id,
            name,
            attempt=attempt,
            status="completed",
            input_hash=input_hash,
            output=pickle.dumps(result),
        )
        return result
