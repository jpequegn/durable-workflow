"""Example: intentional failures demonstrating retry and resume behaviour.

Shows how max_retries + exponential backoff works for transient failures
(network blips, rate limits, model API timeouts, etc.).

Usage
-----
    # Run — step_b will fail twice then succeed
    uv run wf run examples/flaky_steps.py --seed 42

    # Inspect the retry history
    uv run wf runs inspect <run_id> --show-output
"""

from __future__ import annotations

import random

from workflow.step import step


# ---------------------------------------------------------------------------
# Simulated exception
# ---------------------------------------------------------------------------


class SimulatedCrash(Exception):
    """Raised intentionally to simulate a transient failure."""


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------


def flaky_fetch(seed: int, fail_times: int = 2) -> str:
    """Simulate a network fetch that fails *fail_times* before succeeding.

    Uses a module-level counter so retries (same call) advance past the
    failure threshold.
    """
    flaky_fetch._calls = getattr(flaky_fetch, "_calls", 0) + 1
    if flaky_fetch._calls <= fail_times:
        raise SimulatedCrash(f"Transient network error (attempt {flaky_fetch._calls})")
    return f"raw_data_seed_{seed}"


def process(data: str) -> str:
    print(f"  [process] data={data!r}")
    return data.upper()


def save(result: str) -> dict:
    print(f"  [save] result={result!r}")
    return {"saved": True, "result": result}


# ---------------------------------------------------------------------------
# Workflow function
# ---------------------------------------------------------------------------


def flaky_pipeline(seed: int) -> dict:
    """3-step pipeline where the first step is flaky (fails 2× before succeeding).

    Demonstrates max_retries=3 with base_delay=0.0 (instant retries for the demo).
    In production use base_delay=1.0 for exponential backoff.
    """
    # Reset per-run so each engine.run() starts fresh.
    flaky_fetch._calls = 0

    raw    = step("fetch",   flaky_fetch, seed, fail_times=2, max_retries=3, base_delay=0.0)
    result = step("process", process,     raw)
    return   step("save",    save,        result)


# ---------------------------------------------------------------------------
# CLI discovery hooks
# ---------------------------------------------------------------------------

WORKFLOW = flaky_pipeline

INPUT_SCHEMA: dict[str, type] = {
    "seed": int,
}
