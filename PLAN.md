# Durable Workflow Engine — Implementation Plan

## What We're Building

A minimal durable workflow engine in ~300 lines of Python. No Temporal, no Celery, no framework deps.

A **durable workflow** is a function where each step is persisted before execution. If the process crashes mid-way and restarts, completed steps are skipped and execution resumes exactly where it left off. This is the foundational primitive under every serious agent system.

## Why This Matters

Every podcast discussing AI agents (SaaStr, Latent Space, Thoughtworks) eventually hits the same problem: agents fail mid-task, retrying from scratch is expensive and incorrect, and "just wrap it in try/except" doesn't work across process restarts. Temporal solves this commercially. This project teaches you *why* it's hard.

## Architecture

```
workflow/
├── __init__.py
├── engine.py       # WorkflowEngine: run(), resume(), status()
├── step.py         # Step decorator and StepRecord dataclass
├── store.py        # SQLite-backed persistence layer
├── executor.py     # Step execution with pre/post persistence
└── cli.py          # `wf run`, `wf status`, `wf resume` commands

examples/
├── podcast_pipeline.py   # 4-step podcast processing workflow
├── data_pipeline.py      # download → transform → load → notify
└── flaky_steps.py        # intentional failures for testing

tests/
├── test_engine.py
├── test_store.py
└── test_resume.py

pyproject.toml
README.md
```

## Core Concepts

### Step State Machine
```
PENDING → RUNNING → COMPLETED
                 ↘ FAILED → (retry) → RUNNING
```

### The Step Record (persisted to SQLite before execution)
```python
@dataclass
class StepRecord:
    workflow_run_id: str
    step_name: str
    status: str           # pending | running | completed | failed
    input_hash: str       # sha256 of serialized inputs
    output: bytes | None  # pickle of return value
    attempt: int
    started_at: datetime
    finished_at: datetime | None
    error: str | None
```

### Idempotency via input hashing
If a step is called again with the same inputs and is already COMPLETED, return the cached output without re-executing. This is the key insight: **steps are pure functions of their inputs**.

### The workflow decorator
```python
@workflow
def process_podcast(episode_id: str):
    audio_path = step("download", download_audio, episode_id)
    transcript = step("transcribe", transcribe_audio, audio_path)
    summary    = step("summarize", summarize_text, transcript)
    return step("export", export_to_db, episode_id, summary)

# Run it
engine = WorkflowEngine("~/.wf/runs.db")
run_id = engine.run("process_podcast", episode_id="ep-123")

# Crash here. Restart. Resume:
engine.resume(run_id)   # skips download + transcribe, continues at summarize
```

## Implementation Phases

### Phase 1: Storage layer (store.py)
SQLite schema for workflow runs and step records. CRUD operations. No business logic.

Key tables:
```sql
CREATE TABLE workflow_runs (
    id TEXT PRIMARY KEY,
    workflow_name TEXT,
    input_json TEXT,
    status TEXT,           -- running | completed | failed
    created_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE step_records (
    id INTEGER PRIMARY KEY,
    run_id TEXT,
    step_name TEXT,
    attempt INTEGER DEFAULT 0,
    status TEXT,
    input_hash TEXT,
    output BLOB,           -- pickled return value
    error TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    UNIQUE(run_id, step_name, attempt)
);
```

### Phase 2: Step execution (step.py + executor.py)
The `step()` function is the core primitive:
1. Check store: is this step already COMPLETED with same input_hash? → return cached output
2. Write RUNNING record to store
3. Execute the function
4. Write COMPLETED + output to store
5. On exception: write FAILED + error, re-raise

### Phase 3: Workflow engine (engine.py)
- `run(workflow_name, **inputs)` → creates run_id, sets up context, calls workflow function
- `resume(run_id)` → loads existing run, replays completed steps from cache, continues
- `status(run_id)` → returns run status + per-step status

The tricky part: making `step()` context-aware (which run_id to write to) without passing it explicitly. Use Python's `contextvars.ContextVar`.

### Phase 4: Retry logic
Add `max_retries` and `retry_delay` params to `step()`. On FAILED: if `attempt < max_retries`, write a new attempt record with exponential backoff delay.

### Phase 5: CLI
```bash
wf run podcast_pipeline --episode-id ep-123
wf status <run_id>
wf resume <run_id>
wf runs list              # recent runs with status
wf runs inspect <run_id>  # full step-by-step trace
```

### Phase 6: P³ integration
Replace P³'s manual pipeline (`fetch → transcribe → digest → export`) with a durable workflow. Every step becomes resumable. Re-running after a crash no longer re-downloads audio.

### Phase 7: Eval — chaos testing
Write a test harness that:
1. Runs a 5-step workflow
2. Injects a crash at a random step (raises `SimulatedCrash`)
3. Resumes the workflow
4. Asserts: only steps after the crash re-execute, total step executions = N (not 2*N)

## Key Design Decisions

**Why pickle for output storage?**
Simple and works for any Python object. Downside: not human-readable, version-sensitive. Document this tradeoff. Alternative: JSON with a custom encoder.

**Why not async?**
Keep it synchronous for clarity. The concepts are identical; async adds noise when learning the core pattern.

**Why SQLite, not a message queue?**
A message queue (Redis, RabbitMQ) is a different primitive — push-based, not pull-based. SQLite makes the state fully inspectable and the persistence model obvious.

**What we're NOT building**
- Distributed execution (steps run in one process)
- Scheduling / cron
- Fan-out / parallel steps (that's a follow-on project)

## Acceptance Criteria

1. A 4-step workflow survives a `SIGKILL` mid-execution and resumes correctly
2. Resuming re-uses cached step outputs (verified by step execution count)
3. `wf status` shows per-step timing and status
4. P³ pipeline refactored to use the engine, runs end-to-end
5. Chaos test passes: 10 random crash/resume cycles, 0 step double-executions

## Learning Outcomes

After building this you will understand:
- Why Temporal's core abstraction is "deterministic replay"
- What "exactly-once execution semantics" means in practice
- Why idempotency is the foundational property of resilient systems
- The tradeoff between serialization simplicity and durability
- Why every serious agent framework eventually needs this layer
