# Architecture

Internals, design decisions, and data flow for the durable workflow engine.

---

## Overview

The engine is built in four layers, each with a single responsibility:

```
┌─────────────────────────────────────────┐
│  CLI (workflow/cli.py)                  │  operator interface
├─────────────────────────────────────────┤
│  WorkflowEngine (workflow/engine.py)    │  public Python API
├─────────────────────────────────────────┤
│  step() (workflow/step.py)              │  core primitive
├─────────────────────────────────────────┤
│  WorkflowStore (workflow/store.py)      │  SQLite persistence
└─────────────────────────────────────────┘
```

Each layer depends only on the one below it. The CLI knows nothing about
SQLite; `step()` knows nothing about the CLI. This makes each layer
independently testable and replaceable.

---

## The core idea: steps as pure functions of their inputs

The entire durability model rests on one property:

> **A step is a pure function of its inputs.**
> Same inputs → same output → safe to cache forever.

When `step()` is called:

1. It computes `input_hash = sha256(pickle((args, kwargs)))`.
2. It checks the store for a `COMPLETED` record with that `(run_id, step_name, input_hash)`.
3. If found: return `pickle.loads(record.output)` — **no execution**.
4. If not found: execute, persist, return.

This means that when a workflow resumes after a crash, every step that
completed before the crash is a cache hit. The function body is never called
again. The workflow function re-runs top to bottom, but it reads from the
store rather than re-executing.

---

## Data flow

### First run

```
engine.run(my_workflow, x=42)
│
├─ store.create_run("my_workflow", '{"x": 42}')  → run_id
├─ set_current_run(store, run_id)                 [ContextVar]
│
└─ my_workflow(x=42)
   │
   ├─ step("fetch", fetch_data, 42)
   │  ├─ input_hash = sha256(pickle((42,), {}))
   │  ├─ store.get_step(run_id, "fetch")  → None
   │  ├─ store.write_step(..., status="running")   ← checkpoint BEFORE work
   │  ├─ fetch_data(42)                            ← actual work
   │  └─ store.write_step(..., status="completed", output=pickle(result))
   │
   ├─ step("transform", transform, result)
   │  └─ ... same pattern ...
   │
   └─ step("save", save, final)
      └─ ... same pattern ...
│
├─ store.update_run_status(run_id, "completed")
└─ reset_current_run(token)
```

### Crash + resume

```
# Process crashes after "fetch" completes but before "transform" finishes.
# In the database:
#   fetch       → COMPLETED ✓
#   transform   → RUNNING   (incomplete — crash happened mid-execution)

engine.resume(run_id)
│
└─ my_workflow(x=42)           ← re-runs the whole function
   │
   ├─ step("fetch", fetch_data, 42)
   │  ├─ input_hash = sha256(pickle((42,), {}))
   │  ├─ store.get_step(run_id, "fetch") → COMPLETED, hash matches
   │  └─ return pickle.loads(record.output)    ← CACHE HIT, no execution
   │
   ├─ step("transform", transform, result)
   │  ├─ store.get_step(run_id, "transform") → RUNNING (stale from crash)
   │  ├─ attempt = existing.attempt + 1 = 1
   │  ├─ store.write_step(..., attempt=1, status="running")
   │  ├─ transform(result)                   ← executes
   │  └─ store.write_step(..., status="completed")
   │
   └─ step("save", save, final)
      └─ ... executes normally ...
```

The key observation: the workflow function is re-run from the top on every
resume, but completed steps are transparent no-ops from the caller's
perspective. The function body of each step function only runs when needed.

---

## Module reference

### `workflow/store.py` — persistence layer

**Responsibility:** SQLite CRUD. No business logic.

**Schema:**

```sql
CREATE TABLE workflow_runs (
    id            TEXT PRIMARY KEY,        -- uuid4
    workflow_name TEXT NOT NULL,
    input_json    TEXT,                    -- JSON of **inputs kwargs
    status        TEXT DEFAULT 'running',  -- running | completed | failed
    created_at    TIMESTAMP,
    finished_at   TIMESTAMP
);

CREATE TABLE step_records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,
    step_name   TEXT NOT NULL,
    attempt     INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'pending',    -- pending | running | completed | failed
    input_hash  TEXT,                      -- sha256(pickle((args, kwargs)))
    output      BLOB,                      -- pickle.dumps(return_value)
    error       TEXT,                      -- traceback on failure
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    UNIQUE (run_id, step_name, attempt),
    FOREIGN KEY (run_id) REFERENCES workflow_runs (id)
);
```

**Key choices:**

- `UNIQUE(run_id, step_name, attempt)` — enforced at the DB level; prevents
  phantom double-writes even under concurrent access.
- `write_step()` uses `INSERT ... ON CONFLICT DO UPDATE` (upsert) so the same
  call can transition `RUNNING → COMPLETED` without needing a separate UPDATE.
- WAL journal mode (`PRAGMA journal_mode=WAL`) allows readers and one writer
  to operate concurrently without blocking each other.
- `get_step()` returns the **latest attempt** (`ORDER BY attempt DESC LIMIT 1`),
  so callers always see the most recent state.

### `workflow/step.py` — the core primitive

**Responsibility:** idempotency, input hashing, retry loop, ContextVar plumbing.

**ContextVar:** `_current_run` holds `(WorkflowStore, run_id)` for the
currently-executing workflow. The engine sets it before calling the workflow
function and resets it in a `finally` block. This means `step()` can resolve
the active run without the caller passing it explicitly.

This is safe for concurrent use: `ContextVar` is per-task/per-thread, so two
workflow runs executing concurrently get independent contexts.

**Input hashing:**

```python
input_hash = sha256(pickle((args, kwargs)))
```

- `pickle` handles arbitrary Python objects (lists, dicts, dataclasses, etc.).
- `sha256` produces a 64-character hex string that fits in any VARCHAR.
- Hashing the full `(args, kwargs)` tuple means positional and keyword
  arguments are treated equivalently (no ambiguity).

**Retry loop:**

```
attempt = 0
while True:
    write_step(RUNNING)
    try:
        result = func(*args, **kwargs)
    except Exception:
        write_step(FAILED, error=traceback)
        if attempt < max_retries:
            sleep(base_delay * 2 ** attempt)
            attempt += 1
            continue
        raise
    write_step(COMPLETED, output=pickle(result))
    return result
```

Backoff: attempt 0 → sleep `base_delay * 1`, attempt 1 → `base_delay * 2`,
attempt 2 → `base_delay * 4`, etc.

Each attempt is a new row (incremented `attempt` number), so `wf runs inspect`
shows the full retry history.

### `workflow/engine.py` — public API

**Responsibility:** orchestrating store + step, managing the function registry.

**Function registry:** `WorkflowEngine` keeps a `dict[str, Callable]` mapping
`func.__name__` to the callable. This is populated automatically by `run()`
(which always has the function in hand) and manually by `register()`.

`resume()` looks up the function by `workflow_name` (stored in
`workflow_runs.workflow_name`). This is why `register()` (or `--workflow-file`
in the CLI) is required when resuming from a different process — the engine
can't reconstruct a callable from a name alone.

**Exception handling:** `run()` and `resume()` do **not** swallow exceptions.
The run is marked `FAILED` and the exception propagates to the caller, who can
decide whether to retry, alert, or resume later. This mirrors how Temporal
and Prefect handle workflow exceptions.

### `workflow/cli.py` — operator interface

**Responsibility:** human-readable output, file loading, flag parsing.

**Workflow file convention:**

```python
# my_pipeline.py
WORKFLOW = my_function          # required: the callable to run
INPUT_SCHEMA = {"x": int}       # optional: maps flag names to Python types
```

The CLI loads the file with `importlib.util.spec_from_file_location`, which
runs the module in a fresh namespace. `WORKFLOW` and `INPUT_SCHEMA` are then
read as module attributes.

**Flag parsing:** extra CLI flags (`--key value`) are parsed manually from
`ctx.args`. Kebab-case is converted to snake_case (`--episode-id` →
`episode_id`) and values are coerced using `INPUT_SCHEMA` types. This avoids
Click's option system for dynamically-typed workflows.

---

## Step state machine

```
                ┌──────────┐
                │  PENDING │  (initial state before first write)
                └────┬─────┘
                     │ write_step(RUNNING)
                     ▼
                ┌──────────┐
                │  RUNNING │
                └────┬─────┘
          success │       │ exception
                  ▼       ▼
           ┌──────────┐  ┌────────┐
           │COMPLETED │  │ FAILED │
           └──────────┘  └───┬────┘
                              │ attempt < max_retries
                              │ sleep(backoff)
                              │ attempt += 1
                              └──► RUNNING  (new attempt row)
```

The `PENDING` state is conceptual — a step that has never been written to the
store is "pending". The database only stores `RUNNING`, `COMPLETED`, or
`FAILED` records.

---

## Durability guarantee

**Guarantee:** a step that reached `COMPLETED` is never re-executed, regardless
of how many times the workflow is resumed.

**Proof:** `step()` checks for `(COMPLETED, matching input_hash)` before doing
any work. If found, it returns early. The only way to bypass this is to call
the workflow with different inputs (which changes the `input_hash` and is
treated as a new execution).

**Chaos test verification:** see [CHAOS_RESULTS.md](../CHAOS_RESULTS.md). 10
random crash/resume cycles, crash at a random step each time, 0 completed
steps re-executed — 10/10 passed.

---

## Design decisions and tradeoffs

### Why `pickle` for step outputs?

**Pro:** works for any Python object; zero configuration; fast.

**Con:** not human-readable; version-sensitive (unpickling an object from an
older class definition can fail); not portable across Python versions.

**Alternatives considered:**
- JSON with a custom encoder: more portable, but requires every step return
  value to be JSON-serialisable. Many real-world objects aren't.
- `cloudpickle`: handles closures and lambdas that stdlib `pickle` can't, but
  is an external dependency.
- MessagePack / CBOR: compact binary formats, but add a dependency and don't
  solve the version-sensitivity issue.

**Decision:** stdlib `pickle` for simplicity. Document the tradeoff and let
users swap the serialisation layer in `store.py` if needed.

### Why SQLite, not a message queue?

Message queues (Redis Streams, RabbitMQ, Kafka) are **push-based**: work is
dispatched to workers that consume it. This engine is **pull-based**: the
workflow function re-runs and reads from the store.

SQLite gives you:
- A fully inspectable state machine (every step's history is a SELECT away).
- No external service to run or configure.
- ACID guarantees without distributed coordination.
- The ability to copy the `.db` file for debugging.

If you need distributed fan-out, a message queue is the right tool. For
sequential, single-process durability, SQLite is simpler and more inspectable.

### Why synchronous?

Async adds noise when learning the core pattern. The concepts — idempotency,
input hashing, crash recovery — are identical in async code. An async port
would replace `time.sleep` with `asyncio.sleep` and `sqlite3` with an async
driver like `aiosqlite`, but the architecture would be unchanged.

### Why no `@workflow` decorator?

A decorator-based API (`@workflow / def my_pipeline(...)`) is ergonomic but
adds magic: it hides the engine wiring, makes stack traces harder to read, and
couples the function definition to the engine. A plain function that the engine
wraps at call time is easier to test in isolation, easier to reason about, and
doesn't require the engine to exist at import time.

### Why `step()` instead of a method on the function?

`step()` is a free function that reads the current run from a `ContextVar`.
The alternative — `engine.step(...)` or `run.step(...)` — would require
threading the engine or run object through every level of the call stack, or
using global state. `ContextVar` is the correct Python primitive for
"ambient context that is per-task and per-thread".

---

## What this engine doesn't do

- **Distributed execution:** all steps run in one process. Fan-out (parallel
  steps) would require a scheduler and a worker pool.
- **Scheduling / cron:** no time-based triggers. Use your OS scheduler or a
  separate tool.
- **Fan-out / parallel steps:** steps run sequentially. A parallel primitive
  would need to manage multiple sub-run-ids and join their results.
- **Sub-workflows:** no first-class support for calling one workflow from
  inside another (though you could use a step that calls `engine.run()`).
- **Async execution:** synchronous only. An async port is straightforward.
- **Cross-machine state:** SQLite is a local file. For distributed state, swap
  `WorkflowStore` for a Postgres or Redis-backed implementation.
