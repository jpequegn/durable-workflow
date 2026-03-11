# API Reference

Full reference for the `workflow` Python package.

---

## `workflow.step`

### `step(name, func, *args, max_retries=0, base_delay=1.0, **kwargs)`

Execute `func` as a durable, idempotent workflow step.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | Unique logical name for this step within the workflow. Used as the lookup key in the store. |
| `func` | `Callable[..., T]` | — | The function to execute. |
| `*args` | `Any` | — | Positional arguments forwarded to `func`. |
| `max_retries` | `int` | `0` | Number of additional attempts after the first failure. Total attempts = `max_retries + 1`. |
| `base_delay` | `float` | `1.0` | Base sleep time in seconds for exponential backoff. Attempt `k` sleeps `base_delay × 2ᵏ` seconds before retrying. |
| `**kwargs` | `Any` | — | Keyword arguments forwarded to `func`. |

**Returns** `T` — the return value of `func(*args, **kwargs)`, either freshly
computed or deserialized from the step cache.

**Raises**

- `RuntimeError` — if called outside a workflow execution context (no active
  `WorkflowEngine.run()` or `resume()` on the call stack).
- Any exception raised by `func` on the final attempt (after all retries are
  exhausted).

**Behaviour**

1. Compute `input_hash = sha256(pickle((args, kwargs)))`.
2. Look up `(run_id, name)` in the store.
   - If status is `COMPLETED` and `input_hash` matches → return cached output.
3. Write a `RUNNING` record (durability checkpoint, happens *before* execution).
4. Call `func(*args, **kwargs)`.
5. On success → write `COMPLETED` + pickled result → return result.
6. On exception → write `FAILED` + traceback.
   - If `attempt < max_retries`: sleep `base_delay × 2ᵅᵗᵗᵉᵐᵖᵗ` seconds, increment attempt, go to step 3.
   - Otherwise: re-raise.

**Examples**

```python
# Basic step — no retries
result = step("compute", my_function, arg1, arg2)

# With retries and backoff
# Attempt 0 fails → sleep 1s
# Attempt 1 fails → sleep 2s
# Attempt 2 succeeds
result = step("fetch", call_api, url, max_retries=2, base_delay=1.0)

# Kwargs forwarded to func
result = step("query", db_query, table="users", limit=100)
```

**Step name uniqueness**

Step names must be unique within a single workflow run. If you call `step()`
twice with the same name and same inputs, the second call is a cache hit and
the function is not re-executed. If you call it with the same name but
*different* inputs, a new attempt is created.

---

### `get_current_run() → tuple[WorkflowStore, str]`

Return the `(store, run_id)` for the currently-executing workflow run.

Raises `RuntimeError` if called outside a workflow execution context. Useful
for advanced use cases where you need direct store access from inside a step.

---

### `set_current_run(store, run_id) → Token`

Install `(store, run_id)` as the active context. Returns a `ContextVar` token
that must be passed to `reset_current_run()` to restore the previous state.

Called internally by `WorkflowEngine._execute()`. Only needed if you are
building custom engine integrations.

---

### `reset_current_run(token)`

Restore the `ContextVar` to its previous state using the token returned by
`set_current_run()`. Always called in a `finally` block by the engine.

---

## `workflow.engine`

### `WorkflowEngine`

Runs and resumes durable workflow functions.

```python
from workflow.engine import WorkflowEngine

engine = WorkflowEngine()                      # ~/.wf/runs.db
engine = WorkflowEngine("path/to/runs.db")    # custom path
engine = WorkflowEngine(Path("runs.db"))       # pathlib.Path also accepted
```

**Constructor parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `db_path` | `str \| Path` | `"~/.wf/runs.db"` | Path to the SQLite database. Created (with parent dirs) on first use. |

**Context manager**

```python
with WorkflowEngine("runs.db") as engine:
    run_id = engine.run(my_workflow, x=1)
# DB connection closed automatically
```

---

#### `engine.run(func, **inputs) → str`

Execute `func` as a new durable workflow run.

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `func` | `Callable[..., Any]` | The workflow function. No decorator required. |
| `**inputs` | `Any` | Keyword arguments forwarded to `func`. Serialised as JSON and stored with the run record. |

**Returns** `str` — the `run_id` (uuid4). Use this with `resume()` and `status()`.

**Raises** Any exception raised by `func` (after marking the run `FAILED`).

**Behaviour**

1. Creates a run record in the store (`status=running`).
2. Sets the `ContextVar` so `step()` calls inside `func` resolve the run.
3. Calls `func(**inputs)`.
4. On success: marks run `COMPLETED`, returns `run_id`.
5. On exception: marks run `FAILED`, re-raises.

```python
run_id = engine.run(process_podcast, episode_id="ep-123")
# process_podcast runs; all steps are persisted
```

---

#### `engine.resume(run_id)`

Resume a previously interrupted workflow run.

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `run_id` | `str` | The id returned by `run()`. |

**Raises**

- `WorkflowError` — if `run_id` is not found in the store.
- `WorkflowError` — if the workflow function is not registered (see `register()`).
- Any exception raised by the workflow function on the resumed attempt.

**Behaviour**

Re-invokes the workflow function with the original inputs. Completed steps are
cache hits in `step()` — only failed or not-yet-started steps execute.

```python
# Initial run crashes mid-way
try:
    run_id = engine.run(my_workflow, x=42)
except SomeError:
    pass  # run is marked FAILED, run_id is retrievable from engine.store

# Fix the bug, then resume
engine.resume(run_id)   # completed steps are skipped automatically
```

**Cross-process resume**

If you resume from a different process (different Python interpreter or
shell session), the function won't be in the engine's in-memory registry.
Register it first:

```python
engine.register(my_workflow)   # or: engine = WorkflowEngine(); engine.register(...)
engine.resume(run_id)
```

The CLI handles this via `wf resume <run_id> --workflow-file my_pipeline.py`.

---

#### `engine.status(run_id) → RunStatus`

Return a snapshot of the run and all its steps.

**Raises** `WorkflowError` if `run_id` is not found.

```python
s = engine.status(run_id)

# Run-level
s.run.status          # "completed" | "failed" | "running"
s.run.workflow_name   # "process_podcast"
s.run.created_at      # datetime
s.run.finished_at     # datetime | None
s.is_completed        # bool
s.is_failed           # bool
s.is_running          # bool

# Step-level
for step in s.steps:
    step.step_name    # "download"
    step.attempt      # 0  (increments on retry)
    step.status       # "completed" | "failed" | "running" | "pending"
    step.input_hash   # sha256 hex string
    step.output       # bytes (pickle.loads(step.output) → return value)
    step.error        # traceback string | None
    step.started_at   # datetime | None
    step.finished_at  # datetime | None
```

---

#### `engine.register(func) → func`

Register `func` so it can be looked up by name during `resume()`.

Can be used as a decorator:

```python
@engine.register
def my_workflow(x: int) -> int:
    return step("s", lambda v: v * 2, x)

run_id = engine.run(my_workflow, x=21)
engine.resume(run_id)   # works — my_workflow is registered
```

Or called directly:

```python
engine.register(my_workflow)
```

`run()` automatically registers the function, so you only need to call
`register()` explicitly when resuming in a different process.

---

#### `engine.close()`

Close the underlying SQLite connection. Called automatically when using the
context manager.

---

### `RunStatus`

Point-in-time snapshot of a workflow run returned by `engine.status()`.

```python
@dataclass
class RunStatus:
    run: RunRecord
    steps: list[StepRecord]

    is_completed: bool   # run.status == "completed"
    is_failed: bool      # run.status == "failed"
    is_running: bool     # run.status == "running"
```

---

### `WorkflowError`

Raised by `engine.resume()` and `engine.status()` for engine-level errors:
unknown `run_id`, unregistered workflow function.

```python
from workflow.engine import WorkflowError

try:
    engine.resume("does-not-exist")
except WorkflowError as e:
    print(e)  # "No workflow run found with id 'does-not-exist'"
```

---

## `workflow.store`

Low-level persistence layer. Most users don't need to interact with this
directly — use `WorkflowEngine` instead. The store is accessible via
`engine.store` for advanced inspection.

### `WorkflowStore`

```python
from workflow.store import WorkflowStore

store = WorkflowStore("path/to/runs.db")
```

**Methods**

| Method | Signature | Description |
|---|---|---|
| `create_run` | `(workflow_name, input_json=None) → str` | Insert a new run record; returns uuid4 `run_id`. |
| `update_run_status` | `(run_id, status)` | Update run status; sets `finished_at` for terminal statuses. |
| `get_run` | `(run_id) → RunRecord` | Raises `KeyError` if not found. |
| `list_runs` | `(limit=50) → list[RunRecord]` | Most recent runs, newest first. |
| `write_step` | `(run_id, step_name, attempt, status, input_hash, output, error)` | Upsert a step record. |
| `get_step` | `(run_id, step_name) → StepRecord \| None` | Returns latest attempt, or `None`. |
| `get_steps` | `(run_id) → list[StepRecord]` | All steps for a run, ordered by `started_at`. |
| `close` | `()` | Close the SQLite connection. |

**Context manager:** `with WorkflowStore(...) as store:`

---

### `RunRecord`

```python
@dataclass
class RunRecord:
    id: str                    # uuid4
    workflow_name: str
    input_json: str | None     # JSON of original **inputs
    status: str                # "running" | "completed" | "failed"
    created_at: datetime
    finished_at: datetime | None
```

---

### `StepRecord`

```python
@dataclass
class StepRecord:
    id: int | None
    run_id: str
    step_name: str
    attempt: int               # 0-indexed; increments on retry
    status: str                # "pending" | "running" | "completed" | "failed"
    input_hash: str | None     # sha256(pickle((args, kwargs)))
    output: bytes | None       # pickle.dumps(return_value); None on failure
    error: str | None          # full traceback string; None on success
    started_at: datetime | None
    finished_at: datetime | None
```

To read the step's return value:

```python
import pickle
if record.output is not None:
    value = pickle.loads(record.output)
```

---

## Writing a workflow file for the CLI

Any `.py` file can be used with `wf run` provided it exposes:

```python
# Required: the callable to run
WORKFLOW = my_function

# Optional: maps --flag-names to Python types for type coercion
INPUT_SCHEMA: dict[str, type] = {
    "episode_id": str,
    "max_items":  int,
    "dry_run":    bool,
}
```

**Flag conversion rules:**
- `--episode-id ep-123` → `episode_id = "ep-123"` (kebab → snake)
- Type coercion uses `INPUT_SCHEMA`: `--max-items 10` → `max_items = int("10") = 10`
- Booleans: `--dry-run true/false/yes/no/1/0`
- Flags not in `INPUT_SCHEMA` are passed as strings

**Full example:**

```python
# my_pipeline.py
from workflow.step import step

def my_pipeline(episode_id: str, max_retries: int = 2) -> dict:
    raw    = step("fetch",  fetch_data,  episode_id)
    result = step("save",   save_result, raw, max_retries=max_retries)
    return result

WORKFLOW = my_pipeline
INPUT_SCHEMA = {
    "episode_id":  str,
    "max_retries": int,
}
```

```bash
wf run my_pipeline.py --episode-id ep-42 --max-retries 3
```
