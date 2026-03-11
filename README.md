# Durable Workflow Engine

A minimal, crash-safe workflow engine in ~300 lines of Python.  
No Temporal, no Celery, no framework deps — just SQLite and stdlib.

```
uv run wf run examples/podcast_pipeline.py --episode-id ep-123
✓ Completed  run_id=f3f983a4-d492-46c1-b818-5f2ead66c227

# Process killed mid-way. Restart:
uv run wf resume f3f983a4 --workflow-file examples/podcast_pipeline.py
✓ Completed  (download + transcribe skipped — already cached)
```

---

## Why this exists

Every agent system eventually hits the same problem: a 4-step pipeline crashes
at step 3, and on restart the expensive steps (download, transcription, LLM
call) run again from the beginning.

The fix is **durability** — persist each step's output before moving to the
next one. If the process dies, replay completed steps from cache and continue
from the failure point. This is the foundational primitive under Temporal,
Prefect, and every serious workflow system.

---

## Quick start

```bash
# Install
git clone https://github.com/jpequegn/durable-workflow
cd durable-workflow
uv sync

# Run the example pipeline
uv run wf run examples/podcast_pipeline.py --episode-id ep-123

# List all runs
uv run wf runs list

# Inspect step-by-step trace
uv run wf runs inspect <run_id> --show-output
```

Requirements: Python ≥ 3.12, [uv](https://docs.astral.sh/uv/).

---

## Writing a workflow

A workflow is a plain Python function that calls `step()` for each unit of
work. No decorators, no subclassing.

```python
# my_pipeline.py
from workflow.step import step

def my_pipeline(user_id: str) -> dict:
    raw    = step("fetch",     fetch_data,   user_id)
    clean  = step("transform", clean_data,   raw)
    result = step("load",      save_results, user_id, clean)
    return result

# CLI discovery hooks
WORKFLOW = my_pipeline
INPUT_SCHEMA = {"user_id": str}
```

Then run it:

```bash
uv run wf run my_pipeline.py --user-id u-42
```

### The `step()` function

```python
step(name, func, *args, max_retries=0, base_delay=1.0, **kwargs)
```

| Parameter | Description |
|---|---|
| `name` | Unique name for this step within the workflow |
| `func` | The callable to execute |
| `*args` | Positional arguments forwarded to `func` |
| `max_retries` | Extra attempts after first failure (default: 0) |
| `base_delay` | Base seconds for exponential backoff (default: 1.0) |
| `**kwargs` | Keyword arguments forwarded to `func` |

**Idempotency guarantee:** if a step has already completed with the same
inputs, `step()` returns the cached result without calling `func` again.
This is the mechanism that makes resume safe.

### Retries

```python
# Retry up to 3 times with exponential backoff: 1s, 2s, 4s
transcript = step("transcribe", call_api, audio_path,
                  max_retries=3, base_delay=1.0)
```

Every attempt gets its own row in the database, so the full history is
visible in `wf runs inspect`.

---

## CLI reference

```
wf run <file> [--key val ...]      Run a workflow
wf status <run_id>                 Per-step table with timing
wf resume <run_id> [-f <file>]     Resume from last failure
wf runs list [-n N]                Recent runs, colour-coded
wf runs inspect <run_id>           Full step-by-step trace
```

### `wf run`

```bash
wf run examples/podcast_pipeline.py --episode-id ep-123
wf run my_pipeline.py --user-id u-42 --dry-run true
```

- Loads `WORKFLOW` and `INPUT_SCHEMA` from the `.py` file.
- Passes `--kebab-flags` as kwargs (converted to `snake_case`, type-coerced
  via `INPUT_SCHEMA`).
- Prints the `run_id` on success; exits 1 on failure with an inspect hint.

### `wf status`

```
  Workflow : process_podcast
  Run ID   : f3f983a4-…
  Status   : completed
  Duration : 1.23s

  STEP        ATT  STATUS      STARTED   DURATION
  ──────────────────────────────────────────────
  download      0  completed  14:02:01      0.31s
  transcribe    0  completed  14:02:01      0.88s
  summarize     0  completed  14:02:02      0.04s
  save          0  completed  14:02:02      0.00s
```

### `wf resume`

```bash
# Resume from a fresh shell (function not in memory — reload from file)
wf resume f3f983a4-… --workflow-file examples/podcast_pipeline.py
```

All steps that completed before the crash are cache hits; only the failed
step (and any after it) actually execute.

### `wf runs list`

```bash
wf runs list          # last 20
wf runs list -n 50    # last 50
```

Colour-coded: **green** = completed, **red** = failed, **yellow** = running.

### `wf runs inspect`

```bash
wf runs inspect <run_id>
wf runs inspect <run_id> --show-output   # also print step return values
```

Shows ✓/✗ markers, attempt numbers, durations, and full tracebacks on
failures. With `--show-output`, prints the pickled return value of each step.

---

## Python API

For programmatic use without the CLI:

```python
from workflow.engine import WorkflowEngine
from workflow.step import step

def process(x: int) -> int:
    a = step("double", lambda v: v * 2, x)
    return step("add_one", lambda v: v + 1, a)

engine = WorkflowEngine()               # default: ~/.wf/runs.db
engine = WorkflowEngine("my_runs.db")  # custom path

# Run
run_id = engine.run(process, x=21)     # returns "42"

# Inspect
status = engine.status(run_id)
print(status.run.status)               # "completed"
for s in status.steps:
    print(s.step_name, s.status, s.finished_at)

# Resume (only needed if a previous run failed)
engine.resume(run_id)

# Use as context manager (auto-closes DB connection)
with WorkflowEngine("my_runs.db") as engine:
    run_id = engine.run(process, x=21)
```

### Register for cross-process resume

`WorkflowEngine` keeps an in-memory registry of workflow functions. If you
resume from a different process (e.g. a new shell), register the function
first:

```python
engine = WorkflowEngine()
engine.register(process_podcast)   # or: @engine.register
engine.resume(run_id)
```

The CLI handles this automatically via `--workflow-file`.

---

## Examples

| File | Description |
|---|---|
| `examples/podcast_pipeline.py` | `download → transcribe (retries) → summarize → save` |
| `examples/data_pipeline.py` | ETL: `download → transform → load → notify` |
| `examples/flaky_steps.py` | Intentional failures showing retry history |

---

## Running tests

```bash
uv run pytest              # all 116 tests
uv run pytest tests/test_chaos.py -v   # chaos test (10 crash/resume cycles)
```

See [CHAOS_RESULTS.md](CHAOS_RESULTS.md) for the latest chaos test run:
10/10 iterations passed, 0 completed steps re-executed after a crash.

---

## Project layout

```
workflow/
├── __init__.py      # package root
├── cli.py           # wf CLI (click)
├── engine.py        # WorkflowEngine: run() / resume() / status()
├── step.py          # step() primitive + ContextVar plumbing
└── store.py         # SQLite persistence layer

examples/
├── podcast_pipeline.py
├── data_pipeline.py
└── flaky_steps.py

tests/
├── test_store.py    # 24 tests — storage layer
├── test_step.py     # 17 tests — step() idempotency
├── test_engine.py   # 22 tests — engine run/resume/status
├── test_retry.py    # 15 tests — retry + backoff
├── test_cli.py      # 24 tests — CLI commands
├── test_examples.py #  9 tests — example workflows
└── test_chaos.py    #  5 tests — chaos test

docs/
├── architecture.md  # internals, design decisions, data flow
└── api.md           # full Python API reference
```

---

## Design decisions

**Why pickle for step outputs?**  
Simple and works for any Python object. Downside: not human-readable and
version-sensitive. See [docs/architecture.md](docs/architecture.md) for the
full tradeoff discussion.

**Why SQLite, not a message queue?**  
A message queue is push-based; this engine is pull-based. SQLite makes state
fully inspectable and the persistence model obvious. The entire history of a
run is a SELECT away.

**Why synchronous?**  
The concepts are identical in async code; sync keeps the implementation
readable. An async port is straightforward.

**Why no `@workflow` decorator?**  
A plain function is simpler to test, easier to understand, and avoids magic.
The engine wraps the function; the function doesn't wrap the engine.

Full discussion in [docs/architecture.md](docs/architecture.md).
