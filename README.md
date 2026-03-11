# Durable Workflow Engine

A minimal durable workflow engine in ~300 lines of Python.
No Temporal, no Celery, no framework deps — just SQLite and stdlib.

A **durable workflow** is a function where each step is persisted before
execution. If the process crashes mid-way and restarts, completed steps are
skipped and execution resumes exactly where it left off.

## Quick start

```bash
uv run wf --help
```

## Installation (dev)

```bash
uv sync
uv run wf --help
```

## CLI

```
Usage: wf [OPTIONS] COMMAND [ARGS]...

  Durable Workflow Engine — resumable, crash-safe workflow execution.

Commands:
  run      Run a workflow from a Python file.
  status   Show status of a workflow run (per-step timing and result).
  resume   Resume a previously interrupted workflow run.
  runs     Manage workflow runs (list / inspect).
```

## Architecture

```
workflow/
├── __init__.py      # package root
├── cli.py           # `wf` CLI (click)
├── engine.py        # WorkflowEngine.run() / resume() / status()  [TODO]
├── step.py          # @step decorator + StepRecord dataclass       [TODO]
├── store.py         # SQLite persistence layer                      [TODO]
└── executor.py      # step execution with pre/post persistence      [TODO]

examples/
├── podcast_pipeline.py   # 4-step podcast processing workflow
├── data_pipeline.py      # download → transform → load → notify
└── flaky_steps.py        # intentional failures for chaos testing
```

See [PLAN.md](PLAN.md) for the full implementation plan.
