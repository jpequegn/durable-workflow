"""CLI entrypoint for the durable workflow engine.

Usage:
    uv run wf --help
    uv run wf run examples/podcast_pipeline.py --episode-id ep-123
    uv run wf status <run_id>
    uv run wf resume <run_id>
    uv run wf runs list
    uv run wf runs inspect <run_id>
"""

from __future__ import annotations

import importlib.util
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import click

from workflow.engine import WorkflowEngine, WorkflowError

# ---------------------------------------------------------------------------
# Colour / formatting helpers
# ---------------------------------------------------------------------------

_STATUS_COLOURS: dict[str, str] = {
    "completed": "green",
    "failed": "red",
    "running": "yellow",
    "pending": "cyan",
}


def _colourise(status: str) -> str:
    colour = _STATUS_COLOURS.get(status, "white")
    return click.style(status, fg=colour, bold=True)


def _fmt_ts(ts: datetime | None) -> str:
    if ts is None:
        return "—"
    return ts.strftime("%H:%M:%S")


def _fmt_duration(started: datetime | None, finished: datetime | None) -> str:
    if started is None or finished is None:
        return "—"
    delta = finished - started
    secs = delta.total_seconds()
    if secs < 60:
        return f"{secs:.2f}s"
    return f"{int(secs // 60)}m {int(secs % 60)}s"


def _truncate(text: str, width: int = 60) -> str:
    if len(text) <= width:
        return text
    return text[:width] + "…"


def _short_id(run_id: str) -> str:
    """Return the first 8 chars of a uuid4 for compact display."""
    return run_id[:8]


# ---------------------------------------------------------------------------
# File loader — imports a workflow module from a .py path
# ---------------------------------------------------------------------------


def _load_workflow_module(path: str) -> Any:
    """Import *path* as a module and return it."""
    p = Path(path).resolve()
    if not p.exists():
        raise click.ClickException(f"File not found: {path}")
    if not p.suffix == ".py":
        raise click.ClickException(f"Expected a .py file, got: {path}")

    spec = importlib.util.spec_from_file_location("_wf_module", p)
    if spec is None or spec.loader is None:
        raise click.ClickException(f"Cannot load module from: {path}")

    mod = importlib.util.module_from_spec(spec)
    sys.modules["_wf_module"] = mod
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:
        raise click.ClickException(f"Error loading {path}: {exc}") from exc
    return mod


# ---------------------------------------------------------------------------
# CLI root
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(version="0.1.0", prog_name="wf")
def cli() -> None:
    """Durable Workflow Engine — resumable, crash-safe workflow execution."""


# ---------------------------------------------------------------------------
# wf run
# ---------------------------------------------------------------------------


@cli.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
@click.argument("workflow_file")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="SQLite database path.")
@click.pass_context
def run(ctx: click.Context, workflow_file: str, db: str) -> None:
    """Run a workflow defined in WORKFLOW_FILE.

    WORKFLOW_FILE must expose a WORKFLOW callable and optionally an
    INPUT_SCHEMA dict mapping argument names to Python types.

    Extra flags are forwarded as keyword arguments to the workflow function.
    Use kebab-case flags: --episode-id becomes episode_id.

    Example:

        wf run examples/podcast_pipeline.py --episode-id ep-123
    """
    mod = _load_workflow_module(workflow_file)

    func = getattr(mod, "WORKFLOW", None)
    if func is None:
        raise click.ClickException(
            f"{workflow_file} must define a module-level WORKFLOW variable "
            "pointing to the workflow function."
        )

    schema: dict[str, type] = getattr(mod, "INPUT_SCHEMA", {})

    # Parse extra args into kwargs, converting types from INPUT_SCHEMA.
    extra: list[str] = ctx.args
    inputs: dict[str, Any] = {}
    i = 0
    while i < len(extra):
        token = extra[i]
        if token.startswith("--"):
            key = token[2:].replace("-", "_")
            if i + 1 < len(extra) and not extra[i + 1].startswith("--"):
                raw_value = extra[i + 1]
                i += 2
            else:
                raw_value = "true"
                i += 1
            # Coerce type if declared in INPUT_SCHEMA
            target_type = schema.get(key, str)
            try:
                if target_type is bool:
                    inputs[key] = raw_value.lower() not in ("false", "0", "no")
                else:
                    inputs[key] = target_type(raw_value)
            except (ValueError, TypeError) as exc:
                raise click.ClickException(
                    f"Cannot convert --{key.replace('_', '-')} value {raw_value!r} "
                    f"to {target_type.__name__}: {exc}"
                )
        else:
            i += 1

    with WorkflowEngine(db_path=db) as engine:
        click.echo(
            f"▶ Running {click.style(func.__name__, bold=True)}"
            + (f" with {inputs}" if inputs else "")
        )
        try:
            run_id = engine.run(func, **inputs)
        except Exception as exc:
            # Engine already persisted FAILED — surface a clean error.
            runs = engine.store.list_runs(limit=1)
            failed_id = runs[0].id if runs else "unknown"
            click.echo(
                click.style("✗ Workflow failed", fg="red", bold=True)
                + f"  run_id={_short_id(failed_id)}…\n"
                + click.style(str(exc), fg="red")
            )
            click.echo(f"\nInspect with:  wf runs inspect {failed_id}")
            sys.exit(1)

        click.echo(
            click.style("✓ Completed", fg="green", bold=True)
            + f"  run_id={click.style(run_id, bold=True)}"
        )
        click.echo(f"\nInspect with:  wf status {run_id}")


# ---------------------------------------------------------------------------
# wf status
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("run_id")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="SQLite database path.")
def status(run_id: str, db: str) -> None:
    """Show per-step status and timing for a workflow run."""
    with WorkflowEngine(db_path=db) as engine:
        try:
            s = engine.status(run_id)
        except WorkflowError as exc:
            raise click.ClickException(str(exc))

    run = s.run

    # Run header
    click.echo()
    click.echo(f"  Workflow : {click.style(run.workflow_name, bold=True)}")
    click.echo(f"  Run ID   : {run.id}")
    click.echo(f"  Status   : {_colourise(run.status)}")
    click.echo(f"  Started  : {_fmt_ts(run.created_at)}")
    click.echo(
        f"  Duration : {_fmt_duration(run.created_at, run.finished_at)}"
    )
    click.echo()

    if not s.steps:
        click.echo("  (no steps recorded)")
        return

    # Column widths
    name_w = max(len(st.step_name) for st in s.steps)
    name_w = max(name_w, 4)

    # Header
    header = (
        f"  {'STEP':<{name_w}}  {'ATT':>3}  {'STATUS':<9}  "
        f"{'STARTED':>8}  {'DURATION':>9}  {'CACHED':>6}"
    )
    click.echo(click.style(header, dim=True))
    click.echo(click.style("  " + "─" * (len(header) - 2), dim=True))

    for st in s.steps:
        cached = "yes" if st.status == "completed" and st.attempt > 0 else ""
        # A cache-hit has no started_at because it never ran again — infer from store
        duration = _fmt_duration(st.started_at, st.finished_at)
        click.echo(
            f"  {st.step_name:<{name_w}}  {st.attempt:>3}  "
            f"{_colourise(st.status):<9}  "
            f"{_fmt_ts(st.started_at):>8}  {duration:>9}  {cached:>6}"
        )
        if st.status == "failed" and st.error:
            first_line = st.error.strip().splitlines()[-1]
            click.echo(
                "  " + " " * name_w + "     "
                + click.style(_truncate(first_line, 70), fg="red")
            )

    click.echo()


# ---------------------------------------------------------------------------
# wf resume
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("run_id")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="SQLite database path.")
@click.option(
    "--workflow-file", "-f", default=None,
    help="Path to the .py file defining the workflow (required when the function "
         "was not run from the same process).",
)
def resume(run_id: str, db: str, workflow_file: str | None) -> None:
    """Resume a previously interrupted workflow run.

    Completed steps are skipped automatically via the step cache.

    If the workflow function is not already registered (e.g. when resuming from
    a fresh shell), pass --workflow-file to reload it:

        wf resume <run_id> --workflow-file examples/podcast_pipeline.py
    """
    with WorkflowEngine(db_path=db) as engine:
        try:
            run_rec = engine.store.get_run(run_id)
        except KeyError:
            raise click.ClickException(f"No workflow run found with id {run_id!r}")

        # Load + register the workflow function if a file was provided.
        if workflow_file is not None:
            mod = _load_workflow_module(workflow_file)
            func = getattr(mod, "WORKFLOW", None)
            if func is None:
                raise click.ClickException(
                    f"{workflow_file} must define a module-level WORKFLOW variable."
                )
            engine.register(func)

        click.echo(
            f"↺ Resuming {click.style(run_rec.workflow_name, bold=True)}"
            f"  run_id={run_id}"
        )
        try:
            engine.resume(run_id)
        except WorkflowError as exc:
            raise click.ClickException(str(exc))
        except Exception as exc:
            click.echo(
                click.style("✗ Resume failed", fg="red", bold=True)
                + f": {exc}"
            )
            click.echo(f"\nInspect with:  wf runs inspect {run_id}")
            sys.exit(1)

        click.echo(
            click.style("✓ Completed", fg="green", bold=True)
            + f"  run_id={click.style(run_id, bold=True)}"
        )
        click.echo(f"\nInspect with:  wf status {run_id}")


# ---------------------------------------------------------------------------
# wf runs list
# ---------------------------------------------------------------------------


@cli.group()
def runs() -> None:
    """Manage and inspect workflow runs."""


@runs.command("list")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="SQLite database path.")
@click.option("-n", "--limit", default=20, show_default=True, help="Maximum rows to show.")
def runs_list(db: str, limit: int) -> None:
    """List recent workflow runs, newest first.

    Colour coding: green=completed  red=failed  yellow=running
    """
    with WorkflowEngine(db_path=db) as engine:
        all_runs = engine.store.list_runs(limit=limit)

    if not all_runs:
        click.echo("No workflow runs found.")
        return

    click.echo()
    header = f"  {'ID':>8}  {'WORKFLOW':<24}  {'STATUS':<9}  {'STARTED':>8}  {'DURATION':>9}"
    click.echo(click.style(header, dim=True))
    click.echo(click.style("  " + "─" * (len(header) - 2), dim=True))

    for r in all_runs:
        click.echo(
            f"  {_short_id(r.id):>8}…  {r.workflow_name:<24}  "
            f"{_colourise(r.status):<9}  "
            f"{_fmt_ts(r.created_at):>8}  "
            f"{_fmt_duration(r.created_at, r.finished_at):>9}"
        )

    click.echo()
    click.echo(f"  Showing {len(all_runs)} run(s).  Full id: wf runs inspect <id>")
    click.echo()


# ---------------------------------------------------------------------------
# wf runs inspect
# ---------------------------------------------------------------------------


@runs.command("inspect")
@click.argument("run_id")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="SQLite database path.")
@click.option("--show-output", is_flag=True, default=False, help="Print pickled output repr.")
def runs_inspect(run_id: str, db: str, show_output: bool) -> None:
    """Show a full step-by-step execution trace for a workflow run.

    Displays step name, attempt number, status, duration and — on failure —
    the full error message.  Use --show-output to also print step outputs.
    """
    import pickle

    with WorkflowEngine(db_path=db) as engine:
        try:
            s = engine.status(run_id)
        except WorkflowError as exc:
            raise click.ClickException(str(exc))

    run = s.run
    click.echo()
    click.echo(f"  Workflow  : {click.style(run.workflow_name, bold=True)}")
    click.echo(f"  Run ID    : {run.id}")
    click.echo(f"  Status    : {_colourise(run.status)}")
    click.echo(
        f"  Started   : "
        + (run.created_at.strftime("%Y-%m-%d %H:%M:%S") if run.created_at else "—")
    )
    click.echo(
        f"  Finished  : "
        + (run.finished_at.strftime("%Y-%m-%d %H:%M:%S") if run.finished_at else "—")
    )
    click.echo(
        f"  Duration  : {_fmt_duration(run.created_at, run.finished_at)}"
    )
    click.echo()

    if not s.steps:
        click.echo("  (no steps recorded)")
        click.echo()
        return

    for st in s.steps:
        marker = {
            "completed": click.style("✓", fg="green", bold=True),
            "failed":    click.style("✗", fg="red",   bold=True),
            "running":   click.style("⟳", fg="yellow", bold=True),
            "pending":   click.style("·", fg="cyan"),
        }.get(st.status, " ")

        duration = _fmt_duration(st.started_at, st.finished_at)
        click.echo(
            f"  {marker} {click.style(st.step_name, bold=True)}"
            f"  attempt={st.attempt}"
            f"  status={_colourise(st.status)}"
            f"  duration={duration}"
        )

        if show_output and st.output is not None:
            try:
                value = pickle.loads(st.output)
                click.echo(
                    "      output : "
                    + click.style(_truncate(repr(value), 80), fg="cyan")
                )
            except Exception:
                click.echo("      output : <unpickling failed>")

        if st.error:
            lines = st.error.strip().splitlines()
            # Always show last line; show full traceback on inspect
            click.echo(
                "      error  : "
                + click.style(_truncate(lines[-1], 80), fg="red")
            )
            if len(lines) > 1:
                for line in lines[:-1]:
                    click.echo(
                        "               " + click.style(line, fg="red", dim=True)
                    )

        click.echo()

    click.echo()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    cli()
