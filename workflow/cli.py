"""CLI entrypoint for the durable workflow engine.

Usage:
    uv run wf --help
    uv run wf run <workflow_file> [--args ...]
    uv run wf status <run_id>
    uv run wf resume <run_id>
    uv run wf runs list
    uv run wf runs inspect <run_id>
"""

import click


@click.group()
@click.version_option(version="0.1.0", prog_name="wf")
def cli() -> None:
    """Durable Workflow Engine — resumable, crash-safe workflow execution."""


@cli.command()
@click.argument("workflow_file")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="Path to the SQLite database.")
def run(workflow_file: str, db: str) -> None:
    """Run a workflow from WORKFLOW_FILE.

    WORKFLOW_FILE is a Python file that defines a @workflow-decorated function.
    """
    click.echo(f"[wf] run: {workflow_file!r}  (db={db})")
    click.echo("Not yet implemented — coming in a future issue.")


@cli.command()
@click.argument("run_id")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="Path to the SQLite database.")
def status(run_id: str, db: str) -> None:
    """Show status of a workflow run (per-step timing and result)."""
    click.echo(f"[wf] status: {run_id!r}  (db={db})")
    click.echo("Not yet implemented — coming in a future issue.")


@cli.command()
@click.argument("run_id")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="Path to the SQLite database.")
def resume(run_id: str, db: str) -> None:
    """Resume a previously interrupted workflow run."""
    click.echo(f"[wf] resume: {run_id!r}  (db={db})")
    click.echo("Not yet implemented — coming in a future issue.")


@cli.group()
def runs() -> None:
    """Manage workflow runs."""


@runs.command("list")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="Path to the SQLite database.")
@click.option("-n", "--limit", default=20, show_default=True, help="Maximum number of runs to show.")
def runs_list(db: str, limit: int) -> None:
    """List recent workflow runs."""
    click.echo(f"[wf] runs list  (db={db}, limit={limit})")
    click.echo("Not yet implemented — coming in a future issue.")


@runs.command("inspect")
@click.argument("run_id")
@click.option("--db", default="~/.wf/runs.db", show_default=True, help="Path to the SQLite database.")
def runs_inspect(run_id: str, db: str) -> None:
    """Show a full step-by-step trace for a workflow run."""
    click.echo(f"[wf] runs inspect: {run_id!r}  (db={db})")
    click.echo("Not yet implemented — coming in a future issue.")


def main() -> None:
    cli()
