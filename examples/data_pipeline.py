"""Example: download → transform → load → notify ETL pipeline.

Demonstrates a 4-step durable ETL workflow.

Usage
-----
    uv run wf run examples/data_pipeline.py --source-url https://example.com/data.csv --table staging
    uv run wf status <run_id>
"""

from __future__ import annotations

from workflow.step import step


# ---------------------------------------------------------------------------
# Step functions — pure, no engine awareness
# ---------------------------------------------------------------------------


def download_data(source_url: str) -> bytes:
    """Fetch raw data from *source_url*. Returns raw bytes."""
    print(f"  [download_data] Fetching {source_url} …")
    return b"id,name,value\n1,foo,42\n2,bar,99\n"


def transform_data(raw: bytes) -> list[dict]:
    """Parse CSV bytes into a list of dicts."""
    print(f"  [transform_data] Parsing {len(raw)} bytes …")
    lines = raw.decode().strip().splitlines()
    headers = lines[0].split(",")
    records = [dict(zip(headers, line.split(","))) for line in lines[1:]]
    print(f"  [transform_data] {len(records)} records parsed")
    return records


def load_to_db(records: list[dict], table: str) -> int:
    """Simulate inserting records into *table*. Returns row count."""
    print(f"  [load_to_db] Inserting {len(records)} rows into {table!r} …")
    return len(records)


def notify(rows_written: int) -> str:
    """Emit a completion notification."""
    msg = f"ETL complete — {rows_written} rows written."
    print(f"  [notify] {msg}")
    return msg


# ---------------------------------------------------------------------------
# Workflow function
# ---------------------------------------------------------------------------


def etl_pipeline(source_url: str, table: str) -> str:
    """4-step durable ETL: download → transform → load → notify."""
    raw          = step("download",  download_data,  source_url)
    records      = step("transform", transform_data, raw)
    rows_written = step("load",      load_to_db,     records, table)
    return         step("notify",    notify,          rows_written)


# ---------------------------------------------------------------------------
# CLI discovery hooks
# ---------------------------------------------------------------------------

WORKFLOW = etl_pipeline

INPUT_SCHEMA: dict[str, type] = {
    "source_url": str,
    "table": str,
}
