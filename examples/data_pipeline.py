"""Example: download → transform → load → notify data pipeline.

Placeholder — will be wired to the engine in a future issue.
"""

# from workflow import workflow, step, WorkflowEngine


def download_data(source_url: str) -> bytes:
    print(f"[download_data] url={source_url}")
    return b"raw,data,here"


def transform_data(raw: bytes) -> list[dict]:
    print(f"[transform_data] bytes={len(raw)}")
    return [{"col": "value"}]


def load_to_db(records: list[dict], table: str) -> int:
    print(f"[load_to_db] table={table} rows={len(records)}")
    return len(records)


def notify(rows_written: int) -> None:
    print(f"[notify] Wrote {rows_written} rows — pipeline complete.")


# @workflow
# def etl_pipeline(source_url: str, table: str):
#     raw          = step("download",  download_data, source_url)
#     records      = step("transform", transform_data, raw)
#     rows_written = step("load",      load_to_db, records, table)
#     step("notify", notify, rows_written)


if __name__ == "__main__":
    source_url = "https://example.com/data.csv"
    table = "staging"

    raw = download_data(source_url)
    records = transform_data(raw)
    rows_written = load_to_db(records, table)
    notify(rows_written)
