from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Sequence


DATASET_DIR = Path(r"E:\Programming\buildcanada\canadian-laws")
STRING_PREVIEW_LENGTH = 500
JSON_PREVIEW_ITEMS = 5


def format_value(value: Any) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(decoded, dict):
                    preview = dict(list(decoded.items())[:JSON_PREVIEW_ITEMS])
                    return (
                        f"JSON string object with {len(decoded)} keys\n"
                        f"{json.dumps(preview, ensure_ascii=False, indent=2)}"
                    )
                if isinstance(decoded, list):
                    preview = decoded[:JSON_PREVIEW_ITEMS]
                    return (
                        f"JSON string array with {len(decoded)} items\n"
                        f"{json.dumps(preview, ensure_ascii=False, indent=2)}"
                    )

        if len(value) > STRING_PREVIEW_LENGTH:
            preview = value[:STRING_PREVIEW_LENGTH]
            return (
                repr(preview)
                + f" ... [truncated, total_length={len(value)} chars]"
            )

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2, default=str)
    return repr(value)


def list_parquet_files(dataset_dir: Path) -> list[Path]:
    return sorted(path for path in dataset_dir.rglob("*.parquet") if path.is_file())


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    try:
        import duckdb
    except ImportError as exc:
        print(
            "Error: duckdb is required. Install it with `pip install duckdb`.",
            file=sys.stderr,
        )
        return 1

    parquet_files = list_parquet_files(DATASET_DIR)
    if not parquet_files:
        print(
            f"Error: no parquet files found under {DATASET_DIR}",
            file=sys.stderr,
        )
        return 1

    con = duckdb.connect()
    scan_target = [str(path) for path in parquet_files]

    print("\nReading dataset...\n")

    schema_rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [scan_target],
    ).fetchall()
    column_names = [row[0] for row in schema_rows]

    print("Columns:\n")
    for name, column_type, *_ in schema_rows:
        print(f"- {name}: {column_type}")

    row_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [scan_target],
    ).fetchone()[0]

    sample_columns: list[str] = []
    for name, column_type, *_ in schema_rows:
        identifier = quote_identifier(name)
        if column_type == "TIMESTAMP WITH TIME ZONE":
            sample_columns.append(f"CAST({identifier} AS VARCHAR) AS {identifier}")
        else:
            sample_columns.append(identifier)

    first_row = con.execute(
        f"SELECT {', '.join(sample_columns)} FROM read_parquet(?) LIMIT 1",
        [scan_target],
    ).fetchone()

    print(f"\nTotal rows: {row_count}")

    print("\nFirst row sample:\n")
    if first_row is None:
        print("(dataset is empty)")
    else:
        for name, value in zip(column_names, first_row):
            print(f"{name}:")
            print(format_value(value))
            print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
