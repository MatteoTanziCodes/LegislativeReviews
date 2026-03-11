from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Sequence


SECTIONS_PATH = Path(r"E:\Programming\buildcanada\canadian-laws\processed\sections_en.parquet")
STRING_PREVIEW_LENGTH = 300


def format_value(value: Any) -> str:
    if value is None:
        return "null"

    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                pass
            else:
                return json.dumps(decoded, ensure_ascii=False, indent=2)

        if len(value) > STRING_PREVIEW_LENGTH:
            preview = value[:STRING_PREVIEW_LENGTH]
            return (
                repr(preview)
                + f" ... [truncated, total_length={len(value)} chars]"
            )

    return repr(value)


def print_rows(title: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    print(f"\n{title}:\n")
    if not rows:
        print("(no rows)")
        return

    for index, row in enumerate(rows, start=1):
        print(f"Row {index}:")
        for column, value in zip(columns, row):
            print(f"  {column}: {format_value(value)}")
        print()


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    try:
        import duckdb
    except ImportError:
        print(
            "Error: duckdb is required. Install it with `pip install duckdb`.",
            file=sys.stderr,
        )
        return 1

    if not SECTIONS_PATH.exists():
        print(f"Error: parquet file not found at {SECTIONS_PATH}", file=sys.stderr)
        return 1

    con = duckdb.connect()
    try:
        source_path = str(SECTIONS_PATH)

        total_rows = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [source_path],
        ).fetchone()[0]
        distinct_document_ids = con.execute(
            "SELECT COUNT(DISTINCT document_id) FROM read_parquet(?)",
            [source_path],
        ).fetchone()[0]

        sample_query = """
            SELECT document_id, title_en, citation_en, section_key, section_text
            FROM read_parquet(?)
            LIMIT 5
        """
        sample_rows = con.execute(sample_query, [source_path]).fetchall()
        sample_columns = [column[0] for column in con.description]

        act_query = """
            SELECT document_id, title_en, citation_en, section_key, section_text
            FROM read_parquet(?)
            WHERE document_id = 'A-0.6'
            ORDER BY TRY_CAST(section_key AS DOUBLE) NULLS LAST, section_key
            LIMIT 10
        """
        act_rows = con.execute(act_query, [source_path]).fetchall()
        act_columns = [column[0] for column in con.description]

        barrier_query = """
            SELECT document_id, title_en, citation_en, section_key, section_text
            FROM read_parquet(?)
            WHERE section_text ILIKE '%barrier%'
            ORDER BY document_id, TRY_CAST(section_key AS DOUBLE) NULLS LAST, section_key
            LIMIT 10
        """
        barrier_rows = con.execute(barrier_query, [source_path]).fetchall()
        barrier_columns = [column[0] for column in con.description]
    finally:
        con.close()

    print("\nInspecting processed sections parquet...\n")
    print(f"Parquet file: {SECTIONS_PATH}")
    print(f"Total section rows: {total_rows}")
    print(f"Distinct document_id count: {distinct_document_ids}")

    print_rows("5 sample rows", sample_columns, sample_rows)
    print_rows(
        "Rows where document_id = 'A-0.6' (limit 10)",
        act_columns,
        act_rows,
    )
    print_rows(
        "Rows where section_text contains 'barrier' (limit 10)",
        barrier_columns,
        barrier_rows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
