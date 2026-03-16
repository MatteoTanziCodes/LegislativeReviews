from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Sequence


CLASSIFICATION_PATH = Path(
    r"E:\Programming\buildcanada\canadian-laws\processed\document_domains_en.parquet"
)
SAMPLE_BUCKETS = (
    "indigenous_crown_relations",
    "other",
    "governance_administrative",
)
SAMPLE_LIMIT = 20
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


def print_key_value_rows(title: str, rows: Sequence[Sequence[Any]]) -> None:
    print(f"\n{title}:\n")
    if not rows:
        print("(no rows)")
        return

    for key, value in rows:
        print(f"  {key}: {value}")


def print_sample_rows(
    title: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> None:
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

    if not CLASSIFICATION_PATH.exists():
        print(
            f"Error: parquet file not found at {CLASSIFICATION_PATH}",
            file=sys.stderr,
        )
        return 1

    con = duckdb.connect()
    try:
        source_path = str(CLASSIFICATION_PATH)
        schema_rows = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [source_path],
        ).fetchall()
        available_columns = {row[0] for row in schema_rows}

        primary_domain_counts = con.execute(
            """
            SELECT primary_domain, COUNT(*) AS row_count
            FROM read_parquet(?)
            GROUP BY 1
            ORDER BY row_count DESC, primary_domain
            """,
            [source_path],
        ).fetchall()

        classification_method_counts: list[Sequence[Any]] = []
        if "classification_method" in available_columns:
            classification_method_counts = con.execute(
                """
                SELECT classification_method, COUNT(*) AS row_count
                FROM read_parquet(?)
                GROUP BY 1
                ORDER BY row_count DESC, classification_method
                """,
                [source_path],
            ).fetchall()

        sample_columns = [
            column
            for column in (
                "document_id",
                "title_en",
                "primary_domain",
                "top_similarity_score",
                "classification_method",
                "matched_taxonomy_description",
            )
            if column in available_columns
        ]

        bucket_rows: dict[str, list[Sequence[Any]]] = {}
        select_list = ", ".join(sample_columns)
        for bucket in SAMPLE_BUCKETS:
            bucket_rows[bucket] = con.execute(
                f"""
                SELECT {select_list}
                FROM read_parquet(?)
                WHERE primary_domain = ?
                ORDER BY top_similarity_score DESC NULLS LAST, document_id
                LIMIT {SAMPLE_LIMIT}
                """,
                [source_path, bucket],
            ).fetchall()
    finally:
        con.close()

    print("\nInspecting document classification parquet...\n")
    print(f"Parquet file: {CLASSIFICATION_PATH}")

    print_key_value_rows("Counts by primary_domain", primary_domain_counts)
    if classification_method_counts:
        print_key_value_rows(
            "Counts by classification_method",
            classification_method_counts,
        )

    for bucket in SAMPLE_BUCKETS:
        print_sample_rows(
            f"Sample rows for {bucket} (limit {SAMPLE_LIMIT})",
            sample_columns,
            bucket_rows[bucket],
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
