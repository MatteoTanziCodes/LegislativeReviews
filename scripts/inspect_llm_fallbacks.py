from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Sequence


INPUT_PATH = Path(r"E:\Programming\buildcanada\canadian-laws\processed\document_domains_en.parquet")
LLM_SAMPLE_SIZE = 50
LOW_MARGIN_SAMPLE_SIZE = 25
REQUIRED_COLUMNS = [
    "document_id",
    "title_en",
    "primary_domain",
    "classification_method",
    "top_similarity_score",
    "second_best_domain",
    "second_best_score",
    "score_margin",
    "llm_raw_label",
]


def get_required_columns(con: Any, path: Path) -> set[str]:
    rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    return {row[0] for row in rows}


def print_counts(title: str, rows: Sequence[tuple[Any, ...]]) -> None:
    print(title)
    if not rows:
        print("  None")
        return

    for label, count in rows:
        print(f"  {label}: {count}")


def print_sample_rows(title: str, rows: Sequence[tuple[Any, ...]]) -> None:
    print()
    print(title)
    if not rows:
        print("  No matching rows.")
        return

    for row in rows:
        (
            document_id,
            title_en,
            primary_domain,
            second_best_domain,
            top_similarity_score,
            second_best_score,
            score_margin,
            llm_raw_label,
        ) = row
        print(f"- document_id: {document_id}")
        print(f"  title_en: {title_en}")
        print(f"  primary_domain: {primary_domain}")
        print(f"  second_best_domain: {second_best_domain}")
        print(f"  top_similarity_score: {float(top_similarity_score):.6f}")
        print(f"  second_best_score: {float(second_best_score):.6f}")
        print(f"  score_margin: {float(score_margin):.6f}")
        print(f"  llm_raw_label: {llm_raw_label}")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    try:
        import duckdb
    except ImportError:
        print("Error: duckdb is required. Install it with `pip install duckdb`.", file=sys.stderr)
        return 1

    if not INPUT_PATH.exists():
        print(f"Error: input parquet not found at {INPUT_PATH}", file=sys.stderr)
        return 1

    con = duckdb.connect()
    try:
        available_columns = get_required_columns(con, INPUT_PATH)
        missing_columns = [column for column in REQUIRED_COLUMNS if column not in available_columns]
        if missing_columns:
            print(
                "Error: document_domains parquet is missing required columns: "
                + ", ".join(missing_columns),
                file=sys.stderr,
            )
            return 1

        classification_method_counts = con.execute(
            """
            SELECT classification_method, COUNT(*) AS row_count
            FROM read_parquet(?)
            GROUP BY classification_method
            ORDER BY row_count DESC, classification_method
            """,
            [str(INPUT_PATH)],
        ).fetchall()
        print_counts("Counts by classification_method:", classification_method_counts)

        llm_fallback_total = con.execute(
            """
            SELECT COUNT(*)
            FROM read_parquet(?)
            WHERE classification_method = 'llm_fallback'
            """,
            [str(INPUT_PATH)],
        ).fetchone()[0]
        print()
        print(f"Total llm_fallback rows: {llm_fallback_total}")

        llm_fallback_domain_counts = con.execute(
            """
            SELECT primary_domain, COUNT(*) AS row_count
            FROM read_parquet(?)
            WHERE classification_method = 'llm_fallback'
            GROUP BY primary_domain
            ORDER BY row_count DESC, primary_domain
            """,
            [str(INPUT_PATH)],
        ).fetchall()
        print_counts("Counts by primary_domain within llm_fallback:", llm_fallback_domain_counts)

        llm_fallback_rows = con.execute(
            """
            SELECT
                document_id,
                title_en,
                primary_domain,
                second_best_domain,
                top_similarity_score,
                second_best_score,
                score_margin,
                llm_raw_label
            FROM read_parquet(?)
            WHERE classification_method = 'llm_fallback'
            ORDER BY score_margin ASC, top_similarity_score DESC, document_id
            LIMIT ?
            """,
            [str(INPUT_PATH), LLM_SAMPLE_SIZE],
        ).fetchall()
        print_sample_rows("50 sample llm_fallback rows:", llm_fallback_rows)

        semantic_low_margin_rows = con.execute(
            """
            SELECT
                document_id,
                title_en,
                primary_domain,
                second_best_domain,
                top_similarity_score,
                second_best_score,
                score_margin,
                llm_raw_label
            FROM read_parquet(?)
            WHERE classification_method = 'semantic_low_margin'
            ORDER BY score_margin ASC, top_similarity_score DESC, document_id
            LIMIT ?
            """,
            [str(INPUT_PATH), LOW_MARGIN_SAMPLE_SIZE],
        ).fetchall()
        print_sample_rows("25 sample semantic_low_margin rows:", semantic_low_margin_rows)
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
