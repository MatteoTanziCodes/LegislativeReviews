from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Sequence


INPUT_PATH = Path(
    r"E:\Programming\buildcanada\canadian-laws\processed\reviews_governance_administrative_prosperity_sample_en.parquet"
)
DECISION_LABELS = ["retain", "amend", "repeal_candidate", "escalate"]
SAMPLE_SIZE = 10


def print_counts(title: str, rows: Sequence[tuple[Any, ...]]) -> None:
    print(title)
    if not rows:
        print("  None")
        return

    for label, value in rows:
        if isinstance(value, float):
            print(f"  {label}: {value:.3f}")
        else:
            print(f"  {label}: {value}")


def print_sample_rows(decision: str, rows: Sequence[tuple[Any, ...]]) -> None:
    print()
    print(f"Sample rows for decision = {decision}:")
    if not rows:
        print("  No rows.")
        return

    for row in rows:
        (
            document_id,
            title_en,
            row_decision,
            decision_confidence,
            operational_relevance_score,
            prosperity_alignment_score,
            administrative_burden_score,
            repeal_risk_score,
            evidence_section_keys,
            rationale,
        ) = row
        print(f"- document_id: {document_id}")
        print(f"  title_en: {title_en}")
        print(f"  decision: {row_decision}")
        print(f"  decision_confidence: {float(decision_confidence):.3f}")
        print(f"  operational_relevance_score: {operational_relevance_score}")
        print(f"  prosperity_alignment_score: {prosperity_alignment_score}")
        print(f"  administrative_burden_score: {administrative_burden_score}")
        print(f"  repeal_risk_score: {repeal_risk_score}")
        print(f"  evidence_section_keys: {evidence_section_keys}")
        print(f"  rationale: {rationale}")


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

    if not INPUT_PATH.exists():
        print(f"Error: input parquet not found at {INPUT_PATH}", file=sys.stderr)
        return 1

    con = duckdb.connect()
    try:
        total_row_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(INPUT_PATH)],
        ).fetchone()[0]
        decision_counts = con.execute(
            """
            SELECT decision, COUNT(*) AS row_count
            FROM read_parquet(?)
            GROUP BY decision
            ORDER BY row_count DESC, decision
            """,
            [str(INPUT_PATH)],
        ).fetchall()
        decision_confidence_averages = con.execute(
            """
            SELECT decision, AVG(decision_confidence) AS avg_confidence
            FROM read_parquet(?)
            GROUP BY decision
            ORDER BY decision
            """,
            [str(INPUT_PATH)],
        ).fetchall()

        print(f"Total row count: {total_row_count}")
        print()
        print_counts("Counts by decision:", decision_counts)
        print()
        print_counts("Average decision_confidence by decision:", decision_confidence_averages)

        existing_decisions = {row[0] for row in decision_counts}
        for decision in DECISION_LABELS:
            if decision not in existing_decisions:
                continue
            sample_rows = con.execute(
                """
                SELECT
                    document_id,
                    title_en,
                    decision,
                    decision_confidence,
                    operational_relevance_score,
                    prosperity_alignment_score,
                    administrative_burden_score,
                    repeal_risk_score,
                    evidence_section_keys,
                    rationale
                FROM read_parquet(?)
                WHERE decision = ?
                ORDER BY document_id
                LIMIT ?
                """,
                [str(INPUT_PATH), decision, SAMPLE_SIZE],
            ).fetchall()
            print_sample_rows(decision, sample_rows)
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
