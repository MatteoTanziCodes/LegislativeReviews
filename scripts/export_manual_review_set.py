from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Sequence


INPUT_PATH = Path(
    r"E:\Programming\buildcanada\canadian-laws\processed\reviews_governance_administrative_prosperity_sample_en.parquet"
)
OUTPUT_PATH = Path(
    r"E:\Programming\buildcanada\canadian-laws\processed\manual_review_governance_administrative_sample.csv"
)
EXPORT_DECISIONS = ("repeal_candidate", "escalate", "amend")
OUTPUT_COLUMNS = [
    "document_id",
    "title_en",
    "decision",
    "decision_confidence",
    "operational_relevance_score",
    "prosperity_alignment_score",
    "administrative_burden_score",
    "repeal_risk_score",
    "evidence_section_keys",
    "rationale",
    "human_agrees_decision",
    "human_corrected_decision",
    "human_notes",
]


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

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        rows = con.execute(
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
            WHERE decision IN (?, ?, ?)
            ORDER BY
                CASE decision
                    WHEN 'repeal_candidate' THEN 1
                    WHEN 'escalate' THEN 2
                    WHEN 'amend' THEN 3
                    ELSE 4
                END,
                document_id
            """,
            [str(INPUT_PATH), *EXPORT_DECISIONS],
        ).fetchall()
    finally:
        con.close()

    decision_counts: Counter[str] = Counter()
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for row in rows:
            (
                document_id,
                title_en,
                decision,
                decision_confidence,
                operational_relevance_score,
                prosperity_alignment_score,
                administrative_burden_score,
                repeal_risk_score,
                evidence_section_keys,
                rationale,
            ) = row
            decision_counts[decision] += 1
            writer.writerow(
                {
                    "document_id": document_id,
                    "title_en": title_en,
                    "decision": decision,
                    "decision_confidence": decision_confidence,
                    "operational_relevance_score": operational_relevance_score,
                    "prosperity_alignment_score": prosperity_alignment_score,
                    "administrative_burden_score": administrative_burden_score,
                    "repeal_risk_score": repeal_risk_score,
                    "evidence_section_keys": evidence_section_keys,
                    "rationale": rationale,
                    "human_agrees_decision": "",
                    "human_corrected_decision": "",
                    "human_notes": "",
                }
            )

    print(f"Total exported rows: {len(rows)}")
    print("Counts by decision:")
    for decision, count in sorted(decision_counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {decision}: {count}")
    print(f"Output written to: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
