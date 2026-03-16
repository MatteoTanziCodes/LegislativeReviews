from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Sequence


PROCESSED_DIR = Path(r"E:\Programming\buildcanada\canadian-laws\processed")
DOCUMENTS_PATH = PROCESSED_DIR / "documents_en.parquet"
SECTIONS_PATH = PROCESSED_DIR / "sections_en.parquet"
DOCUMENT_DOMAINS_PATH = PROCESSED_DIR / "document_domains_en.parquet"
DOCUMENT_DOMAIN_SCORES_PATH = PROCESSED_DIR / "document_domain_scores_en.parquet"
MANDATE_PATH = Path(r"config/review_mandates/obsolescence_modernization_v1.json")
TARGET_DOMAIN = "governance_administrative"
OUTPUT_PATH = PROCESSED_DIR / "review_inputs_governance_administrative_en.parquet"

MAX_SECTION_TEXT_CHARS = 1_500
PREVIEW_COUNT = 3
PREVIEW_TEXT_CHARS = 700


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def delete_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def truncate_text(value: str, limit: int) -> str:
    normalized = normalize_whitespace(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 15)].rstrip() + " ... [truncated]"


def preview_text(value: str) -> str:
    if len(value) <= PREVIEW_TEXT_CHARS:
        return value
    return value[:PREVIEW_TEXT_CHARS].rstrip() + " ... [truncated]"


def load_mandate(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Mandate config not found at {path}")

    with path.open("r", encoding="utf-8") as handle:
        mandate = json.load(handle)

    required_fields = {
        "mandate_id",
        "mandate_name",
        "evidence_requirements",
    }
    missing = [field for field in required_fields if field not in mandate]
    if missing:
        raise RuntimeError(
            "Mandate config is missing required fields: " + ", ".join(sorted(missing))
        )

    return mandate


def build_review_text(
    title_en: str,
    citation_en: str | None,
    primary_domain: str,
    mandate_name: str,
    sections: Sequence[tuple[str, str]],
) -> tuple[str, list[str], int]:
    lines = [
        f"Title: {title_en.strip()}",
        f"Citation: {(citation_en or '').strip() or '(none)'}",
        f"Domain: {primary_domain}",
        f"Mandate: {mandate_name}",
    ]

    selected_section_keys: list[str] = []
    for index, (section_key, section_text) in enumerate(sections, start=1):
        truncated_section_text = truncate_text(section_text, MAX_SECTION_TEXT_CHARS)
        lines.append(f"Section {index} ({section_key}): {truncated_section_text}")
        selected_section_keys.append(section_key)

    return "\n".join(lines), selected_section_keys, len(selected_section_keys)


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

    for path in (
        DOCUMENTS_PATH,
        SECTIONS_PATH,
        DOCUMENT_DOMAINS_PATH,
        DOCUMENT_DOMAIN_SCORES_PATH,
    ):
        if not path.exists():
            print(f"Error: input parquet not found at {path}", file=sys.stderr)
            return 1

    try:
        mandate = load_mandate(MANDATE_PATH)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    evidence_requirements = mandate.get("evidence_requirements", {})
    max_sections_per_document = int(
        evidence_requirements.get("minimum_sections_to_review", 5)
    )

    ensure_directory(PROCESSED_DIR)

    con = duckdb.connect()
    try:
        document_rows = con.execute(
            """
            SELECT
                c.document_id,
                c.title_en,
                d.citation_en,
                c.primary_domain,
                c.classification_method,
                COALESCE(s.similarity_score, c.classification_confidence) AS classification_confidence,
                c.score_margin
            FROM read_parquet(?) AS c
            INNER JOIN read_parquet(?) AS d
                ON c.document_id = d.document_id
            LEFT JOIN read_parquet(?) AS s
                ON c.document_id = s.document_id
               AND s.domain = c.primary_domain
            WHERE c.primary_domain = ?
            ORDER BY c.document_id
            """,
            [
                str(DOCUMENT_DOMAINS_PATH),
                str(DOCUMENTS_PATH),
                str(DOCUMENT_DOMAIN_SCORES_PATH),
                TARGET_DOMAIN,
            ],
        ).fetchall()

        section_rows = con.execute(
            f"""
            WITH ranked_sections AS (
                SELECT
                    document_id,
                    section_key,
                    section_text,
                    ROW_NUMBER() OVER (
                        PARTITION BY document_id
                        ORDER BY
                            CASE WHEN TRY_CAST(section_key AS DOUBLE) IS NULL THEN 1 ELSE 0 END,
                            TRY_CAST(section_key AS DOUBLE) NULLS LAST,
                            section_key
                    ) AS section_rank
                FROM read_parquet(?)
                WHERE document_id IN (
                    SELECT document_id
                    FROM read_parquet(?)
                    WHERE primary_domain = ?
                )
            )
            SELECT document_id, section_key, section_text
            FROM ranked_sections
            WHERE section_rank <= {max_sections_per_document}
            ORDER BY document_id, section_rank
            """,
            [
                str(SECTIONS_PATH),
                str(DOCUMENT_DOMAINS_PATH),
                TARGET_DOMAIN,
            ],
        ).fetchall()

        sections_by_document: dict[str, list[tuple[str, str]]] = {}
        for document_id, section_key, section_text in section_rows:
            sections_by_document.setdefault(document_id, []).append(
                (section_key, section_text)
            )

        output_rows: list[tuple[Any, ...]] = []
        total_section_count_used = 0

        for (
            document_id,
            title_en,
            citation_en,
            primary_domain,
            classification_method,
            classification_confidence,
            score_margin,
        ) in document_rows:
            review_text, evidence_section_keys, section_count_used = build_review_text(
                title_en=title_en,
                citation_en=citation_en,
                primary_domain=primary_domain,
                mandate_name=mandate["mandate_name"],
                sections=sections_by_document.get(document_id, []),
            )
            output_rows.append(
                (
                    mandate["mandate_id"],
                    document_id,
                    title_en,
                    citation_en,
                    primary_domain,
                    classification_method,
                    float(classification_confidence) if classification_confidence is not None else None,
                    float(score_margin) if score_margin is not None else None,
                    review_text,
                    json.dumps(evidence_section_keys, ensure_ascii=False),
                    section_count_used,
                )
            )
            total_section_count_used += section_count_used

        temp_output_path = OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet")
        delete_if_exists(temp_output_path)
        delete_if_exists(OUTPUT_PATH)

        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE review_inputs (
                mandate_id VARCHAR,
                document_id VARCHAR,
                title_en VARCHAR,
                citation_en VARCHAR,
                primary_domain VARCHAR,
                classification_method VARCHAR,
                classification_confidence DOUBLE,
                score_margin DOUBLE,
                review_text VARCHAR,
                evidence_section_keys VARCHAR,
                section_count_used BIGINT
            )
            """
        )
        if output_rows:
            con.executemany(
                "INSERT INTO review_inputs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                output_rows,
            )
        con.execute("COPY review_inputs TO ? (FORMAT PARQUET)", [str(temp_output_path)])
        temp_output_path.replace(OUTPUT_PATH)
    finally:
        con.close()
        delete_if_exists(OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet"))

    total_rows_written = len(output_rows)
    average_section_count_used = (
        total_section_count_used / total_rows_written if total_rows_written else 0.0
    )

    print(f"Review input rows written: {total_rows_written}")
    print(f"Average section_count_used: {average_section_count_used:.2f}")
    print("\nSample review_text previews:\n")
    for index, row in enumerate(output_rows[:PREVIEW_COUNT], start=1):
        (
            _mandate_id,
            document_id,
            title_en,
            _citation_en,
            _primary_domain,
            _classification_method,
            _classification_confidence,
            _score_margin,
            review_text,
            _evidence_section_keys,
            section_count_used,
        ) = row
        print(f"Sample {index}:")
        print(f"  document_id: {document_id}")
        print(f"  title_en: {title_en}")
        print(f"  section_count_used: {section_count_used}")
        print("  review_text:")
        for line in preview_text(review_text).splitlines():
            print(f"    {line}")
        print()

    print(f"Output written to: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
