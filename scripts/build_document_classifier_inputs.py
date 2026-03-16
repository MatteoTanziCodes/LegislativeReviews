from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence


PROCESSED_DIR = Path(r"E:\Programming\buildcanada\canadian-laws\processed")
DOCUMENTS_PATH = PROCESSED_DIR / "documents_en.parquet"
SECTIONS_PATH = PROCESSED_DIR / "sections_en.parquet"
OUTPUT_PATH = PROCESSED_DIR / "document_classifier_inputs_en.parquet"

MAX_SECTIONS_PER_DOCUMENT = 5
MAX_SECTION_TEXT_CHARS = 1_200
MAX_CLASSIFIER_INPUT_CHARS = 6_000
PREVIEW_COUNT = 5
PREVIEW_TEXT_CHARS = 500


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


def build_classifier_input_text(
    title_en: str,
    citation_en: str | None,
    sections: Sequence[tuple[str, str]],
) -> tuple[str, int]:
    lines = [f"Title: {title_en.strip()}"]
    lines.append(f"Citation: {(citation_en or '').strip() or '(none)'}")

    used_section_count = 0
    current_text = "\n".join(lines)

    for section_key, section_text in sections[:MAX_SECTIONS_PER_DOCUMENT]:
        truncated_section_text = truncate_text(section_text, MAX_SECTION_TEXT_CHARS)
        section_line = f"Section {section_key}: {truncated_section_text}"
        candidate_text = current_text + "\n" + section_line

        if len(candidate_text) <= MAX_CLASSIFIER_INPUT_CHARS:
            lines.append(section_line)
            current_text = candidate_text
            used_section_count += 1
            continue

        remaining_chars = MAX_CLASSIFIER_INPUT_CHARS - len(current_text) - 1
        if remaining_chars <= len(f"Section {section_key}: "):
            break

        allowed_text_chars = remaining_chars - len(f"Section {section_key}: ")
        if allowed_text_chars <= 0:
            break

        adjusted_line = (
            f"Section {section_key}: "
            f"{truncate_text(truncated_section_text, allowed_text_chars)}"
        )
        lines.append(adjusted_line)
        current_text = current_text + "\n" + adjusted_line
        used_section_count += 1
        break

    return current_text, used_section_count


def preview_text(value: str) -> str:
    if len(value) <= PREVIEW_TEXT_CHARS:
        return value
    return value[:PREVIEW_TEXT_CHARS].rstrip() + " ... [truncated]"


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

    if not DOCUMENTS_PATH.exists():
        print(f"Error: documents parquet not found at {DOCUMENTS_PATH}", file=sys.stderr)
        return 1
    if not SECTIONS_PATH.exists():
        print(f"Error: sections parquet not found at {SECTIONS_PATH}", file=sys.stderr)
        return 1

    ensure_directory(PROCESSED_DIR)

    con = duckdb.connect()
    try:
        document_rows = con.execute(
            """
            SELECT document_id, title_en, citation_en
            FROM read_parquet(?)
            ORDER BY document_id
            """,
            [str(DOCUMENTS_PATH)],
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
            )
            SELECT document_id, section_key, section_text
            FROM ranked_sections
            WHERE section_rank <= {MAX_SECTIONS_PER_DOCUMENT}
            ORDER BY document_id, section_rank
            """,
            [str(SECTIONS_PATH)],
        ).fetchall()

        sections_by_document: dict[str, list[tuple[str, str]]] = {}
        for document_id, section_key, section_text in section_rows:
            sections_by_document.setdefault(document_id, []).append(
                (section_key, section_text)
            )

        output_rows: list[tuple[str, str, str | None, str, int]] = []
        total_section_count_used = 0

        for document_id, title_en, citation_en in document_rows:
            classifier_input_text, section_count_used = build_classifier_input_text(
                title_en=title_en,
                citation_en=citation_en,
                sections=sections_by_document.get(document_id, []),
            )
            output_rows.append(
                (
                    document_id,
                    title_en,
                    citation_en,
                    classifier_input_text,
                    section_count_used,
                )
            )
            total_section_count_used += section_count_used

        temp_output_path = OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet")
        delete_if_exists(temp_output_path)
        delete_if_exists(OUTPUT_PATH)

        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE classifier_inputs (
                document_id VARCHAR,
                title_en VARCHAR,
                citation_en VARCHAR,
                classifier_input_text VARCHAR,
                section_count_used BIGINT
            )
            """
        )
        con.executemany(
            "INSERT INTO classifier_inputs VALUES (?, ?, ?, ?, ?)",
            output_rows,
        )
        con.execute("COPY classifier_inputs TO ? (FORMAT PARQUET)", [str(temp_output_path)])
        temp_output_path.replace(OUTPUT_PATH)
    finally:
        con.close()
        delete_if_exists(OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet"))

    total_documents_written = len(output_rows)
    average_section_count_used = (
        total_section_count_used / total_documents_written
        if total_documents_written
        else 0.0
    )

    print(f"Total documents written: {total_documents_written}")
    print(f"Average section_count_used: {average_section_count_used:.2f}")
    print("\nSample classifier_input_text previews:\n")
    for index, row in enumerate(output_rows[:PREVIEW_COUNT], start=1):
        document_id, title_en, _, classifier_input_text, section_count_used = row
        print(f"Sample {index}:")
        print(f"  document_id: {document_id}")
        print(f"  title_en: {title_en}")
        print(f"  section_count_used: {section_count_used}")
        print("  classifier_input_text:")
        for line in preview_text(classifier_input_text).splitlines():
            print(f"    {line}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
