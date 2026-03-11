from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence


RAW_DATASET_DIR = Path(r"E:\Programming\buildcanada\canadian-laws")
PROCESSED_DIR = RAW_DATASET_DIR / "processed"
OUTPUT_PATH = PROCESSED_DIR / "sections_en.parquet"
DOCUMENT_BATCH_SIZE = 100


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def list_source_parquet_files(root: Path, excluded_root: Path) -> list[Path]:
    parquet_files: list[Path] = []
    for path in root.rglob("*.parquet"):
        if not path.is_file():
            continue
        if is_relative_to(path, excluded_root):
            continue
        parquet_files.append(path)
    return sorted(parquet_files)


def delete_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def choose_document_id(
    citation2_en: str | None,
    citation_en: str | None,
    name_en: str | None,
) -> str | None:
    for candidate in (citation2_en, citation_en, name_en):
        if candidate is None:
            continue
        stripped = candidate.strip()
        if stripped:
            return stripped
    return None


def serialize_section_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def write_empty_output(con: Any, output_path: Path) -> None:
    con.execute(
        """
        COPY (
            SELECT
                CAST(NULL AS VARCHAR) AS document_id,
                CAST(NULL AS VARCHAR) AS title_en,
                CAST(NULL AS VARCHAR) AS citation_en,
                CAST(NULL AS VARCHAR) AS section_key,
                CAST(NULL AS VARCHAR) AS section_text
            WHERE FALSE
        ) TO ? (FORMAT PARQUET)
        """,
        [str(output_path)],
    )


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

    ensure_directory(PROCESSED_DIR)

    source_files = list_source_parquet_files(RAW_DATASET_DIR, PROCESSED_DIR)
    if not source_files:
        print(
            f"Error: no source parquet files found under {RAW_DATASET_DIR}",
            file=sys.stderr,
        )
        return 1

    scan_target = [str(path) for path in source_files]
    temp_output_path = OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet")
    temp_work_dir = Path(
        tempfile.mkdtemp(prefix="sections_en_", dir=str(PROCESSED_DIR))
    )
    temp_jsonl_path = temp_work_dir / "sections_en.jsonl"

    source_document_count = 0
    parsed_document_count = 0
    skipped_document_count = 0
    section_row_count = 0

    con = duckdb.connect()
    try:
        cursor = con.execute(
            """
            SELECT
                citation2_en,
                citation_en,
                name_en,
                unofficial_sections_en
            FROM read_parquet(?)
            """,
            [scan_target],
        )

        with temp_jsonl_path.open("w", encoding="utf-8") as handle:
            while True:
                document_rows = cursor.fetchmany(DOCUMENT_BATCH_SIZE)
                if not document_rows:
                    break

                for citation2_en, citation_en, name_en, unofficial_sections_en in document_rows:
                    source_document_count += 1

                    if unofficial_sections_en is None:
                        skipped_document_count += 1
                        continue

                    try:
                        sections = json.loads(unofficial_sections_en)
                    except json.JSONDecodeError:
                        skipped_document_count += 1
                        continue

                    if not isinstance(sections, dict):
                        skipped_document_count += 1
                        continue

                    parsed_document_count += 1
                    document_id = choose_document_id(citation2_en, citation_en, name_en)

                    for section_key, section_value in sections.items():
                        record = {
                            "document_id": document_id,
                            "title_en": name_en,
                            "citation_en": citation_en,
                            "section_key": str(section_key),
                            "section_text": serialize_section_text(section_value),
                        }
                        handle.write(json.dumps(record, ensure_ascii=False))
                        handle.write("\n")
                        section_row_count += 1

        delete_if_exists(temp_output_path)
        delete_if_exists(OUTPUT_PATH)

        if section_row_count == 0:
            write_empty_output(con, temp_output_path)
        else:
            con.execute(
                """
                COPY (
                    SELECT
                        CAST(document_id AS VARCHAR) AS document_id,
                        CAST(title_en AS VARCHAR) AS title_en,
                        CAST(citation_en AS VARCHAR) AS citation_en,
                        CAST(section_key AS VARCHAR) AS section_key,
                        CAST(section_text AS VARCHAR) AS section_text
                    FROM read_json_auto(?)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(temp_output_path), str(temp_jsonl_path)],
            )

        temp_output_path.replace(OUTPUT_PATH)
    finally:
        con.close()
        delete_if_exists(temp_output_path)
        shutil.rmtree(temp_work_dir, ignore_errors=True)

    print(f"Source documents read: {source_document_count}")
    print(f"Documents successfully parsed: {parsed_document_count}")
    print(f"Skipped documents: {skipped_document_count}")
    print(f"Section rows written: {section_row_count}")
    print(f"Output written to: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
