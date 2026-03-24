from __future__ import annotations

import json
import re
import shutil
import sys
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Sequence

from env_utils import get_data_root, get_processed_dir, load_project_env


load_project_env()


RAW_DATASET_DIR = get_data_root()
PROCESSED_DIR = get_processed_dir()
OUTPUT_PATH = PROCESSED_DIR / "documents_en.parquet"
DOCUMENT_BATCH_SIZE = 250


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


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")
    return slug or "document"


def choose_document_id_base(
    citation2_en: str | None,
    citation_en: str | None,
    name_en: str,
) -> str:
    for candidate in (citation2_en, citation_en):
        if candidate is None:
            continue
        stripped = candidate.strip()
        if stripped:
            return stripped
    return slugify(name_en)


def make_unique_document_id(base_id: str, used_ids: set[str]) -> str:
    candidate = base_id
    suffix = 2
    while candidate in used_ids:
        candidate = f"{base_id}-{suffix}"
        suffix += 1
    used_ids.add(candidate)
    return candidate


def write_empty_output(con: Any, output_path: Path) -> None:
    con.execute(
        """
        COPY (
            SELECT
                CAST(NULL AS VARCHAR) AS document_id,
                CAST(NULL AS VARCHAR) AS title_en,
                CAST(NULL AS VARCHAR) AS citation_en,
                CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS document_date_en,
                CAST(NULL AS BIGINT) AS num_sections_en,
                CAST(NULL AS VARCHAR) AS dataset,
                CAST(NULL AS VARCHAR) AS source_url_en,
                CAST(NULL AS TIMESTAMP WITH TIME ZONE) AS scraped_timestamp_en
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
        tempfile.mkdtemp(prefix="documents_en_", dir=str(PROCESSED_DIR))
    )
    temp_jsonl_path = temp_work_dir / "documents_en.jsonl"

    source_row_count = 0
    output_row_count = 0
    used_ids: set[str] = set()

    con = duckdb.connect()
    try:
        cursor = con.execute(
            """
            SELECT
                citation2_en,
                citation_en,
                name_en,
                CAST(document_date_en AS VARCHAR) AS document_date_en,
                num_sections_en,
                dataset,
                source_url_en,
                CAST(scraped_timestamp_en AS VARCHAR) AS scraped_timestamp_en
            FROM read_parquet(?)
            """,
            [scan_target],
        )

        with temp_jsonl_path.open("w", encoding="utf-8") as handle:
            while True:
                rows = cursor.fetchmany(DOCUMENT_BATCH_SIZE)
                if not rows:
                    break

                for (
                    citation2_en,
                    citation_en,
                    name_en,
                    document_date_en,
                    num_sections_en,
                    dataset,
                    source_url_en,
                    scraped_timestamp_en,
                ) in rows:
                    source_row_count += 1

                    title_en = name_en.strip() if name_en else ""
                    if not title_en:
                        continue

                    base_id = choose_document_id_base(
                        citation2_en,
                        citation_en,
                        title_en,
                    )
                    document_id = make_unique_document_id(base_id, used_ids)

                    record = {
                        "document_id": document_id,
                        "title_en": title_en,
                        "citation_en": citation_en.strip() if citation_en else None,
                        "document_date_en": document_date_en,
                        "num_sections_en": num_sections_en,
                        "dataset": dataset,
                        "source_url_en": source_url_en,
                        "scraped_timestamp_en": scraped_timestamp_en,
                    }
                    handle.write(json.dumps(record, ensure_ascii=False))
                    handle.write("\n")
                    output_row_count += 1

        delete_if_exists(temp_output_path)
        delete_if_exists(OUTPUT_PATH)

        if output_row_count == 0:
            write_empty_output(con, temp_output_path)
        else:
            con.execute(
                """
                COPY (
                    SELECT
                        CAST(document_id AS VARCHAR) AS document_id,
                        CAST(title_en AS VARCHAR) AS title_en,
                        CAST(citation_en AS VARCHAR) AS citation_en,
                        TRY_CAST(document_date_en AS TIMESTAMP WITH TIME ZONE) AS document_date_en,
                        CAST(num_sections_en AS BIGINT) AS num_sections_en,
                        CAST(dataset AS VARCHAR) AS dataset,
                        CAST(source_url_en AS VARCHAR) AS source_url_en,
                        TRY_CAST(scraped_timestamp_en AS TIMESTAMP WITH TIME ZONE) AS scraped_timestamp_en
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

    print(f"Source rows: {source_row_count}")
    print(f"Output rows: {output_row_count}")
    print(f"Output written to: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
