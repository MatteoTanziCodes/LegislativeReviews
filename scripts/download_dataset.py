from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


DATASET_NAME = "a2aj/canadian-laws"
DATASET_REVISION = "refs/convert/parquet"
DATASET_REPO_TYPE = "dataset"
TARGET_DIR = Path(r"E:\Programming\buildcanada\canadian-laws")
METADATA_FILENAME = "metadata.json"
JSONL_FILENAME = "canadian-laws.jsonl"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the raw parquet snapshot for the Hugging Face dataset "
            f"{DATASET_NAME}."
        )
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download parquet files even if they already exist locally.",
    )
    parser.add_argument(
        "--export-jsonl",
        action="store_true",
        help="Convert the downloaded parquet files into a single JSONL file.",
    )
    return parser.parse_args(argv)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def list_parquet_files(root: Path) -> list[Path]:
    return sorted(
        path for path in root.rglob("*.parquet") if path.is_file()
    )


def download_snapshot(target_dir: Path, force: bool) -> list[Path]:
    try:
        from huggingface_hub import snapshot_download
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required. Install it with `pip install huggingface_hub`."
        ) from exc

    ensure_directory(target_dir)
    snapshot_download(
        repo_id=DATASET_NAME,
        repo_type=DATASET_REPO_TYPE,
        revision=DATASET_REVISION,
        local_dir=target_dir,
        allow_patterns=["*.parquet", "**/*.parquet"],
        force_download=force,
    )

    parquet_files = list_parquet_files(target_dir)
    if not parquet_files:
        raise RuntimeError(
            f"No parquet files were found in {target_dir} after download."
        )
    return parquet_files


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def export_jsonl(parquet_files: Iterable[Path], output_path: Path) -> int:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required for --export-jsonl. "
            "Install it with `pip install pyarrow`."
        ) from exc

    row_count = 0
    with output_path.open("w", encoding="utf-8") as handle:
        for parquet_file in parquet_files:
            parquet = pq.ParquetFile(parquet_file)
            for batch in parquet.iter_batches(batch_size=1_000):
                for record in batch.to_pylist():
                    handle.write(
                        json.dumps(record, ensure_ascii=False, default=json_default)
                    )
                    handle.write("\n")
                    row_count += 1
    return row_count


def write_metadata(
    target_dir: Path,
    parquet_files: Sequence[Path],
    jsonl_path: Path | None,
) -> Path:
    metadata_path = target_dir / METADATA_FILENAME
    metadata: dict[str, Any] = {
        "dataset_name": DATASET_NAME,
        "revision": DATASET_REVISION,
        "download_timestamp": datetime.now(timezone.utc).isoformat(),
        "local_path": str(target_dir.resolve()),
        "file_count": len(parquet_files),
        "downloaded_files": [
            str(path.relative_to(target_dir).as_posix()) for path in parquet_files
        ],
    }

    if jsonl_path is not None:
        metadata["jsonl_export"] = str(jsonl_path.resolve())

    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return metadata_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    parquet_files = download_snapshot(TARGET_DIR, force=args.force)

    jsonl_path: Path | None = None
    if args.export_jsonl:
        jsonl_path = TARGET_DIR / JSONL_FILENAME
        export_jsonl(parquet_files, jsonl_path)

    write_metadata(TARGET_DIR, parquet_files, jsonl_path)

    print(f"Downloaded {len(parquet_files)} parquet files to {TARGET_DIR}")
    if jsonl_path is not None:
        print(f"Exported JSONL to {jsonl_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
