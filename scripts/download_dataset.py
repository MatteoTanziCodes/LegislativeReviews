from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from env_utils import get_data_root, load_project_env


load_project_env()


DATASET_NAME = "a2aj/canadian-laws"
DATASET_REVISION = "refs/convert/parquet"
DATASET_REPO_TYPE = "dataset"
TARGET_DIR = get_data_root()
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
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Check whether the remote dataset revision changed without downloading.",
    )
    return parser.parse_args(argv)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def list_parquet_files(root: Path) -> list[Path]:
    return sorted(
        path for path in root.rglob("*.parquet") if path.is_file()
    )


def load_metadata(target_dir: Path) -> dict[str, Any] | None:
    metadata_path = target_dir / METADATA_FILENAME
    if not metadata_path.exists():
        return None

    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def get_remote_dataset_revision() -> str:
    try:
        from huggingface_hub import HfApi
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required. Install it with `pip install huggingface_hub`."
        ) from exc

    info = HfApi().dataset_info(DATASET_NAME, revision=DATASET_REVISION)
    if not info.sha:
        raise RuntimeError(
            f"Unable to resolve the current remote revision for {DATASET_NAME}."
        )
    return info.sha


def needs_download(target_dir: Path, remote_revision: str, force: bool) -> bool:
    if force:
        return True

    metadata = load_metadata(target_dir)
    local_revision = metadata.get("resolved_revision") if metadata else None
    if local_revision != remote_revision:
        return True

    parquet_files = list_parquet_files(target_dir)
    return not parquet_files


def inspect_dataset_status(target_dir: Path, force: bool) -> tuple[list[Path], str, bool]:
    ensure_directory(target_dir)
    remote_revision = get_remote_dataset_revision()
    should_download = needs_download(target_dir, remote_revision, force)
    parquet_files = list_parquet_files(target_dir)
    return parquet_files, remote_revision, should_download


def download_snapshot(target_dir: Path, force: bool) -> tuple[list[Path], str, bool]:
    try:
        from huggingface_hub import snapshot_download
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required. Install it with `pip install huggingface_hub`."
        ) from exc

    ensure_directory(target_dir)
    _, remote_revision, should_download = inspect_dataset_status(target_dir, force)

    if should_download:
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
    return parquet_files, remote_revision, should_download


def print_dataset_status(
    *,
    target_dir: Path,
    parquet_files: Sequence[Path],
    remote_revision: str,
    downloaded: bool,
) -> None:
    action = "Downloaded" if downloaded else "Already up to date"
    print(f"{action}: {len(parquet_files)} parquet files under {target_dir}")
    print(f"Remote revision: {remote_revision}")


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
    remote_revision: str,
    downloaded: bool,
    jsonl_path: Path | None,
) -> Path:
    metadata_path = target_dir / METADATA_FILENAME
    metadata: dict[str, Any] = {
        "dataset_name": DATASET_NAME,
        "revision": DATASET_REVISION,
        "resolved_revision": remote_revision,
        "last_checked_timestamp": datetime.now(timezone.utc).isoformat(),
        "download_performed": downloaded,
        "local_path": str(target_dir.resolve()),
        "file_count": len(parquet_files),
        "downloaded_files": [
            str(path.relative_to(target_dir).as_posix()) for path in parquet_files
        ],
    }

    if downloaded:
        metadata["download_timestamp"] = metadata["last_checked_timestamp"]

    if jsonl_path is not None:
        metadata["jsonl_export"] = str(jsonl_path.resolve())

    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return metadata_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if args.check_only:
        parquet_files, remote_revision, needs_refresh = inspect_dataset_status(
            TARGET_DIR,
            force=args.force,
        )
        print_dataset_status(
            target_dir=TARGET_DIR,
            parquet_files=parquet_files,
            remote_revision=remote_revision,
            downloaded=not needs_refresh,
        )
        return 0

    parquet_files, remote_revision, downloaded = download_snapshot(
        TARGET_DIR,
        force=args.force,
    )

    jsonl_path: Path | None = None
    if args.export_jsonl:
        jsonl_path = TARGET_DIR / JSONL_FILENAME
        export_jsonl(parquet_files, jsonl_path)

    write_metadata(TARGET_DIR, parquet_files, remote_revision, downloaded, jsonl_path)
    print_dataset_status(
        target_dir=TARGET_DIR,
        parquet_files=parquet_files,
        remote_revision=remote_revision,
        downloaded=downloaded,
    )
    if jsonl_path is not None:
        print(f"Exported JSONL to {jsonl_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
