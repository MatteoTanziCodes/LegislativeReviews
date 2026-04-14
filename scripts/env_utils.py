from __future__ import annotations

import os
from pathlib import Path


def get_project_root() -> Path:
	return Path(__file__).resolve().parent.parent


DEFAULT_DATA_ROOT = get_project_root() / "docs" / "canadian-laws"


def load_project_env() -> None:
	root = Path(__file__).resolve().parent.parent
	env_path = root / ".env"
	if not env_path.exists():
		return

	try:
		from dotenv import load_dotenv
	except ImportError:
		return

	load_dotenv(env_path, override=False)


def resolve_project_path(value: str) -> Path:
	path = Path(value).expanduser()
	if path.is_absolute():
		return path
	return get_project_root() / path


def get_data_root() -> Path:
	override = os.getenv("LEGISLATIVE_REVIEW_DATA_ROOT")
	if override:
		return resolve_project_path(override)
	return DEFAULT_DATA_ROOT


def get_processed_dir() -> Path:
	override = os.getenv("LEGISLATIVE_REVIEW_PROCESSED_DIR")
	if override:
		return resolve_project_path(override)
	return get_data_root() / "processed"


def get_documents_output_path() -> Path:
	return get_processed_dir() / "documents_en.parquet"


def derive_total_document_count() -> int:
	documents_output_path = get_documents_output_path()
	if not documents_output_path.exists():
		raise RuntimeError(
			f"Processed documents parquet not found at {documents_output_path}. "
			"Run preprocessing first."
		)

	try:
		import duckdb
	except ImportError as exc:
		raise RuntimeError(
			"duckdb is required to derive the current document count. "
			"Install it with `pip install duckdb`."
		) from exc

	con = duckdb.connect()
	try:
		total_count = con.execute(
			"SELECT COUNT(*) FROM read_parquet(?)",
			[str(documents_output_path)],
		).fetchone()[0]
	finally:
		con.close()

	if not isinstance(total_count, int) or total_count <= 0:
		raise RuntimeError(
			f"Derived document count is invalid for {documents_output_path}: {total_count!r}"
		)

	return total_count
