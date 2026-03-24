from __future__ import annotations

import os
from pathlib import Path


DEFAULT_DATA_ROOT = Path(r"E:\Programming\buildcanada\canadian-laws")


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


def get_project_root() -> Path:
	return Path(__file__).resolve().parent.parent


def get_data_root() -> Path:
	override = os.getenv("LEGISLATIVE_REVIEW_DATA_ROOT")
	if override:
		return Path(override).expanduser()
	return DEFAULT_DATA_ROOT


def get_processed_dir() -> Path:
	override = os.getenv("LEGISLATIVE_REVIEW_PROCESSED_DIR")
	if override:
		return Path(override).expanduser()
	return get_data_root() / "processed"
