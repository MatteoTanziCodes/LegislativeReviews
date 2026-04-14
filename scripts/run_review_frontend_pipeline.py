from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

from env_utils import derive_total_document_count, get_processed_dir, load_project_env


load_project_env()


import review_documents as review_runner


SCRIPT_DIR = Path(__file__).resolve().parent
BUILD_REVIEW_INPUTS_SCRIPT = SCRIPT_DIR / "build_review_inputs.py"
REVIEW_DOCUMENTS_SCRIPT = SCRIPT_DIR / "review_documents.py"
EXPORT_FRONTEND_REVIEW_DATA_SCRIPT = SCRIPT_DIR / "export_frontend_review_data.py"
DEFAULT_MANDATE_PATH = Path(
	r"config/review_mandates/obsolescence_modernization_prosperity_v1.json"
)
DEFAULT_PROCESSED_DIR = get_processed_dir()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Run the domain review pipeline end-to-end: build review inputs, "
			"run reviews, and export frontend dashboard artifacts."
		)
	)
	parser.add_argument("--domain", required=True, help="Primary domain to review.")
	parser.add_argument(
		"--mandate-path",
		type=Path,
		default=DEFAULT_MANDATE_PATH,
		help="Mandate config JSON for the review batch.",
	)
	parser.add_argument(
		"--review-input-path",
		type=Path,
		help="Optional explicit review input parquet path.",
	)
	parser.add_argument(
		"--review-output-path",
		type=Path,
		help="Optional explicit review output parquet path.",
	)
	parser.add_argument(
		"--summary-output-path",
		type=Path,
		default=Path(r"src/data/review-summary.json"),
		help="Frontend summary JSON output path.",
	)
	parser.add_argument(
		"--details-output-path",
		type=Path,
		default=Path(r"src/data/review-details.json"),
		help="Frontend drilldown JSON output path.",
	)
	parser.add_argument(
		"--limit",
		type=int,
		help="Optional review limit to pass through to review_documents.py.",
	)
	parser.add_argument(
		"--checkpoint-every",
		type=int,
		default=25,
		help="How many successful reviews between parquet checkpoints.",
	)
	parser.add_argument(
		"--no-resume",
		action="store_true",
		help="Ignore existing review output/journal and start from scratch.",
	)
	parser.add_argument(
		"--total-count",
		type=int,
		help=(
			"Optional explicit total corpus size for frontend progress metrics. "
			"Defaults to the current documents_en.parquet row count."
		),
	)
	parser.add_argument(
		"--daily-capacity",
		type=int,
		default=200,
		help="Daily review capacity to include in frontend summary data.",
	)
	parser.add_argument(
		"--frontend-export-every",
		type=int,
		default=10,
		help="How many successful reviews between live frontend refreshes.",
	)
	return parser.parse_args(argv)


def run_step(command: list[str]) -> int:
	print()
	print("Running:")
	print("  " + " ".join(f'"{part}"' if " " in part else part for part in command))
	process = subprocess.run(command, check=False)
	return process.returncode


def build_review_journal_path(output_path: Path) -> Path:
	return output_path.with_name(f"{output_path.stem}.journal.jsonl")


def build_review_manifest_path(output_path: Path) -> Path:
	return output_path.with_name(f"{output_path.stem}.manifest.json")


def build_review_lock_path(output_path: Path) -> Path:
	return output_path.with_name(f"{output_path.stem}.lock")


def build_legacy_backup_path(path: Path, timestamp: str) -> Path:
	return path.with_name(f"{path.stem}.legacy-{timestamp}{path.suffix}")


def archive_path(path: Path, timestamp: str) -> Path:
	backup_path = build_legacy_backup_path(path, timestamp)
	sequence = 1
	while backup_path.exists():
		backup_path = path.with_name(
			f"{path.stem}.legacy-{timestamp}-{sequence}{path.suffix}"
		)
		sequence += 1
	path.replace(backup_path)
	return backup_path


def migrate_legacy_review_state(
	*,
	python_executable: str,
	review_output_path: Path,
	summary_output_path: Path,
	details_output_path: Path,
	total_count: int,
	daily_capacity: int,
) -> bool:
	journal_path = build_review_journal_path(review_output_path)
	manifest_path = build_review_manifest_path(review_output_path)
	lock_path = build_review_lock_path(review_output_path)
	legacy_paths = [
		path
		for path in (review_output_path, journal_path, lock_path)
		if path.exists()
	]
	if not legacy_paths or manifest_path.exists():
		return False

	print()
	print(
		"Legacy review artifacts detected without a run manifest. "
		"Bootstrapping dashboard state, archiving the old files, and restarting fresh.",
		flush=True,
	)

	if review_output_path.exists():
		bootstrap_export_command = [
			python_executable,
			str(EXPORT_FRONTEND_REVIEW_DATA_SCRIPT),
			"--review-output-path",
			str(review_output_path),
			"--summary-output-path",
			str(summary_output_path),
			"--details-output-path",
			str(details_output_path),
			"--total-count",
			str(total_count),
			"--daily-capacity",
			str(daily_capacity),
			"--pipeline-status",
			"in_progress",
		]
		return_code = run_step(bootstrap_export_command)
		if return_code != 0:
			raise RuntimeError(
				"Failed to bootstrap frontend state from legacy review output before archiving."
			)

	timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
	for path in legacy_paths:
		backup_path = archive_path(path, timestamp)
		print(f"Archived legacy artifact: {path} -> {backup_path}", flush=True)
	return True


def migrate_incompatible_review_state(
	*,
	python_executable: str,
	review_input_path: Path,
	review_output_path: Path,
	mandate_path: Path,
	review_limit: int | None,
	summary_output_path: Path,
	details_output_path: Path,
	total_count: int,
	daily_capacity: int,
) -> bool:
	journal_path = build_review_journal_path(review_output_path)
	manifest_path = build_review_manifest_path(review_output_path)
	lock_path = build_review_lock_path(review_output_path)
	if not manifest_path.exists():
		return False

	existing_manifest = review_runner.load_review_run_manifest(manifest_path)
	if existing_manifest is None:
		return False

	mandate = review_runner.load_mandate(mandate_path)
	review_config = review_runner.get_review_config()
	current_review_inputs = review_runner.load_review_inputs(review_input_path, review_limit)
	current_manifest = review_runner.build_review_run_manifest(
		input_path=review_input_path,
		output_path=review_output_path,
		journal_path=journal_path,
		review_inputs=current_review_inputs,
		mandate_id=mandate["mandate_id"],
		review_model=review_config.model,
	)

	comparable_fields = (
		"manifest_version",
		"input_path",
		"output_path",
		"journal_path",
		"mandate_id",
		"review_model",
		"prompt_version",
		"selected_row_count",
		"document_range_start",
		"document_range_end",
		"primary_domains",
		"input_fingerprint",
	)
	mismatches = [
		field_name
		for field_name in comparable_fields
		if getattr(existing_manifest, field_name) != getattr(current_manifest, field_name)
	]
	if not mismatches:
		return False

	print()
	print(
		"Existing review state is incompatible with the current run configuration. "
		"Bootstrapping dashboard state, archiving the old files, and restarting fresh.",
		flush=True,
	)
	print("Manifest mismatch fields: " + ", ".join(mismatches[:6]), flush=True)

	if review_output_path.exists():
		bootstrap_export_command = [
			python_executable,
			str(EXPORT_FRONTEND_REVIEW_DATA_SCRIPT),
			"--review-output-path",
			str(review_output_path),
			"--summary-output-path",
			str(summary_output_path),
			"--details-output-path",
			str(details_output_path),
			"--total-count",
			str(total_count),
			"--daily-capacity",
			str(daily_capacity),
			"--pipeline-status",
			"in_progress",
		]
		return_code = run_step(bootstrap_export_command)
		if return_code != 0:
			raise RuntimeError(
				"Failed to bootstrap frontend state from incompatible review output before archiving."
			)

	timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
	for path in (review_output_path, journal_path, manifest_path, lock_path):
		if not path.exists():
			continue
		backup_path = archive_path(path, timestamp)
		print(f"Archived incompatible artifact: {path} -> {backup_path}", flush=True)
	return True


def load_mandate_id(path: Path) -> str:
	with path.open("r", encoding="utf-8") as handle:
		payload = json.load(handle)

	mandate_id = payload.get("mandate_id")
	if not isinstance(mandate_id, str) or not mandate_id.strip():
		raise RuntimeError(f"Mandate config at {path} is missing a valid mandate_id")
	return mandate_id


def main(argv: Sequence[str] | None = None) -> int:
	if hasattr(sys.stdout, "reconfigure"):
		sys.stdout.reconfigure(encoding="utf-8")
	if hasattr(sys.stderr, "reconfigure"):
		sys.stderr.reconfigure(encoding="utf-8")

	args = parse_args(argv)
	if args.limit is not None and args.limit <= 0:
		print("Error: --limit must be a positive integer.", file=sys.stderr)
		return 1
	if args.checkpoint_every <= 0:
		print("Error: --checkpoint-every must be a positive integer.", file=sys.stderr)
		return 1
	if args.frontend_export_every <= 0:
		print("Error: --frontend-export-every must be a positive integer.", file=sys.stderr)
		return 1
	if not args.mandate_path.exists():
		print(f"Error: mandate config not found at {args.mandate_path}", file=sys.stderr)
		return 1

	try:
		mandate_id = load_mandate_id(args.mandate_path)
	except RuntimeError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		return 1

	try:
		total_count = (
			derive_total_document_count()
			if args.total_count is None
			else args.total_count
		)
	except RuntimeError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		return 1
	if total_count <= 0:
		print("Error: --total-count must be a positive integer.", file=sys.stderr)
		return 1

	python_executable = sys.executable

	build_inputs_command = [
		python_executable,
		str(BUILD_REVIEW_INPUTS_SCRIPT),
		"--domain",
		args.domain,
		"--mandate-path",
		str(args.mandate_path),
	]
	if args.review_input_path is not None:
		build_inputs_command.extend(["--output-path", str(args.review_input_path)])

	review_input_path = args.review_input_path
	if review_input_path is None:
		review_input_path = (
			DEFAULT_PROCESSED_DIR / f"review_inputs_{args.domain}_{mandate_id}_en.parquet"
		)

	review_documents_command = [
		python_executable,
		str(REVIEW_DOCUMENTS_SCRIPT),
		"--input-path",
		str(review_input_path),
	]
	if args.review_output_path is not None:
		review_documents_command.extend(["--output-path", str(args.review_output_path)])
	if args.limit is not None:
		review_documents_command.extend(["--limit", str(args.limit)])
	review_documents_command.extend(["--checkpoint-every", str(args.checkpoint_every)])
	review_documents_command.extend(
		[
			"--frontend-summary-output-path",
			str(args.summary_output_path),
			"--frontend-details-output-path",
			str(args.details_output_path),
			"--frontend-total-count",
			str(total_count),
			"--frontend-daily-capacity",
			str(args.daily_capacity),
			"--frontend-export-every",
			str(args.frontend_export_every),
		]
	)

	review_output_path = args.review_output_path
	if review_output_path is None:
		input_name = review_input_path.name
		if input_name.startswith("review_inputs_"):
			review_output_path = review_input_path.with_name(
				"reviews_" + input_name[len("review_inputs_") :]
			)
		else:
			review_output_path = review_input_path.with_name(f"reviews_{input_name}")

	force_no_resume = False
	if not args.no_resume:
		try:
			force_no_resume = migrate_legacy_review_state(
				python_executable=python_executable,
				review_output_path=review_output_path,
				summary_output_path=args.summary_output_path,
				details_output_path=args.details_output_path,
				total_count=total_count,
				daily_capacity=args.daily_capacity,
			)
			if not force_no_resume:
				force_no_resume = migrate_incompatible_review_state(
					python_executable=python_executable,
					review_input_path=review_input_path,
					review_output_path=review_output_path,
					mandate_path=args.mandate_path,
					review_limit=args.limit,
					summary_output_path=args.summary_output_path,
					details_output_path=args.details_output_path,
					total_count=total_count,
					daily_capacity=args.daily_capacity,
				)
		except RuntimeError as exc:
			print(f"Error: {exc}", file=sys.stderr)
			return 1

	if args.no_resume or force_no_resume:
		review_documents_command.append("--no-resume")

	export_frontend_command = [
		python_executable,
		str(EXPORT_FRONTEND_REVIEW_DATA_SCRIPT),
		"--review-output-path",
		str(review_output_path),
		"--summary-output-path",
		str(args.summary_output_path),
		"--details-output-path",
		str(args.details_output_path),
		"--total-count",
		str(total_count),
		"--daily-capacity",
		str(args.daily_capacity),
		"--pipeline-status",
		"complete",
	]

	for command in (
		build_inputs_command,
		review_documents_command,
		export_frontend_command,
	):
		return_code = run_step(command)
		if return_code != 0:
			return return_code

	print()
	print("Pipeline complete.")
	print(f"Review inputs: {review_input_path}")
	print(f"Review output: {review_output_path}")
	print(f"Frontend summary: {args.summary_output_path}")
	print(f"Frontend details: {args.details_output_path}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
