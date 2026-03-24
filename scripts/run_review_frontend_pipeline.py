from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
BUILD_REVIEW_INPUTS_SCRIPT = SCRIPT_DIR / "build_review_inputs.py"
REVIEW_DOCUMENTS_SCRIPT = SCRIPT_DIR / "review_documents.py"
EXPORT_FRONTEND_REVIEW_DATA_SCRIPT = SCRIPT_DIR / "export_frontend_review_data.py"
DEFAULT_MANDATE_PATH = Path(
	r"config/review_mandates/obsolescence_modernization_prosperity_v1.json"
)


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
		"--total-count",
		type=int,
		default=5796,
		help="Known total corpus size for frontend progress metrics.",
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
		review_input_path = Path(
			rf"E:\Programming\buildcanada\canadian-laws\processed\review_inputs_{args.domain}_{mandate_id}_en.parquet"
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
	review_documents_command.extend(
		[
			"--frontend-summary-output-path",
			str(args.summary_output_path),
			"--frontend-details-output-path",
			str(args.details_output_path),
			"--frontend-total-count",
			str(args.total_count),
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
		str(args.total_count),
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
