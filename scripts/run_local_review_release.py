from __future__ import annotations

import argparse
import getpass
import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence

import classify_documents as classifier
from env_utils import (
    get_data_root,
    get_processed_dir,
    load_project_env,
    resolve_project_path,
)


load_project_env()


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DOWNLOAD_DATASET_SCRIPT = SCRIPT_DIR / "download_dataset.py"
BUILD_DOCUMENTS_SCRIPT = SCRIPT_DIR / "build_documents.py"
PARSE_SECTIONS_SCRIPT = SCRIPT_DIR / "parse_sections.py"
BUILD_DOCUMENT_CLASSIFIER_INPUTS_SCRIPT = (
    SCRIPT_DIR / "build_document_classifier_inputs.py"
)
CLASSIFY_DOCUMENTS_SCRIPT = SCRIPT_DIR / "classify_documents.py"
BUILD_DOCUMENT_DOMAIN_SCORES_SCRIPT = SCRIPT_DIR / "build_document_domain_scores.py"
RUN_REVIEW_FRONTEND_PIPELINE_SCRIPT = SCRIPT_DIR / "run_review_frontend_pipeline.py"
DEFAULT_MANDATE_PATH = PROJECT_ROOT / "config" / "review_mandates" / "obsolescence_modernization_prosperity_v1.json"
DEFAULT_SUMMARY_OUTPUT_PATH = PROJECT_ROOT / "src" / "data" / "review-summary.json"
DEFAULT_DETAILS_OUTPUT_PATH = PROJECT_ROOT / "src" / "data" / "review-details.json"
PREPROCESS_STEPS: tuple[tuple[str, Path], ...] = (
    ("Build documents", BUILD_DOCUMENTS_SCRIPT),
    ("Parse sections", PARSE_SECTIONS_SCRIPT),
    ("Build classifier inputs", BUILD_DOCUMENT_CLASSIFIER_INPUTS_SCRIPT),
    ("Classify documents", CLASSIFY_DOCUMENTS_SCRIPT),
    ("Build domain scores", BUILD_DOCUMENT_DOMAIN_SCORES_SCRIPT),
)
DOMAIN_OPTIONS = tuple(classifier.TAXONOMY_PROTOTYPES.keys())
DOMAIN_OR_ALL = ("all", *DOMAIN_OPTIONS)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the local legislative review pipeline and publish dashboard artifacts. "
            "If --api-key or --domain are omitted, the script will prompt for them."
        )
    )
    parser.add_argument(
        "--api-key",
        help="Claude API key. If omitted, uses CLAUDE_API_KEY or prompts securely.",
    )
    parser.add_argument(
        "--domain",
        choices=DOMAIN_OR_ALL,
        help="Domain to process, or 'all' to process every supported domain.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        help="Optional override for LEGISLATIVE_REVIEW_DATA_ROOT.",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        help="Optional override for LEGISLATIVE_REVIEW_PROCESSED_DIR.",
    )
    parser.add_argument(
        "--mandate-path",
        type=Path,
        default=DEFAULT_MANDATE_PATH,
        help="Mandate config JSON for the review batch.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit. When using --domain all, the limit applies per domain.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="How many successful reviews between parquet checkpoints.",
    )
    parser.add_argument(
        "--frontend-export-every",
        type=int,
        default=10,
        help="How many successful reviews between frontend refresh checkpoints.",
    )
    parser.add_argument(
        "--total-count",
        type=int,
        default=5796,
        help="Known total corpus size for dashboard progress metrics.",
    )
    parser.add_argument(
        "--daily-capacity",
        type=int,
        default=200,
        help="Daily review capacity to include in the exported summary JSON.",
    )
    parser.add_argument(
        "--summary-output-path",
        type=Path,
        default=DEFAULT_SUMMARY_OUTPUT_PATH,
        help="Frontend summary JSON output path.",
    )
    parser.add_argument(
        "--details-output-path",
        type=Path,
        default=DEFAULT_DETAILS_OUTPUT_PATH,
        help="Frontend details JSON output path.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore existing review output/journal and restart from scratch.",
    )
    parser.add_argument(
        "--skip-preprocess",
        action="store_true",
        help="Skip rebuilding processed dataset artifacts before reviews.",
    )
    parser.add_argument(
        "--preprocess-only",
        action="store_true",
        help="Refresh the local processed dataset artifacts and stop before reviews.",
    )
    parser.add_argument(
        "--refresh-source",
        action="store_true",
        help=(
            "Force a refresh of the source dataset before preprocessing, even if the "
            "current local snapshot metadata already matches the remote revision."
        ),
    )
    parser.add_argument(
        "--skip-source-sync",
        action="store_true",
        help="Skip checking Hugging Face for source dataset updates before preprocessing.",
    )
    return parser.parse_args(argv)


def prompt_for_api_key(existing_value: str | None) -> str:
    if existing_value and existing_value.strip():
        return existing_value.strip()

    entered = getpass.getpass("Claude API key: ").strip()
    if not entered:
        raise RuntimeError("A Claude API key is required.")
    return entered


def prompt_for_domain() -> str:
    print("Select a domain to process:")
    print("  0. all")
    for index, domain in enumerate(DOMAIN_OPTIONS, start=1):
        print(f"  {index}. {domain}")

    while True:
        selection = input("Domain selection: ").strip().lower()
        if selection == "0" or selection == "all":
            return "all"
        if selection in DOMAIN_OPTIONS:
            return selection
        if selection.isdigit():
            numeric_selection = int(selection)
            if 1 <= numeric_selection <= len(DOMAIN_OPTIONS):
                return DOMAIN_OPTIONS[numeric_selection - 1]
        print("Invalid selection. Enter a number, a domain slug, or 'all'.")


def resolve_target_domains(selected_domain: str) -> list[str]:
    if selected_domain == "all":
        return list(DOMAIN_OPTIONS)
    return [selected_domain]


def resolve_path_override(
    explicit_value: Path | None,
    env_key: str,
    fallback: Path,
) -> Path:
    if explicit_value is not None:
        return (
            explicit_value
            if explicit_value.is_absolute()
            else resolve_project_path(str(explicit_value))
        )

    env_value = os.getenv(env_key)
    if env_value:
        return resolve_project_path(env_value)

    return fallback


def build_subprocess_env(
    *,
    api_key: str,
    data_root: Path,
    processed_dir: Path,
) -> dict[str, str]:
    env = os.environ.copy()
    env["CLAUDE_API_KEY"] = api_key
    env["LEGISLATIVE_REVIEW_DATA_ROOT"] = str(data_root)
    env["LEGISLATIVE_REVIEW_PROCESSED_DIR"] = str(processed_dir)
    return env


def format_command(command: Sequence[str]) -> str:
    return " ".join(f'"{part}"' if " " in part else part for part in command)


def run_domain_pipeline(
    *,
    domain: str,
    args: argparse.Namespace,
    env: dict[str, str],
) -> int:
    command = [
        sys.executable,
        str(RUN_REVIEW_FRONTEND_PIPELINE_SCRIPT),
        "--domain",
        domain,
        "--mandate-path",
        str(args.mandate_path),
        "--checkpoint-every",
        str(args.checkpoint_every),
        "--frontend-export-every",
        str(args.frontend_export_every),
        "--total-count",
        str(args.total_count),
        "--daily-capacity",
        str(args.daily_capacity),
        "--summary-output-path",
        str(args.summary_output_path),
        "--details-output-path",
        str(args.details_output_path),
    ]
    if args.limit is not None:
        command.extend(["--limit", str(args.limit)])
    if args.no_resume:
        command.append("--no-resume")

    print()
    print(f"=== Running domain: {domain} ===")
    print(format_command(command))
    process = subprocess.run(command, env=env, check=False)
    return process.returncode


def run_named_command(
    *,
    label: str,
    command: Sequence[str],
    env: dict[str, str],
) -> int:
    print()
    print(f"=== {label} ===")
    print(format_command(command))
    process = subprocess.run(list(command), env=env, check=False)
    return process.returncode


def run_preprocess_pipeline(
    *,
    args: argparse.Namespace,
    env: dict[str, str],
) -> int:
    if not args.skip_source_sync or args.refresh_source:
        refresh_command = [sys.executable, str(DOWNLOAD_DATASET_SCRIPT)]
        if args.refresh_source:
            refresh_command.append("--force")
        return_code = run_named_command(
            label="Sync source dataset",
            command=refresh_command,
            env=env,
        )
        if return_code != 0:
            return return_code

    for label, script_path in PREPROCESS_STEPS:
        return_code = run_named_command(
            label=label,
            command=[sys.executable, str(script_path)],
            env=env,
        )
        if return_code != 0:
            return return_code

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args(argv)
    if args.skip_preprocess and args.preprocess_only:
        print(
            "Error: --skip-preprocess and --preprocess-only cannot be used together.",
            file=sys.stderr,
        )
        return 1
    if args.skip_preprocess and args.refresh_source:
        print(
            "Error: --skip-preprocess and --refresh-source cannot be used together.",
            file=sys.stderr,
        )
        return 1
    if args.skip_preprocess and args.skip_source_sync:
        print(
            "Error: --skip-preprocess and --skip-source-sync cannot be used together.",
            file=sys.stderr,
        )
        return 1
    if args.limit is not None and args.limit <= 0:
        print("Error: --limit must be a positive integer.", file=sys.stderr)
        return 1
    if args.checkpoint_every <= 0:
        print("Error: --checkpoint-every must be a positive integer.", file=sys.stderr)
        return 1
    if args.frontend_export_every <= 0:
        print(
            "Error: --frontend-export-every must be a positive integer.",
            file=sys.stderr,
        )
        return 1
    if args.total_count <= 0:
        print("Error: --total-count must be a positive integer.", file=sys.stderr)
        return 1
    if args.daily_capacity <= 0:
        print("Error: --daily-capacity must be a positive integer.", file=sys.stderr)
        return 1
    if not args.preprocess_only and not args.mandate_path.exists():
        print(f"Error: mandate config not found at {args.mandate_path}", file=sys.stderr)
        return 1

    try:
        api_key = prompt_for_api_key(args.api_key or os.getenv("CLAUDE_API_KEY"))
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    target_domains: list[str] = []
    if not args.preprocess_only:
        selected_domain = args.domain or prompt_for_domain()
        target_domains = resolve_target_domains(selected_domain)
    data_root = resolve_path_override(
        args.data_root,
        "LEGISLATIVE_REVIEW_DATA_ROOT",
        get_data_root(),
    )
    processed_dir = resolve_path_override(
        args.processed_dir,
        "LEGISLATIVE_REVIEW_PROCESSED_DIR",
        get_processed_dir(),
    )

    env = build_subprocess_env(
        api_key=api_key,
        data_root=data_root,
        processed_dir=processed_dir,
    )

    print("Local review release configuration:")
    print(
        "  Domains: "
        + (", ".join(target_domains) if target_domains else "(preprocess only)")
    )
    print(f"  Data root: {data_root}")
    print(f"  Processed dir: {processed_dir}")
    print(
        "  Source sync: "
        + (
            "skipped"
            if args.skip_source_sync
            else ("forced refresh" if args.refresh_source else "auto-check")
        )
    )
    print(
        "  Preprocess: "
        + ("skipped" if args.skip_preprocess else "enabled")
    )
    print(
        "  R2 publish: "
        + (
            f"enabled ({env['CLOUDFLARE_R2_BUCKET']})"
            if env.get("CLOUDFLARE_R2_BUCKET")
            else "disabled (local JSON only)"
        )
    )
    print(f"  Summary output: {args.summary_output_path}")
    print(f"  Details output: {args.details_output_path}")
    if args.limit is not None:
        print(f"  Limit per domain: {args.limit}")

    if not args.skip_preprocess:
        return_code = run_preprocess_pipeline(args=args, env=env)
        if return_code != 0:
            print(
                f"Error: preprocessing failed with exit code {return_code}.",
                file=sys.stderr,
            )
            return return_code

    if args.preprocess_only:
        print()
        print("Preprocessing complete.")
        return 0

    completed_domains: list[str] = []
    for domain in target_domains:
        return_code = run_domain_pipeline(domain=domain, args=args, env=env)
        if return_code != 0:
            print(
                f"Error: domain {domain} failed with exit code {return_code}.",
                file=sys.stderr,
            )
            if completed_domains:
                print(
                    "Completed before failure: " + ", ".join(completed_domains),
                    file=sys.stderr,
                )
            return return_code
        completed_domains.append(domain)

    print()
    print("Review release complete.")
    print("Domains completed: " + ", ".join(completed_domains))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
