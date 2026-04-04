from __future__ import annotations

import os
import subprocess
import sys
from typing import Sequence


def with_optional_limit(message_base: str) -> str:
    limit = os.environ.get("REVIEW_LIMIT", "").strip()
    if limit:
        return f"{message_base} (limit {limit})."
    return f"{message_base}."


def run_subprocess(command: list[str]) -> int:
    return subprocess.run(command, check=False).returncode


def build_admin_state_command(
    *,
    worker_status: str,
    event_level: str,
    event_message: str,
    clear_current_run: bool = False,
    last_error: str | None = None,
) -> list[str]:
    command = [
        sys.executable,
        "scripts/update_review_admin_state.py",
        "--worker-status",
        worker_status,
        "--domain",
        os.environ["REVIEW_DOMAIN"],
        "--last-run-id",
        os.environ["GITHUB_RUN_ID"],
        "--last-run-html-url",
        os.environ["REVIEW_RUN_URL"],
        "--event-level",
        event_level,
        "--event-message",
        event_message,
    ]

    if clear_current_run:
        command.append("--clear-current-run")

    if last_error:
        command.extend(["--last-error", last_error])

    limit = os.environ.get("REVIEW_LIMIT", "").strip()
    if limit:
        command.extend(["--limit", limit])

    command_id = os.environ.get("REVIEW_COMMAND_ID", "").strip()
    if command_id:
        command.extend(["--command-id", command_id])

    requested_at = os.environ.get("REVIEW_REQUESTED_AT", "").strip()
    if requested_at:
        command.extend(["--requested-at", requested_at])

    return command


def run_mark_running() -> int:
    return run_subprocess(
        build_admin_state_command(
            worker_status="running",
            event_level="info",
            event_message=with_optional_limit(
                f"GitHub Actions review workflow started for {os.environ['REVIEW_DOMAIN']}"
            ),
        )
    )


def run_mark_complete() -> int:
    return run_subprocess(
        build_admin_state_command(
            worker_status="complete",
            event_level="info",
            event_message=with_optional_limit(
                f"GitHub Actions review workflow completed for {os.environ['REVIEW_DOMAIN']}"
            ),
            clear_current_run=True,
        )
    )


def run_mark_error() -> int:
    domain = os.environ["REVIEW_DOMAIN"]
    return run_subprocess(
        build_admin_state_command(
            worker_status="error",
            event_level="error",
            event_message=with_optional_limit(
                f"GitHub Actions review workflow failed for {domain}"
            ),
            clear_current_run=True,
            last_error=f"GitHub Actions review workflow failed for {domain}.",
        )
    )


def run_pipeline() -> int:
    command = [
        sys.executable,
        "scripts/run_review_frontend_pipeline.py",
        "--domain",
        os.environ["REVIEW_DOMAIN"],
    ]

    limit = os.environ.get("REVIEW_LIMIT", "").strip()
    if limit:
        command.extend(["--limit", limit])

    return run_subprocess(command)


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    if len(args) != 1:
        print(
            "Usage: python scripts/run_review_workflow_step.py "
            "{mark-running|run-pipeline|mark-complete|mark-error}",
            file=sys.stderr,
        )
        return 1

    action = args[0]
    if action == "mark-running":
        return run_mark_running()
    if action == "run-pipeline":
        return run_pipeline()
    if action == "mark-complete":
        return run_mark_complete()
    if action == "mark-error":
        return run_mark_error()

    print(f"Unsupported workflow step action: {action}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
