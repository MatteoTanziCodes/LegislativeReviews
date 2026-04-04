from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from env_utils import get_project_root, load_project_env


load_project_env()


PROJECT_ROOT = get_project_root()
DEFAULT_ADMIN_STATE_PATH = PROJECT_ROOT / "src" / "data" / "review-admin-state.json"
DEFAULT_R2_ADMIN_STATE_OBJECT_KEY = "review-admin-state.json"
MAX_RECENT_EVENTS = 20


class StorageBackend:
    def read_json(self) -> dict[str, Any] | None:
        raise NotImplementedError

    def write_json(self, payload: dict[str, Any]) -> None:
        raise NotImplementedError


class LocalJsonStorage(StorageBackend):
    def __init__(self, path: Path) -> None:
        self.path = path

    def read_json(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        return json.loads(self.path.read_text(encoding="utf-8"))

    def write_json(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_name(f"{self.path.name}.{os.getpid()}.tmp")
        temp_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(self.path)


class R2JsonStorage(StorageBackend):
    def __init__(
        self,
        *,
        bucket_name: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        object_key: str,
    ) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError(
                "boto3 is required for Cloudflare R2 admin-state publishing."
            ) from exc

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
        )
        self.bucket_name = bucket_name
        self.object_key = object_key

    def read_json(self) -> dict[str, Any] | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=self.object_key,
            )
        except Exception as exc:
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404"}:
                return None
            raise

        return json.loads(response["Body"].read().decode("utf-8"))

    def write_json(self, payload: dict[str, Any]) -> None:
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.object_key,
            Body=(json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode(
                "utf-8"
            ),
            ContentType="application/json; charset=utf-8",
            CacheControl="no-store, no-cache, must-revalidate, max-age=0",
        )


def iso_now() -> str:
    return datetime.now(tz=timezone.utc).astimezone().isoformat(timespec="seconds")


def create_default_admin_state() -> dict[str, Any]:
    return {
        "currentDomain": None,
        "currentLimit": None,
        "lastCommand": None,
        "lastCompletedAt": None,
        "lastError": None,
        "lastHeartbeatAt": None,
        "lastRequestedAt": None,
        "lastRunHtmlUrl": None,
        "lastRunId": None,
        "lastStartedAt": None,
        "recentEvents": [],
        "workerHost": None,
        "workerPid": None,
        "workerStatus": "idle",
    }


def append_event(
    state: dict[str, Any],
    *,
    level: str,
    message: str,
    timestamp: str,
) -> None:
    events = list(state.get("recentEvents") or [])
    events.insert(
        0,
        {
            "level": level,
            "message": message,
            "timestamp": timestamp,
        },
    )
    state["recentEvents"] = events[:MAX_RECENT_EVENTS]


def build_storage_backend() -> StorageBackend:
    bucket_name = os.getenv("CLOUDFLARE_R2_BUCKET")
    if not bucket_name:
        return LocalJsonStorage(DEFAULT_ADMIN_STATE_PATH)

    endpoint_url = os.getenv("CLOUDFLARE_R2_ENDPOINT")
    if not endpoint_url:
        account_id = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
        if account_id:
            endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    access_key_id = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY")
    if not endpoint_url or not access_key_id or not secret_access_key:
        raise RuntimeError(
            "Cloudflare R2 admin-state publishing is enabled but the required "
            "CLOUDFLARE_R2_* credentials are incomplete."
        )

    return R2JsonStorage(
        bucket_name=bucket_name,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        object_key=(
            os.getenv("CLOUDFLARE_R2_ADMIN_STATE_KEY")
            or DEFAULT_R2_ADMIN_STATE_OBJECT_KEY
        ),
    )


def load_admin_state(storage: StorageBackend) -> dict[str, Any]:
    payload = storage.read_json()
    state = create_default_admin_state()
    if payload is None:
        return state

    state.update(payload)
    state["recentEvents"] = list(payload.get("recentEvents") or [])
    return state


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update the persisted review admin state JSON."
    )
    parser.add_argument(
        "--worker-status",
        choices=("idle", "pending", "running", "complete", "error"),
        help="Next workflow status.",
    )
    parser.add_argument("--domain", help="Current review domain.")
    parser.add_argument("--limit", type=int, help="Current review limit.")
    parser.add_argument("--command-id", help="Command identifier for the current run.")
    parser.add_argument(
        "--requested-by",
        default="github_actions",
        help="Originator for the last command payload.",
    )
    parser.add_argument("--requested-at", help="ISO timestamp for when the run was requested.")
    parser.add_argument("--started-at", help="ISO timestamp for when the run started.")
    parser.add_argument("--completed-at", help="ISO timestamp for when the run completed.")
    parser.add_argument("--last-error", help="Optional last error message.")
    parser.add_argument("--event-level", choices=("info", "error"), help="Recent event level.")
    parser.add_argument("--event-message", help="Recent event message to append.")
    parser.add_argument("--last-run-id", type=int, help="GitHub Actions workflow run id.")
    parser.add_argument("--last-run-html-url", help="GitHub Actions workflow run URL.")
    parser.add_argument(
        "--clear-current-run",
        action="store_true",
        help="Clear currentDomain/currentLimit after updating the state.",
    )
    return parser.parse_args(argv)


def build_last_command(
    state: dict[str, Any],
    args: argparse.Namespace,
    *,
    now: str,
) -> dict[str, Any] | None:
    existing_command = state.get("lastCommand")
    if not isinstance(existing_command, dict):
        existing_command = {}

    command_id = args.command_id or existing_command.get("commandId")
    domain = args.domain or existing_command.get("domain") or state.get("currentDomain")
    if not command_id or not domain:
        return state.get("lastCommand")

    command_limit = args.limit if args.limit is not None else existing_command.get("limit")
    requested_at = args.requested_at or existing_command.get("requestedAt") or state.get("lastRequestedAt") or now
    status = args.worker_status or existing_command.get("status") or state.get("workerStatus") or "pending"

    return {
        "commandId": command_id,
        "domain": domain,
        "limit": command_limit,
        "requestedAt": requested_at,
        "requestedBy": args.requested_by or existing_command.get("requestedBy") or "github_actions",
        "status": status,
    }


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        print("Error: --limit must be a positive integer.", file=sys.stderr)
        return 1

    try:
        storage = build_storage_backend()
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    state = load_admin_state(storage)
    now = iso_now()

    if args.worker_status:
        state["workerStatus"] = args.worker_status

    if args.domain:
        state["currentDomain"] = args.domain
    if args.limit is not None:
        state["currentLimit"] = args.limit

    if args.requested_at:
        state["lastRequestedAt"] = args.requested_at
    if args.started_at:
        state["lastStartedAt"] = args.started_at
    elif args.worker_status == "running":
        state["lastStartedAt"] = now
    if args.completed_at:
        state["lastCompletedAt"] = args.completed_at
    elif args.worker_status in {"complete", "error"}:
        state["lastCompletedAt"] = now

    if args.last_error is not None:
        state["lastError"] = args.last_error
    elif args.worker_status in {"running", "complete", "idle"}:
        state["lastError"] = None

    if args.last_run_id is not None:
        state["lastRunId"] = args.last_run_id
    if args.last_run_html_url is not None:
        state["lastRunHtmlUrl"] = args.last_run_html_url

    state["lastHeartbeatAt"] = now
    state["lastCommand"] = build_last_command(state, args, now=now)

    if args.clear_current_run or args.worker_status in {"complete", "error", "idle"}:
        state["currentDomain"] = None
        state["currentLimit"] = None

    if args.event_message and args.event_level:
        append_event(
            state,
            level=args.event_level,
            message=args.event_message,
            timestamp=now,
        )

    try:
        storage.write_json(state)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Review admin state updated: {state['workerStatus']}")
    if state.get("lastRunHtmlUrl"):
        print(f"Run URL: {state['lastRunHtmlUrl']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
