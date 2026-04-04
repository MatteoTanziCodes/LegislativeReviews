from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from env_utils import load_project_env


load_project_env()


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
RUN_REVIEW_FRONTEND_PIPELINE_SCRIPT = SCRIPT_DIR / "run_review_frontend_pipeline.py"
DEFAULT_ADMIN_STATE_PATH = PROJECT_ROOT / "src" / "data" / "review-admin-state.json"
DEFAULT_CONTROL_PATH = PROJECT_ROOT / "src" / "data" / "review-control.json"
DEFAULT_R2_ADMIN_STATE_OBJECT_KEY = "review-admin-state.json"
DEFAULT_R2_CONTROL_OBJECT_KEY = "review-control.json"
DEFAULT_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_IDLE_HEARTBEAT_SECONDS = 30.0
MAX_RECENT_EVENTS = 20


class StorageBackend:
    def read_json(self, key: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def write_json(self, key: str, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def delete(self, key: str) -> None:
        raise NotImplementedError


class LocalJsonStorage(StorageBackend):
    def __init__(self, admin_state_path: Path, control_path: Path) -> None:
        self.paths = {
            "admin_state": admin_state_path,
            "control": control_path,
        }

    def read_json(self, key: str) -> dict[str, Any] | None:
        path = self.paths[key]
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def write_json(self, key: str, payload: dict[str, Any]) -> None:
        path = self.paths[key]
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        temp_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(path)

    def delete(self, key: str) -> None:
        self.paths[key].unlink(missing_ok=True)


class R2JsonStorage(StorageBackend):
    def __init__(
        self,
        *,
        bucket_name: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        admin_state_key: str,
        control_key: str,
    ) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError(
                "boto3 is required for Cloudflare R2 control-plane access."
            ) from exc

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
        )
        self.bucket_name = bucket_name
        self.keys = {
            "admin_state": admin_state_key,
            "control": control_key,
        }

    def read_json(self, key: str) -> dict[str, Any] | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=self.keys[key],
            )
        except self.client.exceptions.NoSuchKey:
            return None
        except Exception as exc:
            error_code = (
                getattr(exc, "response", {})
                .get("Error", {})
                .get("Code")
            )
            if error_code in {"NoSuchKey", "404"}:
                return None
            raise

        body = response["Body"].read().decode("utf-8")
        return json.loads(body)

    def write_json(self, key: str, payload: dict[str, Any]) -> None:
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.keys[key],
            Body=(json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode(
                "utf-8"
            ),
            ContentType="application/json; charset=utf-8",
            CacheControl="no-store, no-cache, must-revalidate, max-age=0",
        )

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket_name, Key=self.keys[key])


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
    timestamp: str | None = None,
) -> dict[str, Any]:
    events = list(state.get("recentEvents") or [])
    events.insert(
        0,
        {
            "level": level,
            "message": message,
            "timestamp": timestamp or iso_now(),
        },
    )
    state["recentEvents"] = events[:MAX_RECENT_EVENTS]
    return state


def build_storage_backend() -> StorageBackend:
    bucket_name = os.getenv("CLOUDFLARE_R2_BUCKET")
    if not bucket_name:
        return LocalJsonStorage(
            admin_state_path=DEFAULT_ADMIN_STATE_PATH,
            control_path=DEFAULT_CONTROL_PATH,
        )

    endpoint_url = os.getenv("CLOUDFLARE_R2_ENDPOINT")
    if not endpoint_url:
        account_id = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
        if account_id:
            endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    access_key_id = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY")
    if not endpoint_url or not access_key_id or not secret_access_key:
        raise RuntimeError(
            "Cloudflare R2 control-plane access is enabled but the required "
            "CLOUDFLARE_R2_* credentials are incomplete."
        )

    return R2JsonStorage(
        bucket_name=bucket_name,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        admin_state_key=(
            os.getenv("CLOUDFLARE_R2_ADMIN_STATE_KEY")
            or DEFAULT_R2_ADMIN_STATE_OBJECT_KEY
        ),
        control_key=(
            os.getenv("CLOUDFLARE_R2_CONTROL_KEY") or DEFAULT_R2_CONTROL_OBJECT_KEY
        ),
    )


def load_admin_state(storage: StorageBackend) -> dict[str, Any]:
    payload = storage.read_json("admin_state")
    if payload is None:
        return create_default_admin_state()
    default_state = create_default_admin_state()
    default_state.update(payload)
    default_state["recentEvents"] = list(payload.get("recentEvents") or [])
    return default_state


def persist_admin_state(storage: StorageBackend, state: dict[str, Any]) -> None:
    storage.write_json("admin_state", state)


def validate_control_request(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload.get("commandId"), str) or not payload["commandId"].strip():
        raise RuntimeError("Control request is missing commandId.")
    if not isinstance(payload.get("domain"), str) or not payload["domain"].strip():
        raise RuntimeError("Control request is missing domain.")
    limit = payload.get("limit")
    if limit is not None:
        if not isinstance(limit, int) or limit <= 0:
            raise RuntimeError("Control request limit must be a positive integer.")
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Background review worker that polls for dashboard-triggered review "
            "requests and executes the review pipeline."
        )
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="Seconds between control-plane polls.",
    )
    parser.add_argument(
        "--idle-heartbeat-seconds",
        type=float,
        default=DEFAULT_IDLE_HEARTBEAT_SECONDS,
        help="Seconds between idle heartbeat writes.",
    )
    parser.add_argument(
        "--python-executable",
        default=sys.executable,
        help="Python executable used to launch the review pipeline subprocess.",
    )
    return parser.parse_args(argv)


class WorkerDaemon:
    def __init__(self, *, storage: StorageBackend, python_executable: str) -> None:
        self.storage = storage
        self.python_executable = python_executable
        self.host = os.uname().nodename if hasattr(os, "uname") else os.getenv("COMPUTERNAME", "unknown")
        self.pid = os.getpid()
        self._state_lock = threading.Lock()
        self.state = load_admin_state(storage)
        self.state["workerHost"] = self.host
        self.state["workerPid"] = self.pid
        self.state["lastHeartbeatAt"] = iso_now()
        append_event(self.state, level="info", message="Review worker daemon started.")
        persist_admin_state(self.storage, self.state)

    def _snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            return deepcopy(self.state)

    def _update_state(self, **updates: Any) -> None:
        with self._state_lock:
            self.state.update(updates)
            self.state["workerHost"] = self.host
            self.state["workerPid"] = self.pid
            self.state["lastHeartbeatAt"] = iso_now()
            persist_admin_state(self.storage, self.state)

    def _append_event(self, *, level: str, message: str) -> None:
        with self._state_lock:
            append_event(self.state, level=level, message=message)
            self.state["workerHost"] = self.host
            self.state["workerPid"] = self.pid
            self.state["lastHeartbeatAt"] = iso_now()
            persist_admin_state(self.storage, self.state)

    def _mark_idle(self) -> None:
        self._update_state(workerStatus="idle")

    def _heartbeat_loop(self, stop_event: threading.Event) -> None:
        while not stop_event.wait(15):
            with self._state_lock:
                self.state["workerHost"] = self.host
                self.state["workerPid"] = self.pid
                self.state["lastHeartbeatAt"] = iso_now()
                persist_admin_state(self.storage, self.state)

    def _run_requested_batch(self, request_payload: dict[str, Any]) -> None:
        request = validate_control_request(request_payload)
        command: list[str] = [
            self.python_executable,
            str(RUN_REVIEW_FRONTEND_PIPELINE_SCRIPT),
            "--domain",
            request["domain"],
        ]
        if request.get("limit") is not None:
            command.extend(["--limit", str(request["limit"])])

        start_timestamp = iso_now()
        running_command = {
            **request,
            "status": "running",
        }
        self._update_state(
            currentDomain=request["domain"],
            currentLimit=request.get("limit"),
            lastCommand=running_command,
            lastError=None,
            lastRequestedAt=request.get("requestedAt", start_timestamp),
            lastStartedAt=start_timestamp,
            workerStatus="running",
        )
        self._append_event(
            level="info",
            message=(
                f"Starting review run for {request['domain']}"
                + (
                    f" (limit {request['limit']})."
                    if request.get("limit") is not None
                    else "."
                )
            ),
        )

        stop_event = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(stop_event,),
            daemon=True,
        )
        heartbeat_thread.start()

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            bufsize=1,
            cwd=PROJECT_ROOT,
        )

        try:
            assert process.stdout is not None
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                print(line, flush=True)
                level = "error" if line.lower().startswith("error") else "info"
                self._append_event(level=level, message=line)
        finally:
            process.wait()
            stop_event.set()
            heartbeat_thread.join(timeout=5)

        finished_timestamp = iso_now()
        if process.returncode == 0:
            self._update_state(
                currentDomain=None,
                currentLimit=None,
                lastCommand={**request, "status": "complete"},
                lastCompletedAt=finished_timestamp,
                workerStatus="complete",
            )
            self._append_event(
                level="info",
                message=f"Review run completed successfully for {request['domain']}.",
            )
        else:
            self._update_state(
                currentDomain=None,
                currentLimit=None,
                lastCommand={**request, "status": "error"},
                lastCompletedAt=finished_timestamp,
                lastError=(
                    f"Review run for {request['domain']} exited with code {process.returncode}."
                ),
                workerStatus="error",
            )
            self._append_event(
                level="error",
                message=(
                    f"Review run failed for {request['domain']} "
                    f"(exit code {process.returncode})."
                ),
            )

    def run_forever(
        self,
        *,
        poll_interval: float,
        idle_heartbeat_seconds: float,
    ) -> None:
        last_idle_heartbeat = 0.0
        while True:
            control_request = self.storage.read_json("control")
            if control_request is not None:
                try:
                    validated_request = validate_control_request(control_request)
                except RuntimeError as exc:
                    self.storage.delete("control")
                    self._update_state(
                        lastError=str(exc),
                        workerStatus="error",
                    )
                    self._append_event(level="error", message=str(exc))
                    time.sleep(poll_interval)
                    continue

                self.storage.delete("control")
                self._run_requested_batch(validated_request)
                last_idle_heartbeat = time.time()
            else:
                now = time.time()
                if now - last_idle_heartbeat >= idle_heartbeat_seconds:
                    self._mark_idle()
                    last_idle_heartbeat = now

            time.sleep(poll_interval)


def main(argv: Sequence[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args(argv)
    if args.poll_interval <= 0:
        print("Error: --poll-interval must be positive.", file=sys.stderr)
        return 1
    if args.idle_heartbeat_seconds <= 0:
        print("Error: --idle-heartbeat-seconds must be positive.", file=sys.stderr)
        return 1

    try:
        storage = build_storage_backend()
        daemon = WorkerDaemon(
            storage=storage,
            python_executable=args.python_executable,
        )
        daemon.run_forever(
            poll_interval=args.poll_interval,
            idle_heartbeat_seconds=args.idle_heartbeat_seconds,
        )
    except KeyboardInterrupt:
        print("Review worker daemon stopped.", flush=True)
        return 0
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
