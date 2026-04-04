from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

from env_utils import get_data_root, get_processed_dir, get_project_root, load_project_env


load_project_env()


REQUIRED_ENV_KEYS = (
    "CLAUDE_API_KEY",
    "CLOUDFLARE_R2_ACCOUNT_ID",
    "CLOUDFLARE_R2_BUCKET",
    "CLOUDFLARE_R2_ENDPOINT",
    "CLOUDFLARE_R2_ACCESS_KEY_ID",
    "CLOUDFLARE_R2_SECRET_ACCESS_KEY",
)


def print_result(label: str, success: bool, detail: str) -> None:
    prefix = "PASS" if success else "FAIL"
    print(f"[{prefix}] {label}: {detail}")


def check_env_file() -> tuple[bool, str]:
    env_path = get_project_root() / ".env"
    if not env_path.exists():
        return False, f"Missing {env_path}"
    return True, str(env_path)


def check_required_env() -> tuple[bool, str]:
    import os

    missing = [key for key in REQUIRED_ENV_KEYS if not os.getenv(key, "").strip()]
    if missing:
        return False, "Missing env vars: " + ", ".join(missing)
    return True, "Required local and R2 env vars are present"


def list_source_parquet_files(root: Path, excluded_root: Path) -> list[Path]:
    parquet_files: list[Path] = []
    for path in root.rglob("*.parquet"):
        if not path.is_file():
            continue
        try:
            path.relative_to(excluded_root)
            continue
        except ValueError:
            pass
        parquet_files.append(path)
    return sorted(parquet_files)


def check_source_parquet() -> tuple[bool, str]:
    data_root = get_data_root()
    processed_dir = get_processed_dir()
    if not data_root.exists():
        return False, f"Data root not found: {data_root}"

    parquet_files = list_source_parquet_files(data_root, processed_dir)
    if not parquet_files:
        return False, f"No source parquet files found under {data_root}"

    preview = ", ".join(path.relative_to(data_root).as_posix() for path in parquet_files[:3])
    return True, f"{len(parquet_files)} parquet file(s) found under {data_root} ({preview})"


def check_processed_dir() -> tuple[bool, str]:
    processed_dir = get_processed_dir()
    processed_dir.mkdir(parents=True, exist_ok=True)
    probe_path = processed_dir / ".smoke-test-write-check"
    try:
        probe_path.write_text("ok\n", encoding="utf-8")
        probe_path.unlink()
    except OSError as exc:
        return False, f"Processed dir is not writable: {processed_dir} ({exc})"
    return True, str(processed_dir)


def check_r2_access() -> tuple[bool, str]:
    import os

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as exc:
        return False, f"boto3 is required for R2 smoke tests ({exc})"

    bucket = os.getenv("CLOUDFLARE_R2_BUCKET", "").strip()
    endpoint = os.getenv("CLOUDFLARE_R2_ENDPOINT", "").strip()
    access_key_id = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID", "").strip()
    secret_access_key = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "").strip()

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )

    try:
        client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    except (BotoCoreError, ClientError) as exc:
        return False, f"Unable to access bucket {bucket}: {exc}"

    return True, f"Authenticated to {bucket}"


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    checks = (
        ("Env file", check_env_file),
        ("Required env", check_required_env),
        ("Source parquet", check_source_parquet),
        ("Processed dir", check_processed_dir),
        ("R2 credentials", check_r2_access),
    )

    failures = 0
    for label, check in checks:
        success, detail = check()
        print_result(label, success, detail)
        if not success:
            failures += 1

    if failures:
        print()
        print(f"Smoke test failed with {failures} issue(s).", file=sys.stderr)
        return 1

    print()
    print("Smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
