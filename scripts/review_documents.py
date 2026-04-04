from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence

from env_utils import load_project_env

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

load_project_env()

import classify_documents as classifier
import export_frontend_review_data as frontend_export


MANDATE_PATH = Path(
    r"config/review_mandates/obsolescence_modernization_prosperity_v1.json"
)

DEFAULT_REVIEW_MODEL = "claude-sonnet-4-20250514"
DEFAULT_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_REVIEW_MAX_TOKENS = 900
REVIEW_PROGRESS_EVERY = 100
INITIAL_RESULT_PREVIEW_COUNT = 5
DEFAULT_FRONTEND_TOTAL_COUNT = 5796
DEFAULT_FRONTEND_DAILY_CAPACITY = 200
DEFAULT_FRONTEND_EXPORT_EVERY = 10
DEFAULT_REVIEW_CHECKPOINT_EVERY = 25
DEFAULT_LOCK_STALE_AFTER_SECONDS = 60 * 60 * 12
REVIEW_PROMPT_VERSION = "2026-03-24.qualitative-v1"
REVIEW_RUN_MANIFEST_VERSION = 1

ALLOWED_DECISIONS = {"retain", "amend", "repeal_candidate", "escalate"}
VALIDATION_RANGES = {
    "operational_relevance_score": (0, 3),
    "prosperity_alignment_score": (-2, 2),
    "administrative_burden_score": (0, 3),
    "repeal_risk_score": (0, 3),
}
QUALITATIVE_LEVEL_SCORES = {
    "strong": 1.0,
    "moderate": 0.6,
    "weak": 0.2,
}

CLAUDE_RATE_LIMIT_MAX_RETRIES = 5
CLAUDE_RATE_LIMIT_COOLDOWN_SECONDS = 15.0
CLAUDE_RATE_LIMIT_BACKOFF_MULTIPLIER = 2.0
ANTHROPIC_VERSION = "2023-06-01"

CLAUDE_COOLDOWN_UNTIL = 0.0


@dataclass(frozen=True)
class ReviewConfig:
    api_key: str
    model: str
    api_url: str
    max_tokens: int


@dataclass(frozen=True)
class ReviewInputRow:
    mandate_id: str
    document_id: str
    title_en: str
    citation_en: str | None
    primary_domain: str
    review_text: str
    evidence_section_keys: str


@dataclass(frozen=True)
class FrontendExportConfig:
    summary_output_path: Path
    details_output_path: Path
    total_count: int
    daily_capacity: int
    export_every: int
    r2_publish_config: frontend_export.R2PublishConfig | None = None


@dataclass(frozen=True)
class ReviewRunManifest:
    manifest_version: int
    input_path: str
    output_path: str
    journal_path: str
    mandate_id: str
    review_model: str
    prompt_version: str
    selected_row_count: int
    document_range_start: str | None
    document_range_end: str | None
    primary_domains: list[str]
    input_fingerprint: str


@dataclass(frozen=True)
class CompletedReviewRecord:
    mandate_id: str
    document_id: str
    title_en: str
    citation_en: str | None
    primary_domain: str
    decision: str
    decision_confidence: float
    evidence_sufficiency_score: float
    rationale: str
    evidence_section_keys: list[str]
    operational_relevance_score: int
    prosperity_alignment_score: int
    administrative_burden_score: int
    repeal_risk_score: int
    operational_status_assessment: str
    obsolescence_evidence: str
    administrative_burden_evidence: str
    repeal_risk_assessment: str
    evidence_sufficiency: str
    policy_tenet_alignment_clarity: str
    prosperity_tenets_used: list[str]
    review_model: str
    raw_response_json: str

    def to_output_row(self) -> tuple[Any, ...]:
        return (
            self.mandate_id,
            self.document_id,
            self.title_en,
            self.citation_en,
            self.primary_domain,
            self.decision,
            self.decision_confidence,
            self.evidence_sufficiency_score,
            self.rationale,
            json.dumps(self.evidence_section_keys, ensure_ascii=False),
            self.operational_relevance_score,
            self.prosperity_alignment_score,
            self.administrative_burden_score,
            self.repeal_risk_score,
            self.operational_status_assessment,
            self.obsolescence_evidence,
            self.administrative_burden_evidence,
            self.repeal_risk_assessment,
            self.evidence_sufficiency,
            self.policy_tenet_alignment_clarity,
            json.dumps(self.prosperity_tenets_used, ensure_ascii=False),
            self.review_model,
            self.raw_response_json,
        )

    def to_frontend_row(self) -> frontend_export.ReviewRow:
        return frontend_export.ReviewRow(
            document_id=self.document_id,
            title_en=self.title_en,
            citation_en=self.citation_en,
            decision=self.decision,
            decision_confidence=self.decision_confidence,
            rationale=self.rationale,
            evidence_section_keys=list(self.evidence_section_keys),
            operational_relevance_score=self.operational_relevance_score,
            prosperity_alignment_score=self.prosperity_alignment_score,
            administrative_burden_score=self.administrative_burden_score,
            repeal_risk_score=self.repeal_risk_score,
            review_model=self.review_model,
        )


@dataclass
class ReviewRunLock:
    lock_path: Path
    metadata: dict[str, Any]

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.metadata, indent=2, ensure_ascii=False) + "\n"

        while True:
            try:
                fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                return
            except FileExistsError:
                existing_metadata = load_lock_metadata(self.lock_path)
                if existing_metadata is None:
                    lock_age_seconds = time.time() - self.lock_path.stat().st_mtime
                    if lock_age_seconds >= DEFAULT_LOCK_STALE_AFTER_SECONDS:
                        self.lock_path.unlink(missing_ok=True)
                        continue
                if existing_metadata is not None and not is_pid_running(
                    existing_metadata.get("pid")
                ):
                    self.lock_path.unlink(missing_ok=True)
                    continue

                raise RuntimeError(build_lock_conflict_message(self.lock_path, existing_metadata))

    def release(self) -> None:
        try:
            self.lock_path.unlink(missing_ok=True)
        except OSError:
            pass


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run prosperity-first review over a reviewer input parquet."
    )
    parser.add_argument(
        "--input-path",
        required=True,
        type=Path,
        help="Path to the review input parquet.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="Optional parquet output path. Defaults beside the input parquet.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of rows to review after ordering by document_id.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=DEFAULT_REVIEW_CHECKPOINT_EVERY,
        help="How many successful reviews between parquet checkpoints.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore existing review output/journal and start the batch from scratch.",
    )
    parser.add_argument(
        "--frontend-summary-output-path",
        type=Path,
        help="Optional frontend summary JSON path for live dashboard updates.",
    )
    parser.add_argument(
        "--frontend-details-output-path",
        type=Path,
        help="Optional frontend drilldown JSON path for live dashboard updates.",
    )
    parser.add_argument(
        "--frontend-total-count",
        type=int,
        default=DEFAULT_FRONTEND_TOTAL_COUNT,
        help="Known total corpus size for frontend progress metrics.",
    )
    parser.add_argument(
        "--frontend-daily-capacity",
        type=int,
        default=DEFAULT_FRONTEND_DAILY_CAPACITY,
        help="Daily review capacity to include in frontend summary data.",
    )
    parser.add_argument(
        "--frontend-export-every",
        type=int,
        default=DEFAULT_FRONTEND_EXPORT_EVERY,
        help="How many successful reviews between live frontend exports.",
    )
    return parser.parse_args(argv)


def delete_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(path)


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes:d}m {secs:02d}s"
    return f"{secs:d}s"


def format_decision_counts_inline(counter: Counter[str]) -> str:
    if not counter:
        return "(none yet)"
    ordered_items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{decision}={count}" for decision, count in ordered_items)


def load_mandate(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Mandate config not found at {path}")

    with path.open("r", encoding="utf-8") as handle:
        mandate = json.load(handle)

    required_fields = {
        "mandate_id",
        "mandate_name",
        "purpose",
        "policy_tenets",
        "decision_labels",
        "evaluation_criteria",
        "scoring_rubric",
        "repeal_risk_rules",
        "default_decision_policy",
    }
    missing = [field for field in required_fields if field not in mandate]
    if missing:
        raise RuntimeError(
            "Mandate config is missing required fields: " + ", ".join(sorted(missing))
        )

    return mandate


def build_default_output_path(input_path: Path) -> Path:
    filename = input_path.name
    if filename.startswith("review_inputs_"):
        output_name = "reviews_" + filename[len("review_inputs_") :]
    else:
        output_name = f"reviews_{filename}"
    return input_path.with_name(output_name)


def build_review_journal_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.journal.jsonl")


def build_review_manifest_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.manifest.json")


def build_review_lock_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}.lock")


def build_completed_review_record(
    *,
    row: ReviewInputRow,
    validated: dict[str, Any],
    review_model: str,
    raw_response_json: str,
) -> CompletedReviewRecord:
    return CompletedReviewRecord(
        mandate_id=row.mandate_id,
        document_id=row.document_id,
        title_en=row.title_en,
        citation_en=row.citation_en,
        primary_domain=row.primary_domain,
        decision=validated["decision"],
        decision_confidence=float(validated["decision_confidence"]),
        evidence_sufficiency_score=float(validated["evidence_sufficiency_score"]),
        rationale=validated["rationale"],
        evidence_section_keys=list(validated["evidence_section_keys"]),
        operational_relevance_score=int(validated["operational_relevance_score"]),
        prosperity_alignment_score=int(validated["prosperity_alignment_score"]),
        administrative_burden_score=int(validated["administrative_burden_score"]),
        repeal_risk_score=int(validated["repeal_risk_score"]),
        operational_status_assessment=validated["operational_status_assessment"],
        obsolescence_evidence=validated["obsolescence_evidence"],
        administrative_burden_evidence=validated["administrative_burden_evidence"],
        repeal_risk_assessment=validated["repeal_risk_assessment"],
        evidence_sufficiency=validated["evidence_sufficiency"],
        policy_tenet_alignment_clarity=validated["policy_tenet_alignment_clarity"],
        prosperity_tenets_used=list(validated["prosperity_tenets_used"]),
        review_model=review_model,
        raw_response_json=raw_response_json,
    )


def append_review_journal_record(
    journal_path: Path,
    record: CompletedReviewRecord,
) -> None:
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    with journal_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_lock_metadata(lock_path: Path) -> dict[str, Any] | None:
    if not lock_path.exists():
        return None
    try:
        return json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def is_pid_running(pid: Any) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def build_lock_conflict_message(
    lock_path: Path,
    existing_metadata: dict[str, Any] | None,
) -> str:
    if not existing_metadata:
        return (
            f"Another review run appears to hold the output lock at {lock_path}. "
            "If this is stale, remove it only after confirming the worker is stopped."
        )

    holder_pid = existing_metadata.get("pid", "unknown")
    holder_host = existing_metadata.get("host", "unknown")
    holder_started_at = existing_metadata.get("startedAt", "unknown")
    holder_output_path = existing_metadata.get("outputPath", "unknown")
    return (
        "Another review run already holds the output lock. "
        f"lock={lock_path} pid={holder_pid} host={holder_host} "
        f"started={holder_started_at} output={holder_output_path}"
    )


def load_output_review_records(output_path: Path) -> list[CompletedReviewRecord]:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required. Install it with `pip install duckdb`.") from exc

    if not output_path.exists():
        return []

    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT
                mandate_id,
                document_id,
                title_en,
                citation_en,
                primary_domain,
                decision,
                decision_confidence,
                evidence_sufficiency_score,
                rationale,
                evidence_section_keys,
                operational_relevance_score,
                prosperity_alignment_score,
                administrative_burden_score,
                repeal_risk_score,
                operational_status_assessment,
                obsolescence_evidence,
                administrative_burden_evidence,
                repeal_risk_assessment,
                evidence_sufficiency,
                policy_tenet_alignment_clarity,
                prosperity_tenets_used,
                review_model,
                raw_response_json
            FROM read_parquet(?)
            ORDER BY document_id
            """,
            [str(output_path)],
        ).fetchall()
    finally:
        con.close()

    records: list[CompletedReviewRecord] = []
    for row in rows:
        evidence_section_keys = json.loads(row[9]) if row[9] else []
        prosperity_tenets_used = json.loads(row[20]) if row[20] else []
        records.append(
            CompletedReviewRecord(
                mandate_id=str(row[0]),
                document_id=str(row[1]),
                title_en=str(row[2]),
                citation_en=str(row[3]) if row[3] is not None else None,
                primary_domain=str(row[4]),
                decision=str(row[5]),
                decision_confidence=float(row[6]),
                evidence_sufficiency_score=float(row[7]),
                rationale=str(row[8]),
                evidence_section_keys=[str(item) for item in evidence_section_keys],
                operational_relevance_score=int(row[10]),
                prosperity_alignment_score=int(row[11]),
                administrative_burden_score=int(row[12]),
                repeal_risk_score=int(row[13]),
                operational_status_assessment=str(row[14]),
                obsolescence_evidence=str(row[15]),
                administrative_burden_evidence=str(row[16]),
                repeal_risk_assessment=str(row[17]),
                evidence_sufficiency=str(row[18]),
                policy_tenet_alignment_clarity=str(row[19]),
                prosperity_tenets_used=[str(item) for item in prosperity_tenets_used],
                review_model=str(row[21]),
                raw_response_json=str(row[22]),
            )
        )
    return records


def load_journal_review_records(journal_path: Path) -> list[CompletedReviewRecord]:
    if not journal_path.exists():
        return []

    records: list[CompletedReviewRecord] = []
    with journal_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            candidate = line.strip()
            if not candidate:
                continue
            try:
                payload = json.loads(candidate)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Resume journal at {journal_path} is invalid on line {line_number}"
                ) from exc
            records.append(
                CompletedReviewRecord(
                    mandate_id=str(payload["mandate_id"]),
                    document_id=str(payload["document_id"]),
                    title_en=str(payload["title_en"]),
                    citation_en=(
                        str(payload["citation_en"])
                        if payload.get("citation_en") is not None
                        else None
                    ),
                    primary_domain=str(payload["primary_domain"]),
                    decision=str(payload["decision"]),
                    decision_confidence=float(payload["decision_confidence"]),
                    evidence_sufficiency_score=float(payload["evidence_sufficiency_score"]),
                    rationale=str(payload["rationale"]),
                    evidence_section_keys=[
                        str(item) for item in payload["evidence_section_keys"]
                    ],
                    operational_relevance_score=int(payload["operational_relevance_score"]),
                    prosperity_alignment_score=int(payload["prosperity_alignment_score"]),
                    administrative_burden_score=int(payload["administrative_burden_score"]),
                    repeal_risk_score=int(payload["repeal_risk_score"]),
                    operational_status_assessment=str(payload["operational_status_assessment"]),
                    obsolescence_evidence=str(payload["obsolescence_evidence"]),
                    administrative_burden_evidence=str(
                        payload["administrative_burden_evidence"]
                    ),
                    repeal_risk_assessment=str(payload["repeal_risk_assessment"]),
                    evidence_sufficiency=str(payload["evidence_sufficiency"]),
                    policy_tenet_alignment_clarity=str(
                        payload["policy_tenet_alignment_clarity"]
                    ),
                    prosperity_tenets_used=[
                        str(item) for item in payload["prosperity_tenets_used"]
                    ],
                    review_model=str(payload["review_model"]),
                    raw_response_json=str(payload["raw_response_json"]),
                )
            )
    return records


def hydrate_resume_records(
    *,
    review_inputs: Sequence[ReviewInputRow],
    output_path: Path,
    journal_path: Path,
) -> list[CompletedReviewRecord]:
    valid_document_ids = {row.document_id for row in review_inputs}
    record_by_document_id: dict[str, CompletedReviewRecord] = {}

    for record in load_output_review_records(output_path):
        if record.document_id in valid_document_ids:
            record_by_document_id[record.document_id] = record

    for record in load_journal_review_records(journal_path):
        if record.document_id in valid_document_ids:
            record_by_document_id[record.document_id] = record

    return [
        record_by_document_id[row.document_id]
        for row in review_inputs
        if row.document_id in record_by_document_id
    ]


def assert_review_inputs_match_mandate(
    review_inputs: Sequence[ReviewInputRow],
    mandate_id: str,
) -> None:
    mismatched_document_ids = [
        row.document_id for row in review_inputs if row.mandate_id != mandate_id
    ]
    if mismatched_document_ids:
        sample = ", ".join(mismatched_document_ids[:3])
        raise RuntimeError(
            "Review input mandate_id does not match the loaded mandate config. "
            f"Expected {mandate_id}; mismatched documents include: {sample}"
        )


def compute_review_inputs_fingerprint(review_inputs: Sequence[ReviewInputRow]) -> str:
    digest = hashlib.sha256()
    for row in review_inputs:
        fields = (
            row.mandate_id,
            row.document_id,
            row.title_en,
            row.citation_en or "",
            row.primary_domain,
            row.review_text,
            row.evidence_section_keys,
        )
        for field in fields:
            digest.update(field.encode("utf-8"))
            digest.update(b"\0")
    return digest.hexdigest()


def build_review_run_manifest(
    *,
    input_path: Path,
    output_path: Path,
    journal_path: Path,
    review_inputs: Sequence[ReviewInputRow],
    mandate_id: str,
    review_model: str,
) -> ReviewRunManifest:
    ordered_review_inputs = list(review_inputs)
    primary_domains = sorted({row.primary_domain for row in ordered_review_inputs})
    return ReviewRunManifest(
        manifest_version=REVIEW_RUN_MANIFEST_VERSION,
        input_path=str(input_path.resolve(strict=False)),
        output_path=str(output_path.resolve(strict=False)),
        journal_path=str(journal_path.resolve(strict=False)),
        mandate_id=mandate_id,
        review_model=review_model,
        prompt_version=REVIEW_PROMPT_VERSION,
        selected_row_count=len(ordered_review_inputs),
        document_range_start=ordered_review_inputs[0].document_id if ordered_review_inputs else None,
        document_range_end=ordered_review_inputs[-1].document_id if ordered_review_inputs else None,
        primary_domains=primary_domains,
        input_fingerprint=compute_review_inputs_fingerprint(ordered_review_inputs),
    )


def load_review_run_manifest(manifest_path: Path) -> ReviewRunManifest | None:
    if not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Review run manifest at {manifest_path} is unreadable.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"Review run manifest at {manifest_path} is not a JSON object.")

    try:
        return ReviewRunManifest(
            manifest_version=int(payload["manifest_version"]),
            input_path=str(payload["input_path"]),
            output_path=str(payload["output_path"]),
            journal_path=str(payload["journal_path"]),
            mandate_id=str(payload["mandate_id"]),
            review_model=str(payload["review_model"]),
            prompt_version=str(payload["prompt_version"]),
            selected_row_count=int(payload["selected_row_count"]),
            document_range_start=(
                str(payload["document_range_start"])
                if payload.get("document_range_start") is not None
                else None
            ),
            document_range_end=(
                str(payload["document_range_end"])
                if payload.get("document_range_end") is not None
                else None
            ),
            primary_domains=[str(item) for item in payload.get("primary_domains", [])],
            input_fingerprint=str(payload["input_fingerprint"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Review run manifest at {manifest_path} is missing required fields."
        ) from exc


def write_review_run_manifest(manifest_path: Path, manifest: ReviewRunManifest) -> None:
    atomic_write_text(
        manifest_path,
        json.dumps(asdict(manifest), indent=2, ensure_ascii=False) + "\n",
    )


def ensure_resume_manifest_compatible(
    *,
    manifest_path: Path,
    current_manifest: ReviewRunManifest,
    existing_state_present: bool,
) -> None:
    existing_manifest = load_review_run_manifest(manifest_path)
    if existing_manifest is None:
        if existing_state_present:
            raise RuntimeError(
                "Existing review output or journal was found but no manifest is present. "
                "Resume is unsafe without a manifest. Re-run with --no-resume or move the old artifacts aside."
            )
        write_review_run_manifest(manifest_path, current_manifest)
        return

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
    mismatches: list[str] = []
    for field_name in comparable_fields:
        existing_value = getattr(existing_manifest, field_name)
        current_value = getattr(current_manifest, field_name)
        if existing_value != current_value:
            mismatches.append(
                f"{field_name}: existing={existing_value!r} current={current_value!r}"
            )

    if mismatches:
        mismatch_preview = "; ".join(mismatches[:4])
        raise RuntimeError(
            "Resume manifest does not match the current review run configuration. "
            f"{mismatch_preview}. Use --no-resume or choose a new output path."
        )


def build_frontend_export_config(args: argparse.Namespace) -> FrontendExportConfig | None:
    if args.frontend_summary_output_path is None and args.frontend_details_output_path is None:
        return None
    if args.frontend_summary_output_path is None or args.frontend_details_output_path is None:
        raise RuntimeError(
            "Both --frontend-summary-output-path and --frontend-details-output-path are required "
            "when live frontend export is enabled."
        )
    if args.frontend_total_count <= 0:
        raise RuntimeError("--frontend-total-count must be a positive integer.")
    if args.frontend_daily_capacity <= 0:
        raise RuntimeError("--frontend-daily-capacity must be a positive integer.")
    if args.frontend_export_every <= 0:
        raise RuntimeError("--frontend-export-every must be a positive integer.")

    return FrontendExportConfig(
        summary_output_path=args.frontend_summary_output_path,
        details_output_path=args.frontend_details_output_path,
        total_count=args.frontend_total_count,
        daily_capacity=args.frontend_daily_capacity,
        export_every=args.frontend_export_every,
        r2_publish_config=frontend_export.build_r2_publish_config(),
    )


def get_review_config() -> ReviewConfig:
    api_key = os.getenv("CLAUDE_API_KEY")
    if not api_key:
        raise RuntimeError("CLAUDE_API_KEY is missing. Add it to .env before running this script.")

    model = os.getenv("CLAUDE_REVIEW_MODEL", DEFAULT_REVIEW_MODEL).strip() or DEFAULT_REVIEW_MODEL
    api_url = os.getenv("CLAUDE_API_URL", DEFAULT_CLAUDE_API_URL).strip() or DEFAULT_CLAUDE_API_URL
    max_tokens = int(os.getenv("CLAUDE_REVIEW_MAX_TOKENS", str(DEFAULT_REVIEW_MAX_TOKENS)))
    return ReviewConfig(
        api_key=api_key,
        model=model,
        api_url=api_url,
        max_tokens=max_tokens,
    )


def load_review_inputs(input_path: Path, limit: int | None) -> list[ReviewInputRow]:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required. Install it with `pip install duckdb`.") from exc

    if not input_path.exists():
        raise RuntimeError(f"Input parquet not found at {input_path}")

    con = duckdb.connect()
    try:
        if limit is None:
            rows = con.execute(
                """
                SELECT
                    mandate_id,
                    document_id,
                    title_en,
                    citation_en,
                    primary_domain,
                    review_text,
                    evidence_section_keys
                FROM read_parquet(?)
                ORDER BY document_id
                """,
                [str(input_path)],
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT
                    mandate_id,
                    document_id,
                    title_en,
                    citation_en,
                    primary_domain,
                    review_text,
                    evidence_section_keys
                FROM read_parquet(?)
                ORDER BY document_id
                LIMIT ?
                """,
                [str(input_path), limit],
            ).fetchall()
    finally:
        con.close()

    return [ReviewInputRow(*row) for row in rows]


def parse_evidence_section_keys(value: str) -> list[str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("input evidence_section_keys is not valid JSON") from exc

    if not isinstance(parsed, list) or any(not isinstance(item, str) for item in parsed):
        raise ValueError("input evidence_section_keys must be a JSON array of strings")
    return parsed


def build_review_prompt(
    mandate: dict[str, Any],
    review_text: str,
    allowed_evidence_keys: Sequence[str],
) -> str:
    payload = {
        "mandate_id": mandate["mandate_id"],
        "mandate_name": mandate["mandate_name"],
        "purpose": mandate["purpose"],
        "policy_tenets": mandate["policy_tenets"],
        "decision_labels": mandate["decision_labels"],
        "evaluation_criteria": mandate["evaluation_criteria"],
        "scoring_rubric": mandate["scoring_rubric"],
        "repeal_risk_rules": mandate["repeal_risk_rules"],
        "default_decision_policy": mandate["default_decision_policy"],
    }
    return (
        "This is an internal policy-analysis prototype, not legal advice.\n"
        "Use only the provided review_text. Do not rely on outside knowledge.\n"
        "Be conservative with repeal_candidate.\n"
        "Prefer escalate when evidence is weak or repeal risk is high.\n"
        "Do not return any numeric confidence. Python will compute decision_confidence.\n"
        "You may only use evidence_section_keys from the allowed list below.\n"
        "Do not invent or infer section keys.\n"
        "If none are appropriate, return an empty array.\n"
        "Return valid JSON only with no markdown, preamble, or trailing text.\n\n"
        "Apply this mandate configuration:\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        "Use only these qualitative labels for the assessment fields: strong, moderate, weak.\n"
        "Interpret the qualitative fields as follows:\n"
        "- operational_status_assessment: strength of evidence that the law is still actively operational today.\n"
        "- obsolescence_evidence: strength of evidence that the law is obsolete, transitional, spent, duplicated, or no longer needed.\n"
        "- administrative_burden_evidence: strength of evidence that the law creates red tape, duplication, rigidity, or unnecessary process burden.\n"
        "- repeal_risk_assessment: strength of evidence that repeal or modification is risky because the law affects rights, benefits, offences, taxation, core administration, major regulatory powers, or constitutional structures.\n"
        "- evidence_sufficiency: how sufficient the provided title, citation, and sections are to support a recommendation.\n"
        "- policy_tenet_alignment_clarity: how clearly the recommendation aligns with one or more mandate policy tenets.\n\n"
        "Allowed evidence_section_keys:\n"
        f"{json.dumps(list(allowed_evidence_keys), ensure_ascii=False)}\n\n"
        "Return a JSON object with exactly these keys:\n"
        "{\n"
        '  "decision": "retain|amend|repeal_candidate|escalate",\n'
        '  "rationale": "string",\n'
        '  "operational_relevance_score": 0,\n'
        '  "prosperity_alignment_score": 0,\n'
        '  "administrative_burden_score": 0,\n'
        '  "repeal_risk_score": 0,\n'
        '  "operational_status_assessment": "strong|moderate|weak",\n'
        '  "obsolescence_evidence": "strong|moderate|weak",\n'
        '  "administrative_burden_evidence": "strong|moderate|weak",\n'
        '  "repeal_risk_assessment": "strong|moderate|weak",\n'
        '  "evidence_sufficiency": "strong|moderate|weak",\n'
        '  "policy_tenet_alignment_clarity": "strong|moderate|weak",\n'
        '  "prosperity_tenets_used": ["exact mandate tenet"],\n'
        '  "evidence_section_keys": ["1", "2"]\n'
        "}\n\n"
        "review_text:\n"
        f"{review_text}"
    )


def build_evidence_key_repair_prompt(
    raw_response_text: str,
    allowed_evidence_keys: Sequence[str],
) -> str:
    return (
        "The previous JSON response used invalid evidence_section_keys.\n"
        "Return corrected JSON only with the exact same schema.\n"
        "You may only use evidence_section_keys from the allowed list below.\n"
        "Do not invent or infer section keys.\n"
        "If none are appropriate, return an empty array.\n"
        "Do not add any numeric confidence field.\n"
        "Keep the decision, rationale, rubric scores, qualitative assessments, and prosperity_tenets_used unchanged unless required to make the JSON valid.\n\n"
        "Allowed evidence_section_keys:\n"
        f"{json.dumps(list(allowed_evidence_keys), ensure_ascii=False)}\n\n"
        "Original invalid response:\n"
        f"{raw_response_text}"
    )


def extract_response_text(body: dict[str, Any]) -> str:
    parts = body.get("content", [])
    text = " ".join(
        part.get("text", "")
        for part in parts
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    ).strip()
    return text


def parse_json_response(raw_text: str) -> dict[str, Any]:
    candidate = raw_text.strip()

    if candidate.startswith("```"):
        lines = [line for line in candidate.splitlines() if not line.startswith("```")]
        candidate = "\n".join(lines).strip()

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        parsed = json.loads(candidate[start : end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("Claude response is not a JSON object")
    return parsed


def coerce_integer_score(field_name: str, value: Any) -> int:
    numeric = int(value)
    minimum, maximum = VALIDATION_RANGES[field_name]
    if numeric < minimum or numeric > maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return numeric


def coerce_qualitative_assessment(field_name: str, value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be one of: strong, moderate, weak")

    normalized = value.strip().casefold()
    if normalized not in QUALITATIVE_LEVEL_SCORES:
        raise ValueError(f"{field_name} must be one of: strong, moderate, weak")
    return normalized


def qualitative_score(value: str) -> float:
    return QUALITATIVE_LEVEL_SCORES[value]


def clamp_unit_interval(value: float) -> float:
    return max(0.0, min(1.0, value))


def compute_evidence_sufficiency_score(evidence_sufficiency: str) -> float:
    return round(qualitative_score(evidence_sufficiency), 3)


def compute_amend_prosperity_signal(prosperity_alignment_score: int) -> float:
    mapped_value = (prosperity_alignment_score + 2) / 4
    if prosperity_alignment_score <= 0:
        return 1.0 - mapped_value
    return 0.3


def compute_decision_confidence(
    validated: dict[str, Any],
    *,
    evidence_sufficiency_score: float,
) -> float:
    operational_status_assessment_score = qualitative_score(
        validated["operational_status_assessment"]
    )
    obsolescence_evidence_score = qualitative_score(validated["obsolescence_evidence"])
    administrative_burden_evidence_score = qualitative_score(
        validated["administrative_burden_evidence"]
    )
    repeal_risk_assessment_score = qualitative_score(
        validated["repeal_risk_assessment"]
    )
    policy_tenet_alignment_clarity_score = qualitative_score(
        validated["policy_tenet_alignment_clarity"]
    )

    decision = validated["decision"]
    if decision == "retain":
        confidence = (
            0.30 * operational_status_assessment_score
            + 0.25 * repeal_risk_assessment_score
            + 0.20 * evidence_sufficiency_score
            + 0.15 * policy_tenet_alignment_clarity_score
            + 0.10 * (1.0 - obsolescence_evidence_score)
        )
    elif decision == "amend":
        prosperity_signal_from_rubric = compute_amend_prosperity_signal(
            validated["prosperity_alignment_score"]
        )
        confidence = (
            0.25 * operational_status_assessment_score
            + 0.25 * administrative_burden_evidence_score
            + 0.20 * evidence_sufficiency_score
            + 0.20 * policy_tenet_alignment_clarity_score
            + 0.10 * prosperity_signal_from_rubric
        )
    elif decision == "repeal_candidate":
        confidence = (
            0.35 * obsolescence_evidence_score
            + 0.25 * (1.0 - repeal_risk_assessment_score)
            + 0.20 * evidence_sufficiency_score
            + 0.20 * administrative_burden_evidence_score
        )
    elif decision == "escalate":
        ambiguity_signal = 1.0 - abs(
            operational_status_assessment_score - obsolescence_evidence_score
        )
        confidence = (
            0.35 * repeal_risk_assessment_score
            + 0.25 * (1.0 - evidence_sufficiency_score)
            + 0.20 * ambiguity_signal
            + 0.20
            * max(operational_status_assessment_score, obsolescence_evidence_score)
        )
    else:
        raise ValueError(f"Unsupported decision: {decision}")

    return round(clamp_unit_interval(confidence), 3)


def normalize_policy_tenet(value: str) -> str:
    return " ".join(value.strip().rstrip(".").split()).casefold()


def canonicalize_policy_tenets(
    values: Any,
    *,
    allowed_values: Sequence[str],
) -> list[str]:
    if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
        raise ValueError("prosperity_tenets_used must be a JSON array of strings")
    if not values:
        raise ValueError("prosperity_tenets_used must not be empty")

    canonical_by_normalized = {
        normalize_policy_tenet(item): item for item in allowed_values
    }
    canonicalized: list[str] = []
    invalid: list[str] = []

    for value in values:
        normalized = normalize_policy_tenet(value)
        canonical = canonical_by_normalized.get(normalized)
        if canonical is None:
            invalid.append(value)
            continue
        if canonical not in canonicalized:
            canonicalized.append(canonical)

    if invalid:
        raise ValueError(
            "prosperity_tenets_used contains unsupported values: "
            + ", ".join(sorted(set(invalid)))
        )

    return canonicalized


def canonicalize_evidence_section_keys(
    values: Any,
    *,
    allowed_values: set[str],
) -> list[str]:
    if not isinstance(values, list):
        raise ValueError("evidence_section_keys must be a JSON array")

    canonicalized: list[str] = []
    invalid: list[str] = []
    for item in values:
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            candidate = str(int(item))
        elif isinstance(item, str):
            candidate = item.strip()
        else:
            invalid.append(str(item))
            continue

        if candidate not in allowed_values:
            invalid.append(candidate)
            continue
        if candidate not in canonicalized:
            canonicalized.append(candidate)

    if invalid:
        raise ValueError(
            "evidence_section_keys contains unsupported values: "
            + ", ".join(sorted(set(invalid)))
        )

    return canonicalized


def is_evidence_key_validation_error(exc: Exception) -> bool:
    message = str(exc)
    return message.startswith("evidence_section_keys")


def validate_review_result(
    parsed: dict[str, Any],
    *,
    allowed_evidence_keys: set[str],
    allowed_policy_tenets: Sequence[str],
) -> dict[str, Any]:
    decision = parsed.get("decision")
    if decision not in ALLOWED_DECISIONS:
        raise ValueError("decision must be one of the allowed labels")

    rationale = parsed.get("rationale")
    if not isinstance(rationale, str) or not rationale.strip():
        raise ValueError("rationale must be a non-empty string")

    operational_relevance_score = coerce_integer_score(
        "operational_relevance_score",
        parsed.get("operational_relevance_score"),
    )
    prosperity_alignment_score = coerce_integer_score(
        "prosperity_alignment_score",
        parsed.get("prosperity_alignment_score"),
    )
    administrative_burden_score = coerce_integer_score(
        "administrative_burden_score",
        parsed.get("administrative_burden_score"),
    )
    repeal_risk_score = coerce_integer_score(
        "repeal_risk_score",
        parsed.get("repeal_risk_score"),
    )
    operational_status_assessment = coerce_qualitative_assessment(
        "operational_status_assessment",
        parsed.get("operational_status_assessment"),
    )
    obsolescence_evidence = coerce_qualitative_assessment(
        "obsolescence_evidence",
        parsed.get("obsolescence_evidence"),
    )
    administrative_burden_evidence = coerce_qualitative_assessment(
        "administrative_burden_evidence",
        parsed.get("administrative_burden_evidence"),
    )
    repeal_risk_assessment = coerce_qualitative_assessment(
        "repeal_risk_assessment",
        parsed.get("repeal_risk_assessment"),
    )
    evidence_sufficiency = coerce_qualitative_assessment(
        "evidence_sufficiency",
        parsed.get("evidence_sufficiency"),
    )
    policy_tenet_alignment_clarity = coerce_qualitative_assessment(
        "policy_tenet_alignment_clarity",
        parsed.get("policy_tenet_alignment_clarity"),
    )

    prosperity_tenets_used = canonicalize_policy_tenets(
        parsed.get("prosperity_tenets_used"),
        allowed_values=allowed_policy_tenets,
    )
    evidence_section_keys = canonicalize_evidence_section_keys(
        parsed.get("evidence_section_keys"),
        allowed_values=allowed_evidence_keys,
    )

    return {
        "decision": decision,
        "rationale": rationale.strip(),
        "operational_relevance_score": operational_relevance_score,
        "prosperity_alignment_score": prosperity_alignment_score,
        "administrative_burden_score": administrative_burden_score,
        "repeal_risk_score": repeal_risk_score,
        "operational_status_assessment": operational_status_assessment,
        "obsolescence_evidence": obsolescence_evidence,
        "administrative_burden_evidence": administrative_burden_evidence,
        "repeal_risk_assessment": repeal_risk_assessment,
        "evidence_sufficiency": evidence_sufficiency,
        "policy_tenet_alignment_clarity": policy_tenet_alignment_clarity,
        "prosperity_tenets_used": prosperity_tenets_used,
        "evidence_section_keys": evidence_section_keys,
    }


def get_retry_delay_seconds(exc: urllib.error.HTTPError, attempt: int) -> float:
    retry_after = exc.headers.get("retry-after")
    if retry_after:
        try:
            retry_after_seconds = float(retry_after)
            if retry_after_seconds > 0:
                return retry_after_seconds
        except ValueError:
            pass

    return CLAUDE_RATE_LIMIT_COOLDOWN_SECONDS * (
        CLAUDE_RATE_LIMIT_BACKOFF_MULTIPLIER ** attempt
    )


def call_claude_review(
    prompt: str,
    config: ReviewConfig,
) -> tuple[str, str]:
    global CLAUDE_COOLDOWN_UNTIL

    payload = json.dumps(
        {
            "model": config.model,
            "max_tokens": config.max_tokens,
            "temperature": 0,
            "system": (
                "You review Canadian federal laws for internal policy-analysis prototypes. "
                "Return JSON only."
            ),
            "messages": [{"role": "user", "content": prompt}],
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        config.api_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": config.api_key,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )

    response_body: dict[str, Any] | None = None
    for attempt in range(CLAUDE_RATE_LIMIT_MAX_RETRIES + 1):
        remaining_cooldown = CLAUDE_COOLDOWN_UNTIL - time.monotonic()
        if remaining_cooldown > 0:
            time.sleep(remaining_cooldown)

        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                response_body = json.loads(response.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as exc:
            try:
                error_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                error_body = None

            is_rate_limited = (
                exc.code == 429
                or classifier.parse_claude_error_type(error_body) == "rate_limit_error"
            )
            if is_rate_limited and attempt < CLAUDE_RATE_LIMIT_MAX_RETRIES:
                retry_delay = get_retry_delay_seconds(exc, attempt)
                CLAUDE_COOLDOWN_UNTIL = max(CLAUDE_COOLDOWN_UNTIL, time.monotonic() + retry_delay)
                print(
                    "Claude rate limit hit. "
                    f"Cooling down for {retry_delay:.1f}s before retry {attempt + 1}/"
                    f"{CLAUDE_RATE_LIMIT_MAX_RETRIES}.",
                    flush=True,
                )
                continue

            if error_body:
                raise RuntimeError(error_body) from exc
            raise RuntimeError(f"Claude request failed with HTTP {exc.code}") from exc
        except Exception as exc:
            raise RuntimeError(f"Claude request failed: {exc}") from exc

    if response_body is None:
        raise RuntimeError("Claude request failed without a response body")

    raw_text = extract_response_text(response_body)
    if not raw_text:
        raise RuntimeError("Claude returned an empty response")

    return raw_text, json.dumps(response_body, ensure_ascii=False)


def write_output(rows: list[tuple[Any, ...]], output_path: Path) -> None:
    import duckdb

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_name(f"{output_path.stem}.tmp.parquet")

    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE reviews (
                mandate_id VARCHAR,
                document_id VARCHAR,
                title_en VARCHAR,
                citation_en VARCHAR,
                primary_domain VARCHAR,
                decision VARCHAR,
                decision_confidence DOUBLE,
                evidence_sufficiency_score DOUBLE,
                rationale VARCHAR,
                evidence_section_keys VARCHAR,
                operational_relevance_score INTEGER,
                prosperity_alignment_score INTEGER,
                administrative_burden_score INTEGER,
                repeal_risk_score INTEGER,
                operational_status_assessment VARCHAR,
                obsolescence_evidence VARCHAR,
                administrative_burden_evidence VARCHAR,
                repeal_risk_assessment VARCHAR,
                evidence_sufficiency VARCHAR,
                policy_tenet_alignment_clarity VARCHAR,
                prosperity_tenets_used VARCHAR,
                review_model VARCHAR,
                raw_response_json VARCHAR
            )
            """
        )
        if rows:
            con.executemany(
                "INSERT INTO reviews VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        delete_if_exists(temp_output_path)
        delete_if_exists(output_path)
        con.execute("COPY reviews TO ? (FORMAT PARQUET)", [str(temp_output_path)])
        temp_output_path.replace(output_path)
    finally:
        con.close()
        delete_if_exists(temp_output_path)


def export_frontend_checkpoint(
    *,
    frontend_rows: Sequence[frontend_export.ReviewRow],
    export_config: FrontendExportConfig,
    pipeline_status: str,
) -> None:
    frontend_export.export_frontend_payloads(
        frontend_rows,
        summary_output_path=export_config.summary_output_path,
        details_output_path=export_config.details_output_path,
        total_count=export_config.total_count,
        daily_capacity=export_config.daily_capacity,
        last_updated=frontend_export.get_last_updated_iso(),
        pipeline_status=pipeline_status,
        r2_publish_config=export_config.r2_publish_config,
    )


def print_decision_counts(counter: Counter[str]) -> None:
    print("Counts by decision:")
    for decision, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {decision}: {count}")


def print_average_confidence_by_decision(
    confidence_totals: dict[str, float],
    decision_counter: Counter[str],
) -> None:
    print("Average confidence by decision:")
    for decision, count in sorted(decision_counter.items(), key=lambda item: item[0]):
        if count == 0:
            continue
        average_confidence = confidence_totals[decision] / count
        print(f"  {decision}: {average_confidence:.3f}")


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

    input_path = args.input_path
    output_path = args.output_path or build_default_output_path(input_path)
    journal_path = build_review_journal_path(output_path)
    manifest_path = build_review_manifest_path(output_path)
    lock_path = build_review_lock_path(output_path)
    limit = args.limit
    try:
        frontend_export_config = build_frontend_export_config(args)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    classifier.load_env_file(Path(".env"))

    lock = ReviewRunLock(
        lock_path=lock_path,
        metadata={
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "startedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "inputPath": str(input_path.resolve(strict=False)),
            "outputPath": str(output_path.resolve(strict=False)),
        },
    )
    try:
        lock.acquire()
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    try:
        try:
            mandate = load_mandate(MANDATE_PATH)
            review_config = get_review_config()
            review_inputs = load_review_inputs(input_path, limit)
            assert_review_inputs_match_mandate(review_inputs, mandate["mandate_id"])
            current_manifest = build_review_run_manifest(
                input_path=input_path,
                output_path=output_path,
                journal_path=journal_path,
                review_inputs=review_inputs,
                mandate_id=mandate["mandate_id"],
                review_model=review_config.model,
            )
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

        total_rows = len(review_inputs)
        if args.no_resume:
            delete_if_exists(output_path)
            delete_if_exists(journal_path)
            delete_if_exists(manifest_path)
            write_review_run_manifest(manifest_path, current_manifest)
            resumed_records: list[CompletedReviewRecord] = []
        else:
            existing_state_present = output_path.exists() or journal_path.exists()
            try:
                ensure_resume_manifest_compatible(
                    manifest_path=manifest_path,
                    current_manifest=current_manifest,
                    existing_state_present=existing_state_present,
                )
                resumed_records = hydrate_resume_records(
                    review_inputs=review_inputs,
                    output_path=output_path,
                    journal_path=journal_path,
                )
            except RuntimeError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1

        review_scope = "full dataset" if limit is None else f"limit={limit}"
        print(
            "Review run starting: "
            f"scope={review_scope}, rows={total_rows}, model={review_config.model}",
            flush=True,
        )
        print(f"Input: {input_path}", flush=True)
        print(f"Output: {output_path}", flush=True)
        print(f"Resume journal: {journal_path}", flush=True)
        print(f"Run manifest: {manifest_path}", flush=True)
        print(f"Run lock: {lock_path}", flush=True)
        print(f"Total rows selected for review: {total_rows}", flush=True)
        print(
            f"Checkpoint cadence: every {args.checkpoint_every} successful reviews",
            flush=True,
        )
        print(
            f"Resume mode: {'disabled' if args.no_resume else 'enabled'}",
            flush=True,
        )
        if frontend_export_config is not None:
            print(
                "Live frontend export: "
                f"{frontend_export_config.summary_output_path} | "
                f"{frontend_export_config.details_output_path} | "
                f"every {frontend_export_config.export_every} successful reviews",
                flush=True,
            )
            if frontend_export_config.r2_publish_config is not None:
                print(
                    "Live frontend R2 publish: "
                    f"{frontend_export_config.r2_publish_config.bucket_name} | "
                    f"{frontend_export_config.r2_publish_config.summary_object_key} | "
                    f"{frontend_export_config.r2_publish_config.details_object_key}",
                    flush=True,
                )

        resumed_document_ids = {record.document_id for record in resumed_records}
        pending_review_inputs = [
            row for row in review_inputs if row.document_id not in resumed_document_ids
        ]
        resumed_count = len(resumed_records)
        if review_inputs:
            print(
                f"Document range: {review_inputs[0].document_id} -> {review_inputs[-1].document_id}",
                flush=True,
            )
        print(
            f"Progress updates: first {INITIAL_RESULT_PREVIEW_COUNT} results, then every {REVIEW_PROGRESS_EVERY} rows",
            flush=True,
        )
        print(f"Recovered completed reviews: {resumed_count}", flush=True)
        print(f"Remaining reviews to process: {len(pending_review_inputs)}", flush=True)

        completed_records = list(resumed_records)
        output_rows = [record.to_output_row() for record in completed_records]
        frontend_rows = [record.to_frontend_row() for record in completed_records]
        decision_counter: Counter[str] = Counter(record.decision for record in completed_records)
        confidence_totals: dict[str, float] = {}
        for record in completed_records:
            confidence_totals[record.decision] = (
                confidence_totals.get(record.decision, 0.0) + record.decision_confidence
            )
        failure_count = 0
        recovered_by_retry_count = 0
        run_started_at = time.monotonic()

        if frontend_export_config is not None:
            export_frontend_checkpoint(
                frontend_rows=frontend_rows,
                export_config=frontend_export_config,
                pipeline_status=(
                    "complete"
                    if resumed_count == total_rows and total_rows > 0
                    else "in_progress"
                ),
            )

        for run_index, row in enumerate(pending_review_inputs, start=1):
            try:
                allowed_evidence_key_list = parse_evidence_section_keys(row.evidence_section_keys)
                allowed_evidence_keys = set(allowed_evidence_key_list)
                prompt = build_review_prompt(
                    mandate,
                    row.review_text,
                    allowed_evidence_key_list,
                )
                raw_text, raw_response_json = call_claude_review(prompt, review_config)
                parsed = parse_json_response(raw_text)
                try:
                    validated = validate_review_result(
                        parsed,
                        allowed_evidence_keys=allowed_evidence_keys,
                        allowed_policy_tenets=mandate["policy_tenets"],
                    )
                except Exception as exc:
                    if not is_evidence_key_validation_error(exc):
                        raise

                    repair_prompt = build_evidence_key_repair_prompt(
                        raw_text,
                        allowed_evidence_key_list,
                    )
                    repaired_raw_text, repaired_raw_response_json = call_claude_review(
                        repair_prompt,
                        review_config,
                    )
                    repaired_parsed = parse_json_response(repaired_raw_text)
                    validated = validate_review_result(
                        repaired_parsed,
                        allowed_evidence_keys=allowed_evidence_keys,
                        allowed_policy_tenets=mandate["policy_tenets"],
                    )
                    raw_response_json = repaired_raw_response_json
                    recovered_by_retry_count += 1
            except Exception as exc:
                failure_count += 1
                print(f"Review failed for {row.document_id}: {exc}", file=sys.stderr, flush=True)
                continue

            evidence_sufficiency_score = compute_evidence_sufficiency_score(
                validated["evidence_sufficiency"]
            )
            decision_confidence = compute_decision_confidence(
                validated,
                evidence_sufficiency_score=evidence_sufficiency_score,
            )
            validated["evidence_sufficiency_score"] = evidence_sufficiency_score
            validated["decision_confidence"] = decision_confidence

            completed_record = build_completed_review_record(
                row=row,
                validated=validated,
                review_model=review_config.model,
                raw_response_json=raw_response_json,
            )
            append_review_journal_record(journal_path, completed_record)

            completed_records.append(completed_record)
            output_rows.append(completed_record.to_output_row())
            frontend_rows.append(completed_record.to_frontend_row())
            decision_counter[completed_record.decision] += 1
            confidence_totals[completed_record.decision] = (
                confidence_totals.get(completed_record.decision, 0.0)
                + completed_record.decision_confidence
            )

            elapsed_seconds = time.monotonic() - run_started_at
            processed_count = run_index
            overall_processed_count = resumed_count + run_index
            rows_per_minute = (
                (processed_count / elapsed_seconds) * 60.0 if elapsed_seconds > 0 else 0.0
            )
            remaining_rows = len(pending_review_inputs) - processed_count
            eta_seconds = (
                (remaining_rows / processed_count) * elapsed_seconds
                if processed_count > 0 and elapsed_seconds > 0
                else 0.0
            )

            should_print_result_preview = run_index <= INITIAL_RESULT_PREVIEW_COUNT
            should_print_progress = (
                overall_processed_count == total_rows or run_index % REVIEW_PROGRESS_EVERY == 0
            )
            should_write_checkpoint = (
                run_index == 1
                or len(completed_records) == 1
                or len(completed_records) == total_rows
                or len(completed_records) % args.checkpoint_every == 0
            )
            should_refresh_frontend = (
                frontend_export_config is not None
                and (
                    run_index == 1
                    or len(completed_records) == total_rows
                    or len(completed_records) == 1
                    or len(completed_records) % frontend_export_config.export_every == 0
                )
            )

            if should_write_checkpoint or should_refresh_frontend:
                write_output(output_rows, output_path)
            if should_refresh_frontend:
                export_frontend_checkpoint(
                    frontend_rows=frontend_rows,
                    export_config=frontend_export_config,
                    pipeline_status="in_progress",
                )
                print(
                    f"Live dashboard artifacts refreshed at {len(completed_records)} reviewed rows.",
                    flush=True,
                )

            if should_print_result_preview:
                print(
                    f"Completed {overall_processed_count}/{total_rows}: "
                    f"{row.document_id} -> {completed_record.decision} "
                    f"(confidence={completed_record.decision_confidence:.3f}, "
                    f"elapsed={format_duration(elapsed_seconds)})",
                    flush=True,
                )

            if should_print_progress:
                print(
                    f"Progress {overall_processed_count}/{total_rows} | resumed={resumed_count} "
                    f"successful={len(completed_records)} "
                    f"failed={failure_count} recovered={recovered_by_retry_count} | "
                    f"elapsed={format_duration(elapsed_seconds)} "
                    f"rate={rows_per_minute:.1f}/min "
                    f"eta={format_duration(eta_seconds)}",
                    flush=True,
                )
                print(
                    "Decision counts so far: "
                    f"{format_decision_counts_inline(decision_counter)}",
                    flush=True,
                )
                print(
                    f"Last result: {row.document_id} -> {completed_record.decision} "
                    f"(confidence={completed_record.decision_confidence:.3f})",
                    flush=True,
                )

        write_output(output_rows, output_path)
        if frontend_export_config is not None:
            export_frontend_checkpoint(
                frontend_rows=frontend_rows,
                export_config=frontend_export_config,
                pipeline_status="complete" if failure_count == 0 else "error",
            )
        write_review_run_manifest(manifest_path, current_manifest)

        print(f"Total reviewed successfully: {len(completed_records)}")
        print(f"Failures skipped: {failure_count}")
        print(f"Failures recovered by retry: {recovered_by_retry_count}")
        print_decision_counts(decision_counter)
        print_average_confidence_by_decision(confidence_totals, decision_counter)
        print(f"Output written to: {output_path}")
        print(f"Resume journal written to: {journal_path}")
        print(f"Run manifest written to: {manifest_path}")
        return 0
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
