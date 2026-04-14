from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from env_utils import derive_total_document_count, load_project_env


load_project_env()


DEFAULT_SUMMARY_OUTPUT_PATH = Path(r"src/data/review-summary.json")
DEFAULT_DETAILS_OUTPUT_PATH = Path(r"src/data/review-details.json")
DEFAULT_DAILY_CAPACITY = 200
DEFAULT_PIPELINE_STATUS = "complete"
DECISION_LABELS = ("retain", "amend", "repeal_candidate", "escalate")
DEFAULT_R2_SUMMARY_OBJECT_KEY = "review-summary.json"
DEFAULT_R2_DETAILS_OBJECT_KEY = "review-details.json"


@dataclass(frozen=True)
class ReviewRow:
	document_id: str
	title_en: str
	citation_en: str | None
	decision: str
	decision_confidence: float
	rationale: str
	evidence_section_keys: list[str]
	operational_relevance_score: int
	prosperity_alignment_score: int
	administrative_burden_score: int
	repeal_risk_score: int
	review_model: str


@dataclass(frozen=True)
class R2PublishConfig:
	bucket_name: str
	endpoint_url: str
	access_key_id: str
	secret_access_key: str
	summary_object_key: str
	details_object_key: str


def review_row_from_detail_payload(payload: dict[str, Any]) -> ReviewRow:
	return ReviewRow(
		document_id=str(payload["documentId"]),
		title_en=str(payload["titleEn"]),
		citation_en=(
			str(payload["citationEn"]) if payload.get("citationEn") is not None else None
		),
		decision=str(payload["decision"]),
		decision_confidence=float(payload["decisionConfidence"]),
		rationale=str(payload["rationale"]),
		evidence_section_keys=[
			str(item) for item in (payload.get("evidenceSectionKeys") or [])
		],
		operational_relevance_score=int(payload["operationalRelevanceScore"]),
		prosperity_alignment_score=int(payload["prosperityAlignmentScore"]),
		administrative_burden_score=int(payload["administrativeBurdenScore"]),
		repeal_risk_score=int(payload["repealRiskScore"]),
		review_model=str(payload["reviewModel"]),
	)


def ensure_directory(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


def build_r2_publish_config(
	args: argparse.Namespace | None = None,
) -> R2PublishConfig | None:
	bucket_name = (
		getattr(args, "r2_bucket_name", None)
		or os.getenv("CLOUDFLARE_R2_BUCKET")
	)
	if not bucket_name:
		return None

	endpoint_url = (
		getattr(args, "r2_endpoint_url", None)
		or os.getenv("CLOUDFLARE_R2_ENDPOINT")
	)
	access_key_id = (
		getattr(args, "r2_access_key_id", None)
		or os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID")
	)
	secret_access_key = (
		getattr(args, "r2_secret_access_key", None)
		or os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY")
	)
	summary_object_key = (
		getattr(args, "r2_summary_object_key", None)
		or os.getenv("CLOUDFLARE_R2_SUMMARY_KEY")
		or DEFAULT_R2_SUMMARY_OBJECT_KEY
	)
	details_object_key = (
		getattr(args, "r2_details_object_key", None)
		or os.getenv("CLOUDFLARE_R2_DETAILS_KEY")
		or DEFAULT_R2_DETAILS_OBJECT_KEY
	)

	if not endpoint_url:
		account_id = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
		if account_id:
			endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

	missing = []
	if not endpoint_url:
		missing.append("endpoint")
	if not access_key_id:
		missing.append("access key id")
	if not secret_access_key:
		missing.append("secret access key")
	if missing:
		raise RuntimeError(
			"Cloudflare R2 publishing is enabled but missing: "
			+ ", ".join(missing)
			+ ". Set CLOUDFLARE_R2_* environment variables or pass the --r2-* flags."
		)

	return R2PublishConfig(
		bucket_name=bucket_name,
		endpoint_url=endpoint_url,
		access_key_id=access_key_id,
		secret_access_key=secret_access_key,
		summary_object_key=summary_object_key,
		details_object_key=details_object_key,
	)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Export frontend-ready review summary and drilldown JSON artifacts "
			"from a review parquet."
		)
	)
	parser.add_argument(
		"--review-output-path",
		required=True,
		type=Path,
		help="Path to the review parquet output.",
	)
	parser.add_argument(
		"--summary-output-path",
		type=Path,
		default=DEFAULT_SUMMARY_OUTPUT_PATH,
		help="Path to the frontend summary JSON.",
	)
	parser.add_argument(
		"--details-output-path",
		type=Path,
		default=DEFAULT_DETAILS_OUTPUT_PATH,
		help="Path to the frontend drilldown JSON.",
	)
	parser.add_argument(
		"--total-count",
		type=int,
		help=(
			"Optional explicit total corpus size for progress calculations. "
			"Defaults to the current documents_en.parquet row count."
		),
	)
	parser.add_argument(
		"--daily-capacity",
		type=int,
		default=DEFAULT_DAILY_CAPACITY,
		help="Daily review capacity to include in the summary JSON.",
	)
	parser.add_argument(
		"--pipeline-status",
		default=DEFAULT_PIPELINE_STATUS,
		choices=("in_progress", "idle", "complete", "error"),
		help="Pipeline status to include in the summary JSON.",
	)
	parser.add_argument(
		"--last-updated",
		help="Optional ISO timestamp to include in the summary JSON.",
	)
	parser.add_argument(
		"--r2-bucket-name",
		help="Optional Cloudflare R2 bucket name for remote dashboard publishing.",
	)
	parser.add_argument(
		"--r2-endpoint-url",
		help="Optional Cloudflare R2 S3 endpoint URL.",
	)
	parser.add_argument(
		"--r2-access-key-id",
		help="Optional Cloudflare R2 access key ID.",
	)
	parser.add_argument(
		"--r2-secret-access-key",
		help="Optional Cloudflare R2 secret access key.",
	)
	parser.add_argument(
		"--r2-summary-object-key",
		default=DEFAULT_R2_SUMMARY_OBJECT_KEY,
		help="Object key for the summary JSON in Cloudflare R2.",
	)
	parser.add_argument(
		"--r2-details-object-key",
		default=DEFAULT_R2_DETAILS_OBJECT_KEY,
		help="Object key for the drilldown JSON in Cloudflare R2.",
	)
	return parser.parse_args(argv)


def load_review_rows(review_output_path: Path) -> list[ReviewRow]:
	try:
		import duckdb
	except ImportError as exc:
		raise RuntimeError(
			"duckdb is required. Install it with `pip install duckdb`."
		) from exc

	con = duckdb.connect()
	try:
		rows = con.execute(
			"""
			SELECT
				document_id,
				title_en,
				citation_en,
				decision,
				decision_confidence,
				rationale,
				evidence_section_keys,
				operational_relevance_score,
				prosperity_alignment_score,
				administrative_burden_score,
				repeal_risk_score,
				review_model
			FROM read_parquet(?)
			ORDER BY document_id
			""",
			[str(review_output_path)],
		).fetchall()
	finally:
		con.close()

	review_rows: list[ReviewRow] = []
	for row in rows:
		evidence_section_keys = json.loads(row[6]) if row[6] else []
		if not isinstance(evidence_section_keys, list):
			raise RuntimeError(
				f"Invalid evidence_section_keys payload for {row[0]} in {review_output_path}"
			)
		review_rows.append(
			ReviewRow(
				document_id=str(row[0]),
				title_en=str(row[1]),
				citation_en=str(row[2]) if row[2] is not None else None,
				decision=str(row[3]),
				decision_confidence=float(row[4]),
				rationale=str(row[5]),
				evidence_section_keys=[str(item) for item in evidence_section_keys],
				operational_relevance_score=int(row[7]),
				prosperity_alignment_score=int(row[8]),
				administrative_burden_score=int(row[9]),
				repeal_risk_score=int(row[10]),
				review_model=str(row[11]),
			)
		)

	return review_rows


def load_existing_local_details(details_output_path: Path) -> list[ReviewRow]:
	if not details_output_path.exists():
		return []

	payload = json.loads(details_output_path.read_text(encoding="utf-8"))
	if not isinstance(payload, list):
		raise RuntimeError(
			f"Existing frontend details payload at {details_output_path} is invalid."
		)

	return [review_row_from_detail_payload(item) for item in payload]


def build_s3_client(config: R2PublishConfig):
	try:
		import boto3
	except ImportError as exc:
		raise RuntimeError(
			"boto3 is required for Cloudflare R2 publishing. "
			"Install it with `pip install boto3`."
		) from exc

	return boto3.client(
		"s3",
		endpoint_url=config.endpoint_url,
		aws_access_key_id=config.access_key_id,
		aws_secret_access_key=config.secret_access_key,
		region_name="auto",
	)


def load_existing_r2_details(config: R2PublishConfig) -> list[ReviewRow]:
	client = build_s3_client(config)
	try:
		response = client.get_object(
			Bucket=config.bucket_name,
			Key=config.details_object_key,
		)
	except Exception as exc:
		error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
		if error_code in {"NoSuchKey", "404"}:
			return []
		raise

	payload = json.loads(response["Body"].read().decode("utf-8"))
	if not isinstance(payload, list):
		raise RuntimeError("Existing frontend details payload in R2 is invalid.")
	return [review_row_from_detail_payload(item) for item in payload]


def merge_review_rows(
	existing_rows: Sequence[ReviewRow],
	incoming_rows: Sequence[ReviewRow],
) -> list[ReviewRow]:
	merged_by_document_id: dict[str, ReviewRow] = {
		row.document_id: row for row in existing_rows
	}
	for row in incoming_rows:
		merged_by_document_id[row.document_id] = row

	return [
		merged_by_document_id[document_id]
		for document_id in sorted(merged_by_document_id)
	]
	

def load_existing_frontend_rows(
	*,
	details_output_path: Path,
	pipeline_status: str,
	r2_publish_config: R2PublishConfig | None,
) -> list[ReviewRow]:
	if pipeline_status == "idle":
		return []

	try:
		return load_existing_local_details(details_output_path)
	except RuntimeError:
		if r2_publish_config is None:
			raise

	if r2_publish_config is None:
		return []

	return load_existing_r2_details(r2_publish_config)


def build_summary_payload(
	review_rows: Sequence[ReviewRow],
	*,
	total_count: int,
	daily_capacity: int,
	last_updated: str,
	pipeline_status: str,
) -> dict[str, Any]:
	reviewed_count = len(review_rows)
	remaining_count = max(0, total_count - reviewed_count)
	decision_counts = {label: 0 for label in DECISION_LABELS}
	confidence_sums = {label: 0.0 for label in DECISION_LABELS}

	for row in review_rows:
		if row.decision not in decision_counts:
			continue
		decision_counts[row.decision] += 1
		confidence_sums[row.decision] += row.decision_confidence

	average_confidence_by_decision = {
		label: round(
			(confidence_sums[label] / decision_counts[label]) if decision_counts[label] else 0.0,
			3,
		)
		for label in DECISION_LABELS
	}

	return {
		"totalCount": total_count,
		"reviewedCount": reviewed_count,
		"remainingCount": remaining_count,
		"percentReviewed": round(
			(reviewed_count / total_count) * 100 if total_count else 0.0,
			2,
		),
		"dailyCapacity": daily_capacity,
		"lastUpdated": last_updated,
		"pipelineStatus": pipeline_status,
		"decisionCounts": decision_counts,
		"averageConfidenceByDecision": average_confidence_by_decision,
	}


def build_details_payload(review_rows: Sequence[ReviewRow]) -> list[dict[str, Any]]:
	return [
		{
			"documentId": row.document_id,
			"titleEn": row.title_en,
			"citationEn": row.citation_en,
			"decision": row.decision,
			"decisionConfidence": round(row.decision_confidence, 3),
			"rationale": row.rationale,
			"evidenceSectionKeys": row.evidence_section_keys,
			"operationalRelevanceScore": row.operational_relevance_score,
			"prosperityAlignmentScore": row.prosperity_alignment_score,
			"administrativeBurdenScore": row.administrative_burden_score,
			"repealRiskScore": row.repeal_risk_score,
			"reviewModel": row.review_model,
		}
		for row in review_rows
	]


def write_json(path: Path, payload: Any) -> None:
	ensure_directory(path.parent)
	temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
	temp_path.write_text(
		json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
		encoding="utf-8",
	)
	temp_path.replace(path)


def render_json_bytes(payload: Any) -> bytes:
	return (json.dumps(payload, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def upload_json_to_r2(
	*,
	config: R2PublishConfig,
	object_key: str,
	payload: Any,
) -> None:
	client = build_s3_client(config)
	client.put_object(
		Bucket=config.bucket_name,
		Key=object_key,
		Body=render_json_bytes(payload),
		ContentType="application/json; charset=utf-8",
		CacheControl="no-store, no-cache, must-revalidate, max-age=0",
	)


def get_last_updated_iso(
	*,
	review_output_path: Path | None = None,
	override: str | None = None,
) -> str:
	if override:
		return override

	if review_output_path is not None:
		last_updated_ns = review_output_path.stat().st_mtime_ns
		return (
			datetime.fromtimestamp(last_updated_ns / 1_000_000_000, tz=timezone.utc)
			.astimezone()
			.isoformat(timespec="seconds")
		)

	return datetime.now().astimezone().isoformat(timespec="seconds")


def export_frontend_payloads(
	review_rows: Sequence[ReviewRow],
	*,
	summary_output_path: Path,
	details_output_path: Path,
	total_count: int,
	daily_capacity: int,
	last_updated: str,
	pipeline_status: str,
	r2_publish_config: R2PublishConfig | None = None,
) -> dict[str, Any]:
	existing_rows = load_existing_frontend_rows(
		details_output_path=details_output_path,
		pipeline_status=pipeline_status,
		r2_publish_config=r2_publish_config,
	)
	merged_rows = merge_review_rows(existing_rows, review_rows)
	summary_payload = build_summary_payload(
		merged_rows,
		total_count=total_count,
		daily_capacity=daily_capacity,
		last_updated=last_updated,
		pipeline_status=pipeline_status,
	)
	details_payload = build_details_payload(merged_rows)

	write_json(details_output_path, details_payload)
	write_json(summary_output_path, summary_payload)

	if r2_publish_config is not None:
		upload_json_to_r2(
			config=r2_publish_config,
			object_key=r2_publish_config.details_object_key,
			payload=details_payload,
		)
		upload_json_to_r2(
			config=r2_publish_config,
			object_key=r2_publish_config.summary_object_key,
			payload=summary_payload,
		)
	return summary_payload


def resolve_total_count(total_count: int | None) -> int:
	if total_count is None:
		return derive_total_document_count()
	if total_count <= 0:
		raise RuntimeError("--total-count must be a positive integer.")
	return total_count


def main(argv: Sequence[str] | None = None) -> int:
	if hasattr(sys.stdout, "reconfigure"):
		sys.stdout.reconfigure(encoding="utf-8")
	if hasattr(sys.stderr, "reconfigure"):
		sys.stderr.reconfigure(encoding="utf-8")

	args = parse_args(argv)
	if args.daily_capacity <= 0:
		print("Error: --daily-capacity must be a positive integer.", file=sys.stderr)
		return 1

	try:
		r2_publish_config = build_r2_publish_config(args)
	except RuntimeError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		return 1

	review_output_path: Path = args.review_output_path
	if not review_output_path.exists():
		print(f"Error: input parquet not found at {review_output_path}", file=sys.stderr)
		return 1

	try:
		review_rows = load_review_rows(review_output_path)
	except RuntimeError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		return 1

	try:
		total_count = resolve_total_count(args.total_count)
	except RuntimeError as exc:
		print(f"Error: {exc}", file=sys.stderr)
		return 1

	summary_payload = export_frontend_payloads(
		review_rows,
		summary_output_path=args.summary_output_path,
		details_output_path=args.details_output_path,
		total_count=total_count,
		daily_capacity=args.daily_capacity,
		last_updated=get_last_updated_iso(
			review_output_path=review_output_path,
			override=args.last_updated,
		),
		pipeline_status=args.pipeline_status,
		r2_publish_config=r2_publish_config,
	)

	print(f"Reviewed rows exported: {len(review_rows)}")
	print(f"Summary output: {args.summary_output_path}")
	print(f"Details output: {args.details_output_path}")
	if r2_publish_config is not None:
		print(
			f"R2 publish target: {r2_publish_config.bucket_name} "
			f"({r2_publish_config.summary_object_key}, {r2_publish_config.details_object_key})"
		)
	print("Decision counts:")
	for decision in DECISION_LABELS:
		print(f"  {decision}: {summary_payload['decisionCounts'][decision]}")

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
