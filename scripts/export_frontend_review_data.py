from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


DEFAULT_SUMMARY_OUTPUT_PATH = Path(r"src/data/review-summary.json")
DEFAULT_DETAILS_OUTPUT_PATH = Path(r"src/data/review-details.json")
DEFAULT_TOTAL_COUNT = 5796
DEFAULT_DAILY_CAPACITY = 200
DEFAULT_PIPELINE_STATUS = "complete"
DECISION_LABELS = ("retain", "amend", "repeal_candidate", "escalate")


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


def ensure_directory(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


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
		default=DEFAULT_TOTAL_COUNT,
		help="Known total corpus size for progress calculations.",
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
) -> dict[str, Any]:
	summary_payload = build_summary_payload(
		review_rows,
		total_count=total_count,
		daily_capacity=daily_capacity,
		last_updated=last_updated,
		pipeline_status=pipeline_status,
	)
	details_payload = build_details_payload(review_rows)

	write_json(summary_output_path, summary_payload)
	write_json(details_output_path, details_payload)
	return summary_payload


def main(argv: Sequence[str] | None = None) -> int:
	if hasattr(sys.stdout, "reconfigure"):
		sys.stdout.reconfigure(encoding="utf-8")
	if hasattr(sys.stderr, "reconfigure"):
		sys.stderr.reconfigure(encoding="utf-8")

	args = parse_args(argv)
	if args.total_count <= 0:
		print("Error: --total-count must be a positive integer.", file=sys.stderr)
		return 1
	if args.daily_capacity <= 0:
		print("Error: --daily-capacity must be a positive integer.", file=sys.stderr)
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

	summary_payload = export_frontend_payloads(
		review_rows,
		summary_output_path=args.summary_output_path,
		details_output_path=args.details_output_path,
		total_count=args.total_count,
		daily_capacity=args.daily_capacity,
		last_updated=get_last_updated_iso(
			review_output_path=review_output_path,
			override=args.last_updated,
		),
		pipeline_status=args.pipeline_status,
	)

	print(f"Reviewed rows exported: {len(review_rows)}")
	print(f"Summary output: {args.summary_output_path}")
	print(f"Details output: {args.details_output_path}")
	print("Decision counts:")
	for decision in DECISION_LABELS:
		print(f"  {decision}: {summary_payload['decisionCounts'][decision]}")

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
