from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

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


def build_frontend_review_row(
    *,
    row: ReviewInputRow,
    validated: dict[str, Any],
    review_model: str,
) -> frontend_export.ReviewRow:
    return frontend_export.ReviewRow(
        document_id=row.document_id,
        title_en=row.title_en,
        citation_en=row.citation_en,
        decision=validated["decision"],
        decision_confidence=validated["decision_confidence"],
        rationale=validated["rationale"],
        evidence_section_keys=list(validated["evidence_section_keys"]),
        operational_relevance_score=validated["operational_relevance_score"],
        prosperity_alignment_score=validated["prosperity_alignment_score"],
        administrative_burden_score=validated["administrative_burden_score"],
        repeal_risk_score=validated["repeal_risk_score"],
        review_model=review_model,
    )


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

    input_path = args.input_path
    output_path = args.output_path or build_default_output_path(input_path)
    limit = args.limit
    try:
        frontend_export_config = build_frontend_export_config(args)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    classifier.load_env_file(Path(".env"))

    try:
        mandate = load_mandate(MANDATE_PATH)
        review_config = get_review_config()
        review_inputs = load_review_inputs(input_path, limit)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    total_rows = len(review_inputs)
    review_scope = "full dataset" if limit is None else f"limit={limit}"
    print(
        "Review run starting: "
        f"scope={review_scope}, rows={total_rows}, model={review_config.model}",
        flush=True,
    )
    print(f"Input: {input_path}", flush=True)
    print(f"Output: {output_path}", flush=True)
    print(f"Total rows selected for review: {total_rows}", flush=True)
    if frontend_export_config is not None:
        print(
            "Live frontend export: "
            f"{frontend_export_config.summary_output_path} | "
            f"{frontend_export_config.details_output_path} | "
            f"every {frontend_export_config.export_every} successful reviews",
            flush=True,
        )
    if review_inputs:
        print(
            f"Document range: {review_inputs[0].document_id} -> {review_inputs[-1].document_id}",
            flush=True,
        )
    print(
        f"Progress updates: first {INITIAL_RESULT_PREVIEW_COUNT} results, then every {REVIEW_PROGRESS_EVERY} rows",
        flush=True,
    )

    output_rows: list[tuple[Any, ...]] = []
    frontend_rows: list[frontend_export.ReviewRow] = []
    decision_counter: Counter[str] = Counter()
    confidence_totals: dict[str, float] = {}
    failure_count = 0
    recovered_by_retry_count = 0
    run_started_at = time.monotonic()

    if frontend_export_config is not None:
        export_frontend_checkpoint(
            frontend_rows=[],
            export_config=frontend_export_config,
            pipeline_status="in_progress",
        )

    for index, row in enumerate(review_inputs, start=1):
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

        decision_counter[validated["decision"]] += 1
        confidence_totals[validated["decision"]] = (
            confidence_totals.get(validated["decision"], 0.0)
            + validated["decision_confidence"]
        )
        elapsed_seconds = time.monotonic() - run_started_at
        processed_count = index
        rows_per_minute = (
            (processed_count / elapsed_seconds) * 60.0 if elapsed_seconds > 0 else 0.0
        )
        remaining_rows = total_rows - processed_count
        eta_seconds = (
            (remaining_rows / processed_count) * elapsed_seconds
            if processed_count > 0 and elapsed_seconds > 0
            else 0.0
        )
        output_rows.append(
            (
                row.mandate_id,
                row.document_id,
                row.title_en,
                row.citation_en,
                row.primary_domain,
                validated["decision"],
                validated["decision_confidence"],
                validated["evidence_sufficiency_score"],
                validated["rationale"],
                json.dumps(validated["evidence_section_keys"], ensure_ascii=False),
                validated["operational_relevance_score"],
                validated["prosperity_alignment_score"],
                validated["administrative_burden_score"],
                validated["repeal_risk_score"],
                validated["operational_status_assessment"],
                validated["obsolescence_evidence"],
                validated["administrative_burden_evidence"],
                validated["repeal_risk_assessment"],
                validated["evidence_sufficiency"],
                validated["policy_tenet_alignment_clarity"],
                json.dumps(validated["prosperity_tenets_used"], ensure_ascii=False),
                review_config.model,
                raw_response_json,
            )
        )
        frontend_rows.append(
            build_frontend_review_row(
                row=row,
                validated=validated,
                review_model=review_config.model,
            )
        )

        should_print_result_preview = index <= INITIAL_RESULT_PREVIEW_COUNT
        should_print_progress = (
            index == total_rows or index % REVIEW_PROGRESS_EVERY == 0
        )
        should_checkpoint = (
            frontend_export_config is not None
            and (
                len(output_rows) == 1
                or len(output_rows) == total_rows
                or len(output_rows) % frontend_export_config.export_every == 0
            )
        )

        if should_checkpoint:
            write_output(output_rows, output_path)
            export_frontend_checkpoint(
                frontend_rows=frontend_rows,
                export_config=frontend_export_config,
                pipeline_status="in_progress",
            )
            print(
                f"Live dashboard artifacts refreshed at {len(output_rows)} reviewed rows.",
                flush=True,
            )

        if should_print_result_preview:
            print(
                f"Completed {index}/{total_rows}: {row.document_id} -> {validated['decision']} "
                f"(confidence={validated['decision_confidence']:.3f}, "
                f"elapsed={format_duration(elapsed_seconds)})",
                flush=True,
            )

        if should_print_progress:
            print(
                f"Progress {index}/{total_rows} | successful={len(output_rows)} "
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
                f"Last result: {row.document_id} -> {validated['decision']} "
                f"(confidence={validated['decision_confidence']:.3f})",
                flush=True,
            )

    write_output(output_rows, output_path)
    if frontend_export_config is not None:
        export_frontend_checkpoint(
            frontend_rows=frontend_rows,
            export_config=frontend_export_config,
            pipeline_status="complete" if failure_count == 0 else "error",
        )

    print(f"Total reviewed successfully: {len(output_rows)}")
    print(f"Failures skipped: {failure_count}")
    print(f"Failures recovered by retry: {recovered_by_retry_count}")
    print_decision_counts(decision_counter)
    print_average_confidence_by_decision(confidence_totals, decision_counter)
    print(f"Output written to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
