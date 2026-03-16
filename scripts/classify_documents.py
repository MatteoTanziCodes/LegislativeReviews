from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


INPUT_PATH = Path(
    r"E:\Programming\buildcanada\canadian-laws\processed\document_classifier_inputs_en.parquet"
)
OUTPUT_PATH = Path(r"E:\Programming\buildcanada\canadian-laws\processed\document_domains_en.parquet")

# Enriched title/citation-plus-sections inputs are longer and may need
# threshold calibration as the taxonomy or embedding model changes.
# Override via SEMANTIC_ACCEPT_THRESHOLD and SEMANTIC_MARGIN_THRESHOLD.
SEMANTIC_ACCEPT_THRESHOLD = 0.55
SEMANTIC_MARGIN_THRESHOLD = 0.03
DEFAULT_EMBEDDING_PROVIDER = "fastembed"
DEFAULT_EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
DEFAULT_EMBEDDING_BATCH_SIZE = 128
DEFAULT_FASTEMBED_THREADS = max(1, min(8, os.cpu_count() or 4))
EMBEDDING_PROGRESS_EVERY = 256
CLASSIFICATION_PROGRESS_EVERY = 250
DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_CLAUDE_MAX_TOKENS = 16
CLAUDE_RATE_LIMIT_MAX_RETRIES = 5
CLAUDE_RATE_LIMIT_COOLDOWN_SECONDS = 15.0
CLAUDE_RATE_LIMIT_BACKOFF_MULTIPLIER = 2.0
ANTHROPIC_VERSION = "2023-06-01"

CLAUDE_COOLDOWN_UNTIL = 0.0

TAXONOMY_PROTOTYPES: dict[str, list[str]] = {
    "governance_administrative": [
        "law about public service administration, civil service, and government employees",
        "order about oaths of office, official appointments, boards, commissions, or tribunals",
        "statute about machinery of government, ministries, agencies, and administrative procedure",
        "publication order, Canada Gazette notice, proclamation, or government record-keeping rule",
        "territorial administration or federal administration law not primarily about Indigenous rights",
    ],
    "criminal_public_safety": [
        "law about criminal offences, prosecutions, criminal procedure, or sentencing",
        "statute about policing, investigations, evidence, or courts",
        "regulation about corrections, prisons, parole, or law enforcement",
        "law about firearms, national security, emergency powers, or border enforcement",
    ],
    "labor_employment": [
        "law about labour relations, trade unions, and collective bargaining",
        "statute about employment standards, wages, hours of work, or workplace conditions",
        "regulation about occupational health and safety or workers compensation",
        "law about public service employment, workplace pensions, or labour disputes",
    ],
    "business_commerce": [
        "law about corporations, company governance, or commercial entities",
        "statute about insolvency, bankruptcy, receivership, or secured transactions",
        "regulation about competition, trade, or market conduct",
        "law about intellectual property, patents, copyrights, or trademarks",
        "commercial communications, consumer commerce, or business licensing law",
    ],
    "tax_finance": [
        "law about income tax, excise tax, customs duties, tariffs, or public revenue",
        "statute about banking, insurance, pensions regulation, or financial institutions",
        "regulation about federal finance, public money, or fiscal administration",
        "law about securities, monetary policy, or financial reporting",
    ],
    "transport_infrastructure": [
        "law about airports, aviation safety, air navigation, or aircraft operations",
        "statute about railways, railway safety, or rail transport infrastructure",
        "regulation about shipping, marine transport, ports, harbours, or navigation",
        "law about dangerous goods transportation, transport safety, or transportation systems",
        "statute about roads, bridges, tunnels, ferries, or transport infrastructure",
    ],
    "environment_resources": [
        "law about toxic substances, chemicals, or hazardous environmental contaminants",
        "regulation about asbestos, hazardous materials, waste, or pollution control",
        "statute about emissions, environmental protection, or pollution prevention",
        "law about wildlife, fisheries, oceans, parks, or conservation",
        "regulation about mining, energy, water, forestry, or natural resource management",
    ],
    "rights_privacy_access": [
        "law about privacy, personal information, or data protection",
        "statute about access to information, records access, or government transparency",
        "law about human rights, discrimination, equality, or civil liberties",
        "statute about accessibility, disability access, or accommodation rights",
        "law about language rights or other individual rights to fair treatment and access",
    ],
    # Keep this intentionally narrow so general territorial administration,
    # land/resource regulation, or ordinary government orders do not drift here.
    "indigenous_crown_relations": [
        "Indian Act style law about Indian status, registration, bands, and reserve administration",
        "statute about First Nations bands, reserve lands, or band governance",
        "law implementing treaty rights, specific claims, or land claims agreements",
        "statute about Indigenous self-government or Crown-Indigenous relations",
        "law about Inuit governance, Inuit land claims, or Inuit rights",
        "law about Metis governance, Metis rights, or Metis self-government",
    ],
    "health_social_services": [
        "law about public health, disease control, or health administration",
        "statute about medicines, food safety, or medical products",
        "law about disability supports, income support, or social benefits",
        "statute about veterans benefits, family services, or social service programs",
    ],
    "other": [
        "miscellaneous statute that does not clearly fit the main legal domains",
        "technical or cross-cutting law with no dominant subject matter",
        "ceremonial, transitional, or hard-to-classify regulation",
    ],
}


@dataclass(frozen=True)
class EmbeddingConfig:
    provider: str
    model_name: str
    batch_size: int
    fastembed_threads: int
    cache_dir: str | None
    openai_api_key: str | None
    openai_base_url: str


@dataclass(frozen=True)
class ClaudeConfig:
    api_key: str | None
    model: str
    api_url: str
    max_tokens: int


@dataclass(frozen=True)
class DocumentRow:
    document_id: str
    title_en: str
    citation_en: str | None
    classifier_input_text: str | None


@dataclass(frozen=True)
class SemanticMatch:
    label: str
    similarity: float
    description: str
    second_best_label: str
    second_best_similarity: float
    score_margin: float


@dataclass(frozen=True)
class PrototypeRow:
    label: str
    text: str


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_taxonomy_prototypes() -> dict[str, list[str]]:
    return {label: list(prototypes) for label, prototypes in TAXONOMY_PROTOTYPES.items()}


def build_taxonomy_descriptions(
    taxonomy_prototypes: dict[str, list[str]],
) -> dict[str, str]:
    return {
        label: "; ".join(prototypes)
        for label, prototypes in taxonomy_prototypes.items()
    }


def flatten_taxonomy_prototypes(
    taxonomy_prototypes: dict[str, list[str]],
) -> list[PrototypeRow]:
    prototype_rows: list[PrototypeRow] = []
    for label, prototypes in taxonomy_prototypes.items():
        for prototype_text in prototypes:
            prototype_rows.append(PrototypeRow(label=label, text=prototype_text))
    return prototype_rows


def delete_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def get_embedding_config() -> EmbeddingConfig:
    provider = os.getenv("EMBEDDING_PROVIDER", DEFAULT_EMBEDDING_PROVIDER).strip() or DEFAULT_EMBEDDING_PROVIDER
    model_name = os.getenv("EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL).strip() or DEFAULT_EMBEDDING_MODEL
    batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE", str(DEFAULT_EMBEDDING_BATCH_SIZE)))
    fastembed_threads = int(os.getenv("FASTEMBED_THREADS", str(DEFAULT_FASTEMBED_THREADS)))
    cache_dir = os.getenv("EMBEDDING_CACHE_DIR") or None
    openai_api_key = os.getenv("OPENAI_API_KEY") or None
    openai_base_url = os.getenv("OPENAI_EMBEDDING_BASE_URL", "https://api.openai.com/v1/embeddings")
    return EmbeddingConfig(
        provider=provider,
        model_name=model_name,
        batch_size=batch_size,
        fastembed_threads=fastembed_threads,
        cache_dir=cache_dir,
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
    )


def get_claude_config() -> ClaudeConfig:
    return ClaudeConfig(
        api_key=os.getenv("CLAUDE_API_KEY") or None,
        model=os.getenv("CLAUDE_MODEL", DEFAULT_CLAUDE_MODEL).strip() or DEFAULT_CLAUDE_MODEL,
        api_url=os.getenv("CLAUDE_API_URL", DEFAULT_CLAUDE_API_URL).strip() or DEFAULT_CLAUDE_API_URL,
        max_tokens=int(os.getenv("CLAUDE_MAX_TOKENS", str(DEFAULT_CLAUDE_MAX_TOKENS))),
    )


def build_document_text(title_en: str, citation_en: str | None) -> str:
    parts = [title_en.strip()]
    if citation_en and citation_en.strip():
        parts.append(citation_en.strip())
    return " | ".join(parts)


def get_embedding_text(
    classifier_input_text: str | None,
    title_en: str,
    citation_en: str | None,
) -> str:
    if classifier_input_text and classifier_input_text.strip():
        return classifier_input_text.strip()
    return build_document_text(title_en, citation_en)


def generate_embeddings(texts: Sequence[str], config: EmbeddingConfig) -> list[list[float]]:
    if config.provider == "fastembed":
        return generate_fastembed_embeddings(texts, config)
    if config.provider == "openai":
        return generate_openai_embeddings(texts, config)
    raise RuntimeError(f"Unsupported embedding provider: {config.provider}")


def generate_fastembed_embeddings(
    texts: Sequence[str],
    config: EmbeddingConfig,
) -> list[list[float]]:
    from fastembed import TextEmbedding

    if not texts:
        return []

    model = TextEmbedding(
        model_name=config.model_name,
        cache_dir=config.cache_dir,
        threads=config.fastembed_threads,
    )

    embeddings: list[list[float]] = []
    total = len(texts)
    last_reported = 0
    for index, vector in enumerate(model.embed(texts, batch_size=config.batch_size), start=1):
        embeddings.append([float(value) for value in vector.tolist()])
        if index == total or index - last_reported >= EMBEDDING_PROGRESS_EVERY:
            print(f"Embedded {index}/{total} texts...")
            last_reported = index
    return embeddings


def generate_openai_embeddings(
    texts: Sequence[str],
    config: EmbeddingConfig,
) -> list[list[float]]:
    if not config.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is required when EMBEDDING_PROVIDER=openai")

    embeddings: list[list[float]] = []
    for start in range(0, len(texts), config.batch_size):
        batch = texts[start : start + config.batch_size]
        payload = json.dumps(
            {
                "model": config.model_name,
                "input": list(batch),
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            config.openai_base_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {config.openai_api_key}",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8"))

        data_rows = sorted(body["data"], key=lambda item: item["index"])
        embeddings.extend([[float(value) for value in item["embedding"]] for item in data_rows])
    return embeddings


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    numerator = 0.0
    left_norm = 0.0
    right_norm = 0.0

    for left_value, right_value in zip(left, right):
        numerator += left_value * right_value
        left_norm += left_value * left_value
        right_norm += right_value * right_value

    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return numerator / (math.sqrt(left_norm) * math.sqrt(right_norm))


def classify_with_semantic_similarity(
    document_embedding: Sequence[float],
    prototype_rows: Sequence[PrototypeRow],
    prototype_embeddings: Sequence[Sequence[float]],
) -> SemanticMatch:
    domain_scores = aggregate_domain_scores(
        document_embedding=document_embedding,
        prototype_rows=prototype_rows,
        prototype_embeddings=prototype_embeddings,
    )
    ranked_scores = sorted(
        (
            (label, score, prototype_text)
            for label, (score, prototype_text) in domain_scores.items()
        ),
        key=lambda item: (-item[1], item[0]),
    )

    best_label, best_score, best_prototype = ranked_scores[0]
    if len(ranked_scores) > 1:
        second_best_label, second_best_score, _ = ranked_scores[1]
    else:
        second_best_label = best_label
        second_best_score = best_score

    return SemanticMatch(
        label=best_label,
        similarity=best_score,
        description=best_prototype,
        second_best_label=second_best_label,
        second_best_similarity=second_best_score,
        score_margin=best_score - second_best_score,
    )


def aggregate_domain_scores(
    document_embedding: Sequence[float],
    prototype_rows: Sequence[PrototypeRow],
    prototype_embeddings: Sequence[Sequence[float]],
) -> dict[str, tuple[float, str]]:
    best_scores: dict[str, tuple[float, str]] = {}

    for prototype_row, prototype_embedding in zip(prototype_rows, prototype_embeddings):
        score = cosine_similarity(document_embedding, prototype_embedding)
        current_best = best_scores.get(prototype_row.label)
        if current_best is None or score > current_best[0]:
            best_scores[prototype_row.label] = (score, prototype_row.text)

    return best_scores


def classify_with_claude_fallback(
    title_en: str,
    citation_en: str | None,
    taxonomy_descriptions: dict[str, str],
    config: ClaudeConfig,
) -> tuple[str | None, str | None, str | None, bool]:
    global CLAUDE_COOLDOWN_UNTIL

    if not config.api_key:
        return None, None, "missing_claude_api_key", False

    taxonomy_lines = "\n".join(
        f"- {label}: {description}" for label, description in taxonomy_descriptions.items()
    )
    prompt = (
        "Classify this Canadian federal law into exactly one taxonomy label.\n"
        "Return exactly one label from the list and nothing else.\n\n"
        f"Title: {title_en}\n"
        f"Citation: {citation_en or '(none)'}\n\n"
        "Allowed taxonomy labels:\n"
        f"{taxonomy_lines}\n"
    )

    payload = json.dumps(
        {
            "model": config.model,
            "max_tokens": config.max_tokens,
            "temperature": 0,
            "system": (
                "You classify Canadian federal laws into a fixed taxonomy. "
                "Respond with exactly one valid taxonomy label and nothing else."
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

    body: dict[str, Any] | None = None
    for attempt in range(CLAUDE_RATE_LIMIT_MAX_RETRIES + 1):
        remaining_cooldown = CLAUDE_COOLDOWN_UNTIL - time.monotonic()
        if remaining_cooldown > 0:
            time.sleep(remaining_cooldown)

        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = json.loads(response.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as exc:
            try:
                error_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                error_body = None

            is_rate_limited = exc.code == 429 or parse_claude_error_type(error_body) == "rate_limit_error"
            if is_rate_limited and attempt < CLAUDE_RATE_LIMIT_MAX_RETRIES:
                retry_delay = get_claude_retry_delay_seconds(exc, attempt)
                CLAUDE_COOLDOWN_UNTIL = max(CLAUDE_COOLDOWN_UNTIL, time.monotonic() + retry_delay)
                print(
                    "Claude rate limit hit. "
                    f"Cooling down for {retry_delay:.1f}s before retry {attempt + 1}/"
                    f"{CLAUDE_RATE_LIMIT_MAX_RETRIES}."
                )
                continue

            if is_rate_limited:
                return None, error_body, "claude_rate_limited", True
            return None, error_body, "claude_request_failed", True
        except Exception:
            return None, None, "claude_request_failed", True

    if body is None:
        return None, None, "claude_request_failed", True

    parts = body.get("content", [])
    raw_text = " ".join(
        part.get("text", "") for part in parts if isinstance(part, dict)
    ).strip()
    if raw_text not in taxonomy_descriptions:
        return None, raw_text or None, "claude_invalid_label", True
    return raw_text, raw_text, "semantic_below_threshold", True


def parse_claude_error_type(error_body: str | None) -> str | None:
    if not error_body:
        return None

    try:
        payload = json.loads(error_body)
    except json.JSONDecodeError:
        return None

    error = payload.get("error")
    if isinstance(error, dict):
        error_type = error.get("type")
        if isinstance(error_type, str):
            return error_type
    return None


def get_claude_retry_delay_seconds(
    exc: urllib.error.HTTPError,
    attempt: int,
) -> float:
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


def print_domain_counts(counter: Counter[str]) -> None:
    print("Counts by primary_domain:")
    for label, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        print(f"  {label}: {count}")


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    load_env_file(Path(".env"))

    try:
        import duckdb
    except ImportError:
        print(
            "Error: duckdb is required. Install it with `pip install duckdb`.",
            file=sys.stderr,
        )
        return 1

    if not INPUT_PATH.exists():
        print(f"Error: input parquet not found at {INPUT_PATH}", file=sys.stderr)
        return 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    taxonomy_prototypes = get_taxonomy_prototypes()
    taxonomy_descriptions = build_taxonomy_descriptions(taxonomy_prototypes)
    prototype_rows = flatten_taxonomy_prototypes(taxonomy_prototypes)
    embedding_config = get_embedding_config()
    claude_config = get_claude_config()
    semantic_accept_threshold = float(
        os.getenv("SEMANTIC_ACCEPT_THRESHOLD", str(SEMANTIC_ACCEPT_THRESHOLD))
    )
    semantic_margin_threshold = float(
        os.getenv("SEMANTIC_MARGIN_THRESHOLD", str(SEMANTIC_MARGIN_THRESHOLD))
    )
    temp_output_path = OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet")

    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT document_id, title_en, citation_en, classifier_input_text
            FROM read_parquet(?)
            WHERE title_en IS NOT NULL
              AND trim(title_en) <> ''
            ORDER BY document_id
            """,
            [str(INPUT_PATH)],
        ).fetchall()
    finally:
        con.close()

    documents = [DocumentRow(*row) for row in rows]
    total_documents = len(documents)

    if total_documents == 0:
        con = duckdb.connect()
        try:
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE classification_results (
                    document_id VARCHAR,
                    title_en VARCHAR,
                    primary_domain VARCHAR,
                    classification_method VARCHAR,
                    classification_confidence DOUBLE,
                    top_similarity_score DOUBLE,
                    second_best_domain VARCHAR,
                    second_best_score DOUBLE,
                    score_margin DOUBLE,
                    matched_taxonomy_description VARCHAR,
                    llm_used BOOLEAN,
                    llm_raw_label VARCHAR,
                    fallback_reason VARCHAR
                )
                """
            )
            delete_if_exists(temp_output_path)
            delete_if_exists(OUTPUT_PATH)
            con.execute("COPY classification_results TO ? (FORMAT PARQUET)", [str(temp_output_path)])
            temp_output_path.replace(OUTPUT_PATH)
        finally:
            con.close()
            delete_if_exists(temp_output_path)
        print("Total documents processed: 0")
        print("Total semantic-only classifications: 0")
        print("Total LLM fallback classifications: 0")
        print("Total semantic_low_margin classifications: 0")
        print("Counts by primary_domain:")
        return 0

    document_texts = [
        get_embedding_text(
            classifier_input_text=document.classifier_input_text,
            title_en=document.title_en,
            citation_en=document.citation_en,
        )
        for document in documents
    ]
    prototype_texts = [prototype_row.text for prototype_row in prototype_rows]

    try:
        print(
            "Generating document embeddings "
            f"({len(document_texts)} texts, provider={embedding_config.provider}, "
            f"model={embedding_config.model_name}, batch_size={embedding_config.batch_size}, "
            f"fastembed_threads={embedding_config.fastembed_threads})..."
        )
        document_embeddings = generate_embeddings(document_texts, embedding_config)
        print(
            "Generating taxonomy prototype embeddings "
            f"({len(prototype_texts)} prototypes across {len(taxonomy_prototypes)} labels)..."
        )
        prototype_embeddings = generate_embeddings(prototype_texts, embedding_config)
    except Exception as exc:
        print(f"Error generating embeddings: {exc}", file=sys.stderr)
        return 1

    results: list[tuple[Any, ...]] = []
    domain_counter: Counter[str] = Counter()
    semantic_only_count = 0
    llm_fallback_count = 0
    semantic_low_margin_count = 0

    for index, (document, embedding) in enumerate(zip(documents, document_embeddings), start=1):
        semantic_match = classify_with_semantic_similarity(
            document_embedding=embedding,
            prototype_rows=prototype_rows,
            prototype_embeddings=prototype_embeddings,
        )

        top_similarity_score = float(semantic_match.similarity)
        second_best_domain = semantic_match.second_best_label
        second_best_score = float(semantic_match.second_best_similarity)
        score_margin = float(semantic_match.score_margin)
        confidence = float(top_similarity_score)
        matched_description = semantic_match.description
        llm_used = False
        llm_raw_label: str | None = None
        fallback_reason: str | None = None

        semantic_is_confident = (
            top_similarity_score >= semantic_accept_threshold
            and score_margin >= semantic_margin_threshold
        )

        if semantic_is_confident:
            primary_domain = semantic_match.label
            classification_method = "semantic_similarity"
            semantic_only_count += 1
        else:
            fallback_trigger = []
            if top_similarity_score < semantic_accept_threshold:
                fallback_trigger.append("low_score")
            if score_margin < semantic_margin_threshold:
                fallback_trigger.append("low_margin")
            trigger_reason = "semantic_" + "_".join(fallback_trigger)

            llm_label, llm_raw_label, fallback_reason, llm_used = classify_with_claude_fallback(
                title_en=document.title_en,
                citation_en=document.citation_en,
                taxonomy_descriptions=taxonomy_descriptions,
                config=claude_config,
            )
            if llm_label is not None:
                primary_domain = llm_label
                classification_method = "llm_fallback"
                matched_description = taxonomy_descriptions[primary_domain]
                fallback_reason = trigger_reason
                llm_fallback_count += 1
            else:
                primary_domain = semantic_match.label
                classification_method = "semantic_low_margin"
                if fallback_reason is None:
                    fallback_reason = trigger_reason
                semantic_low_margin_count += 1

        domain_counter[primary_domain] += 1
        results.append(
            (
                document.document_id,
                document.title_en,
                primary_domain,
                classification_method,
                confidence,
                top_similarity_score,
                second_best_domain,
                second_best_score,
                score_margin,
                matched_description,
                llm_used,
                llm_raw_label,
                fallback_reason,
            )
        )

        if index == total_documents or index % CLASSIFICATION_PROGRESS_EVERY == 0:
            print(
                f"Classified {index}/{total_documents} documents "
                f"(semantic={semantic_only_count}, llm={llm_fallback_count}, "
                f"low_margin={semantic_low_margin_count})"
            )

    delete_output = duckdb.connect()
    try:
        delete_output.execute(
            """
            CREATE OR REPLACE TEMP TABLE classification_results (
                document_id VARCHAR,
                title_en VARCHAR,
                primary_domain VARCHAR,
                classification_method VARCHAR,
                classification_confidence DOUBLE,
                top_similarity_score DOUBLE,
                second_best_domain VARCHAR,
                second_best_score DOUBLE,
                score_margin DOUBLE,
                matched_taxonomy_description VARCHAR,
                llm_used BOOLEAN,
                llm_raw_label VARCHAR,
                fallback_reason VARCHAR
            )
            """
        )
        delete_output.executemany(
            "INSERT INTO classification_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            results,
        )
        delete_if_exists(temp_output_path)
        delete_if_exists(OUTPUT_PATH)
        delete_output.execute("COPY classification_results TO ? (FORMAT PARQUET)", [str(temp_output_path)])
        temp_output_path.replace(OUTPUT_PATH)
    finally:
        delete_output.close()
        delete_if_exists(temp_output_path)

    print(f"Total documents processed: {total_documents}")
    print(f"Total semantic-only classifications: {semantic_only_count}")
    print(f"Total LLM fallback classifications: {llm_fallback_count}")
    print(f"Total semantic_low_margin classifications: {semantic_low_margin_count}")
    print_domain_counts(domain_counter)
    print(f"Output written to: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
