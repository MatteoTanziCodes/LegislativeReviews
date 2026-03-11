from __future__ import annotations

import json
import math
import os
import sys
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


INPUT_PATH = Path(r"E:\Programming\buildcanada\canadian-laws\processed\documents_en.parquet")
OUTPUT_PATH = Path(r"E:\Programming\buildcanada\canadian-laws\processed\document_domains_en.parquet")

# Title-plus-citation embeddings on this corpus cluster lower than 0.78;
# this default keeps most clear matches on the semantic path while still
# reserving the LLM for weaker scores. Override via SEMANTIC_ACCEPT_THRESHOLD.
SEMANTIC_ACCEPT_THRESHOLD = 0.55
DEFAULT_EMBEDDING_PROVIDER = "fastembed"
DEFAULT_EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
DEFAULT_EMBEDDING_BATCH_SIZE = 128
DEFAULT_FASTEMBED_THREADS = 1
DEFAULT_CLAUDE_MODEL = "claude-4-5-haiku-latest"
DEFAULT_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_CLAUDE_MAX_TOKENS = 16
ANTHROPIC_VERSION = "2025-10-01"

TAXONOMY_DESCRIPTIONS: dict[str, str] = {
    "governance_administrative": (
        "Laws about government institutions, public administration, administrative "
        "procedure, elections, official government operations, tribunals, agencies, "
        "public service management, and regulatory governance."
    ),
    "criminal_public_safety": (
        "Laws about criminal offences, policing, investigations, evidence, courts, "
        "sentencing, corrections, national security, firearms, border enforcement, "
        "emergency powers, and public safety."
    ),
    "labor_employment": (
        "Laws about employment standards, labour relations, workplace safety, wages, "
        "collective bargaining, pensions tied to employment, workers compensation, "
        "and public service employment."
    ),
    "business_commerce": (
        "Laws about corporations, insolvency, bankruptcy, competition, trade, "
        "intellectual property, commercial transactions, communications businesses, "
        "consumer commerce, and market regulation."
    ),
    "tax_finance": (
        "Laws about taxation, customs duties, tariffs, public revenue, banking, "
        "financial institutions, insurance, monetary policy, securities, and federal "
        "public finance."
    ),
    "transport_infrastructure": (
        "Laws about aviation, rail, shipping, ports, roads, bridges, pipelines, "
        "transportation safety, infrastructure systems, and movement of passengers or goods."
    ),
    "environment_resources": (
        "Laws about environmental protection, climate, pollution, wildlife, fisheries, "
        "oceans, parks, forestry, mining, energy, water, agriculture resources, and "
        "natural resource management."
    ),
    "rights_privacy_access": (
        "Laws about human rights, civil liberties, privacy, data protection, access to "
        "information, accessibility, language rights, discrimination, and public access rights."
    ),
    "indigenous_crown_relations": (
        "Laws about First Nations, Inuit, Metis, reserves, treaties, land claims, "
        "self-government, Indigenous governance, Crown-Indigenous relations, and related institutions."
    ),
    "health_social_services": (
        "Laws about public health, health care, medicines, food safety, disability supports, "
        "income support, social benefits, veterans supports, family services, and other "
        "social service systems."
    ),
    "other": (
        "Laws that do not clearly fit the other specified domains, including miscellaneous, "
        "technical, ceremonial, cross-cutting, or hard-to-classify statutes."
    ),
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


@dataclass(frozen=True)
class SemanticMatch:
    label: str
    similarity: float
    description: str


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


def get_taxonomy_descriptions() -> dict[str, str]:
    return dict(TAXONOMY_DESCRIPTIONS)


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

    model = TextEmbedding(
        model_name=config.model_name,
        cache_dir=config.cache_dir,
        threads=config.fastembed_threads,
    )

    embeddings: list[list[float]] = []
    for vector in model.embed(texts, batch_size=config.batch_size):
        embeddings.append([float(value) for value in vector.tolist()])
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
    taxonomy_labels: Sequence[str],
    taxonomy_embeddings: Sequence[Sequence[float]],
    taxonomy_descriptions: dict[str, str],
) -> SemanticMatch:
    best_label = "other"
    best_score = -1.0

    for label, embedding in zip(taxonomy_labels, taxonomy_embeddings):
        score = cosine_similarity(document_embedding, embedding)
        if score > best_score:
            best_label = label
            best_score = score

    return SemanticMatch(
        label=best_label,
        similarity=best_score,
        description=taxonomy_descriptions[best_label],
    )


def classify_with_claude_fallback(
    title_en: str,
    citation_en: str | None,
    taxonomy_descriptions: dict[str, str],
    config: ClaudeConfig,
) -> tuple[str | None, str | None, str | None, bool]:
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

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            error_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = None
        return None, error_body, "claude_request_failed", True
    except Exception:
        return None, None, "claude_request_failed", True

    parts = body.get("content", [])
    raw_text = " ".join(
        part.get("text", "") for part in parts if isinstance(part, dict)
    ).strip()
    if raw_text not in taxonomy_descriptions:
        return None, raw_text or None, "claude_invalid_label", True
    return raw_text, raw_text, "semantic_below_threshold", True


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

    taxonomy_descriptions = get_taxonomy_descriptions()
    taxonomy_labels = list(taxonomy_descriptions.keys())
    embedding_config = get_embedding_config()
    claude_config = get_claude_config()
    threshold = float(os.getenv("SEMANTIC_ACCEPT_THRESHOLD", str(SEMANTIC_ACCEPT_THRESHOLD)))
    temp_output_path = OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet")

    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT document_id, title_en, citation_en
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
        print("Total low-confidence other classifications: 0")
        print("Counts by primary_domain:")
        return 0

    document_texts = [
        build_document_text(document.title_en, document.citation_en)
        for document in documents
    ]
    taxonomy_texts = [
        f"{label}: {description}" for label, description in taxonomy_descriptions.items()
    ]

    try:
        document_embeddings = generate_embeddings(document_texts, embedding_config)
        taxonomy_embeddings = generate_embeddings(taxonomy_texts, embedding_config)
    except Exception as exc:
        print(f"Error generating embeddings: {exc}", file=sys.stderr)
        return 1

    results: list[tuple[Any, ...]] = []
    domain_counter: Counter[str] = Counter()
    semantic_only_count = 0
    llm_fallback_count = 0
    low_confidence_other_count = 0

    for document, embedding in zip(documents, document_embeddings):
        semantic_match = classify_with_semantic_similarity(
            document_embedding=embedding,
            taxonomy_labels=taxonomy_labels,
            taxonomy_embeddings=taxonomy_embeddings,
            taxonomy_descriptions=taxonomy_descriptions,
        )

        top_similarity_score = float(semantic_match.similarity)
        confidence = float(top_similarity_score)
        matched_description = semantic_match.description
        llm_used = False
        llm_raw_label: str | None = None
        fallback_reason: str | None = None

        if top_similarity_score >= threshold:
            primary_domain = semantic_match.label
            classification_method = "semantic_similarity"
            semantic_only_count += 1
        else:
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
                llm_fallback_count += 1
            else:
                primary_domain = "other"
                classification_method = "other_low_confidence"
                matched_description = taxonomy_descriptions[primary_domain]
                low_confidence_other_count += 1

        domain_counter[primary_domain] += 1
        results.append(
            (
                document.document_id,
                document.title_en,
                primary_domain,
                classification_method,
                confidence,
                top_similarity_score,
                matched_description,
                llm_used,
                llm_raw_label,
                fallback_reason,
            )
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
                matched_taxonomy_description VARCHAR,
                llm_used BOOLEAN,
                llm_raw_label VARCHAR,
                fallback_reason VARCHAR
            )
            """
        )
        delete_output.executemany(
            "INSERT INTO classification_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    print(f"Total low-confidence other classifications: {low_confidence_other_count}")
    print_domain_counts(domain_counter)
    print(f"Output written to: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
