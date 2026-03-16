from __future__ import annotations

import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import classify_documents as classifier


TARGET_DOMAINS = [
    "governance_administrative",
    "rights_privacy_access",
    "indigenous_crown_relations",
]
AMBIGUOUS_SAMPLE_SIZE = 20


@dataclass(frozen=True)
class MarginResult:
    document_id: str
    title_en: str
    best_domain: str
    best_score: float
    second_best_domain: str
    second_best_score: float
    score_margin: float


def load_documents() -> list[classifier.DocumentRow]:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required. Install it with `pip install duckdb`.") from exc

    if not classifier.INPUT_PATH.exists():
        raise RuntimeError(f"Input parquet not found at {classifier.INPUT_PATH}")

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
            [str(classifier.INPUT_PATH)],
        ).fetchall()
    finally:
        con.close()

    return [classifier.DocumentRow(*row) for row in rows]


def compute_percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0

    if percentile <= 0:
        return min(values)
    if percentile >= 1:
        return max(values)

    sorted_values = sorted(values)
    position = percentile * (len(sorted_values) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    weight = position - lower_index
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + (upper_value - lower_value) * weight


def build_margin_result(
    document: classifier.DocumentRow,
    domain_scores: dict[str, tuple[float, str]],
) -> MarginResult:
    ranked = sorted(
        (
            (label, score_and_prototype[0])
            for label, score_and_prototype in domain_scores.items()
        ),
        key=lambda item: (-item[1], item[0]),
    )
    best_domain, best_score = ranked[0]
    second_best_domain, second_best_score = ranked[1]
    return MarginResult(
        document_id=document.document_id,
        title_en=document.title_en,
        best_domain=best_domain,
        best_score=float(best_score),
        second_best_domain=second_best_domain,
        second_best_score=float(second_best_score),
        score_margin=float(best_score - second_best_score),
    )


def print_margin_summary(results: Sequence[MarginResult]) -> None:
    margins = [result.score_margin for result in results]
    print("Overall margin distribution:")
    print(f"  count: {len(margins)}")
    print(f"  min: {min(margins):.6f}")
    print(f"  p10: {compute_percentile(margins, 0.10):.6f}")
    print(f"  p25: {compute_percentile(margins, 0.25):.6f}")
    print(f"  median: {compute_percentile(margins, 0.50):.6f}")
    print(f"  mean: {statistics.fmean(margins):.6f}")
    print(f"  p75: {compute_percentile(margins, 0.75):.6f}")
    print(f"  p90: {compute_percentile(margins, 0.90):.6f}")
    print(f"  p95: {compute_percentile(margins, 0.95):.6f}")
    print(f"  p99: {compute_percentile(margins, 0.99):.6f}")
    print(f"  max: {max(margins):.6f}")
    print()
    print("Counts below margin thresholds:")
    for threshold in (0.01, 0.02, 0.05, 0.10):
        count = sum(1 for value in margins if value < threshold)
        print(f"  < {threshold:.2f}: {count}")


def print_sample_rows(title: str, rows: Sequence[MarginResult]) -> None:
    print()
    print(title)
    if not rows:
        print("  No matching rows.")
        return

    for row in rows:
        print(f"- document_id: {row.document_id}")
        print(f"  title_en: {row.title_en}")
        print(f"  best_domain: {row.best_domain}")
        print(f"  best_score: {row.best_score:.6f}")
        print(f"  second_best_domain: {row.second_best_domain}")
        print(f"  second_best_score: {row.second_best_score:.6f}")
        print(f"  score_margin: {row.score_margin:.6f}")


def sort_ambiguous_rows(rows: Sequence[MarginResult]) -> list[MarginResult]:
    return sorted(
        rows,
        key=lambda row: (row.score_margin, -row.best_score, row.document_id),
    )


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    classifier.load_env_file(Path(".env"))

    try:
        documents = load_documents()
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not documents:
        print("No classifier input rows found.")
        return 0

    taxonomy_prototypes = classifier.get_taxonomy_prototypes()
    prototype_rows = classifier.flatten_taxonomy_prototypes(taxonomy_prototypes)
    embedding_config = classifier.get_embedding_config()

    document_texts = [
        classifier.get_embedding_text(
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
        document_embeddings = classifier.generate_embeddings(document_texts, embedding_config)
        print(
            "Generating taxonomy prototype embeddings "
            f"({len(prototype_texts)} prototypes across {len(taxonomy_prototypes)} labels)..."
        )
        prototype_embeddings = classifier.generate_embeddings(prototype_texts, embedding_config)
    except Exception as exc:
        print(f"Error generating embeddings: {exc}", file=sys.stderr)
        return 1

    results: list[MarginResult] = []
    for document, document_embedding in zip(documents, document_embeddings):
        domain_scores = classifier.aggregate_domain_scores(
            document_embedding=document_embedding,
            prototype_rows=prototype_rows,
            prototype_embeddings=prototype_embeddings,
        )
        results.append(build_margin_result(document, domain_scores))

    print_margin_summary(results)
    print_sample_rows(
        "20 most ambiguous rows overall:",
        sort_ambiguous_rows(results)[:AMBIGUOUS_SAMPLE_SIZE],
    )

    for domain in TARGET_DOMAINS:
        domain_rows = [row for row in results if row.best_domain == domain]
        print_sample_rows(
            f"20 most ambiguous rows for {domain}:",
            sort_ambiguous_rows(domain_rows)[:AMBIGUOUS_SAMPLE_SIZE],
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
