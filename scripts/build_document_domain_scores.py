from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import classify_documents as classifier
from env_utils import get_processed_dir


OUTPUT_PATH = get_processed_dir() / "document_domain_scores_en.parquet"
AMBIGUOUS_SAMPLE_SIZE = 20
TOP_DOMAIN_COUNT = 5


@dataclass(frozen=True)
class AmbiguousDocument:
    document_id: str
    title_en: str
    best_domain: str
    best_score: float
    second_best_domain: str
    second_best_score: float
    score_margin: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build per-domain similarity scores. By default, only new or changed "
            "documents are rescored; existing unchanged rows are reused."
        )
    )
    parser.add_argument(
        "--reclassify-all",
        action="store_true",
        help="Ignore existing domain-score output and recompute every document.",
    )
    return parser.parse_args()


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


def rank_domain_scores(
    domain_scores: dict[str, tuple[float, str]],
) -> list[tuple[str, float]]:
    return sorted(
        ((label, float(score)) for label, (score, _) in domain_scores.items()),
        key=lambda item: (-item[1], item[0]),
    )


def print_top_domains_by_average(
    domain_similarity_totals: dict[str, float],
    total_documents: int,
) -> None:
    averages = [
        (domain, total / total_documents)
        for domain, total in domain_similarity_totals.items()
    ]
    averages.sort(key=lambda item: (-item[1], item[0]))

    print("Top 5 domains by average similarity:")
    for domain, average in averages[:TOP_DOMAIN_COUNT]:
        print(f"  {domain}: {average:.6f}")


def print_ambiguous_documents(rows: list[AmbiguousDocument]) -> None:
    rows.sort(key=lambda row: (row.score_margin, -row.best_score, row.document_id))

    print()
    print("Top 20 most ambiguous documents:")
    for row in rows[:AMBIGUOUS_SAMPLE_SIZE]:
        print(f"- document_id: {row.document_id}")
        print(f"  title_en: {row.title_en}")
        print(f"  best_domain: {row.best_domain}")
        print(f"  best_score: {row.best_score:.6f}")
        print(f"  second_best_domain: {row.second_best_domain}")
        print(f"  second_best_score: {row.second_best_score:.6f}")
        print(f"  score_margin: {row.score_margin:.6f}")


def write_output(rows: list[tuple[Any, ...]]) -> None:
    import duckdb

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = OUTPUT_PATH.with_name(f"{OUTPUT_PATH.stem}.tmp.parquet")

    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE document_domain_scores (
                document_id VARCHAR,
                title_en VARCHAR,
                domain VARCHAR,
                similarity_score DOUBLE,
                input_fingerprint VARCHAR
            )
            """
        )
        if rows:
            con.executemany(
                "INSERT INTO document_domain_scores VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        classifier.delete_if_exists(temp_output_path)
        classifier.delete_if_exists(OUTPUT_PATH)
        con.execute(
            "COPY document_domain_scores TO ? (FORMAT PARQUET)",
            [str(temp_output_path)],
        )
        temp_output_path.replace(OUTPUT_PATH)
    finally:
        con.close()
        classifier.delete_if_exists(temp_output_path)


def load_existing_domain_score_rows() -> dict[str, list[tuple[Any, ...]]]:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required. Install it with `pip install duckdb`.") from exc

    if not OUTPUT_PATH.exists():
        return {}

    con = duckdb.connect()
    try:
        schema_rows = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(OUTPUT_PATH)],
        ).fetchall()
        column_names = {str(row[0]) for row in schema_rows}
        input_fingerprint_expr = (
            "input_fingerprint"
            if "input_fingerprint" in column_names
            else "NULL AS input_fingerprint"
        )
        rows = con.execute(
            f"""
            SELECT
                document_id,
                title_en,
                domain,
                similarity_score,
                {input_fingerprint_expr}
            FROM read_parquet(?)
            ORDER BY document_id, domain
            """,
            [str(OUTPUT_PATH)],
        ).fetchall()
    finally:
        con.close()

    rows_by_document: dict[str, list[tuple[Any, ...]]] = {}
    for row in rows:
        rows_by_document.setdefault(str(row[0]), []).append(row)
    return rows_by_document


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args()
    classifier.load_env_file(Path(".env"))

    try:
        documents = load_documents()
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    if not documents:
        write_output([])
        print("Documents processed: 0")
        print("Documents reused without rescoring: 0")
        print("Documents newly rescored: 0")
        print("Rows written: 0")
        print(f"Output written to: {OUTPUT_PATH}")
        return 0

    taxonomy_prototypes = classifier.get_taxonomy_prototypes()
    taxonomy_labels = list(taxonomy_prototypes.keys())
    prototype_rows = classifier.flatten_taxonomy_prototypes(taxonomy_prototypes)
    prototype_texts = [prototype_row.text for prototype_row in prototype_rows]
    embedding_config = classifier.get_embedding_config()
    existing_rows_by_document = (
        {}
        if args.reclassify_all
        else load_existing_domain_score_rows()
    )
    current_fingerprints = {
        document.document_id: classifier.compute_document_input_fingerprint(document)
        for document in documents
    }
    documents_to_score: list[classifier.DocumentRow] = []
    output_rows_by_document: dict[str, list[tuple[Any, ...]]] = {}
    ambiguous_documents: list[AmbiguousDocument] = []
    domain_similarity_totals = {label: 0.0 for label in taxonomy_labels}
    reused_count = 0

    for document in documents:
        existing_rows = existing_rows_by_document.get(document.document_id)
        if not existing_rows:
            documents_to_score.append(document)
            continue

        existing_domains = {str(row[2]) for row in existing_rows}
        existing_fingerprint = existing_rows[0][4]
        if (
            len(existing_rows) != len(taxonomy_labels)
            or existing_domains != set(taxonomy_labels)
            or (existing_fingerprint and existing_fingerprint != current_fingerprints[document.document_id])
        ):
            documents_to_score.append(document)
            continue

        normalized_rows: list[tuple[Any, ...]] = []
        for row in existing_rows:
            normalized_rows.append(
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    current_fingerprints[document.document_id],
                )
            )
            domain_similarity_totals[str(row[2])] += float(row[3])
        output_rows_by_document[document.document_id] = normalized_rows
        reused_count += 1

    prototype_embeddings: list[list[float]] = []
    document_embeddings: list[list[float]] = []
    if documents_to_score:
        document_texts = [
            classifier.get_embedding_text(
                classifier_input_text=document.classifier_input_text,
                title_en=document.title_en,
                citation_en=document.citation_en,
            )
            for document in documents_to_score
        ]

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
                f"({len(prototype_texts)} prototypes across {len(taxonomy_labels)} labels)..."
            )
            prototype_embeddings = classifier.generate_embeddings(prototype_texts, embedding_config)
        except Exception as exc:
            print(f"Error generating embeddings: {exc}", file=sys.stderr)
            return 1

    output_rows: list[tuple[Any, ...]] = []

    for document, document_embedding in zip(documents_to_score, document_embeddings):
        domain_scores = classifier.aggregate_domain_scores(
            document_embedding=document_embedding,
            prototype_rows=prototype_rows,
            prototype_embeddings=prototype_embeddings,
        )
        ranked_scores = rank_domain_scores(domain_scores)
        best_domain, best_score = ranked_scores[0]
        second_best_domain, second_best_score = ranked_scores[1]
        ambiguous_documents.append(
            AmbiguousDocument(
                document_id=document.document_id,
                title_en=document.title_en,
                best_domain=best_domain,
                best_score=best_score,
                second_best_domain=second_best_domain,
                second_best_score=second_best_score,
                score_margin=best_score - second_best_score,
            )
        )

        document_rows: list[tuple[Any, ...]] = []
        for label in taxonomy_labels:
            similarity_score = float(domain_scores[label][0])
            domain_similarity_totals[label] += similarity_score
            document_rows.append(
                (
                    document.document_id,
                    document.title_en,
                    label,
                    similarity_score,
                    current_fingerprints[document.document_id],
                )
            )
        output_rows_by_document[document.document_id] = document_rows

    for document in documents:
        output_rows.extend(output_rows_by_document[document.document_id])

    write_output(output_rows)

    print(f"Documents processed: {len(documents)}")
    print(f"Documents reused without rescoring: {reused_count}")
    print(f"Documents newly rescored: {len(documents_to_score)}")
    print(f"Rows written: {len(output_rows)}")
    print(f"Output written to: {OUTPUT_PATH}")
    print()
    print_top_domains_by_average(domain_similarity_totals, len(documents))
    print_ambiguous_documents(ambiguous_documents)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
