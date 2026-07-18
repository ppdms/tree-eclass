"""Retrieval evaluation against a real course corpus and a JSONL qrels file."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any, Iterable

from .config import KnowledgeConfig
from .models import SearchRequest
from .service import KnowledgeService


def _relevant_ids(service: KnowledgeService, row: dict[str, Any]) -> set[str]:
    ids = {str(value) for value in row.get("relevant_document_ids", [])}
    for item in row.get("relevant", []):
        if isinstance(item, str):
            ids.add(item)
        elif isinstance(item, dict):
            if item.get("document_id"):
                ids.add(str(item["document_id"]))
            elif item.get("course_id") is not None and item.get("source_path"):
                document = service.store.get_document_by_path(int(item["course_id"]), item["source_path"])
                if document:
                    ids.add(document["id"])
    for item in row.get("relevant_source_paths", []):
        if isinstance(item, dict):
            course_id, source_path = item.get("course_id"), item.get("source_path")
        else:
            course_id, source_path = row.get("course_id"), item
        if course_id is not None and source_path:
            document = service.store.get_document_by_path(int(course_id), str(source_path))
            if document:
                ids.add(document["id"])
    return ids


def evaluate_rows(service: KnowledgeService, rows: Iterable[dict[str, Any]], k: int = 5,
                  retrieval_mode: str = "hybrid") -> dict[str, Any]:
    """Compute Recall@k, MRR@k, hit rate, and latency for qrels rows."""
    if k < 1:
        raise ValueError("k must be at least 1")
    per_query = []
    latencies = []
    for row in rows:
        query = str(row.get("query", "")).strip()
        relevant = _relevant_ids(service, row)
        if not query or not relevant:
            continue
        started = time.perf_counter()
        result = service.search(SearchRequest(
            query=query,
            course_ids=row.get("course_ids") or ([row["course_id"]] if row.get("course_id") is not None else None),
            document_kinds=row.get("document_kinds"),
            academic_year=row.get("academic_year"),
            folder_prefix=row.get("folder_prefix"),
            limit=k,
            retrieval_mode=retrieval_mode,
        ))
        latency_ms = (time.perf_counter() - started) * 1000
        latencies.append(latency_ms)
        ranked = [item["document_id"] for item in result["results"]]
        hits = [index + 1 for index, document_id in enumerate(ranked) if document_id in relevant]
        per_query.append({
            "query": query,
            "relevant_count": len(relevant),
            "retrieved": ranked,
            "hit": bool(hits),
            "recall_at_k": len(set(ranked) & relevant) / len(relevant),
            "reciprocal_rank_at_k": 1 / hits[0] if hits else 0.0,
            "latency_ms": round(latency_ms, 3),
        })
    count = len(per_query)
    return {
        "queries": count,
        "retrieval_mode": retrieval_mode,
        "k": k,
        "hit_rate_at_k": sum(item["hit"] for item in per_query) / count if count else 0.0,
        "recall_at_k": sum(item["recall_at_k"] for item in per_query) / count if count else 0.0,
        "mrr_at_k": sum(item["reciprocal_rank_at_k"] for item in per_query) / count if count else 0.0,
        "latency_ms": {
            "mean": statistics.mean(latencies) if latencies else 0.0,
            "p95": sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)] if latencies else 0.0,
        },
        "per_query": per_query,
    }


def load_qrels(path: str | Path) -> list[dict[str, Any]]:
    rows = []
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at line {line_number}: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"qrels line {line_number} must be an object")
        rows.append(row)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate course knowledge retrieval")
    parser.add_argument("--qrels", required=True, help="JSONL query/relevance file")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--mode", choices=("lexical", "semantic", "hybrid"), default="hybrid")
    parser.add_argument("--output", help="Write JSON metrics to this path")
    args = parser.parse_args(argv)
    result = evaluate_rows(KnowledgeService(config=KnowledgeConfig.from_env()),
                           load_qrels(args.qrels), args.k, args.mode)
    encoded = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
