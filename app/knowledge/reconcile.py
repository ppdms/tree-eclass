"""Idempotently compare current synchronization metadata with the knowledge index."""

import logging
import os
from typing import Iterator

from app.services.tree_builder import File, Node

from .academic_years import academic_year_from_path
from .config import KnowledgeConfig
from .extractors import guess_mime, source_kind
from .models import SourceMetadata
from .normalization import normalize_path
from .store import KnowledgeStore, utc_now


EXCLUDED_PARTS = {".versions", "_deleted"}


def _excluded(path: str) -> bool:
    parts = normalize_path(path).lower().split("/")
    base = os.path.basename(path).lower()
    return any(part in EXCLUDED_PARTS for part in parts) or base.endswith("_diff.pdf")


def unsupported_reason(name: str, mime_type: str | None = None) -> str:
    """Classify a manifest entry that cannot be routed from its metadata alone."""
    if mime_type:
        return "unsupported_mime_type"
    return "unsupported_extension"


def _academic_year(path: str) -> str | None:
    return academic_year_from_path(path)


def iter_tree_files(root: Node) -> Iterator[tuple[File, str]]:
    for item in root.files:
        path = item.local_path or f"{root.local_path.rstrip('/')}/{item.name}"
        yield item, path
    for child in root.children:
        yield from iter_tree_files(child)


class KnowledgeReconciler:
    def __init__(self, store: KnowledgeStore, source_db_file: str | None = None):
        self.store = store
        self.source_db_file = source_db_file or KnowledgeConfig.from_env().source_db_file

    def reconcile_course(self, course_id: int, root: Node, course: dict | None = None) -> dict[str, int]:
        if course is None:
            from app.services.persistence import DatabaseManager
            db = DatabaseManager(self.source_db_file)
            try:
                course = db.get_course(course_id, include_hidden=True)
            finally:
                db.close()
        if not course:
            raise ValueError(f"course {course_id} does not exist")
        counts = {"seen": 0, "enqueued": 0, "unchanged": 0, "external": 0,
                  "unsupported": 0, "pending_detection": 0, "deleted": 0}
        current_paths: set[str] = set()
        for item, path in iter_tree_files(root):
            if _excluded(path):
                continue
            normalized = normalize_path(path)
            current_paths.add(normalized)
            counts["seen"] += 1
            mime = guess_mime(item.name) or guess_mime(path)
            kind = source_kind(item.name, mime) or source_kind(path, mime)
            source = SourceMetadata(
                course_id=course_id, course_name=course["name"], course_short_name=course.get("short_name"),
                source_path=path, source_url=item.url, display_name=item.name,
                source_hash=item.md5_hash or "", mime_type=mime, academic_year=_academic_year(path),
                source_modified_at=item.last_updated,
            )
            existing = self.store.get_document_by_path(course_id, path)
            if item.redirect_url:
                self.store.record_manifest_document(
                    source, kind or "external", "external", diagnostic_reason="external"
                )
                counts["external"] += 1
            elif kind is None:
                # A missing/odd extension is not enough evidence to reject a
                # downloaded object. Let the worker inspect its headers and
                # magic bytes once, then retain a stable unsupported result if
                # the content genuinely has no extractor.
                legacy_unsupported = existing and existing["status"] == "unsupported" and not existing.get("diagnostic_reason")
                if (existing and existing["source_hash"] == source.source_hash
                        and existing["status"] not in {"failed", "pending", "skipped_limit"}
                        and not legacy_unsupported):
                    self.store.refresh_source_metadata(source)
                    counts["unchanged"] += 1
                elif item.local_path:
                    self.store.record_manifest_document(
                        source, "unknown", "pending", diagnostic_reason="content_detection_pending"
                    )
                    if self.store.enqueue(course_id, path, source.source_hash, "upsert"):
                        counts["enqueued"] += 1
                    counts["pending_detection"] += 1
                else:
                    self.store.record_manifest_document(
                        source, "unsupported", "unsupported",
                        diagnostic_reason=unsupported_reason(item.name, mime),
                    )
                    counts["unsupported"] += 1
            elif (not existing or existing["source_hash"] != source.source_hash
                  or existing["status"] in {"failed", "pending", "skipped_limit"}
                  or (existing["status"] == "unsupported" and not existing.get("diagnostic_reason"))):
                self.store.record_manifest_document(source, kind, "pending")
                if self.store.enqueue(course_id, path, source.source_hash, "upsert"):
                    counts["enqueued"] += 1
            else:
                self.store.refresh_source_metadata(source)
                counts["unchanged"] += 1
        missing = self.store.mark_missing(course_id, current_paths)
        counts["deleted"] = len(missing)
        self.store.set_state(f"last_reconcile_course_{course_id}", utc_now())
        return counts

    def reconcile_all(self) -> dict[int, dict[str, int]]:
        from app.services.persistence import DatabaseManager
        db = DatabaseManager(self.source_db_file)
        results: dict[int, dict[str, int]] = {}
        try:
            for course in db.get_courses(include_hidden=True):
                root = db.load_tree(course["id"])
                if root:
                    results[course["id"]] = self.reconcile_course(course["id"], root, course)
        finally:
            db.close()
        self.store.set_state("last_reconcile_all", utc_now())
        return results


def reconcile_after_save(course_id: int, root: Node) -> None:
    """Best-effort hook: indexing can never fail synchronization."""
    config = KnowledgeConfig.from_env()
    if not config.enabled:
        return
    try:
        KnowledgeReconciler(KnowledgeStore(config.db_file), config.source_db_file).reconcile_course(course_id, root)
    except Exception:
        logging.exception("Knowledge reconciliation failed for course %s; synchronization remains successful", course_id)
