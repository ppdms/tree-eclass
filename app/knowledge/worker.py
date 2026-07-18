"""Background extraction worker and recovery CLI."""

import argparse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import logging
from pathlib import Path
import signal
import tempfile
import threading
import time

from app.services.persistence import DatabaseManager
from app.services.webdav_uploader import WebDAVUploader

from .chunking import chunk_units
from .config import KnowledgeConfig
from .embeddings import EmbeddingProvider
from .extractors import (ExtractionError, ExtractionLimitError, ExtractionLimits, detect_source,
                         extractor_for, guess_mime, source_kind)
from .models import SourceMetadata
from .normalization import document_id, normalize_path
from .reconcile import KnowledgeReconciler
from .store import KnowledgeStore, utc_now


def _current_source(db: DatabaseManager, course_id: int, path: str) -> SourceMetadata | None:
    course = db.get_course(course_id, include_hidden=True)
    root = db.load_tree(course_id)
    if not course or not root:
        return None
    from .reconcile import iter_tree_files, _academic_year
    wanted = normalize_path(path)
    for item, item_path in iter_tree_files(root):
        if normalize_path(item_path) == wanted and not item.redirect_url and item.local_path:
            return SourceMetadata(
                course_id=course_id, course_name=course["name"], course_short_name=course.get("short_name"),
                source_path=item_path, source_url=item.url, display_name=item.name,
                source_hash=item.md5_hash or "", mime_type=guess_mime(item.name) or guess_mime(item_path),
                academic_year=_academic_year(item_path), source_modified_at=item.last_updated,
            )
    return None


class KnowledgeWorker:
    def __init__(self, config: KnowledgeConfig | None = None, store: KnowledgeStore | None = None,
                 uploader: WebDAVUploader | None = None):
        self.config = config or KnowledgeConfig.from_env()
        self.store = store or KnowledgeStore(
            self.config.db_file, embedding_provider=EmbeddingProvider.from_config(self.config)
        )
        self.uploader = uploader
        self.stop_event = threading.Event()
        self.operation_lock = threading.RLock()
        self.limits = ExtractionLimits(
            max_characters=self.config.max_extracted_chars, max_units=self.config.max_units,
            archive_max_members=self.config.archive_max_members,
            archive_max_member_bytes=self.config.archive_max_member_bytes,
            archive_max_expanded_bytes=self.config.archive_max_expanded_bytes,
            archive_max_ratio=self.config.archive_max_ratio,
            ocr_enabled=self.config.ocr_enabled,
            ocr_languages=self.config.ocr_languages,
            ocr_dpi=self.config.ocr_dpi,
            ocr_page_timeout_seconds=self.config.ocr_page_timeout_seconds,
            ocr_max_pages=self.config.ocr_max_pages,
        )

    def _uploader(self, db: DatabaseManager) -> WebDAVUploader:
        if self.uploader:
            return self.uploader
        config = db.get_webdav_config()
        if not config:
            raise RuntimeError("WebDAV is not configured")
        return WebDAVUploader(config)

    def _run_once(self) -> bool:
        job = self.store.claim_job()
        if not job:
            return False
        db = DatabaseManager(self.config.source_db_file)
        source = None
        kind = None
        try:
            if job["action"] == "delete":
                self.store.mark_deleted(job["course_id"], job["source_path"])
                self.store.finish_job(job["id"])
                return True
            source = _current_source(db, job["course_id"], job["source_path"])
            if not source or source.source_hash != job["requested_hash"]:
                self.store.finish_job(job["id"], "stale", "source is no longer current")
                return True
            uploader = self._uploader(db)
            if hasattr(uploader, "download_file_with_metadata"):
                data, response_headers = uploader.download_file_with_metadata(source.source_path)
            else:
                # Preserve compatibility with test doubles and older uploaders.
                data = uploader.download_file(source.source_path)
                response_headers = {}
            if data is None:
                raise ExtractionError("WebDAV object could not be downloaded", reason="download_failed")
            response_mime = (response_headers.get("content-type") or "").split(";", 1)[0].strip().lower()
            if response_mime:
                source = replace(source, mime_type=response_mime, response_mime_type=response_mime)
            if len(data) > self.config.max_source_bytes:
                raise ExtractionLimitError(
                    f"source size limit exceeded ({len(data)} bytes)", reason="maximum_size"
                )
            if len(source.source_hash) == 32 and hashlib.md5(data).hexdigest() != source.source_hash.lower():  # nosec B324
                raise ExtractionError(
                    "downloaded object hash does not match current metadata", reason="hash_mismatch"
                )
            # display_name is often a human title (for example "Regression"),
            # not the actual filename. Use the canonical source path for
            # extension-based routing so genuine HTML files are not mistaken
            # for authentication/error-page downloads.
            detection_name = source.source_path or source.display_name
            detected_kind, detected_mime, detection_reason = detect_source(
                detection_name, source.mime_type, data
            )
            if detection_reason:
                raise ExtractionError(
                    f"could not route downloaded content ({detection_reason})",
                    reason=detection_reason,
                )
            if not detected_kind:
                raise ExtractionError("downloaded content has no supported extractor", reason="unsupported_mime_type")
            kind = detected_kind
            source = replace(source, mime_type=detected_mime or source.mime_type)
            suffix = "".join(Path(detection_name).suffixes[-2:]) or Path(detection_name).suffix
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as temp:
                temp.write(data)
                temp.flush()
                kind, extractor = extractor_for(source.display_name, source.mime_type, kind=detected_kind)
                extracted = extractor(temp.name, source, self.limits)
            if not any(unit.text.strip() for unit in extracted.units):
                raise ExtractionError("extractor returned no useful text", reason="empty_text")
            doc_id = document_id(source.course_id, source.source_path)
            chunks = chunk_units(doc_id, source.source_hash, extracted.units)
            count = extracted.metadata.get("page_count") or extracted.metadata.get("slide_count") or len(extracted.units)
            self.store.replace_document(source, kind, chunks, extractor.__module__, warnings=extracted.warnings,
                                        page_count=int(count))
            self.store.finish_job(job["id"])
            self.store.set_state("worker_heartbeat", utc_now())
            self.store.set_state("last_successful_job", {"job_id": job["id"], "at": utc_now()})
        except Exception as exc:
            error = f"{type(exc).__name__}: {str(exc)}"[:1000]
            diagnostic_reason = getattr(exc, "reason", None) or "parser_failure"
            status = "skipped_limit" if isinstance(exc, ExtractionLimitError) else "failed"
            if diagnostic_reason in {"unsupported_extension", "unsupported_mime_type"}:
                status = "unsupported"
            if source:
                self.store.mark_error(
                    source, kind or source_kind(source.display_name, source.mime_type) or "unsupported",
                    status, error, diagnostic_reason=diagnostic_reason,
                )
            if status == "unsupported":
                # Unsupported content is an expected terminal outcome, not a
                # retryable worker failure. Keep the document diagnostic, but
                # settle the queue job so it does not pollute the failure queue.
                self.store.finish_job(job["id"], "completed")
                logging.info("Knowledge job %s skipped unsupported content: %s", job["id"], error)
            elif (status == "failed" and job["attempts"] < self.config.worker_max_attempts
                  and not isinstance(exc, ExtractionLimitError)):
                delay = min(3600, 2 ** job["attempts"] * 5)
                retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
                self.store.finish_job(job["id"], error=error, retry_at=retry_at)
            else:
                self.store.finish_job(job["id"], "failed", error)
                logging.warning("Knowledge job %s failed: %s", job["id"], error)
        finally:
            db.close()
        return True

    def run_once(self) -> bool:
        with self.operation_lock:
            return self._run_once()

    def run_forever(self) -> None:
        self.store.recover_claims()
        next_reconcile = time.monotonic() + self.config.reconcile_interval_seconds
        while not self.stop_event.is_set():
            if time.monotonic() >= next_reconcile:
                try:
                    with self.operation_lock:
                        KnowledgeReconciler(self.store, self.config.source_db_file).reconcile_all()
                except Exception:
                    logging.exception("Periodic knowledge reconciliation failed")
                next_reconcile = time.monotonic() + self.config.reconcile_interval_seconds
            try:
                worked = self.run_once()
                self.store.set_state("worker_heartbeat", utc_now())
            except Exception:
                worked = False
                logging.exception("Knowledge worker loop error")
            if not worked:
                self.stop_event.wait(self.config.worker_poll_seconds)

    def stop(self) -> None:
        self.stop_event.set()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Course knowledge indexing worker")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--once", action="store_true")
    group.add_argument("--forever", action="store_true")
    group.add_argument("--reconcile-all", action="store_true")
    group.add_argument("--rebuild", action="store_true")
    group.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args(argv)
    config = KnowledgeConfig.from_env()
    store = KnowledgeStore(config.db_file, embedding_provider=EmbeddingProvider.from_config(config))
    if args.rebuild:
        store.rebuild()
        KnowledgeReconciler(store, config.source_db_file).reconcile_all()
        worker = KnowledgeWorker(config, store)
        while worker.run_once():
            pass
    elif args.reconcile_all:
        KnowledgeReconciler(store, config.source_db_file).reconcile_all()
    elif args.retry_failed:
        print(store.release_failed())
    else:
        worker = KnowledgeWorker(config, store)
        if args.once:
            worker.run_once()
        else:
            signal.signal(signal.SIGTERM, lambda *_: worker.stop())
            signal.signal(signal.SIGINT, lambda *_: worker.stop())
            worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
