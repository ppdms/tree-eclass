"""Background extraction worker and recovery CLI."""

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
from pathlib import Path
import signal
import tempfile
import threading
import time
from zoneinfo import ZoneInfo

from app.services.persistence import DatabaseManager
from app.services.webdav_uploader import WebDAVUploader

from .chunking import chunk_units
from .config import KnowledgeConfig
from .embeddings import EmbeddingProvider
from .enrichment import EnrichmentError, OllamaEnricher
from .extractors import (ExtractionError, ExtractionLimitError, ExtractionLimits, detect_source,
                         extractor_for, guess_mime, source_kind)
from .metrics import document_metrics, merge_chunk_texts
from .models import SourceMetadata
from .normalization import document_id, normalize_path
from .ollama_quota import OllamaQuotaClient, OllamaQuotaGuard
from .reconcile import KnowledgeReconciler
from .store import KnowledgeStore, utc_now
from .vision import render_pdf_pages, representative_pdf_pages


def _is_server_error(exc: Exception) -> bool:
    status = getattr(exc, "http_status", None)
    return isinstance(status, int) and 500 <= status < 600


def _should_retry_enrichment(exc: Exception, attempts: int, max_attempts: int) -> bool:
    """Keep server failures pending; bound retries for all other failure modes."""
    if not getattr(exc, "retryable", True):
        return False
    return _is_server_error(exc) or attempts < max_attempts


def _enrichment_retry_delay(exc: Exception, attempts: int) -> int:
    """Return exponential backoff in seconds, capped at one hour."""
    exponential = min(3600, 2 ** min(max(0, attempts), 8) * 15)
    retry_after = getattr(exc, "retry_after_seconds", None)
    if isinstance(retry_after, int):
        return min(3600, max(exponential, retry_after))
    return exponential


def _page_attempt_route(primary_model: str, fallback_models: tuple[str, ...],
                        attempts_per_model: int, attempt: int) -> tuple[str, bool]:
    """Select a vision model by attempt, then fall back to primary text-only analysis."""
    models = (primary_model, *fallback_models)
    tier = (max(1, attempt) - 1) // max(1, attempts_per_model)
    if tier < len(models):
        return models[tier], True
    return primary_model, False


def _should_retry_page_enrichment(exc: Exception, attempts: int, max_attempts: int,
                                  fallback_count: int, attempts_per_model: int) -> bool:
    if not getattr(exc, "retryable", True):
        return False
    if _is_server_error(exc):
        return True
    text_only_attempt = (1 + max(0, fallback_count)) * max(1, attempts_per_model) + 1
    return attempts < max(max_attempts, text_only_attempt)


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
        self._pdf_cache_lock = threading.RLock()
        self._page_executor: ThreadPoolExecutor | None = None
        self._page_futures: set[Future[bool]] = set()
        self.ai_enabled = bool(self.config.ai_enrichment_enabled and self.config.ai_api_key)
        self.quota_guard: OllamaQuotaGuard | None = None
        if self.ai_enabled and self.config.ai_quota_enabled:
            self.quota_guard = OllamaQuotaGuard(
                OllamaQuotaClient(
                    self.config.ai_quota_cookie_header,
                    timeout_seconds=min(30, self.config.ai_timeout_seconds),
                ),
                session_limit_percent=self.config.ai_quota_session_limit_percent,
                weekly_limit_percent=self.config.ai_quota_weekly_limit_percent,
                poll_seconds=self.config.ai_quota_poll_seconds,
                max_requests_between_checks=self.config.ai_quota_max_requests_between_checks,
                reset_grace_seconds=self.config.ai_quota_reset_grace_seconds,
                failure_retry_seconds=self.config.ai_quota_failure_retry_seconds,
                state_sink=lambda value: self.store.set_state("ollama_quota", value),
            )
        elif self.ai_enabled:
            self.store.set_state("ollama_quota", {
                "enabled": False,
                "status": "disabled",
                "message": "Ollama quota admission control is disabled.",
            })
        else:
            self.store.set_state("ollama_quota", {
                "enabled": False,
                "status": "ai_disabled",
                "message": "Ollama AI analysis is disabled.",
            })
        self.enricher = OllamaEnricher(
            api_key=self.config.ai_api_key or "",
            model=self.config.ai_model,
            base_url=self.config.ai_base_url,
            timeout_seconds=self.config.ai_timeout_seconds,
            request_observer=self.quota_guard.record_request if self.quota_guard else None,
        ) if self.ai_enabled else None
        self._enrichers: dict[str, OllamaEnricher] = (
            {self.config.ai_model: self.enricher} if self.enricher else {}
        )
        self.course_priorities = self._load_course_priorities() if self.ai_enabled else {}
        self._cached_pdf_id: str | None = None
        self._cached_pdf_hash: str | None = None
        self._cached_pdf_data: bytes | None = None
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
        if (
            self.ai_enabled
            and self.config.ai_page_enrichment_enabled
            and self.config.ai_page_concurrency > 1
        ):
            self._page_executor = ThreadPoolExecutor(
                max_workers=self.config.ai_page_concurrency,
                thread_name_prefix="knowledge-page",
            )
        if self.ai_enabled:
            self.store.ensure_enrichment_jobs(
                self.config.ai_model, self.config.ai_analysis_version, self.course_priorities,
                include_pdfs=not self.config.ai_page_enrichment_enabled,
            )
            if self.config.ai_page_enrichment_enabled:
                self.store.discard_pending_page_jobs_except(set(self.course_priorities))
                self.store.ensure_page_enrichment_jobs(
                    self.config.ai_model, self.config.ai_page_analysis_version,
                    self.course_priorities,
                )
                self._queue_completed_syntheses()

    @property
    def page_synthesis_version(self) -> str:
        return (
            f"{self.config.ai_analysis_version}.pages."
            f"{self.config.ai_page_analysis_version}"
        )

    def _queue_completed_syntheses(self, course_id: int | None = None) -> int:
        if not self.config.ai_page_enrichment_enabled:
            return 0
        rows = self.store.completed_page_documents(self.config.ai_page_analysis_version)
        if course_id is not None:
            rows = [row for row in rows if row["course_id"] == course_id]
        contexts: dict[int, str] = {}
        queued = 0
        for row in rows:
            cid = int(row["course_id"])
            if cid not in self.course_priorities:
                continue
            if cid not in contexts:
                contexts[cid] = self.store.course_context_hash(cid)
            queued += int(self.store.queue_enrichment(
                row["id"], row["source_hash"], self.config.ai_model, contexts[cid],
                self.page_synthesis_version, self.course_priorities.get(cid, 0),
            ))
        return queued

    def _uploader(self, db: DatabaseManager) -> WebDAVUploader:
        if self.uploader:
            return self.uploader
        config = db.get_webdav_config()
        if not config:
            raise RuntimeError("WebDAV is not configured")
        return WebDAVUploader(config)

    def _load_course_priorities(self) -> dict[int, int]:
        db = DatabaseManager(self.config.source_db_file)
        try:
            rows = db.get_course_exam_plans()
        finally:
            db.close()
        today = datetime.now(ZoneInfo("Europe/Athens")).date()
        priorities: dict[int, int] = {}
        for row in rows:
            if not row.get("enabled") or not row.get("exam_at"):
                continue
            try:
                exam_day = datetime.fromisoformat(row["exam_at"]).date()
            except (TypeError, ValueError):
                continue
            days = (exam_day - today).days
            if days < 0:
                continue
            importance = max(0.25, float(row.get("importance") or 1.0))
            priorities[row["course_id"]] = round((10_000 - min(days, 99) * 100) * importance)
        return priorities

    def refresh_enrichment_priorities(self) -> None:
        if not self.ai_enabled:
            return
        with self.operation_lock:
            self.course_priorities = self._load_course_priorities()
            self.store.ensure_enrichment_jobs(
                self.config.ai_model, self.config.ai_analysis_version, self.course_priorities,
                include_pdfs=not self.config.ai_page_enrichment_enabled,
            )
            if self.config.ai_page_enrichment_enabled:
                self.store.discard_pending_page_jobs_except(set(self.course_priorities))
                self.store.ensure_page_enrichment_jobs(
                    self.config.ai_model, self.config.ai_page_analysis_version,
                    self.course_priorities,
                )
                self._queue_completed_syntheses()

    def _run_once(self, *, include_page_enrichment: bool = True,
                  include_synthesis: bool = True, include_standard_enrichment: bool = True) -> bool:
        job = self.store.claim_job()
        if not job:
            return self._run_enrichment_once(
                include_page_enrichment=include_page_enrichment,
                include_synthesis=include_synthesis,
                include_standard_enrichment=include_standard_enrichment,
            )
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
            self.store.replace_document(
                source, kind, chunks, extractor.__module__, warnings=extracted.warnings,
                page_count=int(count),
            )
            self.store.set_document_metrics(
                doc_id,
                document_metrics((unit.text for unit in extracted.units), source_size_bytes=len(data)),
            )
            if self.ai_enabled:
                # Descriptions mention ordering and overlap, so a changed course
                # catalog invalidates cached context for its existing files too.
                self.store.queue_course_enrichments(
                    source.course_id, self.config.ai_model, self.config.ai_analysis_version,
                    self.course_priorities.get(source.course_id, 0),
                    include_pdfs=not self.config.ai_page_enrichment_enabled,
                )
                if self.config.ai_page_enrichment_enabled:
                    if kind == "pdf" and source.course_id in self.course_priorities:
                        self.store.queue_page_enrichments(
                            doc_id, source.source_hash, int(count), self.config.ai_model,
                            self.config.ai_page_analysis_version,
                            self.course_priorities.get(source.course_id, 0),
                        )
                    # A changed course catalog must eventually refresh syntheses,
                    # but only PDFs with complete page evidence are eligible.
                    self._queue_completed_syntheses(source.course_id)
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

    def _vision_payload(self, document: dict) -> list[dict]:
        if (not self.config.ai_pdf_vision_enabled
                or document.get("document_kind") != "pdf"):
            return []
        page_numbers = representative_pdf_pages(
            int(document.get("page_count") or 0), self.config.ai_pdf_max_images
        )
        if not page_numbers:
            return []
        db = DatabaseManager(self.config.source_db_file)
        try:
            data = self._uploader(db).download_file(document["source_path"])
        except Exception:
            logging.exception("Could not download %s for visual enrichment", document["source_path"])
            return []
        finally:
            db.close()
        if not data:
            return []
        return render_pdf_pages(
            data, page_numbers,
            dpi=self.config.ai_pdf_image_dpi,
            max_dimension=self.config.ai_pdf_image_max_dimension,
            max_total_bytes=self.config.ai_pdf_image_max_bytes,
            timeout_seconds=self.config.ai_pdf_render_timeout_seconds,
        )

    def _pdf_bytes(self, document: dict[str, object]) -> bytes:
        with self._pdf_cache_lock:
            opaque_id = str(document["id"])
            source_hash = str(document.get("source_hash") or "")
            if (self._cached_pdf_id == opaque_id and self._cached_pdf_hash == source_hash
                    and self._cached_pdf_data is not None):
                return self._cached_pdf_data
            db = DatabaseManager(self.config.source_db_file)
            try:
                data = self._uploader(db).download_file(str(document["source_path"]))
            finally:
                db.close()
            if not data:
                raise EnrichmentError("PDF could not be downloaded")
            if len(data) > self.config.max_source_bytes:
                raise EnrichmentError("PDF exceeds the configured source-size limit", retryable=False)
            if (len(source_hash) == 32
                    and hashlib.md5(data).hexdigest() != source_hash.lower()):  # nosec B324
                raise EnrichmentError("downloaded PDF hash does not match indexed source")
            self._cached_pdf_id = opaque_id
            self._cached_pdf_hash = source_hash
            self._cached_pdf_data = data
            return data

    def _process_document_enrichment(self, job: dict[str, object], *, from_pages: bool) -> None:
        document, chunks, course_documents = self.store.enrichment_material(str(job["document_id"]))
        if not document or document["source_hash"] != job["source_hash"]:
            self.store.fail_enrichment(str(job["document_id"]), "source is no longer current")
            return
        if from_pages:
            records = self.store.page_enrichment_records(str(job["document_id"]), ready_only=True)
            page_payloads = []
            for record in records:
                try:
                    payload = json.loads(record.get("payload_json") or "{}")
                except (json.JSONDecodeError, TypeError):
                    payload = {}
                page_payloads.append({"page_number": record["page_number"], "payload": payload})
            payload = self.enricher.enrich_from_pages(
                document, chunks, course_documents, page_payloads,
                max_input_characters=self.config.ai_max_input_characters,
                max_page_characters=self.config.ai_page_synthesis_max_characters,
                language=self.config.ai_language,
            )
        else:
            images = self._vision_payload(document)
            payload = self.enricher.enrich(
                document, chunks, course_documents,
                max_input_characters=self.config.ai_max_input_characters,
                language=self.config.ai_language,
                images=images,
            )
        if not self.store.finish_enrichment(
            str(job["document_id"]), str(job["source_hash"]), str(job["context_hash"]),
            str(job["analysis_version"]), self.config.ai_model, payload,
        ):
            logging.info("Discarded stale enrichment for %s", job["document_id"])

    def _run_document_enrichment_once(self, analysis_version: str, *, from_pages: bool) -> bool:
        job = self.store.claim_enrichment(
            analysis_version,
            include_pdfs=from_pages or not self.config.ai_page_enrichment_enabled,
        )
        if not job:
            return False
        try:
            self._process_document_enrichment(job, from_pages=from_pages)
        except Exception as exc:
            self._record_quota_error(exc)
            error = f"{type(exc).__name__}: {str(exc)}"[:1000]
            if _should_retry_enrichment(
                exc, int(job["attempts"]), self.config.ai_max_attempts
            ):
                delay = _enrichment_retry_delay(exc, int(job["attempts"]))
                retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
                self.store.fail_enrichment(job["document_id"], error, retry_at=retry_at)
            else:
                self.store.fail_enrichment(job["document_id"], error)
                logging.warning("Document enrichment failed for %s: %s", job["document_id"], error)
        return True

    def _run_page_enrichment_once(self) -> bool:
        job = self.store.claim_page_enrichment()
        if not job:
            return False
        try:
            document, chunks = self.store.page_enrichment_material(
                job["document_id"], int(job["page_number"])
            )
            if not document or document["source_hash"] != job["source_hash"]:
                self.store.fail_page_enrichment(
                    job["document_id"], job["page_number"], "source is no longer current"
                )
                return True
            selected_model, use_vision = _page_attempt_route(
                self.config.ai_model, self.config.ai_page_fallback_models,
                self.config.ai_page_attempts_per_model, int(job["attempts"]),
            )
            image_base64 = None
            if use_vision:
                images = render_pdf_pages(
                    self._pdf_bytes(document), [int(job["page_number"])],
                    dpi=self.config.ai_pdf_image_dpi,
                    max_dimension=self.config.ai_pdf_image_max_dimension,
                    max_total_bytes=self.config.ai_pdf_image_max_bytes,
                    timeout_seconds=self.config.ai_pdf_render_timeout_seconds,
                )
                if len(images) != 1:
                    raise EnrichmentError("PDF page could not be rendered")
                image_base64 = images[0]["base64"]
            page_texts = merge_chunk_texts(chunks)
            page_text = "\n\n".join(page_texts)[:self.config.ai_page_max_text_characters]
            enricher = self._enricher_for_model(selected_model)
            payload = enricher.analyze_page(
                document, int(job["page_number"]), int(document.get("page_count") or 0),
                page_text, image_base64, language=self.config.ai_language,
            )
            if not self.store.finish_page_enrichment(
                job["document_id"], job["page_number"], job["source_hash"],
                job["analysis_version"], selected_model, payload,
            ):
                logging.info(
                    "Discarded stale page enrichment for %s page %s",
                    job["document_id"], job["page_number"],
                )
                return True
            progress = self.store.page_enrichment_progress(job["document_id"])
            if progress.get("ready", 0) == progress.get("total", 0) > 0:
                context_hash = self.store.course_context_hash(document["course_id"])
                self.store.queue_enrichment(
                    document["id"], document["source_hash"], self.config.ai_model,
                    context_hash, self.page_synthesis_version,
                    self.course_priorities.get(document["course_id"], 0),
                )
        except Exception as exc:
            self._record_quota_error(exc)
            error = f"{type(exc).__name__}: {str(exc)}"[:1000]
            if _should_retry_page_enrichment(
                exc, int(job["attempts"]), self.config.ai_max_attempts,
                len(self.config.ai_page_fallback_models),
                self.config.ai_page_attempts_per_model,
            ):
                delay = _enrichment_retry_delay(exc, int(job["attempts"]))
                retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
                self.store.fail_page_enrichment(
                    job["document_id"], job["page_number"], error, retry_at=retry_at
                )
            else:
                self.store.fail_page_enrichment(job["document_id"], job["page_number"], error)
                logging.warning(
                    "Page enrichment failed for %s page %s: %s",
                    job["document_id"], job["page_number"], error,
                )
        return True

    def _enricher_for_model(self, model: str) -> OllamaEnricher:
        enricher = self._enrichers.get(model)
        if enricher is None:
            enricher = OllamaEnricher(
                api_key=self.config.ai_api_key or "",
                model=model,
                base_url=self.config.ai_base_url,
                timeout_seconds=self.config.ai_timeout_seconds,
                request_observer=(
                    self.quota_guard.record_request if self.quota_guard else None
                ),
            )
            self._enrichers[model] = enricher
        return enricher

    def _record_quota_error(self, exc: Exception) -> None:
        if self.quota_guard and getattr(exc, "http_status", None) == 429:
            self.quota_guard.record_rate_limit(getattr(exc, "retry_after_seconds", None))

    def _run_page_enrichment_guarded_once(self) -> bool:
        if not self.enricher:
            return False
        if self.quota_guard and not self.quota_guard.before_request().allowed:
            return False
        return self._run_page_enrichment_once()

    def _run_enrichment_once(self, *, include_page_enrichment: bool = True,
                             include_synthesis: bool = True,
                             include_standard_enrichment: bool = True) -> bool:
        if not self.enricher:
            return False
        if not (include_page_enrichment or include_synthesis or include_standard_enrichment):
            return False
        if self.quota_guard and not self.quota_guard.before_request().allowed:
            return False
        if self.config.ai_page_enrichment_enabled:
            if (include_synthesis and self._run_document_enrichment_once(
                    self.page_synthesis_version, from_pages=True)):
                return True
            if include_page_enrichment and self._run_page_enrichment_once():
                return True
        if include_standard_enrichment:
            return self._run_document_enrichment_once(
                self.config.ai_analysis_version, from_pages=False
            )
        return False

    def _reap_page_pool(self) -> bool:
        if self._page_executor is None:
            return False
        worked = False
        completed = {future for future in self._page_futures if future.done()}
        for future in completed:
            self._page_futures.remove(future)
            try:
                worked = future.result() or worked
            except Exception:
                logging.exception("Parallel page enrichment task failed unexpectedly")
                worked = True
        return worked

    def _fill_page_pool(self) -> bool:
        if self._page_executor is None or self.stop_event.is_set():
            return False
        available = self.config.ai_page_concurrency - len(self._page_futures)
        pending = self.store.claimable_page_enrichment_count()
        submitted = min(max(0, available), pending)
        for _ in range(submitted):
            self._page_futures.add(
                self._page_executor.submit(self._run_page_enrichment_guarded_once)
            )
        return submitted > 0

    def run_once(self, *, include_page_enrichment: bool = True,
                 include_synthesis: bool = True,
                 include_standard_enrichment: bool = True) -> bool:
        with self.operation_lock:
            return self._run_once(
                include_page_enrichment=include_page_enrichment,
                include_synthesis=include_synthesis,
                include_standard_enrichment=include_standard_enrichment,
            )

    def run_forever(self) -> None:
        self.store.recover_claims()
        if self.ai_enabled:
            self.store.recover_enrichment_claims()
            if self.config.ai_page_enrichment_enabled:
                self.store.recover_page_enrichment_claims()
        next_reconcile = time.monotonic() + self.config.reconcile_interval_seconds
        try:
            while not self.stop_event.is_set():
                if time.monotonic() >= next_reconcile:
                    try:
                        with self.operation_lock:
                            KnowledgeReconciler(self.store, self.config.source_db_file).reconcile_all()
                            self.course_priorities = self._load_course_priorities() if self.ai_enabled else {}
                            if self.ai_enabled:
                                self.store.ensure_enrichment_jobs(
                                    self.config.ai_model, self.config.ai_analysis_version,
                                    self.course_priorities,
                                    include_pdfs=not self.config.ai_page_enrichment_enabled,
                                )
                                if self.config.ai_page_enrichment_enabled:
                                    self.store.discard_pending_page_jobs_except(
                                        set(self.course_priorities)
                                    )
                                    self.store.ensure_page_enrichment_jobs(
                                        self.config.ai_model, self.config.ai_page_analysis_version,
                                        self.course_priorities,
                                    )
                                    self._queue_completed_syntheses()
                    except Exception:
                        logging.exception("Periodic knowledge reconciliation failed")
                    next_reconcile = time.monotonic() + self.config.reconcile_interval_seconds
                try:
                    parallel_pages = self._reap_page_pool()
                    if self._page_executor is None:
                        worked = self.run_once()
                    elif self._page_futures:
                        # Keep all page slots busy unless the final page of a
                        # document just queued its higher-priority synthesis.
                        if not self.store.has_claimable_enrichment(self.page_synthesis_version):
                            parallel_pages = self._fill_page_pool() or parallel_pages
                        # Local extraction may continue beside page requests, but keep
                        # total Ollama concurrency bounded by the page-pool size.
                        worked = self.run_once(
                            include_page_enrichment=False,
                            include_synthesis=False,
                            include_standard_enrichment=False,
                        )
                    else:
                        # Preserve the original priority: completed-document synthesis,
                        # then page work, then ordinary non-PDF enrichment.
                        worked = self.run_once(
                            include_page_enrichment=False,
                            include_synthesis=True,
                            include_standard_enrichment=False,
                        )
                        if not worked:
                            parallel_pages = self._fill_page_pool() or parallel_pages
                        if not worked and not self._page_futures:
                            worked = self.run_once(
                                include_page_enrichment=False,
                                include_synthesis=False,
                                include_standard_enrichment=True,
                            )
                    self.store.set_state("worker_heartbeat", utc_now())
                except Exception:
                    worked = False
                    parallel_pages = False
                    logging.exception("Knowledge worker loop error")
                if not worked and not parallel_pages:
                    self.stop_event.wait(
                        min(1, self.config.worker_poll_seconds)
                        if self._page_futures else self.config.worker_poll_seconds
                    )
        finally:
            if self._page_executor is not None:
                self._page_executor.shutdown(wait=False, cancel_futures=True)

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
    group.add_argument("--retry-enrichment", metavar="DOCUMENT_ID")
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
        print(
            store.release_failed()
            + store.release_failed_enrichments()
            + store.release_failed_page_enrichments()
        )
    elif args.retry_enrichment:
        print(int(store.release_failed_enrichment(args.retry_enrichment)))
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
