"""Configuration for the rebuildable course knowledge index."""

from dataclasses import dataclass
import os
from typing import Optional


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _float(name: str, default: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    try:
        return min(maximum, max(minimum, float(os.getenv(name, str(default)))))
    except ValueError:
        return default


def _csv(name: str, default: str) -> tuple[str, ...]:
    value = os.getenv(name, default)
    return tuple(item.strip() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class KnowledgeConfig:
    db_file: str
    source_db_file: str
    enabled: bool
    worker_enabled: bool
    worker_poll_seconds: int
    reconcile_interval_seconds: int
    max_source_bytes: int
    max_extracted_chars: int
    max_units: int
    archive_max_members: int
    archive_max_member_bytes: int
    archive_max_expanded_bytes: int
    archive_max_ratio: int
    search_limit_max: int
    cross_course_search_limit_max: int
    read_max_chars: int
    worker_max_attempts: int
    ocr_enabled: bool
    ocr_languages: str
    ocr_dpi: int
    ocr_page_timeout_seconds: int
    ocr_max_pages: int
    embedding_backend: str
    embedding_model: str
    embedding_api_key: Optional[str]
    embedding_base_url: str
    embedding_timeout_seconds: int
    embedding_batch_size: int
    embedding_local_fallback: bool
    ai_enrichment_enabled: bool
    ai_model: str
    ai_api_key: Optional[str]
    ai_base_url: str
    ai_timeout_seconds: int
    ai_max_input_characters: int
    ai_max_attempts: int
    ai_language: str
    ai_analysis_version: str
    ai_pdf_vision_enabled: bool
    ai_pdf_max_images: int
    ai_pdf_image_dpi: int
    ai_pdf_image_max_dimension: int
    ai_pdf_image_max_bytes: int
    ai_pdf_render_timeout_seconds: int
    ai_page_enrichment_enabled: bool
    ai_page_concurrency: int
    ai_page_analysis_version: str
    ai_page_fallback_models: tuple[str, ...]
    ai_page_attempts_per_model: int
    ai_page_max_text_characters: int
    ai_page_synthesis_max_characters: int
    ai_quota_enabled: bool
    ai_quota_cookie_header: Optional[str]
    ai_quota_session_limit_percent: float
    ai_quota_weekly_limit_percent: float
    ai_quota_poll_seconds: int
    ai_quota_max_requests_between_checks: int
    ai_quota_reset_grace_seconds: int
    ai_quota_failure_retry_seconds: int
    mcp_http_enabled: bool

    @classmethod
    def from_env(cls) -> "KnowledgeConfig":
        source_db = os.getenv("DB_FILE", "eclass.db")
        default_knowledge = os.path.join(os.path.dirname(source_db) or ".", "knowledge.db")
        embedding_backend = os.getenv("KNOWLEDGE_EMBEDDING_BACKEND", "openrouter").strip().lower()
        embedding_base_url = os.getenv(
            "KNOWLEDGE_EMBEDDING_BASE_URL",
            "https://api.openai.com/v1" if embedding_backend == "openai" else "https://openrouter.ai/api/v1",
        )
        embedding_api_key = os.getenv("KNOWLEDGE_EMBEDDING_API_KEY") or os.getenv(
            "OPENAI_API_KEY" if embedding_backend == "openai" else "OPENROUTER_API_KEY"
        )
        ollama_api_key = os.getenv("OLLAMA_API_KEY")
        return cls(
            db_file=os.getenv("KNOWLEDGE_DB_FILE", default_knowledge),
            source_db_file=source_db,
            enabled=_bool("KNOWLEDGE_ENABLED", True),
            worker_enabled=_bool("KNOWLEDGE_WORKER_ENABLED", True),
            worker_poll_seconds=_int("KNOWLEDGE_WORKER_POLL_SECONDS", 5, 1),
            reconcile_interval_seconds=_int("KNOWLEDGE_RECONCILE_INTERVAL_SECONDS", 21_600, 60),
            max_source_bytes=_int("KNOWLEDGE_MAX_SOURCE_MB", 200, 1) * 1024 * 1024,
            max_extracted_chars=_int("KNOWLEDGE_MAX_EXTRACTED_CHARS", 10_000_000, 1000),
            max_units=_int("KNOWLEDGE_MAX_UNITS", 10_000, 1),
            archive_max_members=_int("KNOWLEDGE_ARCHIVE_MAX_MEMBERS", 1000, 1),
            archive_max_member_bytes=_int("KNOWLEDGE_ARCHIVE_MAX_MEMBER_MB", 50, 1) * 1024 * 1024,
            archive_max_expanded_bytes=_int("KNOWLEDGE_ARCHIVE_MAX_EXPANDED_MB", 500, 1) * 1024 * 1024,
            archive_max_ratio=_int("KNOWLEDGE_ARCHIVE_MAX_RATIO", 100, 1),
            search_limit_max=_int("KNOWLEDGE_SEARCH_LIMIT_MAX", 20, 1),
            cross_course_search_limit_max=_int("KNOWLEDGE_CROSS_COURSE_SEARCH_LIMIT_MAX", 12, 1),
            read_max_chars=_int("KNOWLEDGE_READ_MAX_CHARS", 50_000, 1000),
            worker_max_attempts=_int("KNOWLEDGE_WORKER_MAX_ATTEMPTS", 5, 1),
            ocr_enabled=_bool("KNOWLEDGE_OCR_ENABLED", False),
            ocr_languages=os.getenv("KNOWLEDGE_OCR_LANGUAGES", "ell+eng"),
            ocr_dpi=_int("KNOWLEDGE_OCR_DPI", 200, 72),
            ocr_page_timeout_seconds=_int("KNOWLEDGE_OCR_PAGE_TIMEOUT_SECONDS", 60, 1),
            ocr_max_pages=_int("KNOWLEDGE_OCR_MAX_PAGES", 200, 1),
            embedding_backend=embedding_backend,
            embedding_model=os.getenv("KNOWLEDGE_EMBEDDING_MODEL", "openai/text-embedding-3-small"),
            embedding_api_key=embedding_api_key,
            embedding_base_url=embedding_base_url,
            embedding_timeout_seconds=_int("KNOWLEDGE_EMBEDDING_TIMEOUT_SECONDS", 30, 1),
            embedding_batch_size=_int("KNOWLEDGE_EMBEDDING_BATCH_SIZE", 32, 1),
            embedding_local_fallback=_bool("KNOWLEDGE_EMBEDDING_LOCAL_FALLBACK", True),
            ai_enrichment_enabled=_bool("KNOWLEDGE_AI_ENABLED", bool(ollama_api_key)),
            ai_model=os.getenv("KNOWLEDGE_AI_MODEL", "qwen3.5:397b").strip(),
            ai_api_key=ollama_api_key,
            ai_base_url=os.getenv("KNOWLEDGE_AI_BASE_URL", "https://ollama.com").strip(),
            ai_timeout_seconds=_int("KNOWLEDGE_AI_TIMEOUT_SECONDS", 180, 1),
            ai_max_input_characters=_int("KNOWLEDGE_AI_MAX_INPUT_CHARS", 30_000, 2000),
            ai_max_attempts=_int("KNOWLEDGE_AI_MAX_ATTEMPTS", 4, 1),
            ai_language=os.getenv("KNOWLEDGE_AI_LANGUAGE", "English").strip() or "English",
            ai_analysis_version=os.getenv("KNOWLEDGE_AI_ANALYSIS_VERSION", "2").strip() or "2",
            ai_pdf_vision_enabled=_bool("KNOWLEDGE_AI_PDF_VISION_ENABLED", True),
            ai_pdf_max_images=_int("KNOWLEDGE_AI_PDF_MAX_IMAGES", 4, 1),
            ai_pdf_image_dpi=_int("KNOWLEDGE_AI_PDF_IMAGE_DPI", 120, 72),
            ai_pdf_image_max_dimension=_int("KNOWLEDGE_AI_PDF_IMAGE_MAX_DIMENSION", 1600, 600),
            ai_pdf_image_max_bytes=_int("KNOWLEDGE_AI_PDF_IMAGE_MAX_MB", 12, 1) * 1024 * 1024,
            ai_pdf_render_timeout_seconds=_int("KNOWLEDGE_AI_PDF_RENDER_TIMEOUT_SECONDS", 45, 1),
            ai_page_enrichment_enabled=_bool("KNOWLEDGE_AI_PAGE_ENABLED", True),
            ai_page_concurrency=min(32, _int("KNOWLEDGE_AI_PAGE_CONCURRENCY", 10, 1)),
            ai_page_analysis_version=os.getenv("KNOWLEDGE_AI_PAGE_ANALYSIS_VERSION", "1").strip() or "1",
            ai_page_fallback_models=_csv(
                "KNOWLEDGE_AI_PAGE_FALLBACK_MODELS", "gemma4:31b,kimi-k2.6"
            ),
            ai_page_attempts_per_model=_int(
                "KNOWLEDGE_AI_PAGE_ATTEMPTS_PER_MODEL", 3, 1
            ),
            ai_page_max_text_characters=_int("KNOWLEDGE_AI_PAGE_MAX_TEXT_CHARS", 12_000, 500),
            ai_page_synthesis_max_characters=_int(
                "KNOWLEDGE_AI_PAGE_SYNTHESIS_MAX_CHARS", 300_000, 10_000
            ),
            ai_quota_enabled=_bool("KNOWLEDGE_AI_QUOTA_ENABLED", True),
            ai_quota_cookie_header=os.getenv("OLLAMA_COOKIE_HEADER"),
            ai_quota_session_limit_percent=_float(
                "KNOWLEDGE_AI_QUOTA_SESSION_LIMIT_PERCENT", 95.0
            ),
            ai_quota_weekly_limit_percent=_float(
                "KNOWLEDGE_AI_QUOTA_WEEKLY_LIMIT_PERCENT", 95.0
            ),
            ai_quota_poll_seconds=_int("KNOWLEDGE_AI_QUOTA_POLL_SECONDS", 60, 10),
            ai_quota_max_requests_between_checks=_int(
                "KNOWLEDGE_AI_QUOTA_MAX_REQUESTS_BETWEEN_CHECKS", 20, 1
            ),
            ai_quota_reset_grace_seconds=_int(
                "KNOWLEDGE_AI_QUOTA_RESET_GRACE_SECONDS", 30, 0
            ),
            ai_quota_failure_retry_seconds=_int(
                "KNOWLEDGE_AI_QUOTA_FAILURE_RETRY_SECONDS", 300, 30
            ),
            mcp_http_enabled=_bool("MCP_HTTP_ENABLED", True),
        )
