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
            mcp_http_enabled=_bool("MCP_HTTP_ENABLED", True),
        )
