"""Extractor registry and shared limits."""

from dataclasses import dataclass
import mimetypes
import os
from pathlib import Path
from typing import Callable

from ..models import ExtractedDocument, SourceMetadata


class ExtractionError(RuntimeError):
    """An extraction failure with a stable operator-facing diagnostic reason."""

    def __init__(self, message: str, reason: str = "parser_failure"):
        super().__init__(message)
        self.reason = reason


class ExtractionLimitError(ExtractionError):
    def __init__(self, message: str, reason: str = "maximum_size"):
        super().__init__(message, reason=reason)


@dataclass(frozen=True)
class ExtractionLimits:
    max_characters: int = 5_000_000
    max_units: int = 10_000
    archive_max_members: int = 1000
    archive_max_member_bytes: int = 50 * 1024 * 1024
    archive_max_expanded_bytes: int = 500 * 1024 * 1024
    archive_max_ratio: int = 100
    ocr_enabled: bool = False
    ocr_languages: str = "ell+eng"
    ocr_dpi: int = 200
    ocr_page_timeout_seconds: int = 60
    ocr_max_pages: int = 200


Extractor = Callable[[str, SourceMetadata, ExtractionLimits], ExtractedDocument]


EXTENSION_KINDS = {
    ".pdf": "pdf", ".pptx": "presentation", ".docx": "document", ".xlsx": "spreadsheet",
    ".html": "html", ".htm": "html", ".ipynb": "notebook", ".zip": "archive",
    ".tar": "archive", ".tgz": "archive", ".gz": "archive",
    ".txt": "text", ".md": "text", ".rst": "text", ".csv": "text", ".tsv": "text",
    ".py": "source", ".js": "source", ".ts": "source", ".java": "source", ".c": "source",
    ".h": "source", ".cpp": "source", ".hpp": "source", ".go": "source", ".rs": "source",
    ".sql": "source", ".sh": "source", ".css": "source", ".json": "text", ".xml": "text",
    ".yaml": "text", ".yml": "text", ".tex": "source",
}

MIME_KINDS = {
    "application/pdf": "pdf",
    "application/zip": "archive",
    "application/x-tar": "archive",
    "application/gzip": "archive",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "presentation",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "spreadsheet",
    "text/html": "html",
}


def source_kind(name: str, mime_type: str | None = None) -> str | None:
    suffixes = Path(name).suffixes
    extension = Path(name).suffix.lower()
    if len(suffixes) >= 2 and "".join(suffixes[-2:]).lower() == ".tar.gz":
        extension = ".tgz"
    kind = EXTENSION_KINDS.get(extension)
    if kind:
        return kind
    guessed = mime_type or mimetypes.guess_type(name)[0] or ""
    if guessed in MIME_KINDS:
        return MIME_KINDS[guessed]
    if guessed.startswith("text/"):
        return "text"
    return None


def guess_mime(name: str) -> str | None:
    return mimetypes.guess_type(name)[0]


def sniff_mime(data: bytes) -> str | None:
    """Return a conservative MIME guess from bytes, independent of the filename."""
    header = data[:4096]
    if b"%PDF-" in header[:1024]:
        return "application/pdf"
    stripped = header.lstrip().lower()
    if stripped.startswith((b"<!doctype html", b"<html", b"<head", b"<body")):
        return "text/html"
    if data.startswith(b"PK\x03\x04"):
        return "application/zip"
    return None


def detect_source(name: str, declared_mime: str | None, data: bytes) -> tuple[str | None, str | None, str | None]:
    """Detect an extractable source kind and explain content that cannot be routed.

    The filename remains the primary signal for ordinary files. Magic-byte detection
    takes precedence for PDFs and HTML authentication/error responses so a misleading
    WebDAV MIME type cannot prevent the appropriate diagnostic or extractor from being
    selected.
    """
    detected_mime = sniff_mime(data)
    suffix = Path(name).suffix.lower()
    if detected_mime == "text/html" and suffix not in {".html", ".htm"}:
        return None, detected_mime, "download_html"
    if detected_mime == "application/pdf":
        return "pdf", detected_mime, None

    kind = source_kind(name, detected_mime or declared_mime)
    if kind:
        return kind, detected_mime or declared_mime, None
    if detected_mime == "application/zip":
        return "archive", detected_mime, None
    if declared_mime:
        return None, detected_mime or declared_mime, "unsupported_mime_type"
    return None, detected_mime, "unsupported_extension"


def enforce_limits(document: ExtractedDocument, limits: ExtractionLimits) -> ExtractedDocument:
    if len(document.units) > limits.max_units:
        raise ExtractionLimitError(f"unit limit exceeded ({len(document.units)} > {limits.max_units})")
    total = sum(len(unit.text) for unit in document.units)
    if total > limits.max_characters:
        raise ExtractionLimitError(f"extracted character limit exceeded ({total} > {limits.max_characters})")
    return document


def extractor_for(path: str, mime_type: str | None = None, kind: str | None = None) -> tuple[str, Extractor]:
    kind = kind or source_kind(path, mime_type)
    if kind == "pdf":
        from .pdf import extract
    elif kind == "presentation":
        from .pptx import extract
    elif kind == "document":
        from .docx import extract
    elif kind == "spreadsheet":
        from .xlsx import extract
    elif kind == "html":
        from .html import extract
    elif kind in {"text", "source"}:
        from .text import extract
    elif kind == "notebook":
        from .notebook import extract
    elif kind == "archive":
        from .archive import extract
    else:
        raise ExtractionError(f"unsupported source type: {os.path.basename(path)}")
    return kind, extract
