"""Stable identities and Greek/English search normalization."""

import hashlib
import re
import unicodedata


def normalize_path(path: str) -> str:
    value = unicodedata.normalize("NFC", path.strip())
    value = re.sub(r"/+", "/", value)
    return "/" + value.strip("/") if value.strip("/") else "/"


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def search_normalize(text: str) -> str:
    folded = unicodedata.normalize("NFD", normalize_text(text).casefold())
    without_marks = "".join(ch for ch in folded if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", without_marks)).strip()


def _stable_id(prefix: str, *parts: object) -> str:
    raw = "\0".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(raw).hexdigest()[:32]}"


def document_id(course_id: int, source_path: str) -> str:
    return _stable_id("doc", course_id, normalize_path(source_path))


def chunk_id(document: str, source_hash: str, locator_type: str,
             locator_start: str, locator_end: str | None, ordinal: int) -> str:
    return _stable_id("chk", document, source_hash, locator_type, locator_start, locator_end or "", ordinal)


def text_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()
