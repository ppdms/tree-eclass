"""Shared constants, dataclasses, and validation helpers for archive members."""

from __future__ import annotations

from ..source_files import source_prefix
from dataclasses import dataclass
import hashlib
import os
import signal
from pathlib import PurePosixPath
import unicodedata
from urllib.parse import quote
import zipfile

from ..extractors.base import (
    ExtractionError,
    ExtractionLimitError,
    ExtractionLimits,
    guess_mime,
    sniff_mime,
    source_kind,
)

__all__ = ["ExtractionLimitError", "ExtractionLimits"]


SUPPORTED_MEMBER_KINDS = frozenset({"pdf", "image", "text", "html", "source", "notebook"})
ZIP_COMPRESSION_METHODS = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})
MAX_NESTED_ZIP_DEPTH = 1
MAX_MEMBER_PATH_BYTES = 4096
MAX_MEMBER_COMPONENT_BYTES = 255
MAX_MEMBER_PATH_PARTS = 32
READ_CHUNK_BYTES = 64 * 1024
ARCHIVE_CHILD_ROOT = "/.tree-eclass/archive-members"
RAR4_MAGIC = b"Rar!\x1a\x07\x00"
RAR5_MAGIC = b"Rar!\x1a\x07\x01\x00"
SEVEN_ZIP_LIST_TIMEOUT_SECONDS = 30
SEVEN_ZIP_EXTRACT_TIMEOUT_SECONDS = 60
SEVEN_ZIP_MAX_LIST_BYTES = 8 * 1024 * 1024
SEVEN_ZIP_MAX_ERROR_BYTES = 64 * 1024
SEVEN_ZIP_MAX_ADDRESS_SPACE_BYTES = 1024 * 1024 * 1024
SEVEN_ZIP_MAX_OPEN_FILES = 64


@dataclass(frozen=True)
class ArchiveMemberSpec:
    """One supported leaf that should become a normal child document."""

    member_path: str
    member_chain: tuple[str, ...]
    kind: str
    mime_type: str | None
    depth: int
    crc32: int
    compressed_size: int
    expanded_size: int
    content_hash: str
    archive_format: str = "zip"


@dataclass(frozen=True)
class ArchiveScan:
    members: tuple[ArchiveMemberSpec, ...]
    warnings: tuple[str, ...] = ()


@dataclass
class _Budget:
    members: int = 0
    expanded_bytes: int = 0


def archive_child_path(parent_document_id: str, member_path: str) -> str:
    """Return a reserved path that cannot overlap the scanned WebDAV tree."""

    parent_key = quote(str(parent_document_id), safe="")
    member_key = quote(unicodedata.normalize("NFC", member_path), safe="")
    return f"{ARCHIVE_CHILD_ROOT}/{parent_key}/{member_key}"


def _diagnostic(message: str, reason: str = "archive_unsafe_member") -> ExtractionError:
    return ExtractionError(message, reason=reason)


def _canonical_member_name(name: str) -> str:
    raw = str(name or "")
    if not raw or "\x00" in raw or any(ord(character) < 32 or ord(character) == 127 for character in raw):
        raise _diagnostic("archive member has an empty or control-character-containing path")
    raw = unicodedata.normalize("NFC", raw.replace("\\", "/"))
    if raw.startswith(("/", "//")):
        raise _diagnostic(f"archive member uses an absolute path: {raw[:200]}")
    if len(raw.encode("utf-8")) > MAX_MEMBER_PATH_BYTES:
        raise _diagnostic("archive member path exceeds the configured path limit")
    path = PurePosixPath(raw)
    parts = path.parts
    if (
        not parts
        or ".." in parts
        or any(not part or part == "." for part in parts)
        or len(parts) > MAX_MEMBER_PATH_PARTS
    ):
        raise _diagnostic(f"archive member path is unsafe: {raw[:200]}")
    first = parts[0]
    if len(first) >= 2 and first[1] == ":" and first[0].isalpha():
        raise _diagnostic(f"archive member uses a drive-qualified path: {raw[:200]}")
    if any(len(part.encode("utf-8")) > MAX_MEMBER_COMPONENT_BYTES for part in parts):
        raise _diagnostic("archive member path component exceeds the configured limit")
    return "/".join(parts)


def _collision_key(name: str) -> str:
    return unicodedata.normalize("NFKC", name).casefold()


def _is_metadata_member(name: str) -> bool:
    path = PurePosixPath(name)
    return "__MACOSX" in path.parts or path.name.startswith("._")


def _verified_kind(name: str, data: bytes) -> tuple[str | None, str | None]:
    kind = source_kind(name)
    if kind not in SUPPORTED_MEMBER_KINDS:
        return None, None
    if not data:
        return None, None
    magic = sniff_mime(data)
    if kind == "pdf" and magic != "application/pdf":
        return None, None
    if kind == "image":
        if magic not in {"image/jpeg", "image/png"}:
            return None, None
        return kind, magic
    if kind == "html" and magic and magic != "text/html":
        return None, None
    return kind, magic or guess_mime(name)


def _virtual_member_path(chain: tuple[str, ...]) -> str:
    return "!/".join(chain)


def _append_discovered(
    discovered: list[ArchiveMemberSpec],
    virtual_paths: set[str],
    *,
    chain: tuple[str, ...],
    kind: str,
    mime_type: str | None,
    depth: int,
    crc32: int,
    compressed_size: int,
    expanded_size: int,
    member_data: bytes,
    archive_format: str,
) -> None:
    member_path = _virtual_member_path(chain)
    collision = _collision_key(member_path)
    if collision in virtual_paths:
        raise _diagnostic("archive contains duplicate normalized virtual member paths")
    virtual_paths.add(collision)
    discovered.append(
        ArchiveMemberSpec(
            member_path=member_path,
            member_chain=chain,
            kind=kind,
            mime_type=mime_type,
            depth=depth,
            crc32=crc32,
            compressed_size=compressed_size,
            expanded_size=expanded_size,
            content_hash=hashlib.sha256(member_data).hexdigest(),
            archive_format=archive_format,
        )
    )


def is_rar_bytes(data: bytes) -> bool:
    return source_prefix(data).startswith((RAR4_MAGIC, RAR5_MAGIC))


def _kill_process(process):
    """Keep Go-owned helpers in their cancellation group, including 7-Zip."""
    if os.getpgid(process.pid) == process.pid:
        os.killpg(process.pid, signal.SIGKILL)
    else:
        process.kill()
