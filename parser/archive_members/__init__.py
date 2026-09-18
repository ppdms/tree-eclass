"""Secure discovery and retrieval of virtual archive-member documents.

ZIP containers are never expanded onto their archive-provided paths.  The
functions in this module validate central-directory metadata, stream bounded
member bytes into memory, and retain the exact member chain needed to retrieve
one child again for extraction or page vision.
"""

from __future__ import annotations

from ..source_files import source_input
import zipfile

from ..extractors.base import ExtractionError, ExtractionLimits
from ._core import (
    ArchiveMemberSpec,
    ArchiveScan,
    archive_child_path,
    is_rar_bytes,
)
from ._rar_ops import discover_rar_members, read_rar_member
from ._zip import discover_zip_members, read_zip_member

__all__ = ["ArchiveMemberSpec", "archive_child_path"]


def discover_archive_members(data: bytes, limits: ExtractionLimits) -> ArchiveScan:
    """Dispatch archive discovery without trusting a filename or MIME label.

    The RAR signature is a strict prefix, so it must win the dispatch:
    ``zipfile.is_zipfile`` scans for a central directory anywhere in the
    stream and would otherwise misread a RAR that stores a ZIP member.
    """

    if is_rar_bytes(data):
        return discover_rar_members(data, limits)
    if zipfile.is_zipfile(source_input(data)):
        return discover_zip_members(data, limits)
    raise ExtractionError(
        "archive format is not supported for virtual members",
        reason="archive_invalid",
    )


def read_archive_member(
    data: bytes,
    archive_format: str,
    member_chain: tuple[str, ...],
    limits: ExtractionLimits,
    *,
    expected_hash: str,
    expected_crc32: int,
    expected_compressed_size: int,
    expected_expanded_size: int,
) -> bytes:
    """Dispatch a virtual-member read using its persisted container format."""

    reader = {
        "zip": read_zip_member,
        "rar": read_rar_member,
    }.get(str(archive_format).casefold())
    if reader is None:
        raise ExtractionError(
            f"unsupported persisted archive format: {archive_format}",
            reason="archive_invalid",
        )
    return reader(
        data,
        member_chain,
        limits,
        expected_hash=expected_hash,
        expected_crc32=expected_crc32,
        expected_compressed_size=expected_compressed_size,
        expected_expanded_size=expected_expanded_size,
    )
