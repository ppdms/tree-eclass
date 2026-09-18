"""RAR discovery and member-read entry points built on 7z and nested ZIP walk."""

from __future__ import annotations

from io import BytesIO
import hashlib
from ..source_files import source_path, source_size
import zipfile

from ._core import (
    MAX_NESTED_ZIP_DEPTH,
    SUPPORTED_MEMBER_KINDS,
    ArchiveMemberSpec,
    ArchiveScan,
    ExtractionError,
    ExtractionLimits,
    _Budget,
    _append_discovered,
    _canonical_member_name,
    _collision_key,
    _diagnostic,
    _is_metadata_member,
    _verified_kind,
    is_rar_bytes,
    source_kind,
)
from ._rar import _SevenZipEntry, _rar_entries, _read_rar_entry
from ._zip import _discover_zip_walk, read_zip_member


def discover_rar_members(data: bytes, limits: ExtractionLimits) -> ArchiveScan:
    """Discover supported RAR leaves through a bounded local 7z process."""

    if not is_rar_bytes(data):
        raise ExtractionError("could not identify RAR archive", reason="archive_invalid")
    budget = _Budget()
    warnings: list[str] = []
    discovered: list[ArchiveMemberSpec] = []
    virtual_paths: set[str] = set()
    with source_path(data, suffix=".rar") as archive_file:
        files = _rar_entries(archive_file, source_size(data), limits, budget)
        for name, entry in files.items():
            if _is_metadata_member(name):
                continue
            if source_kind(name) == "archive" and name.casefold().endswith(".zip"):
                _discover_rar_nested(
                    archive_file,
                    name,
                    entry,
                    limits,
                    budget,
                    discovered,
                    warnings,
                    virtual_paths,
                )
                continue
            candidate = source_kind(name)
            if candidate not in SUPPORTED_MEMBER_KINDS:
                continue
            _discover_rar_leaf(
                archive_file,
                name,
                entry,
                limits,
                budget,
                discovered,
                warnings,
                virtual_paths,
            )
    discovered.sort(key=lambda item: _collision_key(item.member_path))
    return ArchiveScan(tuple(discovered), tuple(warnings))


def _discover_rar_nested(
    archive_path: str,
    name: str,
    entry: _SevenZipEntry,
    limits: ExtractionLimits,
    budget: _Budget,
    discovered: list[ArchiveMemberSpec],
    warnings: list[str],
    virtual_paths: set[str],
) -> None:
    nested_data = _read_rar_entry(archive_path, entry, limits)
    if not zipfile.is_zipfile(BytesIO(nested_data)):
        warnings.append(f"skipped invalid nested ZIP: {name[:300]}")
        return
    _discover_zip_walk(
        nested_data,
        limits,
        budget,
        discovered,
        warnings,
        virtual_paths,
        prefix=(name,),
        depth=1,
        root_format="rar",
    )


def _discover_rar_leaf(
    archive_path: str,
    name: str,
    entry: _SevenZipEntry,
    limits: ExtractionLimits,
    budget: _Budget,
    discovered: list[ArchiveMemberSpec],
    warnings: list[str],
    virtual_paths: set[str],
) -> None:
    member_data = _read_rar_entry(archive_path, entry, limits)
    kind, mime_type = _verified_kind(name, member_data)
    if kind is None:
        warnings.append(f"skipped RAR member whose content did not match its supported type: {name[:300]}")
        return
    _append_discovered(
        discovered,
        virtual_paths,
        chain=(name,),
        kind=kind,
        mime_type=mime_type,
        depth=0,
        crc32=entry.crc32,
        compressed_size=entry.packed_size,
        expanded_size=entry.size,
        member_data=member_data,
        archive_format="rar",
    )


def _resolve_rar_outer(
    data: bytes,
    member_chain: tuple[str, ...],
    limits: ExtractionLimits,
    budget: _Budget,
) -> tuple[_SevenZipEntry, str, bytes]:
    with source_path(data, suffix=".rar") as archive_file:
        files = _rar_entries(archive_file, source_size(data), limits, budget)
        outer_name = _canonical_member_name(member_chain[0])
        outer = files.get(outer_name)
        if outer is None:
            raise _diagnostic(f"RAR member no longer exists: {outer_name[:200]}", reason="archive_member_changed")
        outer_data = _read_rar_entry(archive_file, outer, limits)
    return outer, outer_name, outer_data


def _read_nested_rar_zip(
    outer_data: bytes,
    outer_name: str,
    member_chain: tuple[str, ...],
    limits: ExtractionLimits,
    budget: _Budget,
    *,
    expected_hash: str,
    expected_crc32: int,
    expected_compressed_size: int,
    expected_expanded_size: int,
) -> bytes | None:
    """Resolve a member of one nested ZIP, or None for a direct RAR member."""
    if len(member_chain) != 2:
        return None
    if not outer_name.casefold().endswith(".zip") or not zipfile.is_zipfile(BytesIO(outer_data)):
        raise _diagnostic("RAR member chain no longer resolves through a ZIP", reason="archive_member_changed")
    return read_zip_member(
        outer_data,
        (member_chain[1],),
        limits,
        expected_hash=expected_hash,
        expected_crc32=expected_crc32,
        expected_compressed_size=expected_compressed_size,
        expected_expanded_size=expected_expanded_size,
        _budget=budget,
    )


def read_rar_member(
    data: bytes,
    member_chain: tuple[str, ...],
    limits: ExtractionLimits,
    *,
    expected_hash: str,
    expected_crc32: int,
    expected_compressed_size: int,
    expected_expanded_size: int,
) -> bytes:
    """Resolve one direct RAR member or a member of one nested ZIP."""

    if not is_rar_bytes(data) or not member_chain or len(member_chain) > MAX_NESTED_ZIP_DEPTH + 1:
        raise _diagnostic("RAR member chain is invalid", reason="archive_member_changed")
    budget = _Budget()
    outer, outer_name, outer_data = _resolve_rar_outer(data, member_chain, limits, budget)
    nested = _read_nested_rar_zip(
        outer_data,
        outer_name,
        member_chain,
        limits,
        budget,
        expected_hash=expected_hash,
        expected_crc32=expected_crc32,
        expected_compressed_size=expected_compressed_size,
        expected_expanded_size=expected_expanded_size,
    )
    if nested is not None:
        return nested

    return _verify_rar_identity(
        outer,
        outer_name,
        outer_data,
        expected_hash,
        expected_crc32,
        expected_compressed_size,
        expected_expanded_size,
    )


def _verify_rar_identity(
    outer: _SevenZipEntry,
    outer_name: str,
    outer_data: bytes,
    expected_hash: str,
    expected_crc32: int,
    expected_compressed_size: int,
    expected_expanded_size: int,
) -> bytes:
    if (
        outer.crc32 != int(expected_crc32)
        or outer.packed_size != int(expected_compressed_size)
        or outer.size != int(expected_expanded_size)
        or hashlib.sha256(outer_data).hexdigest() != expected_hash
    ):
        raise _diagnostic(
            "RAR member no longer matches its indexed identity",
            reason="archive_member_changed",
        )
    kind, _mime = _verified_kind(outer_name, outer_data)
    if kind is None:
        raise _diagnostic(
            "RAR member content no longer matches its supported type",
            reason="archive_member_changed",
        )
    return outer_data
