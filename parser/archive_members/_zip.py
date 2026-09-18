"""ZIP archive member discovery and bounded retrieval."""

from __future__ import annotations

from ..source_files import source_input
from io import BytesIO
import hashlib
import stat
import zipfile

from ._core import (
    MAX_NESTED_ZIP_DEPTH,
    READ_CHUNK_BYTES,
    ZIP_COMPRESSION_METHODS,
    ArchiveMemberSpec,
    ArchiveScan,
    ExtractionLimitError,
    ExtractionLimits,
    _Budget,
    _append_discovered,
    _canonical_member_name,
    _collision_key,
    _diagnostic,
    _is_metadata_member,
    SUPPORTED_MEMBER_KINDS,
    _verified_kind,
    _virtual_member_path,
    source_kind,
)


def _validate_file_type(info: zipfile.ZipInfo, name: str) -> None:
    mode = (int(info.external_attr) >> 16) & 0xFFFF
    file_type = stat.S_IFMT(mode)
    if file_type and not stat.S_ISREG(mode):
        raise _diagnostic(f"archive member is a link or special file: {name[:200]}")


def _validated_files(
    archive: zipfile.ZipFile,
    limits: ExtractionLimits,
    budget: _Budget,
) -> dict[str, zipfile.ZipInfo]:
    files: dict[str, zipfile.ZipInfo] = {}
    collision_keys: set[str] = set()
    for info in archive.infolist():
        if info.is_dir():
            continue
        budget.members += 1
        if budget.members > limits.archive_max_members:
            raise ExtractionLimitError(
                "archive member-count limit exceeded",
                reason="archive_member_limit",
            )
        name = _validated_member_name(info, limits, budget, collision_keys)
        files[name] = info
    return files


def _validated_member_name(
    info: zipfile.ZipInfo,
    limits: ExtractionLimits,
    budget: _Budget,
    collision_keys: set[str],
) -> str:
    name = _canonical_member_name(info.filename)
    collision = _collision_key(name)
    if collision in collision_keys:
        raise _diagnostic(f"archive contains duplicate normalized member paths: {name[:200]}")
    collision_keys.add(collision)
    _validate_file_type(info, name)
    if info.flag_bits & 0x1:
        raise _diagnostic(
            f"archive member is encrypted: {name[:200]}",
            reason="archive_encrypted_member",
        )
    if info.compress_type not in ZIP_COMPRESSION_METHODS:
        raise _diagnostic(f"archive member uses an unsupported compression method: {name[:200]}")
    _charge_member_budget(info, name, limits, budget)
    return name


def _charge_member_budget(
    info: zipfile.ZipInfo,
    name: str,
    limits: ExtractionLimits,
    budget: _Budget,
) -> None:
    if info.file_size < 0 or info.compress_size < 0:
        raise _diagnostic(f"archive member has invalid sizes: {name[:200]}")
    if info.file_size > limits.archive_max_member_bytes:
        raise ExtractionLimitError(
            f"archive member-size limit exceeded: {name[:200]}",
            reason="archive_member_limit",
        )
    budget.expanded_bytes += info.file_size
    if budget.expanded_bytes > limits.archive_max_expanded_bytes:
        raise ExtractionLimitError(
            "archive expanded-size limit exceeded",
            reason="archive_expanded_limit",
        )
    if info.file_size and not info.compress_size:
        raise ExtractionLimitError(
            f"archive member has an unbounded compression ratio: {name[:200]}",
            reason="archive_ratio_limit",
        )
    if info.compress_size and info.file_size / info.compress_size > limits.archive_max_ratio:
        raise ExtractionLimitError(
            f"archive compression-ratio limit exceeded: {name[:200]}",
            reason="archive_ratio_limit",
        )


def _read_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    limits: ExtractionLimits,
) -> bytes:
    expected = int(info.file_size)
    result = bytearray()
    try:
        with archive.open(info, "r") as source:
            while True:
                block = source.read(READ_CHUNK_BYTES)
                if not block:
                    break
                result.extend(block)
                if len(result) > expected or len(result) > limits.archive_max_member_bytes:
                    raise ExtractionLimitError(
                        "archive member produced more bytes than declared",
                        reason="archive_member_limit",
                    )
    except (RuntimeError, NotImplementedError, zipfile.BadZipFile) as exc:
        raise _diagnostic(
            f"archive member could not be read safely: {info.filename[:200]}",
            reason="archive_invalid",
        ) from exc
    if len(result) != expected:
        raise _diagnostic(
            f"archive member size did not match its directory entry: {info.filename[:200]}",
            reason="archive_invalid",
        )
    return bytes(result)


def _discover_zip_entry(
    archive: zipfile.ZipFile,
    name: str,
    info: zipfile.ZipInfo,
    chain: tuple[str, ...],
    depth: int,
    root_format: str,
    limits: ExtractionLimits,
    budget: _Budget,
    discovered: list[ArchiveMemberSpec],
    warnings: list[str],
    virtual_paths: set[str],
) -> None:
    if source_kind(name) == "archive" and name.casefold().endswith(".zip"):
        _discover_nested_zip(
            archive,
            info,
            name,
            chain,
            depth,
            root_format,
            limits,
            budget,
            discovered,
            warnings,
            virtual_paths,
        )
        return
    if source_kind(name) not in SUPPORTED_MEMBER_KINDS:
        return
    _discover_zip_leaf(
        archive,
        info,
        name,
        chain,
        depth,
        root_format,
        limits,
        budget,
        discovered,
        warnings,
        virtual_paths,
    )


def _discover_zip_walk(
    archive_data: bytes,
    limits: ExtractionLimits,
    budget: _Budget,
    discovered: list[ArchiveMemberSpec],
    warnings: list[str],
    virtual_paths: set[str],
    *,
    prefix: tuple[str, ...],
    depth: int,
    root_format: str,
) -> None:
    try:
        with zipfile.ZipFile(source_input(archive_data)) as archive:
            files = _validated_files(archive, limits, budget)
            for name, info in files.items():
                if _is_metadata_member(name):
                    continue
                _discover_zip_entry(
                    archive,
                    name,
                    info,
                    (*prefix, name),
                    depth,
                    root_format,
                    limits,
                    budget,
                    discovered,
                    warnings,
                    virtual_paths,
                )
    except zipfile.BadZipFile as exc:
        raise _diagnostic("could not open ZIP archive", reason="archive_invalid") from exc


def _discover_nested_zip(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    name: str,
    chain: tuple[str, ...],
    depth: int,
    root_format: str,
    limits: ExtractionLimits,
    budget: _Budget,
    discovered: list[ArchiveMemberSpec],
    warnings: list[str],
    virtual_paths: set[str],
) -> None:
    if depth >= MAX_NESTED_ZIP_DEPTH:
        warnings.append(f"skipped nested archive beyond depth limit: {_virtual_member_path(chain)[:300]}")
        return
    nested_data = _read_member(archive, info, limits)
    if not zipfile.is_zipfile(BytesIO(nested_data)):
        warnings.append(f"skipped invalid nested ZIP: {_virtual_member_path(chain)[:300]}")
        return
    _discover_zip_walk(
        nested_data,
        limits,
        budget,
        discovered,
        warnings,
        virtual_paths,
        prefix=chain,
        depth=depth + 1,
        root_format=root_format,
    )


def _discover_zip_leaf(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    name: str,
    chain: tuple[str, ...],
    depth: int,
    root_format: str,
    limits: ExtractionLimits,
    budget: _Budget,
    discovered: list[ArchiveMemberSpec],
    warnings: list[str],
    virtual_paths: set[str],
) -> None:
    member_data = _read_member(archive, info, limits)
    kind, mime_type = _verified_kind(name, member_data)
    if kind is None:
        warnings.append(
            f"skipped archive member whose content did not match "
            f"its supported type: {_virtual_member_path(chain)[:300]}"
        )
        return
    _append_discovered(
        discovered,
        virtual_paths,
        chain=chain,
        kind=kind,
        mime_type=mime_type,
        depth=depth,
        crc32=int(info.CRC),
        compressed_size=int(info.compress_size),
        expanded_size=int(info.file_size),
        member_data=member_data,
        archive_format=root_format,
    )


def discover_zip_members(data: bytes, limits: ExtractionLimits) -> ArchiveScan:
    """Return every supported direct or one-level-nested ZIP leaf."""

    budget = _Budget()
    warnings: list[str] = []
    discovered: list[ArchiveMemberSpec] = []
    virtual_paths: set[str] = set()
    _discover_zip_walk(
        data,
        limits,
        budget,
        discovered,
        warnings,
        virtual_paths,
        prefix=(),
        depth=0,
        root_format="zip",
    )
    discovered.sort(key=lambda item: _collision_key(item.member_path))
    return ArchiveScan(tuple(discovered), tuple(warnings))


def _resolve_zip_chain(
    data: bytes,
    member_chain: tuple[str, ...],
    limits: ExtractionLimits,
    budget: _Budget,
) -> tuple[zipfile.ZipInfo, bytes]:
    current = data
    final_info: zipfile.ZipInfo | None = None
    final_data = b""
    for depth, recorded_name in enumerate(member_chain):
        wanted = _canonical_member_name(recorded_name)
        try:
            with zipfile.ZipFile(source_input(current)) as archive:
                files = _validated_files(archive, limits, budget)
                info = files.get(wanted)
                if info is None:
                    raise _diagnostic(
                        f"archive member no longer exists: {wanted[:200]}",
                        reason="archive_member_changed",
                    )
                member_data = _read_member(archive, info, limits)
                if depth < len(member_chain) - 1:
                    if not wanted.casefold().endswith(".zip") or not zipfile.is_zipfile(BytesIO(member_data)):
                        raise _diagnostic(
                            "archive member chain no longer resolves through a ZIP",
                            reason="archive_member_changed",
                        )
                    current = member_data
                    continue
                final_info = info
                final_data = member_data
        except zipfile.BadZipFile as exc:
            raise _diagnostic("could not reopen ZIP archive", reason="archive_invalid") from exc

    if final_info is None:
        raise _diagnostic("archive member chain did not resolve")
    return final_info, final_data


def read_zip_member(
    data: bytes,
    member_chain: tuple[str, ...],
    limits: ExtractionLimits,
    *,
    expected_hash: str,
    expected_crc32: int,
    expected_compressed_size: int,
    expected_expanded_size: int,
    _budget: _Budget | None = None,
) -> bytes:
    """Resolve and revalidate one previously discovered member."""

    if not member_chain or len(member_chain) > MAX_NESTED_ZIP_DEPTH + 1:
        raise _diagnostic("archive member chain exceeds the nesting limit")
    budget = _budget or _Budget()
    final_info, final_data = _resolve_zip_chain(data, member_chain, limits, budget)
    if (
        int(final_info.CRC) != int(expected_crc32)
        or int(final_info.compress_size) != int(expected_compressed_size)
        or int(final_info.file_size) != int(expected_expanded_size)
        or hashlib.sha256(final_data).hexdigest() != expected_hash
    ):
        raise _diagnostic(
            "archive member no longer matches its indexed identity",
            reason="archive_member_changed",
        )
    kind, _mime = _verified_kind(member_chain[-1], final_data)
    if kind is None:
        raise _diagnostic(
            "archive member content no longer matches its supported type",
            reason="archive_member_changed",
        )
    return final_data
