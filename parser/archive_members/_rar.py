"""Bounded 7z subprocess execution and RAR listing parsing."""

from __future__ import annotations

from dataclasses import dataclass
import os
import selectors
import shutil
import subprocess
import tempfile
import time

from ..archive_limits import resource_limited_command as _resource_limited_command

from ._core import (
    _kill_process,
    READ_CHUNK_BYTES,
    ExtractionError,
    ExtractionLimitError,
    ExtractionLimits,
    _Budget,
    _canonical_member_name,
    _collision_key,
    _diagnostic,
)


SEVEN_ZIP_LIST_TIMEOUT_SECONDS = 30
SEVEN_ZIP_EXTRACT_TIMEOUT_SECONDS = 60
SEVEN_ZIP_MAX_LIST_BYTES = 8 * 1024 * 1024
SEVEN_ZIP_MAX_ERROR_BYTES = 64 * 1024
SEVEN_ZIP_MAX_ADDRESS_SPACE_BYTES = 1024 * 1024 * 1024
SEVEN_ZIP_MAX_OPEN_FILES = 64


@dataclass(frozen=True)
class _SevenZipEntry:
    name: str
    size: int
    packed_size: int
    crc32: int


def _seven_zip_executable() -> str:
    executable = shutil.which("7z") or shutil.which("7zz")
    if not executable:
        raise ExtractionError(
            "7z is required for bounded RAR ingestion",
            reason="archive_tool_unavailable",
        )
    return executable


def _run_bounded_process(
    command: list[str],
    *,
    max_stdout_bytes: int,
    timeout_seconds: int,
) -> tuple[bytes, bytes]:
    """Run a local archive helper while bounding time and both output pipes."""

    limited_command = _resource_limited_command(command, timeout_seconds)
    private_directory = tempfile.TemporaryDirectory(prefix="tree-eclass-7z-")
    environment = {
        "LC_ALL": "C.UTF-8",
        "LANG": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "HOME": private_directory.name,
        "TMPDIR": private_directory.name,
        "XDG_CONFIG_HOME": private_directory.name,
    }
    try:
        process = subprocess.Popen(  # nosec B603 - fixed executable/arguments
            limited_command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            cwd=private_directory.name,
            close_fds=True,
            start_new_session=os.getenv("TREE_PARSER_ISOLATED") != "1",
        )
    except Exception:
        private_directory.cleanup()
        raise
    if process.stdout is None or process.stderr is None:  # pragma: no cover
        process.kill()
        process.wait()
        private_directory.cleanup()
        raise ExtractionError("could not capture 7z output", reason="archive_invalid")
    output, failure = _pump_process_output(process, max_stdout_bytes, timeout_seconds, private_directory)
    if failure is not None:
        raise failure
    return _process_result(output, process)


def _pump_process_output(
    process: subprocess.Popen[bytes],
    max_stdout_bytes: int,
    timeout_seconds: int,
    private_directory: tempfile.TemporaryDirectory[str],
) -> tuple[dict[str, bytearray], ExtractionError | None]:
    streams = selectors.DefaultSelector()
    streams.register(process.stdout, selectors.EVENT_READ, "stdout")
    streams.register(process.stderr, selectors.EVENT_READ, "stderr")
    output = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {
        "stdout": max(0, int(max_stdout_bytes)),
        "stderr": SEVEN_ZIP_MAX_ERROR_BYTES,
    }
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    failure: ExtractionError | None = None
    try:
        while streams.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                failure = ExtractionLimitError(
                    "7z archive operation timed out",
                    reason="archive_timeout",
                )
                break
            events = streams.select(min(0.25, remaining))
            if not events and process.poll() is not None:
                # The pipes will become readable at EOF on the next select.
                continue
            failure = _drain_stream_events(events, streams, output, limits, failure)
            if failure is not None:
                break
    finally:
        streams.close()
        if failure is not None or process.poll() is None:
            try:
                _kill_process(process)
            except ProcessLookupError:
                pass
        process.wait()
        for pipe in (process.stdout, process.stderr):
            if not pipe.closed:
                pipe.close()
        private_directory.cleanup()
    return output, failure


def _drain_stream_events(
    events,
    streams,
    output,
    limits,
    failure,
) -> ExtractionError | None:
    for key, _mask in events:
        block = os.read(key.fd, READ_CHUNK_BYTES)
        if not block:
            streams.unregister(key.fileobj)
            key.fileobj.close()
            continue
        bucket = output[str(key.data)]
        bucket.extend(block)
        if len(bucket) > limits[str(key.data)]:
            return ExtractionLimitError(
                f"7z {key.data} exceeded its configured byte limit",
                reason=("archive_member_limit" if key.data == "stdout" else "archive_invalid"),
            )
    return failure


def _process_result(output: dict[str, bytearray], process: subprocess.Popen[bytes]) -> tuple[bytes, bytes]:
    if process.returncode != 0:
        diagnostic = bytes(output["stderr"]).decode("utf-8", errors="replace").strip()
        lowered = diagnostic.casefold()
        if (
            process.returncode < 0
            or "can't allocate" in lowered
            or "cannot allocate" in lowered
            or "out of memory" in lowered
        ):
            raise ExtractionLimitError(
                f"7z exceeded its resource limit: {diagnostic[:500]}",
                reason="archive_resource_limit",
            )
        reason = (
            "archive_encrypted_member" if "wrong password" in lowered or "encrypted" in lowered else "archive_invalid"
        )
        raise ExtractionError(
            f"7z could not process RAR archive: {diagnostic[:500]}",
            reason=reason,
        )
    return bytes(output["stdout"]), bytes(output["stderr"])


def _parse_7z_listing(payload: bytes) -> list[dict[str, str]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExtractionError(
            "7z returned non-UTF-8 member metadata",
            reason="archive_invalid",
        ) from exc
    records: list[dict[str, str]] = []
    record: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            if record:
                records.append(record)
                record = {}
            continue
        key, separator, value = line.partition(" = ")
        if not separator:
            continue
        record[key.strip()] = value
    if record:
        records.append(record)
    return records


def _rar_entries(
    archive_path: str,
    archive_size: int,
    limits: ExtractionLimits,
    budget: _Budget,
) -> dict[str, _SevenZipEntry]:
    listing, _stderr = _run_bounded_process(
        [
            _seven_zip_executable(),
            "l",
            "-slt",
            "-ba",
            "-bd",
            "-bb0",
            "-mmt=1",
            "-p__TREE_ECLASS_NO_PASSWORD__",
            "--",
            archive_path,
        ],
        max_stdout_bytes=SEVEN_ZIP_MAX_LIST_BYTES,
        timeout_seconds=SEVEN_ZIP_LIST_TIMEOUT_SECONDS,
    )
    files: dict[str, _SevenZipEntry] = {}
    collision_keys: set[str] = set()
    total_expanded = 0
    for raw in _parse_7z_listing(listing):
        entry = _rar_entry_from_record(raw, limits, budget, collision_keys)
        if entry is None:
            continue
        total_expanded += entry.size
        files[entry.name] = entry
    if total_expanded and (archive_size <= 0 or total_expanded / archive_size > limits.archive_max_ratio):
        raise ExtractionLimitError(
            "RAR aggregate compression-ratio limit exceeded",
            reason="archive_ratio_limit",
        )
    return files


def _rar_entry_name(
    raw: dict[str, str],
    collision_keys: set[str],
) -> str | None:
    if raw.get("Folder") == "+":
        return None
    raw_name = raw.get("Path")
    if raw_name is None:
        return None
    name = _canonical_member_name(raw_name)
    collision = _collision_key(name)
    if collision in collision_keys:
        raise _diagnostic(f"RAR contains duplicate normalized member paths: {name[:200]}")
    collision_keys.add(collision)
    _reject_special_rar_member(raw, name)
    return name


def _reject_special_rar_member(raw: dict[str, str], name: str) -> None:
    if raw.get("Encrypted") == "+":
        raise _diagnostic(f"RAR member is encrypted: {name[:200]}", reason="archive_encrypted_member")
    attributes = raw.get("Attributes", "")
    if (
        raw.get("Symbolic Link")
        or raw.get("Hard Link")
        or raw.get("Alternate Stream") == "+"
        or attributes.casefold().startswith("l")
        or " reparse" in attributes.casefold()
    ):
        raise _diagnostic(f"RAR member is a link or special file: {name[:200]}")


def _rar_entry_sizes(raw: dict[str, str], name: str) -> tuple[int, int, int]:
    try:
        size = int(raw.get("Size", "-1"))
        packed_size = int(raw.get("Packed Size", "0") or 0)
        crc32 = int(raw.get("CRC", "0") or "0", 16)
    except ValueError as exc:
        raise ExtractionError(
            f"RAR member has invalid size or CRC metadata: {name[:200]}", reason="archive_invalid"
        ) from exc
    if size < 0 or packed_size < 0:
        raise ExtractionError(f"RAR member has invalid sizes: {name[:200]}", reason="archive_invalid")
    return size, packed_size, crc32


def _charge_rar_budget(
    budget: _Budget,
    limits: ExtractionLimits,
    name: str,
    size: int,
) -> None:
    budget.members += 1
    if budget.members > limits.archive_max_members:
        raise ExtractionLimitError("archive member-count limit exceeded", reason="archive_member_limit")
    if size > limits.archive_max_member_bytes:
        raise ExtractionLimitError(f"archive member-size limit exceeded: {name[:200]}", reason="archive_member_limit")
    budget.expanded_bytes += size
    if budget.expanded_bytes > limits.archive_max_expanded_bytes:
        raise ExtractionLimitError("archive expanded-size limit exceeded", reason="archive_expanded_limit")


def _rar_entry_from_record(
    raw: dict[str, str],
    limits: ExtractionLimits,
    budget: _Budget,
    collision_keys: set[str],
) -> _SevenZipEntry | None:
    name = _rar_entry_name(raw, collision_keys)
    if name is None:
        return None
    size, packed_size, crc32 = _rar_entry_sizes(raw, name)
    _charge_rar_budget(budget, limits, name, size)
    return _SevenZipEntry(name=name, size=size, packed_size=packed_size, crc32=crc32)


def _read_rar_entry(
    archive_path: str,
    entry: _SevenZipEntry,
    limits: ExtractionLimits,
) -> bytes:
    data, _stderr = _run_bounded_process(
        [
            _seven_zip_executable(),
            "x",
            "-so",
            "-bd",
            "-bb0",
            "-spd",
            "-mmt=1",
            "-p__TREE_ECLASS_NO_PASSWORD__",
            f"-i!{entry.name}",
            "--",
            archive_path,
        ],
        max_stdout_bytes=min(limits.archive_max_member_bytes, entry.size),
        timeout_seconds=SEVEN_ZIP_EXTRACT_TIMEOUT_SECONDS,
    )
    if len(data) != entry.size:
        raise ExtractionError(
            f"RAR member size did not match listing: {entry.name[:200]}",
            reason="archive_invalid",
        )
    return data
