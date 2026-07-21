#!/usr/bin/env python3
"""Resumable orchestration for DiscordChatExporter.Cli JSON exports.

The archive is append-only: every successful export covers a disjoint snowflake
interval and is committed as an immutable segment.  State is updated only after
the segment and its manifest have been atomically moved into place.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence


SCHEMA_VERSION = 1
DISCORD_EPOCH_MS = 1_420_070_400_000
ANSI_ESCAPE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
PARENT_LINE = re.compile(r"^\s*(\d+)\s+\|\s+")
THREAD_LINE = re.compile(r"^\s*\*\s+(\d+)\s+\|")
FAILURE_MARKER = "Failed to export the following channel(s):"


class OrchestratorError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    config_path: Path
    exporter: Path
    output_dir: Path
    state_file: Path
    work_dir: Path
    roots: tuple[str, ...]
    token_env: str
    env_file: Path | None
    include_threads: str
    media: bool
    reuse_media: bool
    utc: bool
    parallel: int
    extra_args: tuple[str, ...]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def cutoff_snowflake(instant: dt.datetime | None = None) -> int:
    """Return the minimum snowflake at an instant's millisecond."""
    instant = instant or utc_now()
    millis = int(instant.timestamp() * 1000)
    if millis < DISCORD_EPOCH_MS:
        raise OrchestratorError("Cutoff is earlier than Discord's epoch.")
    return (millis - DISCORD_EPOCH_MS) << 22


def validate_id(value: Any, label: str) -> str:
    text = str(value)
    if not text.isascii() or not text.isdigit() or int(text) <= 0:
        raise OrchestratorError(f"{label} must be a positive Discord snowflake ID: {value!r}")
    return text


def resolve_path(base: Path, value: str | os.PathLike[str]) -> Path:
    path = Path(value).expanduser()
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def load_settings(path: Path) -> Settings:
    config_path = path.expanduser().resolve()
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as ex:
        raise OrchestratorError(f"Configuration file does not exist: {config_path}") from ex
    except json.JSONDecodeError as ex:
        raise OrchestratorError(f"Invalid JSON in {config_path}: {ex}") from ex

    if not isinstance(raw, dict):
        raise OrchestratorError("Configuration must be a JSON object.")
    base = config_path.parent
    roots_raw = raw.get("channels")
    if not isinstance(roots_raw, list) or not roots_raw:
        raise OrchestratorError("Configuration key 'channels' must be a non-empty list.")
    roots = tuple(dict.fromkeys(validate_id(value, "Channel") for value in roots_raw))

    output_dir = resolve_path(base, raw.get("output_dir", "Exports"))
    state_file = resolve_path(base, raw.get("state_file", output_dir / ".discord-export-state.json"))
    work_dir = resolve_path(base, raw.get("work_dir", output_dir / ".discord-export-work"))
    exporter = resolve_path(base, raw.get("exporter", "Exporter/DiscordChatExporter.Cli"))
    include_threads = str(raw.get("include_threads", "All")).title()
    if include_threads not in {"None", "Active", "All"}:
        raise OrchestratorError("include_threads must be None, Active, or All.")

    extra_raw = raw.get("extra_args", [])
    if not isinstance(extra_raw, list) or not all(isinstance(v, str) for v in extra_raw):
        raise OrchestratorError("extra_args must be a list of strings.")
    extra_args = tuple(extra_raw)
    reserved = {
        "-c", "--channel", "-t", "--token", "-o", "--output", "-f", "--format",
        "--after", "--before", "-p", "--partition", "--include-threads", "--parallel",
        "--reverse", "--filter", "--media", "--reuse-media", "--media-dir", "--utc",
    }
    for argument in extra_args:
        option = argument.split("=", 1)[0]
        if option in reserved:
            raise OrchestratorError(f"extra_args cannot override orchestrator option {option!r}.")

    parallel = raw.get("parallel", 1)
    if not isinstance(parallel, int) or parallel < 1:
        raise OrchestratorError("parallel must be a positive integer.")

    return Settings(
        config_path=config_path,
        exporter=exporter,
        output_dir=output_dir,
        state_file=state_file,
        work_dir=work_dir,
        roots=roots,
        token_env=str(raw.get("token_env", "DISCORD_TOKEN")),
        env_file=(
            resolve_path(base, raw.get("env_file", ".env"))
            if raw.get("env_file", ".env") is not None
            else None
        ),
        include_threads=include_threads,
        media=bool(raw.get("media", True)),
        reuse_media=bool(raw.get("reuse_media", True)),
        utc=bool(raw.get("utc", True)),
        parallel=parallel,
        extra_args=extra_args,
    )


def initial_state() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": iso_now(),
        "roots": {},
        "pending": {},
        "failures": [],
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return initial_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as ex:
        raise OrchestratorError(f"State file is corrupt ({path}): {ex}") from ex
    if state.get("schema_version") != SCHEMA_VERSION:
        raise OrchestratorError(
            f"Unsupported state schema {state.get('schema_version')!r}; expected {SCHEMA_VERSION}."
        )
    if not isinstance(state.get("roots"), dict) or not isinstance(state.get("pending"), dict):
        raise OrchestratorError(f"State file has an invalid structure: {path}")
    state.setdefault("failures", [])
    return state


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    payload = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


@contextlib.contextmanager
def state_lock(settings: Settings) -> Iterator[None]:
    lock_path = settings.state_file.with_suffix(settings.state_file.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as ex:
            raise OrchestratorError(f"Another orchestrator is using {settings.state_file}.") from ex
        yield


def save_state(settings: Settings, state: dict[str, Any]) -> None:
    state["updated_at"] = iso_now()
    atomic_write_json(settings.state_file, state)


def root_state(state: dict[str, Any], root_id: str) -> dict[str, Any]:
    return state["roots"].setdefault(
        root_id,
        {
            "guild_id": None,
            "actual_channels": {},
            "legacy_artifacts": [],
            "adoption_manifest": None,
        },
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def relative_to_output(settings: Settings, path: Path) -> str:
    try:
        return path.resolve().relative_to(settings.output_dir.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def exporter_version(settings: Settings) -> str:
    if not settings.exporter.is_file():
        raise OrchestratorError(f"Exporter executable does not exist: {settings.exporter}")
    result = subprocess.run(
        [str(settings.exporter), "--version"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        raise OrchestratorError(f"Could not run exporter {settings.exporter}: {result.stdout.strip()}")
    output = ANSI_ESCAPE.sub("", result.stdout).strip()
    return output or "unknown"


def read_dotenv_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None
    if not path.is_file():
        raise OrchestratorError(f"Configured env_file is not a regular file: {path}")
    permissions = stat.S_IMODE(path.stat().st_mode)
    if permissions & 0o077:
        print(
            f"WARNING: {path} is readable by other users; run: chmod 600 {path}",
            file=sys.stderr,
        )

    found: str | None = None
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        name, separator, raw_value = line.partition("=")
        if not separator:
            continue
        name = name.strip()
        if name != key:
            continue
        value = raw_value.strip()
        if value[:1] in {"'", '"'}:
            quote = value[0]
            if len(value) < 2 or value[-1] != quote:
                raise OrchestratorError(
                    f"Unclosed quote for {key} in {path} on line {line_number}."
                )
            value = value[1:-1]
            if quote == '"':
                value = (
                    value.replace(r"\n", "\n")
                    .replace(r"\r", "\r")
                    .replace(r"\t", "\t")
                    .replace(r'\"', '"')
                    .replace(r"\\", "\\")
                )
        found = value
    return found


def child_environment(settings: Settings, token_override: str | None = None) -> dict[str, str]:
    token = token_override or os.environ.get(settings.token_env)
    if not token and settings.env_file is not None:
        token = read_dotenv_value(settings.env_file, settings.token_env)
    if not token:
        dotenv_hint = f" or {settings.env_file}" if settings.env_file is not None else ""
        raise OrchestratorError(
            f"Discord token was not found in environment variable {settings.token_env!r}"
            f"{dotenv_hint}."
        )
    environment = os.environ.copy()
    environment["DISCORD_TOKEN"] = token
    environment["FUCK_RUSSIA"] = "true"
    return environment


def display_command(command: Sequence[str]) -> str:
    return " ".join(json.dumps(part) if re.search(r"\s", part) else part for part in command)


def run_process(
    command: list[str], environment: dict[str, str], log_path: Path, *, live: bool = True
) -> tuple[int, str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                lines.append(line)
                log.write(line)
                log.flush()
                if live:
                    print(line, end="", flush=True)
            return_code = process.wait()
        except BaseException:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise
        finally:
            process.stdout.close()
    return return_code, "".join(lines)


def parse_channel_listing(output: str) -> tuple[set[str], dict[str, set[str]]]:
    channels: set[str] = set()
    threads: dict[str, set[str]] = {}
    current_parent: str | None = None
    for raw_line in ANSI_ESCAPE.sub("", output).replace("\r", "\n").splitlines():
        if match := PARENT_LINE.match(raw_line):
            current_parent = match.group(1)
            channels.add(current_parent)
            threads.setdefault(current_parent, set())
        elif match := THREAD_LINE.match(raw_line):
            if current_parent is None:
                raise OrchestratorError("Exporter channel listing contained a thread without a parent.")
            threads.setdefault(current_parent, set()).add(match.group(1))
    return channels, threads


def discover_guild_channels(
    settings: Settings, guild_id: str, environment: dict[str, str], *, live: bool
) -> tuple[set[str], dict[str, set[str]]]:
    command = [
        str(settings.exporter),
        "channels",
        "--guild",
        guild_id,
        "--include-vc",
        "false",
        "--include-threads",
        settings.include_threads,
    ]
    log_path = settings.work_dir / "discovery" / f"{guild_id}-{uuid.uuid4().hex}.log"
    print(f"Discovering channels and threads in guild {guild_id}...")
    code, output = run_process(command, environment, log_path, live=live)
    if code != 0:
        raise OrchestratorError(
            f"Thread discovery failed for guild {guild_id} (exit {code}); log: {log_path}"
        )
    channels, threads = parse_channel_listing(output)
    if not channels:
        raise OrchestratorError(
            f"Exporter returned no parseable channels for guild {guild_id}; log: {log_path}"
        )
    return channels, threads


def walk_strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from walk_strings(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from walk_strings(item)


def rewrite_media_paths(value: Any, media_dir: Path, final_dir: Path) -> tuple[Any, set[Path]]:
    media_root = media_dir.resolve()
    references: set[Path] = set()

    def visit(item: Any) -> Any:
        if isinstance(item, list):
            return [visit(child) for child in item]
        if isinstance(item, dict):
            return {key: visit(child) for key, child in item.items()}
        if not isinstance(item, str) or not os.path.isabs(item):
            return item
        candidate = Path(item).resolve()
        try:
            candidate.relative_to(media_root)
        except ValueError:
            return item
        references.add(candidate)
        return Path(os.path.relpath(candidate, final_dir)).as_posix()

    return visit(value), references


def validate_and_prepare_json(
    path: Path,
    *,
    root_id: str,
    expected_channel_id: str | None,
    after: int | None,
    before: int,
    media_dir: Path,
    final_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], set[Path]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as ex:
        raise OrchestratorError(f"Exporter produced invalid JSON at {path}: {ex}") from ex
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        raise OrchestratorError(f"Exporter JSON has no message array: {path}")

    channel = data.get("channel")
    guild = data.get("guild")
    if not isinstance(channel, dict) or not isinstance(guild, dict):
        raise OrchestratorError(f"Exporter JSON lacks channel/guild metadata: {path}")
    channel_id = validate_id(channel.get("id"), "Exported channel")
    guild_id = validate_id(guild.get("id"), "Exported guild")
    parent_id = str(channel.get("categoryId")) if channel.get("categoryId") is not None else None
    if expected_channel_id is not None and channel_id != expected_channel_id:
        raise OrchestratorError(f"Expected channel {expected_channel_id}, got {channel_id} in {path}.")
    if expected_channel_id is None and channel_id != root_id and parent_id != root_id:
        raise OrchestratorError(f"Unexpected channel {channel_id} in bootstrap export for {root_id}.")

    message_ids: list[int] = []
    for message in data["messages"]:
        if not isinstance(message, dict):
            raise OrchestratorError(f"Non-object message in {path}.")
        message_id = int(validate_id(message.get("id"), "Message"))
        if after is not None and message_id <= after:
            raise OrchestratorError(f"Message {message_id} is not after exclusive bound {after}.")
        if message_id >= before:
            raise OrchestratorError(f"Message {message_id} is not before exclusive bound {before}.")
        message_ids.append(message_id)
    if len(message_ids) != len(set(message_ids)):
        raise OrchestratorError(f"Duplicate message IDs inside {path}.")
    if message_ids != sorted(message_ids):
        raise OrchestratorError(f"Messages are not in chronological snowflake order in {path}.")

    rewritten, media_references = rewrite_media_paths(data, media_dir, final_dir)
    atomic_write_json(path, rewritten)
    artifact = {
        "path": path.name,
        "channel_id": channel_id,
        "channel_name": channel.get("name"),
        "channel_type": channel.get("type"),
        "parent_id": parent_id,
        "guild_id": guild_id,
        "guild_name": guild.get("name"),
        "message_count": len(message_ids),
        "first_message_id": str(message_ids[0]) if message_ids else None,
        "last_message_id": str(message_ids[-1]) if message_ids else None,
        "exported_at": data.get("exportedAt"),
        "size": path.stat().st_size,
        "sha256": sha256_file(path),
    }
    return rewritten, artifact, media_references


def media_metadata(paths: set[Path], root_dir: Path) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for path in sorted(paths):
        if not path.is_file():
            raise OrchestratorError(f"Export references a local media file that is missing: {path}")
        result.append(
            {
                "path": os.path.relpath(path, root_dir),
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return result


def make_run_id(root_id: str, channel_id: str) -> str:
    stamp = utc_now().strftime("%Y%m%dT%H%M%S.%fZ")
    return f"{stamp}-{root_id}-{channel_id}-{uuid.uuid4().hex[:8]}"


def interval_name(after: int | None, before: int, run_id: str) -> str:
    lower = str(after + 1) if after is not None else "origin"
    upper = str(before - 1)
    suffix = run_id.rsplit("-", 1)[-1]
    return f"{lower}-{upper}-{suffix}"


def build_export_command(
    settings: Settings,
    *,
    channel_id: str,
    output_template: Path,
    media_dir: Path,
    after: int | None,
    before: int,
    include_threads: str,
) -> list[str]:
    command = [
        str(settings.exporter),
        "export",
        "--channel",
        channel_id,
        "--format",
        "Json",
        "--output",
        str(output_template),
        "--before",
        str(before),
        "--include-threads",
        include_threads,
        "--parallel",
        str(settings.parallel),
        "--utc",
        str(settings.utc).lower(),
    ]
    if after is not None:
        command.extend(("--after", str(after)))
    if settings.media:
        command.extend(("--media", "true", "--media-dir", str(media_dir) + os.sep))
        if settings.reuse_media:
            command.extend(("--reuse-media", "true"))
    command.extend(settings.extra_args)
    return command


def record_failure(
    settings: Settings,
    state: dict[str, Any],
    pending: dict[str, Any],
    reason: str,
    stage_dir: Path,
) -> None:
    run_id = pending["run_id"]
    failed_dir = settings.work_dir / "failed" / run_id
    if stage_dir.exists() and not failed_dir.exists():
        failed_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(stage_dir, failed_dir)
    state["pending"].pop(run_id, None)
    failures = state.setdefault("failures", [])
    failures.append(
        {
            "run_id": run_id,
            "root_id": pending["root_id"],
            "channel_id": pending["channel_id"],
            "failed_at": iso_now(),
            "reason": reason,
            "diagnostics": relative_to_output(settings, failed_dir),
        }
    )
    del failures[:-50]
    save_state(settings, state)


def apply_manifest_to_state(
    state: dict[str, Any], manifest: dict[str, Any], manifest_path: Path, settings: Settings
) -> None:
    root_id = manifest["root_id"]
    root = root_state(state, root_id)
    upper = str(int(manifest["interval"]["before_exclusive"]) - 1)
    for artifact in manifest["artifacts"]:
        channel_id = artifact["channel_id"]
        root["guild_id"] = root.get("guild_id") or artifact["guild_id"]
        if root["guild_id"] != artifact["guild_id"]:
            raise OrchestratorError(f"Guild mismatch while committing channel {channel_id}.")
        existing = root["actual_channels"].setdefault(channel_id, {})
        existing.update(
            {
                "parent_id": artifact.get("parent_id"),
                "channel_name": artifact.get("channel_name"),
                "channel_type": artifact.get("channel_type"),
                "checkpoint": upper,
                "last_success_at": manifest["completed_at"],
                "last_manifest": relative_to_output(settings, manifest_path),
            }
        )


def recover_pending(settings: Settings, state: dict[str, Any]) -> None:
    changed = False
    for run_id, pending in list(state["pending"].items()):
        target_dir = Path(pending["target_dir"])
        stage_dir = Path(pending["stage_dir"])
        manifest_path = target_dir / "manifest.json"
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("run_id") != run_id or manifest.get("status") != "complete":
                raise OrchestratorError(f"Cannot recover inconsistent manifest {manifest_path}.")
            apply_manifest_to_state(state, manifest, manifest_path, settings)
            state["pending"].pop(run_id, None)
            print(f"Recovered committed run {run_id}.")
            changed = True
            continue

        reason = "Interrupted before the segment was committed; the same interval will be retried."
        failed_dir = settings.work_dir / "failed" / f"{run_id}-interrupted"
        if stage_dir.exists() and not failed_dir.exists():
            failed_dir.parent.mkdir(parents=True, exist_ok=True)
            os.replace(stage_dir, failed_dir)
        state["pending"].pop(run_id, None)
        state.setdefault("failures", []).append(
            {
                "run_id": run_id,
                "root_id": pending["root_id"],
                "channel_id": pending["channel_id"],
                "failed_at": iso_now(),
                "reason": reason,
                "diagnostics": relative_to_output(settings, failed_dir),
            }
        )
        del state["failures"][:-50]
        changed = True
    if changed:
        save_state(settings, state)


def export_interval(
    settings: Settings,
    state: dict[str, Any],
    environment: dict[str, str],
    version: str,
    *,
    root_id: str,
    channel_id: str,
    after: int | None,
    before: int,
    include_threads: str,
    bootstrap: bool,
    live: bool,
) -> dict[str, Any]:
    run_id = make_run_id(root_id, channel_id)
    stage_dir = settings.work_dir / "staging" / run_id
    root_dir = settings.output_dir / root_id
    media_dir = root_dir / "media"
    segment_group = "_bootstrap" if bootstrap else channel_id
    target_dir = root_dir / "segments" / segment_group / interval_name(after, before, run_id)
    stage_dir.mkdir(parents=True, exist_ok=False)
    media_dir.mkdir(parents=True, exist_ok=True)

    command = build_export_command(
        settings,
        channel_id=channel_id,
        output_template=stage_dir / "%c.json",
        media_dir=media_dir,
        after=after,
        before=before,
        include_threads=include_threads,
    )
    started_at = iso_now()
    pending = {
        "run_id": run_id,
        "root_id": root_id,
        "channel_id": channel_id,
        "after_exclusive": str(after) if after is not None else None,
        "before_exclusive": str(before),
        "stage_dir": str(stage_dir),
        "target_dir": str(target_dir),
        "started_at": started_at,
    }
    state["pending"][run_id] = pending
    save_state(settings, state)

    print(
        f"Exporting root {root_id}, channel {channel_id}, interval "
        f"({after if after is not None else '-infinity'}, {before})..."
    )
    print(f"  {display_command(command)}")
    try:
        code, output = run_process(command, environment, stage_dir / "exporter.log", live=live)
        plain_output = ANSI_ESCAPE.sub("", output)
        if code != 0:
            raise OrchestratorError(f"Exporter exited with status {code}.")
        if FAILURE_MARKER in plain_output:
            raise OrchestratorError("Exporter reported a partial channel/thread failure.")

        json_paths = sorted(stage_dir.glob("*.json"))
        if not json_paths:
            raise OrchestratorError("Exporter reported success but produced no JSON files.")
        artifacts: list[dict[str, Any]] = []
        referenced_media: set[Path] = set()
        seen_channels: set[str] = set()
        for json_path in json_paths:
            _, artifact, references = validate_and_prepare_json(
                json_path,
                root_id=root_id,
                expected_channel_id=None if bootstrap else channel_id,
                after=after,
                before=before,
                media_dir=media_dir,
                final_dir=target_dir,
            )
            if artifact["channel_id"] in seen_channels:
                raise OrchestratorError(f"Exporter produced channel {artifact['channel_id']} twice.")
            seen_channels.add(artifact["channel_id"])
            artifacts.append(artifact)
            referenced_media.update(references)
        if not bootstrap and seen_channels != {channel_id}:
            raise OrchestratorError(f"Expected only channel {channel_id}, got {sorted(seen_channels)}.")

        manifest = {
            "schema_version": SCHEMA_VERSION,
            "status": "complete",
            "run_id": run_id,
            "root_id": root_id,
            "requested_channel_id": channel_id,
            "started_at": started_at,
            "completed_at": iso_now(),
            "interval": {
                "after_exclusive": str(after) if after is not None else None,
                "before_exclusive": str(before),
                "first_included_snowflake": str(after + 1) if after is not None else None,
                "last_included_snowflake": str(before - 1),
            },
            "exporter_version": version,
            "command": command,
            "options": {
                "format": "Json",
                "include_threads": include_threads,
                "media": settings.media,
                "reuse_media": settings.reuse_media,
                "utc": settings.utc,
            },
            "artifacts": artifacts,
            "media": media_metadata(referenced_media, root_dir),
        }
        atomic_write_json(stage_dir / "manifest.json", manifest)
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        if target_dir.exists():
            raise OrchestratorError(f"Refusing to overwrite existing segment {target_dir}.")
        os.replace(stage_dir, target_dir)
        manifest_path = target_dir / "manifest.json"
        apply_manifest_to_state(state, manifest, manifest_path, settings)
        state["pending"].pop(run_id, None)
        save_state(settings, state)
        print(
            f"Committed {sum(a['message_count'] for a in artifacts)} message(s) from "
            f"{len(artifacts)} channel(s) to {target_dir}."
        )
        return manifest
    except BaseException as ex:
        reason = str(ex) if str(ex) else ex.__class__.__name__
        # Once the atomic rename has happened, leave the on-disk pending record
        # intact.  Recovery will finish applying the committed manifest instead
        # of running the interval again.
        if not (target_dir / "manifest.json").is_file():
            record_failure(settings, state, pending, reason, stage_dir)
        if isinstance(ex, (KeyboardInterrupt, SystemExit)):
            raise
        if isinstance(ex, OrchestratorError):
            raise
        raise OrchestratorError(f"Unexpected export failure: {ex}") from ex


def iter_legacy_json(root_dir: Path) -> Iterator[Path]:
    if not root_dir.is_dir():
        return
    for path in sorted(root_dir.glob("*.json")):
        if not path.name.startswith(".discord-"):
            yield path


def link_legacy_media(
    root_dir: Path, media_dir: Path
) -> tuple[int, int, list[dict[str, Any]]]:
    linked = 0
    reused = 0
    tracked: set[Path] = set()
    media_dir.mkdir(parents=True, exist_ok=True)
    for asset_dir in sorted(root_dir.glob("*.json_Files")):
        if not asset_dir.is_dir():
            continue
        for source in sorted(asset_dir.rglob("*")):
            if not source.is_file():
                continue
            target = media_dir / source.name
            if target.exists():
                if target.stat().st_size != source.stat().st_size or sha256_file(target) != sha256_file(source):
                    raise OrchestratorError(
                        f"Legacy media filename collision with different content: {source} and {target}"
                    )
                reused += 1
                tracked.add(target)
                continue
            try:
                os.link(source, target)
            except OSError:
                shutil.copy2(source, target)
            linked += 1
            tracked.add(target)
    metadata = [
        {
            "path": os.path.relpath(path, root_dir),
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(tracked)
    ]
    return linked, reused, metadata


def adopt_existing(settings: Settings, state: dict[str, Any]) -> int:
    adopted_total = 0
    for root_id in settings.roots:
        root_dir = settings.output_dir / root_id
        root = root_state(state, root_id)
        adopted: list[dict[str, Any]] = []
        checkpoints: dict[str, int] = {}
        seen_by_channel: dict[str, set[int]] = {}
        for path in iter_legacy_json(root_dir):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as ex:
                raise OrchestratorError(f"Cannot adopt invalid JSON {path}: {ex}") from ex
            channel = data.get("channel", {})
            guild = data.get("guild", {})
            channel_id = validate_id(channel.get("id"), "Legacy channel")
            parent_id = str(channel.get("categoryId")) if channel.get("categoryId") else None
            if channel_id != root_id and parent_id != root_id:
                continue
            guild_id = validate_id(guild.get("id"), "Legacy guild")
            if root.get("guild_id") not in (None, guild_id):
                raise OrchestratorError(f"Legacy exports for root {root_id} disagree on guild ID.")
            root["guild_id"] = guild_id
            messages = data.get("messages")
            if not isinstance(messages, list):
                raise OrchestratorError(f"Legacy export has no message array: {path}")
            ids = [int(validate_id(message.get("id"), "Legacy message")) for message in messages]
            already_seen = seen_by_channel.setdefault(channel_id, set())
            duplicates = already_seen.intersection(ids)
            if duplicates:
                sample = min(duplicates)
                raise OrchestratorError(
                    f"Legacy exports already duplicate message {sample} in channel {channel_id}; "
                    "resolve the overlap before adoption."
                )
            already_seen.update(ids)
            if ids:
                checkpoints[channel_id] = max(checkpoints.get(channel_id, 0), max(ids))
            adopted.append(
                {
                    "path": relative_to_output(settings, path),
                    "channel_id": channel_id,
                    "parent_id": parent_id,
                    "guild_id": guild_id,
                    "message_count": len(ids),
                    "first_message_id": str(min(ids)) if ids else None,
                    "last_message_id": str(max(ids)) if ids else None,
                    "exported_at": data.get("exportedAt"),
                    "size": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
            channel_state = root["actual_channels"].setdefault(channel_id, {})
            channel_state.update(
                {
                    "parent_id": parent_id,
                    "channel_name": channel.get("name"),
                    "channel_type": channel.get("type"),
                    "checkpoint": str(checkpoints[channel_id]) if channel_id in checkpoints else channel_state.get("checkpoint"),
                    "adopted_at": iso_now(),
                }
            )

        if not adopted:
            print(f"No legacy JSON exports found for root {root_id} in {root_dir}.")
            continue
        linked, reused, legacy_media = (0, 0, [])
        if settings.media:
            linked, reused, legacy_media = link_legacy_media(root_dir, root_dir / "media")
        adoption = {
            "schema_version": SCHEMA_VERSION,
            "root_id": root_id,
            "adopted_at": iso_now(),
            "artifacts": adopted,
            "media_files_linked": linked,
            "media_files_already_present": reused,
            "media": legacy_media,
        }
        manifest_path = root_dir / ".discord-adopted.json"
        atomic_write_json(manifest_path, adoption)
        root["legacy_artifacts"] = adopted
        root.pop("legacy_media", None)
        root["adoption_manifest"] = relative_to_output(settings, manifest_path)
        adopted_total += len(adopted)
        print(
            f"Adopted {len(adopted)} export(s) for root {root_id}; "
            f"prepared {linked} legacy media file(s) for reuse."
        )
    save_state(settings, state)
    return adopted_total


def run_exports(
    settings: Settings, state: dict[str, Any], *, live: bool,
    environment: dict[str, str] | None = None,
) -> int:
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    if settings.output_dir.stat().st_dev != settings.work_dir.stat().st_dev:
        raise OrchestratorError(
            "output_dir and work_dir must be on the same filesystem for atomic commits."
        )
    if environment is None:
        environment = child_environment(settings)
    version = exporter_version(settings)
    before = cutoff_snowflake()
    failures: list[str] = []

    # Bootstrap roots for which no export metadata is known.  This is the only
    # multi-channel export; its output and console are checked for partial errors.
    for root_id in settings.roots:
        root = root_state(state, root_id)
        if root.get("guild_id") and root.get("actual_channels"):
            continue
        try:
            export_interval(
                settings,
                state,
                environment,
                version,
                root_id=root_id,
                channel_id=root_id,
                after=None,
                before=before,
                include_threads=settings.include_threads,
                bootstrap=True,
                live=live,
            )
        except OrchestratorError as ex:
            failures.append(f"root {root_id} bootstrap: {ex}")

    # Discover newly created threads without crawling their messages.
    discovery: dict[str, tuple[set[str], dict[str, set[str]]]] = {}
    guild_ids = {
        root_state(state, root_id).get("guild_id")
        for root_id in settings.roots
        if root_state(state, root_id).get("guild_id")
    }
    if settings.include_threads != "None":
        for guild_id in sorted(guild_ids):
            try:
                discovery[guild_id] = discover_guild_channels(
                    settings, guild_id, environment, live=live
                )
            except OrchestratorError as ex:
                failures.append(str(ex))

    # Each known or newly discovered actual channel gets its own checkpoint.
    for root_id in settings.roots:
        root = root_state(state, root_id)
        guild_id = root.get("guild_id")
        actual_ids = set(root.get("actual_channels", {}))
        if settings.include_threads != "None" and guild_id in discovery:
            guild_channels, guild_threads = discovery[guild_id]
            if root_id in guild_channels and root_id in actual_ids:
                actual_ids.add(root_id)
            actual_ids.update(guild_threads.get(root_id, set()))
        if not actual_ids:
            continue

        for channel_id in sorted(actual_ids, key=int):
            channel = root["actual_channels"].setdefault(channel_id, {})
            after_text = channel.get("checkpoint")
            after = int(after_text) if after_text is not None else None
            if after is not None and after >= before - 1:
                continue
            try:
                export_interval(
                    settings,
                    state,
                    environment,
                    version,
                    root_id=root_id,
                    channel_id=channel_id,
                    after=after,
                    before=before,
                    include_threads="None",
                    bootstrap=False,
                    live=live,
                )
            except OrchestratorError as ex:
                failures.append(f"channel {channel_id}: {ex}")

    if failures:
        print("\nCompleted with failures:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    return 0


def iter_manifests(settings: Settings) -> Iterator[Path]:
    for root_id in settings.roots:
        segments = settings.output_dir / root_id / "segments"
        if segments.is_dir():
            yield from sorted(segments.rglob("manifest.json"))


def verify_archive(settings: Settings, state: dict[str, Any]) -> int:
    seen: dict[str, set[int]] = {}
    errors: list[str] = []
    artifact_count = 0
    message_count = 0

    for root_id in settings.roots:
        root = root_state(state, root_id)
        for legacy in root.get("legacy_artifacts", []):
            path = settings.output_dir / legacy["path"]
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if sha256_file(path) != legacy["sha256"]:
                    errors.append(f"Hash mismatch: {path}")
                ids = [int(message["id"]) for message in data["messages"]]
                channel_seen = seen.setdefault(legacy["channel_id"], set())
                overlap = channel_seen.intersection(ids)
                if overlap:
                    errors.append(f"Duplicate message {min(overlap)} in {path}")
                channel_seen.update(ids)
                artifact_count += 1
                message_count += len(ids)
            except (OSError, KeyError, ValueError, json.JSONDecodeError) as ex:
                errors.append(f"Invalid legacy artifact {path}: {ex}")
        root_dir = settings.output_dir / root_id
        adoption_manifest = root.get("adoption_manifest")
        if adoption_manifest:
            try:
                adoption_path = settings.output_dir / adoption_manifest
                adoption = json.loads(adoption_path.read_text(encoding="utf-8"))
                for media in adoption.get("media", []):
                    path = (root_dir / media["path"]).resolve()
                    if not path.is_file() or sha256_file(path) != media["sha256"]:
                        errors.append(f"Missing or changed adopted media: {path}")
            except (OSError, KeyError, json.JSONDecodeError) as ex:
                errors.append(f"Invalid adoption manifest for root {root_id}: {ex}")

    for manifest_path in iter_manifests(settings):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            after_text = manifest["interval"]["after_exclusive"]
            after = int(after_text) if after_text is not None else None
            before = int(manifest["interval"]["before_exclusive"])
            for artifact in manifest["artifacts"]:
                path = manifest_path.parent / artifact["path"]
                data = json.loads(path.read_text(encoding="utf-8"))
                if sha256_file(path) != artifact["sha256"]:
                    errors.append(f"Hash mismatch: {path}")
                ids = [int(message["id"]) for message in data["messages"]]
                bad = [value for value in ids if (after is not None and value <= after) or value >= before]
                if bad:
                    errors.append(f"Out-of-range message {bad[0]} in {path}")
                channel_seen = seen.setdefault(artifact["channel_id"], set())
                overlap = channel_seen.intersection(ids)
                if overlap:
                    errors.append(f"Duplicate message {min(overlap)} in {path}")
                channel_seen.update(ids)
                artifact_count += 1
                message_count += len(ids)
            root_dir = settings.output_dir / manifest["root_id"]
            for media in manifest.get("media", []):
                path = (root_dir / media["path"]).resolve()
                if not path.is_file() or sha256_file(path) != media["sha256"]:
                    errors.append(f"Missing or changed media: {path}")
        except (OSError, KeyError, ValueError, json.JSONDecodeError) as ex:
            errors.append(f"Invalid manifest {manifest_path}: {ex}")

    if errors:
        print(f"Verification failed with {len(errors)} error(s):", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(f"Verified {artifact_count} artifact(s), {message_count} unique message(s), and all tracked media.")
    return 0


def print_status(settings: Settings, state: dict[str, Any]) -> None:
    print(f"State: {settings.state_file}")
    print(f"Pending runs: {len(state['pending'])}; recorded failures: {len(state.get('failures', []))}")
    for root_id in settings.roots:
        root = root_state(state, root_id)
        print(f"Root {root_id} (guild {root.get('guild_id') or 'unknown'}):")
        channels = root.get("actual_channels", {})
        if not channels:
            print("  no adopted or exported channels")
        for channel_id, channel in sorted(channels.items(), key=lambda pair: int(pair[0])):
            print(
                f"  {channel_id}  checkpoint={channel.get('checkpoint') or 'origin'}  "
                f"name={channel.get('channel_name') or '?'}"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("discord-export.json"),
        help="JSON configuration file (default: discord-export.json)",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    run_parser = subparsers.add_parser("run", help="discover and export new messages")
    run_parser.add_argument("--quiet", action="store_true", help="store exporter output only in logs")
    subparsers.add_parser("adopt", help="adopt existing flat JSON exports and media")
    subparsers.add_parser("status", help="show checkpoints and recent state")
    subparsers.add_parser("verify", help="verify hashes, ranges, and message uniqueness")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = load_settings(args.config)
        with state_lock(settings):
            state = load_state(settings.state_file)
            recover_pending(settings, state)
            if args.action == "adopt":
                return 0 if adopt_existing(settings, state) else 1
            if args.action == "status":
                print_status(settings, state)
                return 0
            if args.action == "verify":
                return verify_archive(settings, state)
            if args.action == "run":
                return run_exports(settings, state, live=not args.quiet)
    except OrchestratorError as ex:
        print(f"ERROR: {ex}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
