"""Read committed DiscordChatExporter artifacts and build conversation windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable

from app.knowledge.normalization import search_normalize

from .config import MessageConfig
from .models import ArchiveSource, ConversationRecord, MessageRecord

try:  # The Docker image installs ijson; the stdlib fallback keeps tests lightweight.
    import ijson  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - exercised only in minimal local environments.
    ijson = None


class DiscordArchiveError(RuntimeError):
    pass


@dataclass
class ParsedArtifact:
    source: ArchiveSource
    guild: dict[str, Any]
    channel: dict[str, Any]
    exported_at: str | None
    messages: list[MessageRecord]


def _inside(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise DiscordArchiveError(f"Archive path escapes configured root: {path}") from exc
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscordArchiveError(f"Invalid JSON file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DiscordArchiveError(f"Expected an object in {path}")
    return value


def discover_sources(config: MessageConfig) -> list[ArchiveSource]:
    """Discover only sources committed by archive state or complete manifests."""
    root = Path(config.archive_dir).resolve()
    state_path = root / ".discord-export-state.json"
    if not root.is_dir() or not state_path.is_file():
        return []
    state = _load_json(state_path)
    sources: dict[Path, ArchiveSource] = {}
    state_roots = state.get("roots") if isinstance(state.get("roots"), dict) else {}
    for root_id, course_id in config.course_map.items():
        root_state = state_roots.get(root_id, {}) if isinstance(state_roots, dict) else {}
        for artifact in root_state.get("legacy_artifacts", []) if isinstance(root_state, dict) else []:
            if not isinstance(artifact, dict) or not artifact.get("path"):
                continue
            candidate = Path(str(artifact["path"]))
            path = candidate if candidate.is_absolute() else root / candidate
            path = _inside(path, root)
            sources[path] = ArchiveSource(
                root_id=root_id,
                course_id=course_id,
                path=str(path),
                expected_sha256=str(artifact.get("sha256")) if artifact.get("sha256") else None,
            )

        segment_root = root / root_id / "segments"
        if not segment_root.is_dir():
            continue
        for manifest_path in sorted(segment_root.rglob("manifest.json")):
            manifest = _load_json(manifest_path)
            if manifest.get("status") != "complete" or str(manifest.get("root_id")) != root_id:
                continue
            for artifact in manifest.get("artifacts", []):
                if not isinstance(artifact, dict) or not artifact.get("path"):
                    continue
                path = _inside(manifest_path.parent / str(artifact["path"]), root)
                sources[path] = ArchiveSource(
                    root_id=root_id,
                    course_id=course_id,
                    path=str(path),
                    expected_sha256=str(artifact.get("sha256")) if artifact.get("sha256") else None,
                )
    missing = [source.path for source in sources.values() if not Path(source.path).is_file()]
    if missing:
        raise DiscordArchiveError(f"Archive metadata references missing JSON: {missing[0]}")
    return [sources[path] for path in sorted(sources)]


def source_fingerprint(source: ArchiveSource) -> str:
    stat = Path(source.path).stat()
    return f"{source.expected_sha256 or 'untracked'}:{stat.st_size}:{stat.st_mtime_ns}"


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _iter_export(path: Path) -> tuple[dict[str, Any], dict[str, Any], str | None, Iterable[dict[str, Any]]]:
    if ijson is None:
        export = _load_json(path)
        messages = export.get("messages")
        if not isinstance(messages, list):
            raise DiscordArchiveError(f"Export has no messages array: {path}")
        return (
            export.get("guild") if isinstance(export.get("guild"), dict) else {},
            export.get("channel") if isinstance(export.get("channel"), dict) else {},
            str(export.get("exportedAt")) if export.get("exportedAt") else None,
            (item for item in messages if isinstance(item, dict)),
        )

    def first(prefix: str) -> Any:
        with path.open("rb") as handle:
            return next(ijson.items(handle, prefix), None)

    guild = first("guild")
    channel = first("channel")
    exported_at = first("exportedAt")

    def messages() -> Iterable[dict[str, Any]]:
        with path.open("rb") as handle:
            for item in ijson.items(handle, "messages.item"):
                if isinstance(item, dict):
                    yield item

    return (
        guild if isinstance(guild, dict) else {},
        channel if isinstance(channel, dict) else {},
        str(exported_at) if exported_at else None,
        messages(),
    )


def _timestamp(value: Any) -> tuple[str, float]:
    text = str(value or "")
    if not text:
        raise DiscordArchiveError("Discord message has no timestamp")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DiscordArchiveError(f"Invalid Discord timestamp: {text}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(), parsed.timestamp()


def _message_text(message: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    parts = [str(message.get("content") or "").strip()]
    attachments: list[dict[str, Any]] = []
    for item in message.get("attachments") or []:
        if not isinstance(item, dict):
            continue
        metadata = {
            key: item.get(key) for key in ("id", "fileName", "fileSizeBytes", "url") if item.get(key) is not None
        }
        attachments.append(metadata)
        if item.get("fileName"):
            parts.append(f"[Attachment: {item['fileName']}]")
    for embed in message.get("embeds") or []:
        if not isinstance(embed, dict):
            continue
        for key in ("title", "description", "url"):
            if embed.get(key):
                parts.append(str(embed[key]))
        for field in embed.get("fields") or []:
            if isinstance(field, dict):
                parts.extend(str(field.get(key) or "") for key in ("name", "value"))
    forwarded = message.get("forwardedMessage")
    if isinstance(forwarded, dict):
        forwarded_text, _ = _message_text(forwarded)
        if forwarded_text:
            parts.append(f"[Forwarded message] {forwarded_text}")
    return "\n".join(part for part in parts if part).strip(), attachments


def _author_key(author: dict[str, Any]) -> str | None:
    author_id = str(author.get("id") or "").strip()
    return hashlib.sha256(author_id.encode("utf-8")).hexdigest()[:20] if author_id else None


def parse_artifact(source: ArchiveSource) -> ParsedArtifact:
    path = Path(source.path)
    guild, channel, exported_at, raw_messages = _iter_export(path)
    channel_id_text = str(channel.get("id") or "")
    if not channel_id_text.isdigit():
        raise DiscordArchiveError(f"Export has an invalid channel ID: {path}")
    channel_id = int(channel_id_text)
    messages: list[MessageRecord] = []
    for message in raw_messages:
        message_id_text = str(message.get("id") or "")
        if not message_id_text.isdigit():
            raise DiscordArchiveError(f"Export contains an invalid message ID: {path}")
        timestamp, timestamp_epoch = _timestamp(message.get("timestamp"))
        author = message.get("author") if isinstance(message.get("author"), dict) else {}
        content, attachments = _message_text(message)
        reference = message.get("reference") if isinstance(message.get("reference"), dict) else {}
        reply_text = str(reference.get("messageId") or "")
        reply_to = int(reply_text) if reply_text.isdigit() else None
        reaction_count = sum(
            max(0, int(item.get("count") or 0))
            for item in message.get("reactions") or []
            if isinstance(item, dict)
        )
        messages.append(MessageRecord(
            message_id=int(message_id_text),
            channel_id=channel_id,
            course_id=source.course_id,
            timestamp=timestamp,
            timestamp_epoch=timestamp_epoch,
            author_key=_author_key(author),
            author_name=str(author.get("nickname") or author.get("name") or "Unknown user"),
            content=content,
            searchable_text=search_normalize(content),
            reply_to_message_id=reply_to,
            message_type=str(message.get("type") or "Default"),
            is_pinned=bool(message.get("isPinned")),
            reaction_count=reaction_count,
            attachment_metadata=attachments,
        ))
    messages.sort(key=lambda item: item.message_id)
    return ParsedArtifact(source, guild, channel, exported_at, messages)


_URL_ONLY = re.compile(r"^https?://(?:www\.)?(?:tenor\.com|giphy\.com)/\S+$", re.IGNORECASE)


def _informative(message: MessageRecord) -> bool:
    text = message.content.strip()
    if not text or text.casefold() == "pinned a message.":
        return False
    if _URL_ONLY.fullmatch(text):
        return False
    return any(character.isalnum() for character in text)


def _conversation_id(channel_id: int, source_path: str, first: int, last: int) -> str:
    raw = f"{channel_id}\0{source_path}\0{first}\0{last}".encode("utf-8")
    return f"dconv_{hashlib.sha256(raw).hexdigest()[:32]}"


def build_conversations(
    parsed: ParsedArtifact,
    config: MessageConfig,
    referenced_messages: dict[int, dict[str, Any]] | None = None,
) -> list[ConversationRecord]:
    """Build non-overlapping temporal bursts while retaining explicit reply context."""
    useful = [message for message in parsed.messages if _informative(message)]
    if not useful:
        return []
    local = {message.message_id: message for message in parsed.messages}
    external = referenced_messages or {}
    groups: list[list[MessageRecord]] = []
    current: list[MessageRecord] = []
    current_chars = 0
    for message in useful:
        gap = message.timestamp_epoch - current[-1].timestamp_epoch if current else 0
        projected = current_chars + len(message.content)
        if current and (
            gap > config.window_gap_seconds
            or len(current) >= config.window_max_messages
            or projected > config.window_max_characters
        ):
            groups.append(current)
            current = []
            current_chars = 0
        current.append(message)
        current_chars += len(message.content)
    if current:
        groups.append(current)

    channel_id = int(parsed.channel.get("id"))
    root_id = int(parsed.source.root_id)
    channel_name = str(parsed.channel.get("name") or channel_id)
    channel_type = str(parsed.channel.get("type") or "Unknown")
    result: list[ConversationRecord] = []
    for group in groups:
        group_ids = {message.message_id for message in group}
        lines: list[str] = []
        added_parents: set[int] = set()
        for message in group:
            reply_to = message.reply_to_message_id
            if reply_to and reply_to not in group_ids and reply_to not in added_parents:
                parent = local.get(reply_to)
                parent_text = parent.content if parent else str(external.get(reply_to, {}).get("content") or "")
                if parent_text:
                    lines.append(f"[Reply context from message {reply_to}] {parent_text}")
                    added_parents.add(reply_to)
            reply_marker = f" reply-to={reply_to}" if reply_to else ""
            lines.append(
                f"[{message.timestamp} message={message.message_id}{reply_marker}] "
                f"{message.author_name}: {message.content}"
            )
        text = "\n".join(lines)
        first, last = group[0], group[-1]
        participants = {message.author_key for message in group if message.author_key}
        result.append(ConversationRecord(
            conversation_id=_conversation_id(channel_id, parsed.source.path, first.message_id, last.message_id),
            course_id=parsed.source.course_id,
            root_id=root_id,
            channel_id=channel_id,
            channel_name=channel_name,
            channel_type=channel_type,
            first_message_id=first.message_id,
            last_message_id=last.message_id,
            started_at=first.timestamp,
            ended_at=last.timestamp,
            ended_at_epoch=last.timestamp_epoch,
            text=text,
            normalized_text=search_normalize(text),
            message_ids=[message.message_id for message in group],
            participant_count=len(participants),
            reaction_count=sum(message.reaction_count for message in group),
            is_pinned=any(message.is_pinned for message in group),
            metadata={
                "guild_id": str(parsed.guild.get("id") or ""),
                "guild_name": str(parsed.guild.get("name") or ""),
                "parent_channel_id": str(parsed.channel.get("categoryId") or "") or None,
                "channel_topic": parsed.channel.get("topic"),
            },
        ))
    return result
