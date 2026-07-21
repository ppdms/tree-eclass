"""Frontend-safe management of Discord root-channel to eClass course mappings."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DiscordRootChannel:
    root_id: str
    name: str
    mapped_course_id: int | None


def _object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def discover_root_channels(archive_dir: str, mapping: dict[str, int]) -> list[DiscordRootChannel]:
    """Return configured/exported root channels with human-readable archive names."""
    archive = Path(archive_dir)
    state = _object(archive / ".discord-export-state.json")
    roots = state.get("roots") if isinstance(state.get("roots"), dict) else {}
    root_ids = set(mapping) | {str(value) for value in roots if str(value).isdigit()}
    channels: list[DiscordRootChannel] = []
    for root_id in root_ids:
        root = roots.get(root_id) if isinstance(roots, dict) else None
        actual = root.get("actual_channels") if isinstance(root, dict) else None
        metadata = actual.get(root_id) if isinstance(actual, dict) else None
        raw_name = metadata.get("channel_name") if isinstance(metadata, dict) else None
        name = str(raw_name).strip() if raw_name else "Unavailable channel"
        channels.append(DiscordRootChannel(root_id, name, mapping.get(root_id)))
    return sorted(channels, key=lambda channel: (channel.name.casefold(), int(channel.root_id)))
