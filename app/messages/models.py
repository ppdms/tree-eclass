"""Models used only by the Discord message backend."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ArchiveSource:
    root_id: str
    course_id: int
    path: str
    expected_sha256: str | None


@dataclass
class MessageRecord:
    message_id: int
    channel_id: int
    course_id: int
    timestamp: str
    timestamp_epoch: float
    author_key: str | None
    author_name: str
    content: str
    searchable_text: str
    reply_to_message_id: int | None
    message_type: str
    is_pinned: bool
    reaction_count: int
    attachment_metadata: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ConversationRecord:
    conversation_id: str
    course_id: int
    root_id: int
    channel_id: int
    channel_name: str
    channel_type: str
    first_message_id: int
    last_message_id: int
    started_at: str
    ended_at: str
    ended_at_epoch: float
    text: str
    normalized_text: str
    message_ids: list[int]
    participant_count: int
    reaction_count: int
    is_pinned: bool
    metadata: dict[str, Any] = field(default_factory=dict)
