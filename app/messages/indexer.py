"""Incrementally ingest committed Discord archive artifacts."""

from __future__ import annotations

import logging
from pathlib import Path
import threading

from .archive import (
    DiscordArchiveError,
    build_conversations,
    discover_sources,
    parse_artifact,
    sha256_file,
    source_fingerprint,
)
from .config import MessageConfig
from .store import MessageStore, utc_now


_INDEX_REFRESH_LOCK = threading.RLock()


class MessageIndexer:
    def __init__(self, config: MessageConfig, store: MessageStore):
        self.config = config
        self.store = store

    def refresh(self) -> dict[str, object]:
        with _INDEX_REFRESH_LOCK:
            sources = discover_sources(self.config)
            indexed_sources = indexed_messages = indexed_conversations = skipped = 0
            failures: list[dict[str, str]] = []
            for source in sources:
                fingerprint = source_fingerprint(source)
                if self.store.source_is_current(source.path, fingerprint):
                    skipped += 1
                    continue
                try:
                    if (
                        self.config.verify_hashes
                        and source.expected_sha256
                        and sha256_file(source.path) != source.expected_sha256
                    ):
                        raise DiscordArchiveError(
                            f"Archive JSON hash does not match its manifest: {source.path}"
                        )
                    parsed = parse_artifact(source)
                    reference_ids = [
                        int(message.reply_to_message_id)
                        for message in parsed.messages
                        if message.reply_to_message_id is not None
                    ]
                    conversations = build_conversations(
                        parsed, self.config, self.store.referenced_messages(reference_ids)
                    )
                    self.store.replace_artifact(
                        source, fingerprint, parsed.guild, parsed.channel, parsed.exported_at,
                        parsed.messages, conversations,
                    )
                    indexed_sources += 1
                    indexed_messages += len(parsed.messages)
                    indexed_conversations += len(conversations)
                except Exception as exc:
                    logging.exception("Discord message artifact could not be indexed: %s", source.path)
                    failures.append({
                        "path": source.path,
                        "error": f"{type(exc).__name__}: {str(exc)}"[:1000],
                    })
            result: dict[str, object] = {
                "at": utc_now(),
                "archive_dir": str(Path(self.config.archive_dir)),
                "sources_discovered": len(sources),
                "sources_indexed": indexed_sources,
                "sources_skipped": skipped,
                "messages_indexed": indexed_messages,
                "conversations_indexed": indexed_conversations,
                "failures": failures,
            }
            self.store.set_state("last_refresh", result)
            return result
