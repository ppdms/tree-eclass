"""SQLite storage and source-specific retrieval for Discord course messages."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
import sqlite3
from typing import Any, Iterator

from app.knowledge.embeddings import (
    LOCAL_MODEL_NAME,
    EmbeddingProvider,
    cosine,
    embed_text,
    pack_vector,
    unpack_vector,
)
from app.knowledge.normalization import search_normalize

from .models import ArchiveSource, ConversationRecord, MessageRecord


MESSAGE_SCHEMA_VERSION = 2

MESSAGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS message_schema_version (
    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_sources (
    path TEXT PRIMARY KEY,
    root_id TEXT NOT NULL,
    course_id INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    sha256 TEXT,
    channel_id INTEGER NOT NULL,
    exported_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('ready','failed')),
    indexed_at TEXT NOT NULL,
    error TEXT
);
CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    root_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    guild_id INTEGER,
    channel_name TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    parent_channel_id INTEGER,
    topic TEXT,
    source_path TEXT NOT NULL,
    exported_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_message_channels_course ON channels(course_id, root_id);
CREATE TABLE IF NOT EXISTS messages (
    message_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    timestamp_epoch REAL NOT NULL,
    author_key TEXT,
    author_name TEXT NOT NULL,
    content TEXT NOT NULL,
    searchable_text TEXT NOT NULL,
    reply_to_message_id INTEGER,
    message_type TEXT NOT NULL,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    reaction_count INTEGER NOT NULL DEFAULT 0,
    attachment_metadata_json TEXT NOT NULL DEFAULT '[]',
    source_path TEXT NOT NULL,
    PRIMARY KEY(source_path, message_id),
    FOREIGN KEY(source_path) REFERENCES archive_sources(path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_messages_id ON messages(message_id);
CREATE INDEX IF NOT EXISTS idx_messages_channel_time ON messages(channel_id, message_id);
CREATE INDEX IF NOT EXISTS idx_messages_course_time ON messages(course_id, timestamp_epoch DESC);
CREATE INDEX IF NOT EXISTS idx_messages_reply ON messages(reply_to_message_id);
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    course_id INTEGER NOT NULL,
    root_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    channel_name TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    first_message_id INTEGER NOT NULL,
    last_message_id INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    ended_at_epoch REAL NOT NULL,
    text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    participant_count INTEGER NOT NULL,
    reaction_count INTEGER NOT NULL,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    source_path TEXT NOT NULL,
    FOREIGN KEY(source_path) REFERENCES archive_sources(path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_conversations_course_time
    ON conversations(course_id, ended_at_epoch DESC);
CREATE TABLE IF NOT EXISTS conversation_messages (
    conversation_id TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY(conversation_id, source_path, message_id),
    FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    FOREIGN KEY(source_path, message_id) REFERENCES messages(source_path, message_id)
        ON DELETE CASCADE
);
CREATE VIRTUAL TABLE IF NOT EXISTS conversations_fts USING fts5(
    conversation_id UNINDEXED, text, normalized_text, channel_name,
    tokenize='unicode61 remove_diacritics 2'
);
CREATE TABLE IF NOT EXISTS conversation_embeddings (
    conversation_id TEXT NOT NULL,
    model TEXT NOT NULL,
    vector BLOB NOT NULL,
    dimensions INTEGER NOT NULL,
    PRIMARY KEY(conversation_id, model),
    FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS message_state (
    key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL
);
"""

_POLICY_TERMS = tuple(search_normalize(value) for value in (
    "grading", "grade", "assessment", "exam", "deadline", "instructor", "laboratory",
    "βαθμολογία", "βαθμός", "αξιολόγηση", "εξέταση", "εργαστήριο", "πρόοδος",
    "υποχρεωτική", "διδάσκων", "προθεσμία", "ισχύει", "φέτος",
))
_STOPWORDS = {
    "a", "an", "and", "are", "for", "how", "in", "is", "of", "the", "to", "what",
    "για", "ειναι", "η", "και", "με", "ο", "οι", "ποιο", "πως", "σε", "στη", "στην",
    "στο", "τα", "τη", "την", "τι", "το", "του", "των",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MessageStore:
    def __init__(self, db_file: str, embedding_provider: EmbeddingProvider | None = None):
        self.db_file = db_file
        self.embedding_provider = embedding_provider or EmbeddingProvider.from_env()
        parent = os.path.dirname(os.path.abspath(db_file))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_file, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connection() as connection:
            version_row = connection.execute(
                "SELECT version FROM message_schema_version WHERE id=1"
            ).fetchone() if connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_schema_version'"
            ).fetchone() else None
            if version_row and int(version_row["version"]) < 2:
                self._migrate_v1_to_v2(connection)
            connection.executescript(MESSAGE_SCHEMA)
            connection.execute(
                "INSERT INTO message_schema_version(id,version,updated_at) VALUES(1,?,?) "
                "ON CONFLICT(id) DO UPDATE SET version=excluded.version,updated_at=excluded.updated_at",
                (MESSAGE_SCHEMA_VERSION, utc_now()),
            )
            connection.commit()

    @staticmethod
    def _migrate_v1_to_v2(connection: sqlite3.Connection) -> None:
        """Scope message identity to an artifact without rebuilding indexed sources."""
        connection.commit()
        connection.execute("PRAGMA foreign_keys=OFF")
        try:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                ALTER TABLE conversation_messages RENAME TO conversation_messages_v1;
                ALTER TABLE messages RENAME TO messages_v1;

                CREATE TABLE messages (
                    message_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    course_id INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    timestamp_epoch REAL NOT NULL,
                    author_key TEXT,
                    author_name TEXT NOT NULL,
                    content TEXT NOT NULL,
                    searchable_text TEXT NOT NULL,
                    reply_to_message_id INTEGER,
                    message_type TEXT NOT NULL,
                    is_pinned INTEGER NOT NULL DEFAULT 0,
                    reaction_count INTEGER NOT NULL DEFAULT 0,
                    attachment_metadata_json TEXT NOT NULL DEFAULT '[]',
                    source_path TEXT NOT NULL,
                    PRIMARY KEY(source_path, message_id),
                    FOREIGN KEY(source_path) REFERENCES archive_sources(path) ON DELETE CASCADE
                );
                INSERT INTO messages SELECT * FROM messages_v1;

                CREATE TABLE conversation_messages (
                    conversation_id TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    source_path TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY(conversation_id, source_path, message_id),
                    FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id)
                        ON DELETE CASCADE,
                    FOREIGN KEY(source_path, message_id) REFERENCES messages(source_path, message_id)
                        ON DELETE CASCADE
                );
                INSERT INTO conversation_messages(conversation_id,message_id,source_path,position)
                SELECT cm.conversation_id,cm.message_id,c.source_path,cm.position
                  FROM conversation_messages_v1 cm
                  JOIN conversations c ON c.conversation_id=cm.conversation_id;

                DROP TABLE conversation_messages_v1;
                DROP TABLE messages_v1;
                CREATE INDEX idx_messages_id ON messages(message_id);
                CREATE INDEX idx_messages_channel_time ON messages(channel_id, message_id);
                CREATE INDEX idx_messages_course_time ON messages(course_id, timestamp_epoch DESC);
                CREATE INDEX idx_messages_reply ON messages(reply_to_message_id);
                COMMIT;
                """
            )
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys=ON")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise sqlite3.IntegrityError(
                f"message schema migration produced foreign-key violations: {violations[:3]}"
            )

    def set_state(self, key: str, value: Any) -> None:
        serialized = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        with self.connection() as connection:
            connection.execute(
                "INSERT INTO message_state(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (key, serialized, utc_now()),
            )
            connection.commit()

    def get_state(self, key: str) -> dict[str, Any] | str | None:
        with self.connection() as connection:
            row = connection.execute("SELECT value FROM message_state WHERE key=?", (key,)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return str(row["value"])

    def source_is_current(self, path: str, fingerprint: str) -> bool:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM archive_sources WHERE path=? AND fingerprint=? AND status='ready'",
                (path, fingerprint),
            ).fetchone()
        return bool(row)

    def referenced_messages(self, message_ids: list[int]) -> dict[int, dict[str, Any]]:
        unique = list(dict.fromkeys(message_ids))
        if not unique:
            return {}
        placeholders = ",".join("?" for _ in unique)
        with self.connection() as connection:
            rows = connection.execute(
                f"SELECT message_id,content,author_name,timestamp FROM messages "
                f"WHERE message_id IN ({placeholders}) GROUP BY message_id",
                unique,
            ).fetchall()
        return {int(row["message_id"]): dict(row) for row in rows}

    def _embedding_sets(self, texts: list[str]) -> list[tuple[str, list[list[float]]]]:
        if not texts:
            return []
        batch = self.embedding_provider.embed_texts(texts)
        result = [(batch.model, batch.vectors)]
        if batch.model != LOCAL_MODEL_NAME and self.embedding_provider.local_fallback:
            result.append((LOCAL_MODEL_NAME, [embed_text(text) for text in texts]))
        return result

    def replace_artifact(
        self,
        source: ArchiveSource,
        fingerprint: str,
        guild: dict[str, Any],
        channel: dict[str, Any],
        exported_at: str | None,
        messages: list[MessageRecord],
        conversations: list[ConversationRecord],
    ) -> None:
        embedding_sets = self._embedding_sets([item.text for item in conversations])
        channel_id = int(channel["id"])
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            old_ids = [
                row[0] for row in connection.execute(
                    "SELECT conversation_id FROM conversations WHERE source_path=?", (source.path,)
                )
            ]
            if old_ids:
                connection.executemany(
                    "DELETE FROM conversations_fts WHERE conversation_id=?",
                    ((conversation_id_value,) for conversation_id_value in old_ids),
                )
            connection.execute("DELETE FROM archive_sources WHERE path=?", (source.path,))
            connection.execute(
                """INSERT INTO archive_sources(
                       path,root_id,course_id,fingerprint,sha256,channel_id,exported_at,status,indexed_at,error)
                   VALUES(?,?,?,?,?,?,?,'ready',?,NULL)""",
                (source.path, source.root_id, source.course_id, fingerprint, source.expected_sha256,
                 channel_id, exported_at, utc_now()),
            )
            guild_id_text = str(guild.get("id") or "")
            parent_text = str(channel.get("categoryId") or "")
            connection.execute(
                """INSERT INTO channels(
                       channel_id,root_id,course_id,guild_id,channel_name,channel_type,
                       parent_channel_id,topic,source_path,exported_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(channel_id) DO UPDATE SET
                       root_id=excluded.root_id,course_id=excluded.course_id,guild_id=excluded.guild_id,
                       channel_name=excluded.channel_name,channel_type=excluded.channel_type,
                       parent_channel_id=excluded.parent_channel_id,topic=excluded.topic,
                       source_path=excluded.source_path,exported_at=excluded.exported_at""",
                (channel_id, int(source.root_id), source.course_id,
                 int(guild_id_text) if guild_id_text.isdigit() else None,
                 str(channel.get("name") or channel_id), str(channel.get("type") or "Unknown"),
                 int(parent_text) if parent_text.isdigit() else None,
                 channel.get("topic"), source.path, exported_at),
            )
            connection.executemany(
                """INSERT INTO messages(
                       message_id,channel_id,course_id,timestamp,timestamp_epoch,author_key,author_name,
                       content,searchable_text,reply_to_message_id,message_type,is_pinned,reaction_count,
                       attachment_metadata_json,source_path)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                ((item.message_id, item.channel_id, item.course_id, item.timestamp, item.timestamp_epoch,
                  item.author_key, item.author_name, item.content, item.searchable_text,
                  item.reply_to_message_id, item.message_type, int(item.is_pinned), item.reaction_count,
                  json.dumps(item.attachment_metadata, ensure_ascii=False), source.path)
                 for item in messages),
            )
            for index, item in enumerate(conversations):
                connection.execute(
                    """INSERT INTO conversations(
                           conversation_id,course_id,root_id,channel_id,channel_name,channel_type,
                           first_message_id,last_message_id,started_at,ended_at,ended_at_epoch,text,
                           normalized_text,participant_count,reaction_count,is_pinned,metadata_json,source_path)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (item.conversation_id, item.course_id, item.root_id, item.channel_id,
                     item.channel_name, item.channel_type, item.first_message_id, item.last_message_id,
                     item.started_at, item.ended_at, item.ended_at_epoch, item.text,
                     item.normalized_text, item.participant_count, item.reaction_count,
                     int(item.is_pinned), json.dumps(item.metadata, ensure_ascii=False), source.path),
                )
                connection.executemany(
                    "INSERT INTO conversation_messages("
                    "conversation_id,message_id,source_path,position) VALUES(?,?,?,?)",
                    ((item.conversation_id, message_id, source.path, position)
                     for position, message_id in enumerate(item.message_ids)),
                )
                connection.execute(
                    "INSERT INTO conversations_fts(conversation_id,text,normalized_text,channel_name) "
                    "VALUES(?,?,?,?)",
                    (item.conversation_id, item.text, item.normalized_text, item.channel_name),
                )
                for model, vectors in embedding_sets:
                    vector = vectors[index]
                    connection.execute(
                        "INSERT INTO conversation_embeddings(conversation_id,model,vector,dimensions) "
                        "VALUES(?,?,?,?)",
                        (item.conversation_id, model, pack_vector(vector), len(vector)),
                    )
            connection.commit()

    @staticmethod
    def _course_clause(course_ids: list[int]) -> tuple[str, list[Any]]:
        if not course_ids:
            return "0", []
        return f"c.course_id IN ({','.join('?' for _ in course_ids)})", list(course_ids)

    def _lexical_search(self, query: str, course_ids: list[int], limit: int) -> list[dict[str, Any]]:
        tokens = [
            token for token in search_normalize(query).split()
            if token and token not in _STOPWORDS
        ]
        if not tokens:
            return []
        match = " OR ".join('"' + token.replace('"', '""') + '"' for token in tokens)
        course_clause, params = self._course_clause(course_ids)
        with self.connection() as connection:
            rows = connection.execute(
                f"""SELECT c.*,bm25(conversations_fts,0,1,0.65,0.2) AS lexical_score,
                            snippet(conversations_fts,1,'[',']',' … ',40) AS excerpt
                     FROM conversations_fts JOIN conversations c
                       ON c.conversation_id=conversations_fts.conversation_id
                     WHERE conversations_fts MATCH ? AND {course_clause}
                     ORDER BY lexical_score,c.ended_at_epoch DESC LIMIT ?""",
                [match, *params, limit],
            ).fetchall()
        return [dict(row) for row in rows]

    def _semantic_search(
        self, query: str, course_ids: list[int], limit: int,
        scan_limit: int, lexical_ids: list[str],
    ) -> list[dict[str, Any]]:
        batch = self.embedding_provider.embed_texts([query])
        model = batch.model
        query_vector = batch.vectors[0]
        course_clause, params = self._course_clause(course_ids)
        with self.connection() as connection:
            recent = connection.execute(
                f"SELECT c.conversation_id FROM conversations c WHERE {course_clause} "
                "ORDER BY c.ended_at_epoch DESC LIMIT ?",
                [*params, scan_limit],
            ).fetchall()
            candidate_ids = list(dict.fromkeys([
                *(str(row["conversation_id"]) for row in recent), *lexical_ids,
            ]))
            if not candidate_ids:
                return []
            placeholders = ",".join("?" for _ in candidate_ids)
            rows = connection.execute(
                f"""SELECT c.*,e.vector,e.dimensions,e.model AS embedding_model
                     FROM conversations c JOIN conversation_embeddings e
                       ON e.conversation_id=c.conversation_id
                     WHERE e.model=? AND c.conversation_id IN ({placeholders})""",
                [model, *candidate_ids],
            ).fetchall()
            if not rows and model != LOCAL_MODEL_NAME:
                model = LOCAL_MODEL_NAME
                query_vector = embed_text(query)
                rows = connection.execute(
                    f"""SELECT c.*,e.vector,e.dimensions,e.model AS embedding_model
                         FROM conversations c JOIN conversation_embeddings e
                           ON e.conversation_id=c.conversation_id
                         WHERE e.model=? AND c.conversation_id IN ({placeholders})""",
                    [model, *candidate_ids],
                ).fetchall()
        scored: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["semantic_score"] = cosine(
                query_vector, unpack_vector(item.pop("vector"), item.pop("dimensions"))
            )
            scored.append(item)
        scored.sort(key=lambda item: (-item["semantic_score"], -item["ended_at_epoch"]))
        return scored[:limit]

    @staticmethod
    def _is_policy_query(query: str) -> bool:
        normalized = search_normalize(query)
        return any(term in normalized for term in _POLICY_TERMS)

    @staticmethod
    def _rank_multiplier(item: dict[str, Any], half_life_days: float) -> tuple[float, float, float]:
        age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - float(item["ended_at_epoch"]))
        freshness = 2 ** (-(age_seconds / 86400.0) / max(1.0, half_life_days))
        normalized_channel = search_normalize(str(item.get("channel_name") or ""))
        reliable_channel = "ανακοιν" in normalized_channel or bool(item.get("is_pinned"))
        source_weight = 0.95 if reliable_channel else 0.85
        reactions = max(0, int(item.get("reaction_count") or 0))
        participants = max(0, int(item.get("participant_count") or 0))
        quality = 1.0 + min(0.10, math.log1p(reactions) * 0.02 + min(3, participants) * 0.01)
        return freshness, source_weight, quality

    def search(
        self, query: str, course_ids: list[int], limit: int,
        mode: str, semantic_scan_limit: int,
        policy_half_life_days: float, general_half_life_days: float,
    ) -> list[dict[str, Any]]:
        if mode not in {"lexical", "semantic", "hybrid"}:
            raise ValueError("message search mode must be lexical, semantic, or hybrid")
        candidate_limit = max(40, limit * 8)
        lexical = self._lexical_search(query, course_ids, candidate_limit) if mode != "semantic" else []
        semantic = self._semantic_search(
            query, course_ids, candidate_limit, semantic_scan_limit,
            [str(item["conversation_id"]) for item in lexical],
        ) if mode != "lexical" else []
        merged: dict[str, dict[str, Any]] = {}
        for rank, row in enumerate(lexical, 1):
            item = merged.setdefault(str(row["conversation_id"]), dict(row))
            item["lexical_score"] = row["lexical_score"]
            item["_lexical_rank"] = rank
        for rank, row in enumerate(semantic, 1):
            item = merged.setdefault(str(row["conversation_id"]), dict(row))
            item.update({key: value for key, value in row.items() if key != "semantic_score"})
            item["semantic_score"] = row["semantic_score"]
            item["_semantic_rank"] = rank
        half_life = policy_half_life_days if self._is_policy_query(query) else general_half_life_days
        for item in merged.values():
            lexical_rank = item.pop("_lexical_rank", None)
            semantic_rank = item.pop("_semantic_rank", None)
            if mode == "lexical":
                base = 1.0 / (60 + lexical_rank)
            elif mode == "semantic":
                base = 1.0 / (60 + semantic_rank)
            else:
                base = (
                    0.55 / (60 + lexical_rank) if lexical_rank else 0.0
                ) + (
                    0.45 / (60 + semantic_rank) if semantic_rank else 0.0
                )
            freshness, source_weight, quality = self._rank_multiplier(item, half_life)
            item["freshness_score"] = round(freshness, 6)
            item["source_weight"] = source_weight
            item["quality_multiplier"] = round(quality, 6)
            item["score"] = base * (0.70 + 0.60 * freshness) * source_weight * quality
            item["retrieval_mode"] = mode
            item.setdefault("lexical_score", None)
            item.setdefault("semantic_score", None)
        return sorted(
            merged.values(), key=lambda item: (-item["score"], -item["ended_at_epoch"])
        )[:limit]

    @staticmethod
    def _message_url(guild_id: str | int | None, channel_id: int, message_id: int) -> str | None:
        guild = str(guild_id or "")
        if not guild.isdigit():
            return None
        return f"https://discord.com/channels/{guild}/{channel_id}/{message_id}"

    @staticmethod
    def _academic_year(timestamp: str) -> str | None:
        try:
            value = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return None
        start = value.year if value.month >= 9 else value.year - 1
        return f"{start}-{str(start + 1)[-2:]}"

    def format_hit(self, row: dict[str, Any]) -> dict[str, Any]:
        metadata = json.loads(row.get("metadata_json") or "{}")
        first_id = int(row["first_message_id"])
        last_id = int(row["last_message_id"])
        message_ids = self.conversation_message_ids(str(row["conversation_id"]))
        message_urls = [
            self._message_url(metadata.get("guild_id"), int(row["channel_id"]), message_id)
            for message_id in message_ids
        ]
        return {
            "conversation_id": row["conversation_id"],
            "course_id": row["course_id"],
            "source_type": "discord",
            "evidence_class": "community_discussion",
            "channel_id": str(row["channel_id"]),
            "channel_name": row["channel_name"],
            "channel_type": row["channel_type"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "academic_year": self._academic_year(str(row["ended_at"])),
            "first_message_id": str(first_id),
            "last_message_id": str(last_id),
            "message_count": len(message_ids),
            "message_ids": [str(message_id) for message_id in message_ids],
            "message_urls": [url for url in message_urls if url],
            "participant_count": row["participant_count"],
            "reaction_count": row["reaction_count"],
            "is_pinned": bool(row["is_pinned"]),
            "excerpt": row.get("excerpt") or row["text"][:1200],
            "retrieval_score": row["score"],
            "retrieval_mode": row["retrieval_mode"],
            "lexical_score": row.get("lexical_score"),
            "semantic_score": row.get("semantic_score"),
            "freshness_score": row["freshness_score"],
            "source_weight": row["source_weight"],
            "quality_multiplier": row["quality_multiplier"],
            "message_url": self._message_url(metadata.get("guild_id"), int(row["channel_id"]), first_id),
            "resource_uri": f"discord://conversations/{row['conversation_id']}",
            "metadata": metadata,
            "untrusted_content": True,
        }

    def conversation_message_ids(self, conversation_id: str) -> list[int]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT message_id FROM conversation_messages WHERE conversation_id=? ORDER BY position",
                (conversation_id,),
            ).fetchall()
        return [int(row["message_id"]) for row in rows]

    @staticmethod
    def _message_dict(row: sqlite3.Row, guild_id: str | int | None) -> dict[str, Any]:
        return {
            "message_id": str(row["message_id"]),
            "channel_id": str(row["channel_id"]),
            "timestamp": row["timestamp"],
            "author_name": row["author_name"],
            "content": row["content"],
            "reply_to_message_id": str(row["reply_to_message_id"]) if row["reply_to_message_id"] else None,
            "message_type": row["message_type"],
            "is_pinned": bool(row["is_pinned"]),
            "reaction_count": row["reaction_count"],
            "attachments": json.loads(row["attachment_metadata_json"] or "[]"),
            "message_url": MessageStore._message_url(guild_id, int(row["channel_id"]), int(row["message_id"])),
            "untrusted_content": True,
        }

    def read_conversation(self, conversation_id: str, context_before: int, context_after: int) -> dict[str, Any] | None:
        with self.connection() as connection:
            conversation = connection.execute(
                "SELECT * FROM conversations WHERE conversation_id=?", (conversation_id,)
            ).fetchone()
            if not conversation:
                return None
            metadata = json.loads(conversation["metadata_json"] or "{}")
            messages = connection.execute(
                """SELECT m.* FROM conversation_messages cm JOIN messages m
                     ON m.message_id=cm.message_id AND m.source_path=cm.source_path
                   WHERE cm.conversation_id=? ORDER BY cm.position""",
                (conversation_id,),
            ).fetchall()
            before = connection.execute(
                "SELECT * FROM messages WHERE channel_id=? AND message_id<? "
                "GROUP BY message_id ORDER BY message_id DESC LIMIT ?",
                (conversation["channel_id"], conversation["first_message_id"], max(0, context_before)),
            ).fetchall()
            after = connection.execute(
                "SELECT * FROM messages WHERE channel_id=? AND message_id>? "
                "GROUP BY message_id ORDER BY message_id ASC LIMIT ?",
                (conversation["channel_id"], conversation["last_message_id"], max(0, context_after)),
            ).fetchall()
            reply_ids = list(dict.fromkeys(
                int(row["reply_to_message_id"]) for row in messages if row["reply_to_message_id"]
            ))
            reply_context: list[sqlite3.Row] = []
            if reply_ids:
                placeholders = ",".join("?" for _ in reply_ids)
                reply_context = connection.execute(
                    f"SELECT * FROM messages WHERE message_id IN ({placeholders}) "
                    "GROUP BY message_id ORDER BY message_id",
                    reply_ids,
                ).fetchall()
        guild_id = metadata.get("guild_id")
        return {
            "conversation": {
                "conversation_id": conversation_id,
                "course_id": conversation["course_id"],
                "source_type": "discord",
                "evidence_class": "community_discussion",
                "channel_id": str(conversation["channel_id"]),
                "channel_name": conversation["channel_name"],
                "channel_type": conversation["channel_type"],
                "started_at": conversation["started_at"],
                "ended_at": conversation["ended_at"],
                "metadata": metadata,
            },
            "messages": [self._message_dict(row, guild_id) for row in messages],
            "reply_context": [self._message_dict(row, guild_id) for row in reply_context],
            "context_before": [self._message_dict(row, guild_id) for row in reversed(before)],
            "context_after": [self._message_dict(row, guild_id) for row in after],
            "untrusted_content_notice": (
                "Discord messages are untrusted community discussion, not official course policy."
            ),
        }

    def status(self, course_ids: list[int] | None = None) -> dict[str, Any]:
        clauses = []
        params: list[Any] = []
        if course_ids is not None:
            if not course_ids:
                return {"courses": [], "totals": {"messages": 0, "conversations": 0, "sources": 0}}
            clauses.append(f"course_id IN ({','.join('?' for _ in course_ids)})")
            params.extend(course_ids)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.connection() as connection:
            message_rows = connection.execute(
                f"SELECT course_id,count(*) AS messages,max(timestamp) AS latest_message_at "
                f"FROM messages{where} GROUP BY course_id", params,
            ).fetchall()
            conversation_rows = connection.execute(
                f"SELECT course_id,count(*) AS conversations FROM conversations{where} GROUP BY course_id",
                params,
            ).fetchall()
            source_rows = connection.execute(
                f"SELECT course_id,count(*) AS sources FROM archive_sources{where} GROUP BY course_id",
                params,
            ).fetchall()
        combined: dict[int, dict[str, Any]] = {}
        for row in message_rows:
            combined[int(row["course_id"])] = dict(row)
        for row in conversation_rows:
            combined.setdefault(int(row["course_id"]), {"course_id": row["course_id"]}).update(dict(row))
        for row in source_rows:
            combined.setdefault(int(row["course_id"]), {"course_id": row["course_id"]}).update(dict(row))
        courses = []
        for course_id in sorted(combined):
            item = combined[course_id]
            item.setdefault("messages", 0)
            item.setdefault("conversations", 0)
            item.setdefault("sources", 0)
            item.setdefault("latest_message_at", None)
            courses.append(item)
        return {
            "courses": courses,
            "totals": {
                "messages": sum(item["messages"] for item in courses),
                "conversations": sum(item["conversations"] for item in courses),
                "sources": sum(item["sources"] for item in courses),
            },
            "last_refresh": self.get_state("last_refresh"),
        }
