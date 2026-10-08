CREATE SCHEMA IF NOT EXISTS messages;
SET search_path TO messages, public;

CREATE TABLE messages.message_schema_version (
    id BIGINT PRIMARY KEY CHECK(id=1), version BIGINT NOT NULL, updated_at TEXT NOT NULL
);

CREATE TABLE messages.archive_sources (
    path TEXT PRIMARY KEY,
    root_id TEXT NOT NULL,
    course_id BIGINT NOT NULL,
    fingerprint TEXT NOT NULL,
    sha256 TEXT,
    channel_id BIGINT NOT NULL,
    exported_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('ready','failed')),
    indexed_at TEXT NOT NULL,
    error TEXT
);

CREATE TABLE messages.channels (
    channel_id BIGINT PRIMARY KEY,
    root_id BIGINT NOT NULL,
    course_id BIGINT NOT NULL,
    guild_id BIGINT,
    channel_name TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    parent_channel_id BIGINT,
    topic TEXT,
    source_path TEXT NOT NULL,
    exported_at TEXT
);

CREATE TABLE messages.messages (
    message_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    course_id BIGINT NOT NULL,
    timestamp TEXT NOT NULL,
    timestamp_epoch DOUBLE PRECISION NOT NULL,
    author_key TEXT,
    author_name TEXT NOT NULL,
    content TEXT NOT NULL,
    searchable_text TEXT NOT NULL,
    reply_to_message_id BIGINT,
    message_type TEXT NOT NULL,
    is_pinned BIGINT NOT NULL DEFAULT 0,
    reaction_count BIGINT NOT NULL DEFAULT 0,
    attachment_metadata_json TEXT NOT NULL DEFAULT '[]',
    source_path TEXT NOT NULL,
    PRIMARY KEY(source_path, message_id),
    FOREIGN KEY(source_path) REFERENCES messages.archive_sources(path) ON DELETE CASCADE
);

CREATE TABLE messages.conversations (
    conversation_id TEXT PRIMARY KEY,
    course_id BIGINT NOT NULL,
    root_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    channel_name TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    first_message_id BIGINT NOT NULL,
    last_message_id BIGINT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    ended_at_epoch DOUBLE PRECISION NOT NULL,
    text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    participant_count BIGINT NOT NULL,
    reaction_count BIGINT NOT NULL,
    is_pinned BIGINT NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    source_path TEXT NOT NULL,
    FOREIGN KEY(source_path) REFERENCES messages.archive_sources(path) ON DELETE CASCADE
);

CREATE TABLE messages.conversation_messages (
    conversation_id TEXT NOT NULL,
    message_id BIGINT NOT NULL,
    source_path TEXT NOT NULL,
    position BIGINT NOT NULL,
    PRIMARY KEY(conversation_id, source_path, message_id),
    FOREIGN KEY(conversation_id) REFERENCES messages.conversations(conversation_id)
        ON DELETE CASCADE,
    FOREIGN KEY(source_path, message_id) REFERENCES messages.messages(source_path, message_id)
        ON DELETE CASCADE
);

CREATE TABLE messages.conversation_embeddings (
    conversation_id TEXT NOT NULL,
    model TEXT NOT NULL,
    vector BYTEA NOT NULL,
    dimensions BIGINT NOT NULL,
    PRIMARY KEY(conversation_id, model),
    FOREIGN KEY(conversation_id) REFERENCES messages.conversations(conversation_id) ON DELETE CASCADE
);

CREATE TABLE messages.message_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_message_channels_course ON messages.channels(course_id, root_id);

CREATE INDEX idx_messages_id ON messages.messages(message_id);

CREATE INDEX idx_messages_channel_time ON messages.messages(channel_id, message_id);

CREATE INDEX idx_messages_course_time ON messages.messages(course_id, timestamp_epoch DESC);

CREATE INDEX idx_messages_reply ON messages.messages(reply_to_message_id);

CREATE INDEX idx_conversations_course_time
    ON messages.conversations(course_id, ended_at_epoch DESC);

INSERT INTO messages.message_schema_version VALUES ('1','2','2026-09-07T18:34:09.156280+00:00');
