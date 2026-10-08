-- SQLite port of 003_messages.sql (postgres 003_messages.sql).
-- Schema prefixes stripped (single SQLite database, no schemas).
-- Approximations: BIGINT->INTEGER; DOUBLE PRECISION->REAL (timestamp_epoch); BYTEA->BLOB (vectors).

CREATE TABLE message_schema_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK(id=1), version INTEGER NOT NULL, updated_at TEXT NOT NULL
);

CREATE TABLE archive_sources (
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

CREATE TABLE channels (
    channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
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

CREATE TABLE conversations (
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

CREATE TABLE conversation_embeddings (
    conversation_id TEXT NOT NULL,
    model TEXT NOT NULL,
    vector BLOB NOT NULL,
    dimensions INTEGER NOT NULL,
    PRIMARY KEY(conversation_id, model),
    FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id) ON DELETE CASCADE
);

CREATE TABLE message_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_message_channels_course ON channels(course_id, root_id);

CREATE INDEX idx_messages_id ON messages(message_id);

CREATE INDEX idx_messages_channel_time ON messages(channel_id, message_id);

CREATE INDEX idx_messages_course_time ON messages(course_id, timestamp_epoch DESC);

CREATE INDEX idx_messages_reply ON messages(reply_to_message_id);

CREATE INDEX idx_conversations_course_time
    ON conversations(course_id, ended_at_epoch DESC);

INSERT INTO message_schema_version VALUES ('1','2','2026-09-07T18:34:09.156280+00:00');
