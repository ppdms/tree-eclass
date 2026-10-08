-- SQLite port of 022_notifications.sql.
--
-- SQLite adaptations:
-- * Schema qualifiers dropped (app.).
-- * timestamptz -> TEXT with strftime now() defaults.
-- * octet_length(content) has no SQLite equivalent; length(CAST(content AS BLOB))
--   measures the same UTF-8 byte length.
CREATE TABLE notification_messages (
    id TEXT PRIMARY KEY,
    event_key TEXT NOT NULL,
    position INTEGER NOT NULL,
    target_hash TEXT NOT NULL,
    content TEXT NOT NULL CHECK(length(CAST(content AS BLOB))<=8000),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','sent','failed','canceled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    sent_at TEXT,
    error TEXT,
    UNIQUE(event_key,position)
);
CREATE INDEX notification_pending ON notification_messages(target_hash,available_at,created_at,position) WHERE status='pending';
CREATE TABLE notification_limits (
    target_hash TEXT PRIMARY KEY,
    next_at TEXT NOT NULL
);
