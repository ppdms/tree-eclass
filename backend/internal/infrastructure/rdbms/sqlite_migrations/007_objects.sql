-- SQLite port of 007_objects.sql (postgres dialect source).
-- Schema prefixes dropped (no schemas in SQLite). timestamptz now()
-- defaults become TEXT DEFAULT (strftime(...,'now')). jsonb becomes TEXT.
CREATE TABLE objects (
    id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    media_type TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    UNIQUE (bucket, key, version_id)
);
CREATE TABLE document_revisions (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    logical_path TEXT NOT NULL,
    object_id TEXT NOT NULL REFERENCES objects(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    deleted_at TEXT,
    UNIQUE (document_id, object_id)
);
CREATE INDEX document_revision_path ON document_revisions(course_id, logical_path, created_at DESC);
CREATE TABLE native_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
