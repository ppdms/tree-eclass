-- SQLite port of 009_discord_roots.sql (postgres dialect source).
-- jsonb becomes TEXT; timestamptz now() default becomes strftime TEXT default.
CREATE TABLE discord_root_channels (
    root_channel_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
