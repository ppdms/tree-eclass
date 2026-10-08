-- SQLite port of 021_discord_objects.sql.
-- Raw exports and attachment bytes survive independently of their derived indexes.
--
-- SQLite adaptations:
-- * Schema qualifiers dropped (messages./app.).
-- * boolean -> INTEGER; timestamptz -> TEXT with strftime now() default.
ALTER TABLE archive_sources ADD COLUMN object_id TEXT REFERENCES objects(id);
CREATE TABLE archive_media (
    source_path TEXT NOT NULL REFERENCES archive_sources(path) ON DELETE CASCADE,
    relative_path TEXT NOT NULL,
    object_id TEXT NOT NULL REFERENCES objects(id),
    PRIMARY KEY(source_path,relative_path)
);
CREATE TABLE export_cursors (
    root_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    after_id INTEGER NOT NULL CHECK(after_id >= 0),
    next_at TEXT NOT NULL,
    error TEXT,
    PRIMARY KEY(root_id,channel_id)
);
CREATE TABLE discord_discovered_channels (
    channel_id INTEGER PRIMARY KEY,
    root_id INTEGER NOT NULL,
    guild_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    is_thread INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
