-- Raw exports and attachment bytes survive independently of their derived indexes.
ALTER TABLE messages.archive_sources ADD COLUMN object_id text REFERENCES app.objects(id);
CREATE TABLE messages.archive_media (
    source_path text NOT NULL REFERENCES messages.archive_sources(path) ON DELETE CASCADE,
    relative_path text NOT NULL,
    object_id text NOT NULL REFERENCES app.objects(id),
    PRIMARY KEY(source_path,relative_path)
);
CREATE TABLE messages.export_cursors (
    root_id bigint NOT NULL,
    channel_id bigint NOT NULL,
    after_id bigint NOT NULL CHECK(after_id >= 0),
    next_at timestamptz NOT NULL,
    error text,
    PRIMARY KEY(root_id,channel_id)
);
CREATE TABLE app.discord_discovered_channels (
    channel_id bigint PRIMARY KEY,
    root_id bigint NOT NULL,
    guild_id bigint NOT NULL,
    name text NOT NULL,
    is_thread boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
