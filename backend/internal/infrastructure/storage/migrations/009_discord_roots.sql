CREATE TABLE app.discord_root_channels (
    root_channel_id text PRIMARY KEY,
    name text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    updated_at timestamptz NOT NULL DEFAULT now()
);
