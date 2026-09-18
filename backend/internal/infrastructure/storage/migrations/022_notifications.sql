CREATE TABLE app.notification_messages (
    id text PRIMARY KEY,
    event_key text NOT NULL,
    position integer NOT NULL,
    target_hash text NOT NULL,
    content text NOT NULL CHECK(octet_length(content)<=8000),
    status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','sent','failed','canceled')),
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz,
    error text,
    UNIQUE(event_key,position)
);
CREATE INDEX notification_pending ON app.notification_messages(target_hash,available_at,created_at,position) WHERE status='pending';
CREATE TABLE app.notification_limits (
    target_hash text PRIMARY KEY,
    next_at timestamptz NOT NULL
);
