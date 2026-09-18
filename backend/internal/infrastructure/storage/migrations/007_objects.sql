CREATE TABLE app.objects (
    id text PRIMARY KEY,
    bucket text NOT NULL,
    key text NOT NULL,
    version_id text NOT NULL,
    sha256 text NOT NULL CHECK (length(sha256) = 64),
    bytes bigint NOT NULL CHECK (bytes >= 0),
    media_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket, key, version_id)
);
CREATE TABLE app.document_revisions (
    id text PRIMARY KEY,
    document_id text NOT NULL,
    course_id bigint NOT NULL REFERENCES app.courses(id) ON DELETE CASCADE,
    logical_path text NOT NULL,
    object_id text NOT NULL REFERENCES app.objects(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    UNIQUE (document_id, object_id)
);
CREATE INDEX document_revision_path ON app.document_revisions(course_id,logical_path,created_at DESC);
CREATE TABLE app.native_settings (
    key text PRIMARY KEY,
    value jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
