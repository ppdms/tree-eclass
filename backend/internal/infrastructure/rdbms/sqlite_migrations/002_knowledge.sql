-- SQLite port of 002_knowledge.sql (postgres 002_knowledge.sql).
-- Schema prefixes stripped (single SQLite database, no schemas).
-- Approximations: BIGINT->INTEGER (+AUTOINCREMENT on INTEGER PRIMARY KEY); BYTEA->BLOB; partial WHERE indexes kept as-is (SQLite supports them).

CREATE TABLE knowledge_schema_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (id = 1), version INTEGER NOT NULL, updated_at TEXT NOT NULL
);

CREATE TABLE documents (
    id TEXT PRIMARY KEY,
    course_id INTEGER NOT NULL,
    course_name TEXT NOT NULL,
    course_short_name TEXT,
    source_path TEXT NOT NULL,
    source_origin TEXT NOT NULL DEFAULT 'eclass',
    normalized_path TEXT NOT NULL,
    source_url TEXT,
    display_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL DEFAULT '',
    source_etag TEXT,
    content_hash_verified INTEGER NOT NULL DEFAULT 0,
    mime_type TEXT,
    response_mime_type TEXT,
    document_kind TEXT NOT NULL,
    academic_year TEXT,
    source_modified_at TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    page_count INTEGER,
    source_size_bytes INTEGER,
    character_count INTEGER,
    word_count INTEGER,
    reading_minutes INTEGER,
    complexity_score INTEGER,
    complexity_label TEXT,
    language_hint TEXT,
    extractor_name TEXT,
    extractor_version TEXT,
    indexed_at TEXT,
    error TEXT,
    diagnostic_reason TEXT,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    UNIQUE(course_id, normalized_path)
);

CREATE TABLE archive_members (
    child_document_id TEXT PRIMARY KEY,
    parent_document_id TEXT NOT NULL,
    member_path TEXT NOT NULL,
    normalized_member_path TEXT NOT NULL,
    member_chain_json TEXT NOT NULL,
    depth INTEGER NOT NULL CHECK(depth >= 0),
    archive_format TEXT NOT NULL DEFAULT 'zip',
    parent_source_hash TEXT NOT NULL,
    parent_source_fingerprint TEXT NOT NULL,
    member_hash TEXT NOT NULL,
    crc32 INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL,
    expanded_size INTEGER NOT NULL,
    member_kind TEXT NOT NULL,
    mime_type TEXT,
    FOREIGN KEY(child_document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY(parent_document_id) REFERENCES documents(id) ON DELETE CASCADE,
    UNIQUE(parent_document_id, normalized_member_path)
);

CREATE TABLE chunks (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    locator_type TEXT NOT NULL,
    locator_start TEXT,
    locator_end TEXT,
    heading TEXT,
    text TEXT NOT NULL,
    normalized_text TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
    UNIQUE(document_id, ordinal)
);

CREATE TABLE index_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    requested_hash TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL CHECK(action IN ('upsert', 'delete')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'completed', 'failed', 'stale')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    completed_at TEXT,
    error TEXT,
    error_category TEXT,
    last_error TEXT,
    UNIQUE(course_id, normalized_path, requested_hash, action)
);

CREATE TABLE knowledge_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);

CREATE TABLE chunk_embeddings (
    chunk_id TEXT NOT NULL,
    model TEXT NOT NULL,
    vector BLOB NOT NULL,
    dimensions INTEGER NOT NULL,
    PRIMARY KEY(chunk_id, model),
    FOREIGN KEY(chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
);

CREATE TABLE document_enrichments (
    document_id TEXT PRIMARY KEY,
    source_hash TEXT NOT NULL,
    context_hash TEXT NOT NULL DEFAULT '',
    analysis_version TEXT NOT NULL DEFAULT '1',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'running', 'ready', 'failed')),
    model TEXT NOT NULL,
    payload_json TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    generated_at TEXT,
    error TEXT,
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE page_enrichments (
    document_id TEXT NOT NULL,
    page_number INTEGER NOT NULL CHECK(page_number > 0),
    source_hash TEXT NOT NULL,
    analysis_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'running', 'ready', 'failed')),
    model TEXT NOT NULL,
    requested_model TEXT NOT NULL,
    payload_json TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    generated_at TEXT,
    error TEXT,
    PRIMARY KEY(document_id, page_number),
    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE course_blueprints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER NOT NULL,
    revision INTEGER NOT NULL CHECK(revision > 0),
    revision_hash TEXT NOT NULL UNIQUE,
    evidence_hash TEXT NOT NULL,
    evidence_packet_json TEXT NOT NULL,
    analysis_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'running', 'ready', 'failed', 'stale')),
    requested_model TEXT NOT NULL,
    model TEXT NOT NULL,
    payload_json TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    created_at TEXT NOT NULL,
    generated_at TEXT,
    finished_at TEXT,
    error TEXT,
    superseded_by_revision INTEGER,
    UNIQUE(course_id, revision)
);

CREATE TABLE practice_question_sets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER NOT NULL,
    unit_key TEXT NOT NULL,
    set_hash TEXT NOT NULL UNIQUE,
    blueprint_revision_hash TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    evidence_packet_json TEXT NOT NULL,
    analysis_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'running', 'ready', 'failed', 'stale')),
    requested_model TEXT NOT NULL,
    model TEXT NOT NULL,
    payload_json TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    claimed_at TEXT,
    created_at TEXT NOT NULL,
    generated_at TEXT,
    finished_at TEXT,
    error TEXT
);

CREATE TABLE practice_questions (
    question_id TEXT NOT NULL,
    set_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    unit_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    question_key TEXT NOT NULL,
    response_mode TEXT NOT NULL,
    difficulty TEXT NOT NULL,
    estimated_minutes INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(set_id, question_id),
    FOREIGN KEY(set_id) REFERENCES practice_question_sets(id) ON DELETE CASCADE
);

CREATE INDEX idx_documents_course_current ON documents(course_id, is_current, status);

CREATE INDEX idx_archive_members_parent
    ON archive_members(parent_document_id, normalized_member_path);

CREATE INDEX idx_chunks_document_locator ON chunks(document_id, locator_type, locator_start);

CREATE INDEX idx_jobs_claim ON index_jobs(status, available_at, id);

CREATE INDEX idx_document_enrichments_claim
    ON document_enrichments(status, available_at, document_id);

CREATE INDEX idx_document_enrichments_priority_claim
    ON document_enrichments(status, priority DESC, available_at, document_id);

CREATE INDEX idx_page_enrichments_claim
    ON page_enrichments(status, priority DESC, available_at, document_id, page_number);

CREATE INDEX idx_course_blueprints_claim
    ON course_blueprints(status, priority DESC, available_at, id);

CREATE INDEX idx_course_blueprints_history
    ON course_blueprints(course_id, revision DESC);

CREATE UNIQUE INDEX idx_course_blueprints_ready
    ON course_blueprints(course_id) WHERE status='ready';

CREATE UNIQUE INDEX idx_course_blueprints_active_revision
    ON course_blueprints(course_id, revision_hash)
    WHERE status IN ('pending', 'running', 'ready');

CREATE INDEX idx_practice_sets_claim
    ON practice_question_sets(status, priority DESC, available_at, id);

CREATE INDEX idx_practice_sets_revision
    ON practice_question_sets(course_id, blueprint_revision_hash, status);

CREATE UNIQUE INDEX idx_practice_sets_ready
    ON practice_question_sets(course_id, unit_key) WHERE status='ready';

CREATE UNIQUE INDEX idx_practice_sets_active
    ON practice_question_sets(course_id, unit_key)
    WHERE status IN ('pending', 'running');

CREATE INDEX idx_practice_questions_unit
    ON practice_questions(course_id, unit_key, ordinal);

CREATE INDEX idx_practice_questions_identity
    ON practice_questions(question_id);

INSERT INTO knowledge_schema_version VALUES ('1','17','2026-09-07T18:34:09.079831+00:00');
