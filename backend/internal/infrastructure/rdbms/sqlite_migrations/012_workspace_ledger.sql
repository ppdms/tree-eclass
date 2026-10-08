-- SQLite port of 012_workspace_ledger.sql (postgres dialect source).
-- SQLite allows only one ADD COLUMN per ALTER TABLE, so the two new
-- workspace-session columns are added in separate statements.
ALTER TABLE study_workspace_sessions ADD COLUMN confidence INTEGER CHECK(confidence IS NULL OR (confidence BETWEEN 0 AND 5));
ALTER TABLE study_workspace_sessions ADD COLUMN study_event_id INTEGER;
ALTER TABLE study_reading_beats ADD COLUMN request_hash TEXT;
-- Attention belongs to exact source bytes; a new revision on the same page
-- must not be merged into the first revision's provenance.
-- SQLite has no DROP CONSTRAINT / ADD CONSTRAINT, so the unique scope
-- change from (session_id, document_id, page_number) to
-- (session_id, document_id, source_hash, page_number) is applied with a
-- table rebuild. Column inventory is unchanged from 001 plus this file's
-- ADD COLUMNs (reading beats gains request_hash only; spans gain none).
-- Indexes on study_reading_spans travel with the rename, so they are
-- recreated on the rebuilt table (IF NOT EXISTS keeps reruns safe).
ALTER TABLE study_reading_spans RENAME TO study_reading_spans_legacy_012;
CREATE TABLE study_reading_spans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    document_id TEXT NOT NULL,
    source_hash TEXT NOT NULL DEFAULT '',
    page_number INTEGER NOT NULL CHECK(page_number > 0),
    action_id TEXT NOT NULL DEFAULT '',
    unit_key TEXT NOT NULL DEFAULT '',
    plan_revision TEXT NOT NULL DEFAULT '',
    active_seconds INTEGER NOT NULL DEFAULT 0 CHECK(active_seconds >= 0),
    visible_seconds INTEGER NOT NULL DEFAULT 0 CHECK(visible_seconds >= 0),
    started_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    ended_at TEXT,
    FOREIGN KEY (session_id) REFERENCES study_workspace_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE,
    UNIQUE(session_id, document_id, source_hash, page_number)
);
INSERT INTO study_reading_spans(id, session_id, course_id, document_id, source_hash, page_number, action_id, unit_key, plan_revision, active_seconds, visible_seconds, started_at, ended_at) SELECT id, session_id, course_id, document_id, source_hash, page_number, action_id, unit_key, plan_revision, active_seconds, visible_seconds, started_at, ended_at FROM study_reading_spans_legacy_012;
DROP TABLE study_reading_spans_legacy_012;
CREATE INDEX IF NOT EXISTS idx_reading_spans_document ON study_reading_spans(document_id, page_number);
CREATE INDEX IF NOT EXISTS idx_reading_spans_course_document ON study_reading_spans(course_id, document_id, page_number);
CREATE INDEX IF NOT EXISTS idx_reading_spans_action ON study_reading_spans(course_id, action_id);
