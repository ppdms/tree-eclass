ALTER TABLE app.study_workspace_sessions
    ADD COLUMN confidence bigint CHECK(confidence IS NULL OR confidence BETWEEN 0 AND 5),
    ADD COLUMN study_event_id bigint REFERENCES app.study_unit_events(id) ON DELETE SET NULL;
ALTER TABLE app.study_reading_beats ADD COLUMN request_hash text;
-- Attention belongs to exact source bytes; a new revision on the same page must
-- not be merged into the first revision's provenance.
ALTER TABLE app.study_reading_spans DROP CONSTRAINT study_reading_spans_session_id_document_id_page_number_key;
ALTER TABLE app.study_reading_spans ADD CONSTRAINT reading_span_revision
    UNIQUE(session_id,document_id,source_hash,page_number);
