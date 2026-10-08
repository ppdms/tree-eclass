-- SQLite port of 008_sync_objects.sql (postgres dialect source).
-- One statement per ALTER TABLE: SQLite executes a single statement at a
-- time and allows only one ADD COLUMN per ALTER TABLE.
ALTER TABLE files ADD COLUMN object_id TEXT REFERENCES objects(id);
ALTER TABLE files ADD COLUMN revision_id TEXT REFERENCES document_revisions(id);
ALTER TABLE file_versions ADD COLUMN revision_id TEXT REFERENCES document_revisions(id);
ALTER TABLE file_versions ADD COLUMN diff_revision_id TEXT REFERENCES document_revisions(id);
