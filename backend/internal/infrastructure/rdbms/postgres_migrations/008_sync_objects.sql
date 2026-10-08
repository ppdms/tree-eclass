ALTER TABLE app.files ADD COLUMN object_id text REFERENCES app.objects(id);
ALTER TABLE app.files ADD COLUMN revision_id text REFERENCES app.document_revisions(id);
ALTER TABLE app.file_versions ADD COLUMN revision_id text REFERENCES app.document_revisions(id);
ALTER TABLE app.file_versions ADD COLUMN diff_revision_id text REFERENCES app.document_revisions(id);
