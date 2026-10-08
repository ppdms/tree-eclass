-- SQLite port of 019_analysis_requested_model.sql.
-- A fallback's serving model differs from the requested analysis generation.
-- Nullable for existing rows: their original model remains the requested model.
ALTER TABLE document_enrichments ADD COLUMN requested_model TEXT;
