-- SQLite port of 023_pdf_differences.sql.
--
-- SQLite adaptations:
-- * Schema qualifiers dropped (app.).
-- * timestamptz -> TEXT with strftime now() default.
CREATE TABLE pdf_differences (
    id TEXT PRIMARY KEY,
    course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    old_object_id TEXT NOT NULL REFERENCES objects(id),
    new_object_id TEXT NOT NULL REFERENCES objects(id),
    tool_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','ready','identical','failed')),
    object_id TEXT REFERENCES objects(id),
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    UNIQUE(course_id,old_object_id,new_object_id,tool_version)
);
ALTER TABLE change_record_items ADD COLUMN pdf_difference_id TEXT REFERENCES pdf_differences(id);
ALTER TABLE file_versions ADD COLUMN pdf_difference_id TEXT REFERENCES pdf_differences(id);
