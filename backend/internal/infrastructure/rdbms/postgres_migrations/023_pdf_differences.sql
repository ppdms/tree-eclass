CREATE TABLE app.pdf_differences (
    id text PRIMARY KEY,
    course_id bigint NOT NULL REFERENCES app.courses(id) ON DELETE CASCADE,
    old_object_id text NOT NULL REFERENCES app.objects(id),
    new_object_id text NOT NULL REFERENCES app.objects(id),
    tool_version text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','ready','identical','failed')),
    object_id text REFERENCES app.objects(id),
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(course_id,old_object_id,new_object_id,tool_version)
);
ALTER TABLE app.change_record_items ADD COLUMN pdf_difference_id text REFERENCES app.pdf_differences(id);
ALTER TABLE app.file_versions ADD COLUMN pdf_difference_id text REFERENCES app.pdf_differences(id);
