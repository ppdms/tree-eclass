-- Native AI configuration participates in the same committed generation as
-- evidence. Chat-only changes need no synthesis, but invalidating conservatively
-- is safe; publication also compares the exact analysis fingerprint.
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.native_settings
    FOR EACH STATEMENT EXECUTE FUNCTION read_model.invalidate_all_courses();

-- Coverage is cheap to refresh separately from immutable roadmap content.
CREATE TABLE read_model.learner_generation (
    course_id bigint PRIMARY KEY REFERENCES app.courses(id) ON DELETE CASCADE,
    generation bigint NOT NULL DEFAULT 1
);
INSERT INTO read_model.learner_generation(course_id) SELECT id FROM app.courses;
CREATE FUNCTION read_model.invalidate_learner_course() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO read_model.learner_generation(course_id)
    SELECT id FROM app.courses WHERE id IN (
        (to_jsonb(OLD)->>'course_id')::bigint,(to_jsonb(NEW)->>'course_id')::bigint)
    ON CONFLICT(course_id) DO UPDATE SET generation=read_model.learner_generation.generation+1;
    RETURN NULL;
END $$;
CREATE TRIGGER coverage_changed AFTER INSERT OR UPDATE OR DELETE ON app.file_study
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_learner_course();

ALTER TABLE read_model.course_coverage ADD COLUMN learner_generation bigint NOT NULL DEFAULT 0;
ALTER TABLE read_model.course_coverage ADD COLUMN recent_json jsonb NOT NULL DEFAULT '[]';
