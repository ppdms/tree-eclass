-- A deleted/recreated numeric course ID must not inherit an orphaned coverage
-- row, including when deletion races a processor snapshot.
DELETE FROM read_model.course_coverage p WHERE NOT EXISTS(SELECT 1 FROM app.courses c WHERE c.id=p.course_id);
ALTER TABLE read_model.course_coverage ADD CONSTRAINT coverage_course
    FOREIGN KEY(course_id) REFERENCES app.courses(id) ON DELETE CASCADE;

-- File publication belongs to its node's course. Invalidating every course for
-- every file made a large crawl create unrelated projection debt repeatedly.
CREATE FUNCTION read_model.invalidate_file_course() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO read_model.course_generation(course_id)
    SELECT DISTINCT n.course_id FROM app.nodes n JOIN app.courses c ON c.id=n.course_id
    WHERE n.id IN ((to_jsonb(OLD)->>'node_id')::bigint,(to_jsonb(NEW)->>'node_id')::bigint)
    ON CONFLICT(course_id) DO UPDATE SET generation=read_model.course_generation.generation+1;
    RETURN NULL;
END $$;
DROP TRIGGER navigation_changed ON app.files;
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.files
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_file_course();
