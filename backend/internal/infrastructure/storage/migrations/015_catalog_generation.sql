-- Navigation evidence includes immutable object membership, not only extracted
-- document rows. A soft deletion must hide a saved plan before a worker runs.
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.document_revisions
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course('course_id');

CREATE FUNCTION read_model.invalidate_object_courses() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO read_model.course_generation(course_id)
        SELECT DISTINCT r.course_id FROM app.document_revisions r
        JOIN app.courses c ON c.id=r.course_id
        WHERE r.object_id IN (OLD.id, NEW.id)
        ON CONFLICT(course_id) DO UPDATE
        SET generation=read_model.course_generation.generation+1;
    RETURN NULL;
END $$;
CREATE TRIGGER navigation_changed AFTER UPDATE OR DELETE ON app.objects
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_object_courses();
