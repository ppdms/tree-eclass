CREATE FUNCTION read_model.invalidate_index_command() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO read_model.course_generation(course_id)
        SELECT DISTINCT d.course_id FROM knowledge.documents d
        JOIN app.courses c ON c.id=d.course_id
        WHERE (OLD.queue='index' AND d.id=OLD.payload->>'document_id')
           OR (NEW.queue='index' AND d.id=NEW.payload->>'document_id')
        ON CONFLICT(course_id) DO UPDATE
        SET generation=read_model.course_generation.generation+1;
    RETURN NULL;
END $$;
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.control_commands
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_index_command();
