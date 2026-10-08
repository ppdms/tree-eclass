-- 016_index_readiness_generation.sql (SQLite port of storage/migrations/016_index_readiness_generation.sql)
--
-- Approximation notes vs postgres source:
-- * SQLite has no schemas (app./knowledge./read_model. prefixes dropped).
-- * No plpgsql: invalidate_index_command() is inlined. The JSONB
--   payload->>'document_id' access becomes json_extract(payload, '$.document_id')
--   (control_commands.payload is TEXT holding JSON in SQLite).
-- * documents.id is TEXT; the comparison is textual on both sides.
-- * One trigger per event; names are database-global in SQLite.

CREATE TRIGGER control_commands_navigation_ins AFTER INSERT ON control_commands
    FOR EACH ROW WHEN NEW.queue = 'index'
BEGIN
    INSERT INTO course_generation(course_id)
        SELECT DISTINCT d.course_id FROM documents d
        JOIN courses c ON c.id = d.course_id
        WHERE d.id = json_extract(NEW.payload, '$.document_id')
        ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER control_commands_navigation_upd AFTER UPDATE ON control_commands
    FOR EACH ROW
    WHEN (OLD.queue = 'index' AND OLD.payload IS NOT NULL)
      OR (NEW.queue = 'index' AND NEW.payload IS NOT NULL)
BEGIN
    INSERT INTO course_generation(course_id)
        SELECT DISTINCT d.course_id FROM documents d
        JOIN courses c ON c.id = d.course_id
        WHERE (OLD.queue = 'index' AND d.id = json_extract(OLD.payload, '$.document_id'))
           OR (NEW.queue = 'index' AND d.id = json_extract(NEW.payload, '$.document_id'))
        ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER control_commands_navigation_del AFTER DELETE ON control_commands
    FOR EACH ROW WHEN OLD.queue = 'index'
BEGIN
    INSERT INTO course_generation(course_id)
        SELECT DISTINCT d.course_id FROM documents d
        JOIN courses c ON c.id = d.course_id
        WHERE d.id = json_extract(OLD.payload, '$.document_id')
        ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;
