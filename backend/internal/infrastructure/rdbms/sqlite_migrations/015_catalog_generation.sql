-- 015_catalog_generation.sql (SQLite port of storage/migrations/015_catalog_generation.sql)
--
-- Navigation evidence includes immutable object membership, not only extracted
-- document rows. A soft deletion must hide a saved plan before a worker runs.
--
-- Approximation notes vs postgres source:
-- * SQLite has no schemas (app./knowledge./read_model. prefixes dropped).
-- * No plpgsql: invalidate_course('course_id') is inlined as a
--   course_generation bump scoped to OLD/NEW.course_id; the TG_ARGV
--   indirection via to_jsonb is replaced by direct column access.
-- * No pg-only objects. ON CONFLICT(course_id) DO UPDATE is kept as-is
--   (supported by SQLite). One trigger per event; names are
--   database-global in SQLite.

CREATE TRIGGER document_revisions_navigation_ins AFTER INSERT ON document_revisions
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT id FROM courses WHERE id = NEW.course_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER document_revisions_navigation_upd AFTER UPDATE ON document_revisions
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id)
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER document_revisions_navigation_del AFTER DELETE ON document_revisions
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT id FROM courses WHERE id = OLD.course_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER objects_navigation_upd AFTER UPDATE ON objects
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
        SELECT DISTINCT r.course_id FROM document_revisions r
        JOIN courses c ON c.id = r.course_id
        WHERE r.object_id IN (OLD.id, NEW.id)
        ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER objects_navigation_del AFTER DELETE ON objects
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
        SELECT DISTINCT r.course_id FROM document_revisions r
        JOIN courses c ON c.id = r.course_id
        WHERE r.object_id = OLD.id
        ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;
