-- 017_practice_generation.sql (SQLite port of storage/migrations/017_practice_generation.sql)
--
-- Approximation notes vs postgres source:
-- * SQLite has no schemas (knowledge./read_model. prefixes dropped).
-- * No plpgsql: invalidate_course('course_id') inlined as a
--   course_generation bump scoped to OLD/NEW.course_id.
-- * One trigger per event; names are database-global in SQLite.

CREATE TRIGGER practice_questions_navigation_ins AFTER INSERT ON practice_questions
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT id FROM courses WHERE id = NEW.course_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER practice_questions_navigation_upd AFTER UPDATE ON practice_questions
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id)
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER practice_questions_navigation_del AFTER DELETE ON practice_questions
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT id FROM courses WHERE id = OLD.course_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;
