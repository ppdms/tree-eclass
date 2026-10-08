-- 013_coverage_scope.sql (SQLite port of storage/migrations/013_coverage_scope.sql)
--
-- Approximation notes vs postgres source:
-- * SQLite has no schemas: read_model./app. prefixes dropped.
-- * ALTER TABLE ... ADD CONSTRAINT is unsupported in SQLite, so the
--   coverage_course FK (course_coverage.course_id -> courses.id
--   ON DELETE CASCADE) is enforced with equivalent triggers below
--   (BEFORE INSERT/UPDATE raise on missing parent; AFTER DELETE on
--   courses cascades). The trigger bodies preserve the exact semantics.
-- * files rows carry node_id directly, so the (to_jsonb(OLD)->>'node_id')
--   casts become OLD.node_id / NEW.node_id.
-- * One sqlite trigger per event (INSERT/UPDATE/DELETE); sqlite trigger
--   names are database-global, hence the files_navigation_{ins,upd,del}
--   names. The DROPs below remove whatever the earlier files-scope
--   navigation trigger was called (base name plus known variants).

DELETE FROM course_coverage WHERE NOT EXISTS(SELECT 1 FROM courses WHERE courses.id = course_coverage.course_id);

-- Emulate: ALTER TABLE course_coverage ADD CONSTRAINT coverage_course
-- FOREIGN KEY(course_id) REFERENCES courses(id) ON DELETE CASCADE.
CREATE TRIGGER coverage_course_fk_ins BEFORE INSERT ON course_coverage
    FOR EACH ROW WHEN NOT EXISTS(SELECT 1 FROM courses WHERE id = NEW.course_id)
BEGIN
    SELECT RAISE(ABORT, 'coverage_course FK violation: missing course');
END;

CREATE TRIGGER coverage_course_fk_upd BEFORE UPDATE OF course_id ON course_coverage
    FOR EACH ROW WHEN NOT EXISTS(SELECT 1 FROM courses WHERE id = NEW.course_id)
BEGIN
    SELECT RAISE(ABORT, 'coverage_course FK violation: missing course');
END;

CREATE TRIGGER coverage_course_cascade_del AFTER DELETE ON courses
    FOR EACH ROW
BEGIN
    DELETE FROM course_coverage WHERE course_id = OLD.id;
END;

DROP TRIGGER IF EXISTS navigation_changed_files_ins;
DROP TRIGGER IF EXISTS navigation_changed_files_upd;
DROP TRIGGER IF EXISTS navigation_changed_files_del;
DROP TRIGGER IF EXISTS navigation_changed;
DROP TRIGGER IF EXISTS navigation_changed_files;
DROP TRIGGER IF EXISTS files_navigation_changed;

CREATE TRIGGER files_navigation_ins AFTER INSERT ON files
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT DISTINCT n.course_id FROM nodes n JOIN courses c ON c.id = n.course_id
    WHERE n.id = NEW.node_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER files_navigation_upd AFTER UPDATE ON files
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT DISTINCT n.course_id FROM nodes n JOIN courses c ON c.id = n.course_id
    WHERE n.id IN (OLD.node_id, NEW.node_id)
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER files_navigation_del AFTER DELETE ON files
    FOR EACH ROW
BEGIN
    INSERT INTO course_generation(course_id)
    SELECT DISTINCT n.course_id FROM nodes n JOIN courses c ON c.id = n.course_id
    WHERE n.id = OLD.node_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;
