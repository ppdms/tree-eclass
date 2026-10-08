-- 018_study_projection.sql (SQLite port of storage/migrations/018_study_projection.sql)
--
-- Learner progress affects schedules without invalidating immutable blueprints.
--
-- Approximation notes vs postgres source:
-- * SQLite has no schemas (read_model./app. prefixes dropped).
-- * timestamptz -> TEXT for the new retry_after column.
-- * No plpgsql: invalidate_learner_course() inlined as a
--   learner_generation bump scoped to OLD/NEW.course_id.
-- * One trigger per event; names are database-global in SQLite.

ALTER TABLE study_metrics ADD COLUMN source_fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE study_metrics ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE study_metrics ADD COLUMN retry_after TEXT;

CREATE TRIGGER study_changed_ins AFTER INSERT ON study_unit_events
    FOR EACH ROW
BEGIN
    INSERT INTO learner_generation(course_id)
    SELECT id FROM courses WHERE id = NEW.course_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER study_changed_upd AFTER UPDATE ON study_unit_events
    FOR EACH ROW
BEGIN
    INSERT INTO learner_generation(course_id)
    SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id)
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;

CREATE TRIGGER study_changed_del AFTER DELETE ON study_unit_events
    FOR EACH ROW
BEGIN
    INSERT INTO learner_generation(course_id)
    SELECT id FROM courses WHERE id = OLD.course_id
    ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1;
END;
