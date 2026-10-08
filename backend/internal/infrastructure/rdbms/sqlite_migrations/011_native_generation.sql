-- SQLite port of 011_native_generation.sql (postgres dialect source).
-- Native AI configuration participates in the same committed generation as
-- evidence. Chat-only changes need no synthesis, but invalidating
-- conservatively is safe; publication also compares the exact analysis
-- fingerprint.
-- Postgres used FOR EACH STATEMENT triggers calling
-- read_model.invalidate_all_courses() (UPDATE course_generation SET
-- generation=generation+1). SQLite has no FOR EACH STATEMENT and no
-- multi-event triggers, so one single-statement row trigger per event with
-- the UPDATE inlined. pg-only function objects are dropped.
CREATE TRIGGER navigation_changed_native_ins AFTER INSERT ON native_settings BEGIN UPDATE course_generation SET generation = generation + 1; END;
CREATE TRIGGER navigation_changed_native_upd AFTER UPDATE ON native_settings BEGIN UPDATE course_generation SET generation = generation + 1; END;
CREATE TRIGGER navigation_changed_native_del AFTER DELETE ON native_settings BEGIN UPDATE course_generation SET generation = generation + 1; END;
-- Coverage is cheap to refresh separately from immutable roadmap content.
CREATE TABLE learner_generation (
    course_id INTEGER PRIMARY KEY REFERENCES courses(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL DEFAULT 1
);
INSERT INTO learner_generation(course_id) SELECT id FROM courses;
-- Postgres invalidate_learner_course() read OLD/NEW course_id via
-- to_jsonb indirection. file_study carries course_id directly, so the row
-- triggers use OLD/NEW.course_id. One trigger per event (SQLite executes a
-- single statement at a time, so each body is exactly one UPSERT).
CREATE TRIGGER coverage_changed_ins AFTER INSERT ON file_study FOR EACH ROW BEGIN INSERT INTO learner_generation(course_id) VALUES (NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1; END;
CREATE TRIGGER coverage_changed_upd AFTER UPDATE ON file_study FOR EACH ROW BEGIN INSERT INTO learner_generation(course_id) SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1; END;
CREATE TRIGGER coverage_changed_del AFTER DELETE ON file_study FOR EACH ROW BEGIN INSERT INTO learner_generation(course_id) VALUES (OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1; END;
ALTER TABLE course_coverage ADD COLUMN learner_generation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE course_coverage ADD COLUMN recent_json TEXT NOT NULL DEFAULT '[]';
