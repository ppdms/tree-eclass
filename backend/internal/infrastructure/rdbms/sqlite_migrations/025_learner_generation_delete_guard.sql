-- 025_learner_generation_delete_guard.sql (SQLite only; no postgres source).
--
-- The file_study delete trigger re-asserted the learner generation row with
-- a bare VALUES upsert. During a cascading course delete SQLite may remove
-- the learner_generation row before the file_study cascade fires, so the
-- trigger re-inserted a generation row for a deleted course and the delete
-- failed with a foreign-key violation. Guard the upsert with the surviving
-- parent course, mirroring the update trigger and the postgres function.

DROP TRIGGER IF EXISTS coverage_changed_del;
CREATE TRIGGER coverage_changed_del AFTER DELETE ON file_study FOR EACH ROW BEGIN INSERT INTO learner_generation(course_id) SELECT id FROM courses WHERE id = OLD.course_id ON CONFLICT(course_id) DO UPDATE SET generation = generation + 1; END;
