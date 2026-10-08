-- SQLite port of 006_navigation.sql.
-- Schema prefixes stripped (single SQLite database, no schemas).
-- Approximations: plpgsql invalidate_*/refresh functions inlined per-table/per-event SQLite triggers
--   named navigation_changed_<table>_{ins,upd,del} / progress_changed_study_unit_events_{ins,upd,del}
--   (SQLite trigger names are DB-global and one event per trigger; pg shared one trigger per table).
--   FOR EACH STATEMENT (files/webdav_config/preferences/app_data) -> row-level triggers bumping all rows;
--   pg_advisory_xact_lock() dropped (SQLite serializes writers); array_agg latest-event -> correlated
--   subquery ORDER BY created_at DESC, id DESC LIMIT 1; jsonb->TEXT; timestamptz now() defaults ->
--   TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')).

CREATE TABLE course_generation (
    course_id INTEGER PRIMARY KEY REFERENCES courses(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL DEFAULT 1
);
INSERT INTO course_generation(course_id) SELECT id FROM courses;
CREATE TABLE roadmap_content (
    content_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE navigation (
    course_id INTEGER PRIMARY KEY REFERENCES courses(id) ON DELETE CASCADE,
    source_generation INTEGER NOT NULL,
    config_generation TEXT NOT NULL,
    revision_id TEXT,
    overview TEXT NOT NULL,
    content_id TEXT NOT NULL REFERENCES roadmap_content(content_id),
    generated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
CREATE TABLE roadmap_actions (
    course_id INTEGER REFERENCES courses(id) ON DELETE CASCADE,
    action_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    unit_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(course_id, action_id)
);
CREATE TABLE action_progress (
    course_id INTEGER REFERENCES courses(id) ON DELETE CASCADE,
    action_id TEXT NOT NULL,
    latest_event TEXT NOT NULL,
    progress_minutes INTEGER NOT NULL,
    PRIMARY KEY(course_id, action_id)
);

CREATE TRIGGER navigation_changed_nodes_ins AFTER INSERT ON nodes BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_nodes_upd AFTER UPDATE ON nodes BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_nodes_del AFTER DELETE ON nodes BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_course_exam_plans_ins AFTER INSERT ON course_exam_plans BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_course_exam_plans_upd AFTER UPDATE ON course_exam_plans BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_course_exam_plans_del AFTER DELETE ON course_exam_plans BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_external_material_metadata_ins AFTER INSERT ON external_material_metadata BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_external_material_metadata_upd AFTER UPDATE ON external_material_metadata BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_external_material_metadata_del AFTER DELETE ON external_material_metadata BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_discord_course_channels_ins AFTER INSERT ON discord_course_channels BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_discord_course_channels_upd AFTER UPDATE ON discord_course_channels BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_discord_course_channels_del AFTER DELETE ON discord_course_channels BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_documents_ins AFTER INSERT ON documents BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_documents_upd AFTER UPDATE ON documents BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_documents_del AFTER DELETE ON documents BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_course_blueprints_ins AFTER INSERT ON course_blueprints BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_course_blueprints_upd AFTER UPDATE ON course_blueprints BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_course_blueprints_del AFTER DELETE ON course_blueprints BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_practice_question_sets_ins AFTER INSERT ON practice_question_sets BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_practice_question_sets_upd AFTER UPDATE ON practice_question_sets BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE OLD.course_id IS NOT NULL AND EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT NEW.course_id, 1 WHERE NEW.course_id IS NOT NULL AND (OLD.course_id IS NULL OR NEW.course_id != OLD.course_id) AND EXISTS (SELECT 1 FROM courses WHERE id = NEW.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_practice_question_sets_del AFTER DELETE ON practice_question_sets BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.course_id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.course_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_document_enrichments_ins AFTER INSERT ON document_enrichments BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = NEW.document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_document_enrichments_upd AFTER UPDATE ON document_enrichments BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = OLD.document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = NEW.document_id AND (OLD.document_id IS NULL OR NEW.document_id != OLD.document_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_document_enrichments_del AFTER DELETE ON document_enrichments BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = OLD.document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_page_enrichments_ins AFTER INSERT ON page_enrichments BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = NEW.document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_page_enrichments_upd AFTER UPDATE ON page_enrichments BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = OLD.document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = NEW.document_id AND (OLD.document_id IS NULL OR NEW.document_id != OLD.document_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_page_enrichments_del AFTER DELETE ON page_enrichments BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = OLD.document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_archive_members_ins AFTER INSERT ON archive_members BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = NEW.parent_document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_archive_members_upd AFTER UPDATE ON archive_members BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = OLD.parent_document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = NEW.parent_document_id AND (OLD.parent_document_id IS NULL OR NEW.parent_document_id != OLD.parent_document_id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_archive_members_del AFTER DELETE ON archive_members BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT d.course_id, 1 FROM documents d WHERE d.id = OLD.parent_document_id ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_courses_ins AFTER INSERT ON courses BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.id, 1 ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_courses_upd AFTER UPDATE ON courses BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT NEW.id, 1 ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;
CREATE TRIGGER navigation_changed_courses_del AFTER DELETE ON courses BEGIN
  INSERT INTO course_generation(course_id, generation) SELECT OLD.id, 1 WHERE EXISTS (SELECT 1 FROM courses WHERE id = OLD.id) ON CONFLICT(course_id) DO UPDATE SET generation = course_generation.generation + 1;
END;

CREATE TRIGGER navigation_changed_webdav_config_ins AFTER INSERT ON webdav_config BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_webdav_config_upd AFTER UPDATE ON webdav_config BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_webdav_config_del AFTER DELETE ON webdav_config BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;

CREATE TRIGGER navigation_changed_preferences_ins AFTER INSERT ON preferences BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_preferences_upd AFTER UPDATE ON preferences BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_preferences_del AFTER DELETE ON preferences BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;

CREATE TRIGGER navigation_changed_files_ins AFTER INSERT ON files BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_files_upd AFTER UPDATE ON files BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_files_del AFTER DELETE ON files BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;

CREATE TRIGGER navigation_changed_app_data_ins AFTER INSERT ON app_data BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_app_data_upd AFTER UPDATE ON app_data BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;
CREATE TRIGGER navigation_changed_app_data_del AFTER DELETE ON app_data BEGIN
  UPDATE course_generation SET generation = generation + 1;
END;

-- Backfill action_progress (array_agg -> correlated subquery; advisory lock dropped).
INSERT INTO action_progress(course_id, action_id, latest_event, progress_minutes)
SELECT s1.course_id, s1.action_id,
  (SELECT s2.event_type FROM study_unit_events s2 WHERE s2.course_id = s1.course_id AND s2.action_id = s1.action_id ORDER BY s2.created_at DESC, s2.id DESC LIMIT 1),
  COALESCE((SELECT SUM(s3.actual_minutes) FROM study_unit_events s3 WHERE s3.course_id = s1.course_id AND s3.action_id = s1.action_id AND s3.event_type = 'partial'), 0)
FROM study_unit_events s1 GROUP BY s1.course_id, s1.action_id;
CREATE TRIGGER progress_changed_study_unit_events_ins AFTER INSERT ON study_unit_events BEGIN
  DELETE FROM action_progress WHERE course_id = NEW.course_id AND action_id = NEW.action_id;
  INSERT INTO action_progress(course_id, action_id, latest_event, progress_minutes)
  SELECT NEW.course_id, NEW.action_id,
    (SELECT event_type FROM study_unit_events WHERE course_id = NEW.course_id AND action_id = NEW.action_id ORDER BY created_at DESC, id DESC LIMIT 1),
    COALESCE((SELECT SUM(actual_minutes) FROM study_unit_events WHERE course_id = NEW.course_id AND action_id = NEW.action_id AND event_type = 'partial'), 0);
END;
CREATE TRIGGER progress_changed_study_unit_events_upd AFTER UPDATE ON study_unit_events BEGIN
  DELETE FROM action_progress WHERE course_id = OLD.course_id AND action_id = OLD.action_id;
  INSERT INTO action_progress(course_id, action_id, latest_event, progress_minutes)
  SELECT OLD.course_id, OLD.action_id,
    (SELECT event_type FROM study_unit_events WHERE course_id = OLD.course_id AND action_id = OLD.action_id ORDER BY created_at DESC, id DESC LIMIT 1),
    COALESCE((SELECT SUM(actual_minutes) FROM study_unit_events WHERE course_id = OLD.course_id AND action_id = OLD.action_id AND event_type = 'partial'), 0)
  WHERE EXISTS (SELECT 1 FROM study_unit_events WHERE course_id = OLD.course_id AND action_id = OLD.action_id);
  DELETE FROM action_progress WHERE course_id = NEW.course_id AND action_id = NEW.action_id AND (NEW.course_id != OLD.course_id OR NEW.action_id != OLD.action_id);
  INSERT INTO action_progress(course_id, action_id, latest_event, progress_minutes)
  SELECT NEW.course_id, NEW.action_id,
    (SELECT event_type FROM study_unit_events WHERE course_id = NEW.course_id AND action_id = NEW.action_id ORDER BY created_at DESC, id DESC LIMIT 1),
    COALESCE((SELECT SUM(actual_minutes) FROM study_unit_events WHERE course_id = NEW.course_id AND action_id = NEW.action_id AND event_type = 'partial'), 0)
  WHERE (NEW.course_id != OLD.course_id OR NEW.action_id != OLD.action_id);
END;
CREATE TRIGGER progress_changed_study_unit_events_del AFTER DELETE ON study_unit_events BEGIN
  DELETE FROM action_progress WHERE course_id = OLD.course_id AND action_id = OLD.action_id;
  INSERT INTO action_progress(course_id, action_id, latest_event, progress_minutes)
  SELECT OLD.course_id, OLD.action_id,
    (SELECT event_type FROM study_unit_events WHERE course_id = OLD.course_id AND action_id = OLD.action_id ORDER BY created_at DESC, id DESC LIMIT 1),
    COALESCE((SELECT SUM(actual_minutes) FROM study_unit_events WHERE course_id = OLD.course_id AND action_id = OLD.action_id AND event_type = 'partial'), 0)
  WHERE EXISTS (SELECT 1 FROM study_unit_events WHERE course_id = OLD.course_id AND action_id = OLD.action_id);
END;
