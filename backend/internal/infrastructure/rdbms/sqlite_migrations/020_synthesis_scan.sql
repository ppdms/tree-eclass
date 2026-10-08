-- SQLite port of 020_synthesis_scan.sql.
-- Scan cursors bound background evidence collection; queued revisions remain
-- authoritative across restart. These cursors never carry model output.
--
-- SQLite adaptations:
-- * Schema qualifiers dropped (read_model./app./messages.).
-- * timestamptz -> TEXT.
-- * plpgsql invalidate_course('course_id') inlined: bump course_generation for
--   the affected course_id (INSERT/DELETE touch one side, UPDATE both).
-- * plpgsql invalidate_all_courses() inlined: bump every course_generation row.
-- * FOR EACH STATEMENT has no SQLite equivalent; row-level triggers used.
-- * Trigger names must be database-globally unique in SQLite, so the
--   postgres per-table `navigation_changed` name is suffixed per table/op.
CREATE TABLE synthesis_scan (
    course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    lane TEXT NOT NULL CHECK(lane IN ('course','practice')),
    source_generation INTEGER NOT NULL,
    config_generation TEXT NOT NULL,
    next_at TEXT NOT NULL,
    PRIMARY KEY(course_id,lane)
);
CREATE TRIGGER navigation_changed_conversations_insert AFTER INSERT ON conversations
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id = NEW.course_id
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_conversations_update AFTER UPDATE ON conversations
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id)
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_conversations_delete AFTER DELETE ON conversations
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id = OLD.course_id
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_archive_sources_insert AFTER INSERT ON archive_sources
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id = NEW.course_id
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_archive_sources_update AFTER UPDATE ON archive_sources
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id)
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_archive_sources_delete AFTER DELETE ON archive_sources
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id = OLD.course_id
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_messages_insert AFTER INSERT ON messages
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id = NEW.course_id
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_messages_update AFTER UPDATE ON messages
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id IN (OLD.course_id, NEW.course_id)
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
CREATE TRIGGER navigation_changed_messages_delete AFTER DELETE ON messages
    FOR EACH ROW BEGIN
        INSERT INTO course_generation(course_id)
            SELECT id FROM courses WHERE id = OLD.course_id
            ON CONFLICT(course_id) DO UPDATE
            SET generation = course_generation.generation + 1;
    END;
-- Membership changes also invalidate saved conversation evidence immediately.
CREATE TRIGGER navigation_changed_conversation_messages_insert AFTER INSERT ON conversation_messages
    FOR EACH ROW BEGIN
        UPDATE course_generation SET generation = generation + 1;
    END;
CREATE TRIGGER navigation_changed_conversation_messages_update AFTER UPDATE ON conversation_messages
    FOR EACH ROW BEGIN
        UPDATE course_generation SET generation = generation + 1;
    END;
CREATE TRIGGER navigation_changed_conversation_messages_delete AFTER DELETE ON conversation_messages
    FOR EACH ROW BEGIN
        UPDATE course_generation SET generation = generation + 1;
    END;
