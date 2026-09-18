-- Scan cursors bound background evidence collection; queued revisions remain
-- authoritative across restart. These cursors never carry model output.
CREATE TABLE read_model.synthesis_scan (
    course_id bigint NOT NULL REFERENCES app.courses(id) ON DELETE CASCADE,
    lane text NOT NULL CHECK(lane IN ('course','practice')),
    source_generation bigint NOT NULL,
    config_generation text NOT NULL,
    next_at timestamptz NOT NULL,
    PRIMARY KEY(course_id,lane)
);
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON messages.conversations
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course('course_id');
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON messages.archive_sources
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course('course_id');
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON messages.messages
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course('course_id');
-- Membership changes also invalidate saved conversation evidence immediately.
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON messages.conversation_messages
    FOR EACH STATEMENT EXECUTE FUNCTION read_model.invalidate_all_courses();
