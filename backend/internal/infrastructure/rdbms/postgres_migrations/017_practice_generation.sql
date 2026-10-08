CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON knowledge.practice_questions
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course('course_id');
