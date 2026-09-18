ALTER TABLE read_model.study_metrics ADD COLUMN source_fingerprint text NOT NULL DEFAULT '';
ALTER TABLE read_model.study_metrics ADD COLUMN status text NOT NULL DEFAULT 'pending';
ALTER TABLE read_model.study_metrics ADD COLUMN retry_after timestamptz;

-- Learner progress affects schedules without invalidating immutable blueprints.
CREATE TRIGGER study_changed AFTER INSERT OR UPDATE OR DELETE ON app.study_unit_events
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_learner_course();
