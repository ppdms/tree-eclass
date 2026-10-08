-- Generations change in the same transaction as their sources. A reader cannot
-- present a projection from before a committed evidence mutation as current.
CREATE TABLE read_model.course_generation (
    course_id bigint PRIMARY KEY REFERENCES app.courses(id) ON DELETE CASCADE,
    generation bigint NOT NULL DEFAULT 1
);
INSERT INTO read_model.course_generation(course_id) SELECT id FROM app.courses;
CREATE TABLE read_model.roadmap_content (
    content_id text PRIMARY KEY,
    payload jsonb NOT NULL
);
CREATE TABLE read_model.navigation (
    course_id bigint PRIMARY KEY REFERENCES app.courses(id) ON DELETE CASCADE,
    source_generation bigint NOT NULL,
    config_generation text NOT NULL,
    revision_id text,
    overview jsonb NOT NULL,
    content_id text NOT NULL REFERENCES read_model.roadmap_content(content_id),
    generated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE read_model.roadmap_actions (
    course_id bigint REFERENCES app.courses(id) ON DELETE CASCADE,
    action_id text NOT NULL,
    ordinal bigint NOT NULL,
    unit_key text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY(course_id, action_id)
);
CREATE TABLE read_model.action_progress (
    course_id bigint REFERENCES app.courses(id) ON DELETE CASCADE,
    action_id text NOT NULL,
    latest_event text NOT NULL,
    progress_minutes bigint NOT NULL,
    PRIMARY KEY(course_id, action_id)
);
CREATE OR REPLACE FUNCTION read_model.invalidate_course() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE previous_id bigint; next_id bigint;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        previous_id := (to_jsonb(OLD)->>TG_ARGV[0])::bigint;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        next_id := (to_jsonb(NEW)->>TG_ARGV[0])::bigint;
    END IF;
    INSERT INTO read_model.course_generation(course_id)
        SELECT id FROM app.courses WHERE id IN (previous_id, next_id)
        ON CONFLICT(course_id) DO UPDATE
        SET generation=read_model.course_generation.generation+1;
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION read_model.invalidate_document_course() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO read_model.course_generation(course_id)
        SELECT DISTINCT d.course_id FROM knowledge.documents d
        JOIN app.courses c ON c.id=d.course_id
        WHERE d.id IN (to_jsonb(OLD)->>TG_ARGV[0], to_jsonb(NEW)->>TG_ARGV[0])
        ON CONFLICT(course_id) DO UPDATE
        SET generation=read_model.course_generation.generation+1;
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION read_model.invalidate_all_courses() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE read_model.course_generation SET generation=generation+1;
    RETURN NULL;
END $$;
DO $$
DECLARE item text;
BEGIN
    FOREACH item IN ARRAY ARRAY['app.nodes','app.course_exam_plans',
        'app.external_material_metadata','app.discord_course_channels',
        'knowledge.documents','knowledge.course_blueprints','knowledge.practice_question_sets']
    LOOP
        EXECUTE format('CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON %s '
            'FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course(''course_id'')',item);
    END LOOP;
    FOREACH item IN ARRAY ARRAY['knowledge.document_enrichments','knowledge.page_enrichments']
    LOOP
        EXECUTE format('CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON %s '
            'FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_document_course(''document_id'')',item);
    END LOOP;
END $$;
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.courses
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_course('id');
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON knowledge.archive_members
    FOR EACH ROW EXECUTE FUNCTION read_model.invalidate_document_course('parent_document_id');
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.webdav_config
    FOR EACH STATEMENT EXECUTE FUNCTION read_model.invalidate_all_courses();
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.preferences
    FOR EACH STATEMENT EXECUTE FUNCTION read_model.invalidate_all_courses();
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.files
    FOR EACH STATEMENT EXECUTE FUNCTION read_model.invalidate_all_courses();
-- The append-only event log remains authoritative. Only the affected action is
-- recomputed, including on explicit deletion/reset and out-of-order insertion.
CREATE OR REPLACE FUNCTION read_model.refresh_action_progress() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE cid bigint; aid text;
BEGIN
    FOR cid, aid IN SELECT DISTINCT c,a FROM (VALUES
        ((to_jsonb(OLD)->>'course_id')::bigint, to_jsonb(OLD)->>'action_id'),
        ((to_jsonb(NEW)->>'course_id')::bigint, to_jsonb(NEW)->>'action_id')) AS v(c,a)
        WHERE c IS NOT NULL
    LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(cid::text || ':' || aid, 0));
        DELETE FROM read_model.action_progress WHERE course_id=cid AND action_id=aid;
        INSERT INTO read_model.action_progress
        SELECT course_id, action_id,
            (array_agg(event_type ORDER BY created_at DESC,id DESC))[1],
            coalesce(sum(actual_minutes) FILTER (WHERE event_type='partial'),0)
        FROM app.study_unit_events WHERE course_id=cid AND action_id=aid
        GROUP BY course_id,action_id;
    END LOOP;
    RETURN NULL;
END $$;
INSERT INTO read_model.action_progress
SELECT course_id, action_id,
    (array_agg(event_type ORDER BY created_at DESC,id DESC))[1],
    coalesce(sum(actual_minutes) FILTER (WHERE event_type='partial'),0)
FROM app.study_unit_events GROUP BY course_id,action_id;
CREATE TRIGGER progress_changed AFTER INSERT OR UPDATE OR DELETE ON app.study_unit_events
    FOR EACH ROW EXECUTE FUNCTION read_model.refresh_action_progress();
CREATE TRIGGER navigation_changed AFTER INSERT OR UPDATE OR DELETE ON app.app_data
    FOR EACH STATEMENT EXECUTE FUNCTION read_model.invalidate_all_courses();
