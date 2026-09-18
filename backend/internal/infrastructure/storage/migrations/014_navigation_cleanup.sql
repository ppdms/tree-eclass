CREATE FUNCTION read_model.release_navigation_content() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM read_model.roadmap_content c WHERE c.content_id=OLD.content_id
        AND NOT EXISTS(SELECT 1 FROM read_model.navigation n WHERE n.content_id=c.content_id);
    RETURN NULL;
END $$;
CREATE TRIGGER release_content AFTER DELETE ON read_model.navigation
    FOR EACH ROW EXECUTE FUNCTION read_model.release_navigation_content();
