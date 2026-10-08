-- 014_navigation_cleanup.sql (SQLite port of storage/migrations/014_navigation_cleanup.sql)
--
-- SQLite has no schemas (read_model. prefixes dropped) and no plpgsql;
-- the release_navigation_content() function is inlined into the trigger.

CREATE TRIGGER release_content AFTER DELETE ON navigation
    FOR EACH ROW
    WHEN NOT EXISTS(SELECT 1 FROM navigation n WHERE n.content_id = OLD.content_id)
BEGIN
    DELETE FROM roadmap_content WHERE content_id = OLD.content_id;
END;
