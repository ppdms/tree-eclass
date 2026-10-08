-- SQLite port of 005_search_and_control.sql.
-- Schema prefixes stripped (single SQLite database, no schemas).
-- Approximations: unaccent extension / tree_search text-search config / tree_text() / tree_query() DROPPED (pg-only);
--   tsvector/GIN -> plain TEXT search_vector maintained by triggers as lower() concatenation of the same
--   weighted fields (weights lost: A/B/C setweight has no SQLite equivalent; LIKE/INSTR matching instead of ranking);
--   jsonb->TEXT; timestamptz now() defaults -> TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')).

CREATE TABLE chunks_fts (
  chunk_id TEXT PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
  text TEXT, normalized_text TEXT, heading TEXT, display_name TEXT, source_path TEXT, course_name TEXT,
  search_vector TEXT NOT NULL DEFAULT ''
);
CREATE TRIGGER chunks_fts_search_vector_ins AFTER INSERT ON chunks_fts BEGIN
  UPDATE chunks_fts SET search_vector = lower(coalesce(NEW.text,'') || ' ' || coalesce(NEW.normalized_text,'') || ' ' || coalesce(NEW.heading,'') || ' ' || coalesce(NEW.display_name,'') || ' ' || coalesce(NEW.source_path,'') || ' ' || coalesce(NEW.course_name,'')) WHERE chunk_id = NEW.chunk_id;
END;
CREATE TRIGGER chunks_fts_search_vector_upd AFTER UPDATE ON chunks_fts BEGIN
  UPDATE chunks_fts SET search_vector = lower(coalesce(NEW.text,'') || ' ' || coalesce(NEW.normalized_text,'') || ' ' || coalesce(NEW.heading,'') || ' ' || coalesce(NEW.display_name,'') || ' ' || coalesce(NEW.source_path,'') || ' ' || coalesce(NEW.course_name,'')) WHERE chunk_id = NEW.chunk_id;
END;
CREATE INDEX chunks_search ON chunks_fts(search_vector);

CREATE TABLE conversations_fts (
  conversation_id TEXT PRIMARY KEY REFERENCES conversations(conversation_id) ON DELETE CASCADE,
  text TEXT, normalized_text TEXT, channel_name TEXT,
  search_vector TEXT NOT NULL DEFAULT ''
);
CREATE TRIGGER conversations_fts_search_vector_ins AFTER INSERT ON conversations_fts BEGIN
  UPDATE conversations_fts SET search_vector = lower(coalesce(NEW.text,'') || ' ' || coalesce(NEW.normalized_text,'') || ' ' || coalesce(NEW.channel_name,'')) WHERE conversation_id = NEW.conversation_id;
END;
CREATE TRIGGER conversations_fts_search_vector_upd AFTER UPDATE ON conversations_fts BEGIN
  UPDATE conversations_fts SET search_vector = lower(coalesce(NEW.text,'') || ' ' || coalesce(NEW.normalized_text,'') || ' ' || coalesce(NEW.channel_name,'')) WHERE conversation_id = NEW.conversation_id;
END;
CREATE INDEX conversations_search ON conversations_fts(search_vector);

CREATE TABLE control_commands (
  id TEXT PRIMARY KEY, queue TEXT NOT NULL, action TEXT NOT NULL, payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
  available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')), claimed_at TEXT, error TEXT
);
CREATE INDEX control_claim ON control_commands(queue, status, available_at);
CREATE TABLE snapshot_files (
  path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL,
  generation TEXT NOT NULL, registered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
