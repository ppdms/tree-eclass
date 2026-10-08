CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;
CREATE TEXT SEARCH CONFIGURATION public.tree_search (COPY = pg_catalog.simple);
ALTER TEXT SEARCH CONFIGURATION public.tree_search
  ALTER MAPPING FOR hword, hword_part, word WITH public.unaccent, pg_catalog.simple;
CREATE FUNCTION public.tree_text(text) RETURNS tsvector LANGUAGE sql STABLE AS $$
  SELECT to_tsvector('public.tree_search', replace(replace(coalesce($1, ''), chr(57344)||'0', ' '), chr(57344)||'e', chr(57344)))
$$;
CREATE FUNCTION public.tree_query(text) RETURNS tsquery LANGUAGE sql STABLE AS $$
  SELECT websearch_to_tsquery('public.tree_search', $1)
$$;
CREATE TABLE knowledge.chunks_fts (
  chunk_id text PRIMARY KEY REFERENCES knowledge.chunks(id) ON DELETE CASCADE,
  text text, normalized_text text, heading text, display_name text, source_path text, course_name text,
  search_vector tsvector NOT NULL DEFAULT ''::tsvector
);
CREATE FUNCTION knowledge.index_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_vector := setweight(public.tree_text(NEW.text), 'A') ||
    setweight(public.tree_text(NEW.normalized_text), 'B') ||
    setweight(public.tree_text(concat_ws(' ', NEW.heading, NEW.display_name, NEW.source_path, NEW.course_name)), 'C');
  RETURN NEW;
END $$;
CREATE TRIGGER index_chunk BEFORE INSERT OR UPDATE ON knowledge.chunks_fts
  FOR EACH ROW EXECUTE FUNCTION knowledge.index_chunk();
CREATE INDEX chunks_search ON knowledge.chunks_fts USING gin(search_vector);
CREATE TABLE messages.conversations_fts (
  conversation_id text PRIMARY KEY REFERENCES messages.conversations(conversation_id) ON DELETE CASCADE,
  text text, normalized_text text, channel_name text,
  search_vector tsvector NOT NULL DEFAULT ''::tsvector
);
CREATE FUNCTION messages.index_conversation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.search_vector := setweight(public.tree_text(NEW.text), 'A') ||
    setweight(public.tree_text(NEW.normalized_text), 'B') ||
    setweight(public.tree_text(NEW.channel_name), 'C');
  RETURN NEW;
END $$;
CREATE TRIGGER index_conversation BEFORE INSERT OR UPDATE ON messages.conversations_fts
  FOR EACH ROW EXECUTE FUNCTION messages.index_conversation();
CREATE INDEX conversations_search ON messages.conversations_fts USING gin(search_vector);
CREATE TABLE app.control_commands (
  id text PRIMARY KEY, queue text NOT NULL, action text NOT NULL, payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending', attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(), claimed_at timestamptz, error text
);
CREATE INDEX control_claim ON app.control_commands(queue, status, available_at);
CREATE TABLE app.snapshot_files (
  path text PRIMARY KEY, sha256 text NOT NULL, bytes bigint NOT NULL,
  generation text NOT NULL, registered_at timestamptz NOT NULL DEFAULT now()
);
