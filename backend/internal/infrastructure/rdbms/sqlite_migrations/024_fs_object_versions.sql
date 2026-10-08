-- SQLite port of 024_fs_object_versions.sql.
--
-- SQLite adaptations:
-- * Schema qualifier dropped (app.).
-- * IS DISTINCT FROM rewritten as IS NOT (SQLite's null-safe comparison),
--   which is equivalent and portable across SQLite versions.
UPDATE objects SET version_id = sha256 WHERE version_id IS NOT sha256;
