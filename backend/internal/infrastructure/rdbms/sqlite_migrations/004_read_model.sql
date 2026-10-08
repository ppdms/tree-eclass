-- SQLite port of 004_read_model.sql (postgres 004_read_model.sql).
-- Schema prefixes stripped (single SQLite database, no schemas).
-- Approximations: BIGINT->INTEGER; straight port, no triggers.

CREATE TABLE course_coverage (
    course_id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    generation INTEGER NOT NULL
);

CREATE TABLE recent_materials (
    ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    generation INTEGER NOT NULL
);

CREATE TABLE study_metrics (
    scope TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    generation INTEGER NOT NULL
);

CREATE TABLE read_model_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (id = 1),
    generation INTEGER NOT NULL,
    generated_at TEXT NOT NULL,
    status TEXT NOT NULL,
    last_error TEXT
);

INSERT INTO read_model_metadata VALUES ('1','0','2026-09-07T18:34:09.195914+00:00','empty',NULL);
