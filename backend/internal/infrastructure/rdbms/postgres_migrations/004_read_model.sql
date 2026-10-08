CREATE SCHEMA IF NOT EXISTS read_model;
SET search_path TO read_model, public;

CREATE TABLE read_model.course_coverage (
    course_id BIGINT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    generation BIGINT NOT NULL
);

CREATE TABLE read_model.recent_materials (
    ordinal BIGINT PRIMARY KEY,
    course_id BIGINT NOT NULL,
    payload_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    generation BIGINT NOT NULL
);

CREATE TABLE read_model.study_metrics (
    scope TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    generation BIGINT NOT NULL
);

CREATE TABLE read_model.read_model_metadata (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    generation BIGINT NOT NULL,
    generated_at TEXT NOT NULL,
    status TEXT NOT NULL,
    last_error TEXT
);

INSERT INTO read_model.read_model_metadata VALUES ('1','0','2026-09-07T18:34:09.195914+00:00','empty',NULL);
