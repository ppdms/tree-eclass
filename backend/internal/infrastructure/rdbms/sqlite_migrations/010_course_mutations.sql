-- SQLite port of 010_course_mutations.sql (postgres dialect source).
-- Keep accepted destructive intent across process restarts and course
-- deletion. Deliberately no course FK: reusing an ID must not make an old
-- request valid.
CREATE TABLE course_mutations (
    action TEXT NOT NULL CHECK (action IN ('delete','reset')),
    course_id INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    PRIMARY KEY (action, course_id, idempotency_key)
);
