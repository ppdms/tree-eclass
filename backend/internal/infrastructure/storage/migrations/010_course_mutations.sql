-- Keep accepted destructive intent across process restarts and course deletion.
-- Deliberately no course FK: reusing an ID must not make an old request valid.
CREATE TABLE app.course_mutations (
    action text NOT NULL CHECK (action IN ('delete','reset')),
    course_id bigint NOT NULL,
    idempotency_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (action,course_id,idempotency_key)
);
