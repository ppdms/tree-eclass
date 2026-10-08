-- SQLite port of 001_app.sql (postgres 001_app.sql).
-- Schema prefixes stripped (single SQLite database, no schemas).
-- Approximations: to_char(clock_timestamp()..) defaults -> TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')); BIGINT->INTEGER (+AUTOINCREMENT on INTEGER PRIMARY KEY); DOUBLE PRECISION->REAL; BYTEA->BLOB.

CREATE TABLE schema_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (id = 1),
                version INTEGER NOT NULL,
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
            );

CREATE TABLE credentials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                password TEXT NOT NULL
            );

CREATE TABLE app_data (
                key TEXT PRIMARY KEY,
                value BLOB
            );

CREATE TABLE courses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                webdav_folder TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0,
                hidden INTEGER NOT NULL DEFAULT 0,
                short_name TEXT
            );

CREATE TABLE nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                parent_id INTEGER,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                local_path TEXT NOT NULL,  -- WebDAV path (kept as local_path for backward compatibility)
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES nodes (id) ON DELETE CASCADE
            );

CREATE TABLE files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                name TEXT NOT NULL,
                md5_hash TEXT,
                etag TEXT,
                redirect_url TEXT, last_updated TEXT, local_path TEXT,
                FOREIGN KEY (node_id) REFERENCES nodes (id) ON DELETE CASCADE
            );

CREATE TABLE change_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                timestamp TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE change_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                change_no TEXT NOT NULL,
                timestamp TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                message TEXT,
                changes_count INTEGER DEFAULT 0,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                UNIQUE(course_id, change_no)
            );

CREATE TABLE change_record_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_record_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                display_name TEXT,
                redirect_url TEXT,
                diff_webdav_path TEXT,
                FOREIGN KEY (change_record_id) REFERENCES change_records (id) ON DELETE CASCADE
            );

CREATE TABLE webhook_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                webhook_url TEXT NOT NULL
            );

CREATE TABLE webdav_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hostname TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                disable_check INTEGER DEFAULT 0,
                timeout INTEGER DEFAULT 30
            );

CREATE TABLE preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_interval_minutes INTEGER DEFAULT 60,
                max_concurrent_downloads INTEGER DEFAULT 3,
                request_timeout_seconds INTEGER DEFAULT 30,
                retry_attempts INTEGER DEFAULT 3,
                notification_enabled INTEGER DEFAULT 1,
                notification_on_error INTEGER DEFAULT 1,
                download_base_path TEXT NOT NULL DEFAULT '/University'
            , global_feed_dept_enabled INTEGER DEFAULT 1, global_feed_undergrad_enabled INTEGER DEFAULT 0, global_feed_rector_enabled INTEGER DEFAULT 0, semester_start TEXT DEFAULT NULL, semester_end TEXT DEFAULT NULL);

CREATE TABLE check_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                is_checking INTEGER DEFAULT 0,
                started_at TEXT,
                current_course_id INTEGER,
                last_check_at TEXT,
                last_check_result TEXT,
                last_error TEXT,
                last_files_added INTEGER,
                last_files_changed INTEGER,
                FOREIGN KEY (current_course_id) REFERENCES courses (id) ON DELETE SET NULL
            );

CREATE TABLE announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                announcement_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                description TEXT,
                pub_date TEXT,
                fetched_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                UNIQUE(course_id, announcement_id)
            );

CREATE TABLE file_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                version_webdav_path TEXT,
                change_type TEXT NOT NULL,
                timestamp TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                display_name TEXT,
                redirect_url TEXT,
                diff_webdav_path TEXT,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE file_study (
                course_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                level INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                PRIMARY KEY (course_id, file_path),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE collapsed_course_folders (
                course_id INTEGER NOT NULL,
                folder_key TEXT NOT NULL,
                collapsed INTEGER NOT NULL DEFAULT 1 CHECK(collapsed IN (0, 1)),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                PRIMARY KEY (course_id, folder_key),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE discord_course_channels (
                root_channel_id TEXT PRIMARY KEY,
                course_id INTEGER NOT NULL,
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE discord_export_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (id = 1),
                enabled INTEGER NOT NULL DEFAULT 0,
                token TEXT NOT NULL DEFAULT '',
                interval_seconds INTEGER NOT NULL DEFAULT 3600,
                include_threads TEXT NOT NULL DEFAULT 'All',
                media INTEGER NOT NULL DEFAULT 1,
                parallel INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
            );

CREATE TABLE study_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                timestamp TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                note TEXT,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE study_planner_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT CHECK (id = 1),
                daily_blocks INTEGER NOT NULL DEFAULT 6,
                block_minutes INTEGER NOT NULL DEFAULT 50,
                weekly_minutes_json TEXT NOT NULL DEFAULT
                    '{"0":150,"1":150,"2":150,"3":150,"4":150,"5":400,"6":400}',
                blackout_dates_json TEXT NOT NULL DEFAULT '[]',
                max_courses_per_day INTEGER NOT NULL DEFAULT 2
            );

CREATE TABLE course_exam_plans (
                course_id INTEGER PRIMARY KEY AUTOINCREMENT,
                exam_at TEXT,
                remaining_blocks INTEGER NOT NULL DEFAULT 0,
                importance REAL NOT NULL DEFAULT 1.0,
                max_daily_blocks INTEGER NOT NULL DEFAULT 3,
                enabled INTEGER NOT NULL DEFAULT 0,
                commitment TEXT NOT NULL DEFAULT 'committed',
                target_grade REAL NOT NULL DEFAULT 5.0,
                planning_notes TEXT,
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE study_unit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                plan_revision TEXT NOT NULL,
                action_id TEXT NOT NULL,
                unit_key TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN (
                    'started', 'completed', 'partial', 'stuck', 'deferred',
                    'recall_answered', 'exam_question_attempted'
                )),
                idempotency_key TEXT,
                confidence INTEGER CHECK(confidence IS NULL OR confidence BETWEEN 0 AND 5),
                actual_minutes INTEGER CHECK(actual_minutes IS NULL OR actual_minutes >= 0),
                score REAL,
                note TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE practice_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                unit_key TEXT NOT NULL,
                question_id TEXT NOT NULL,
                question_key TEXT NOT NULL DEFAULT '',
                set_hash TEXT NOT NULL DEFAULT '',
                blueprint_revision_hash TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL CHECK(outcome IN (
                    'correct', 'partial', 'incorrect', 'skipped'
                )),
                grading_mode TEXT NOT NULL DEFAULT 'self'
                    CHECK(grading_mode IN ('self', 'ai')),
                confidence INTEGER
                    CHECK(confidence IS NULL OR confidence BETWEEN 0 AND 5),
                seconds INTEGER CHECK(seconds IS NULL OR seconds >= 0),
                answer TEXT,
                note TEXT,
                idempotency_key TEXT,
                study_event_id INTEGER,
                attempted_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE chat_conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                created_at TEXT NOT NULL
                    DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                updated_at TEXT NOT NULL
                    DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
            );

CREATE TABLE chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
                content TEXT NOT NULL,
                consulted_json TEXT,
                model TEXT,
                created_at TEXT NOT NULL
                    DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (conversation_id)
                    REFERENCES chat_conversations (id) ON DELETE CASCADE
            );

CREATE TABLE study_workspace_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                action_id TEXT NOT NULL DEFAULT '',
                unit_key TEXT NOT NULL DEFAULT '',
                plan_revision TEXT NOT NULL DEFAULT '',
                client_session_key TEXT NOT NULL,
                planned_minutes INTEGER
                    CHECK(planned_minutes IS NULL OR planned_minutes >= 0),
                active_seconds INTEGER NOT NULL DEFAULT 0
                    CHECK(active_seconds >= 0),
                visible_seconds INTEGER NOT NULL DEFAULT 0
                    CHECK(visible_seconds >= 0),
                outcome TEXT CHECK(outcome IS NULL OR outcome IN (
                    'completed', 'partial', 'stuck', 'deferred', 'abandoned'
                )),
                note TEXT,
                started_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                last_seen_at TEXT,
                ended_at TEXT,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE study_reading_spans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                document_id TEXT NOT NULL,
                source_hash TEXT NOT NULL DEFAULT '',
                page_number INTEGER NOT NULL CHECK(page_number > 0),
                action_id TEXT NOT NULL DEFAULT '',
                unit_key TEXT NOT NULL DEFAULT '',
                plan_revision TEXT NOT NULL DEFAULT '',
                active_seconds INTEGER NOT NULL DEFAULT 0
                    CHECK(active_seconds >= 0),
                visible_seconds INTEGER NOT NULL DEFAULT 0
                    CHECK(visible_seconds >= 0),
                started_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                ended_at TEXT,
                FOREIGN KEY (session_id)
                    REFERENCES study_workspace_sessions (id) ON DELETE CASCADE,
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE,
                UNIQUE(session_id, document_id, page_number)
            );

CREATE TABLE study_reading_beats (
                session_id INTEGER NOT NULL,
                sequence INTEGER NOT NULL CHECK(sequence >= 0),
                recorded_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                PRIMARY KEY (session_id, sequence),
                FOREIGN KEY (session_id)
                    REFERENCES study_workspace_sessions (id) ON DELETE CASCADE
            );

CREATE TABLE study_annotations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                document_id TEXT NOT NULL,
                source_hash TEXT NOT NULL DEFAULT '',
                page_number INTEGER NOT NULL CHECK(page_number > 0),
                kind TEXT NOT NULL DEFAULT 'highlight' CHECK(kind IN (
                    'highlight', 'note', 'question', 'bookmark'
                )),
                origin TEXT NOT NULL DEFAULT 'learner'
                    CHECK(origin IN ('learner', 'ai')),
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active', 'orphaned', 'deleted')),
                color TEXT NOT NULL DEFAULT 'yellow',
                quote TEXT NOT NULL DEFAULT '',
                prefix TEXT NOT NULL DEFAULT '',
                suffix TEXT NOT NULL DEFAULT '',
                char_start INTEGER,
                char_end INTEGER,
                rects_json TEXT NOT NULL DEFAULT '[]',
                chunk_id TEXT,
                body TEXT,
                tags_json TEXT NOT NULL DEFAULT '[]',
                action_id TEXT NOT NULL DEFAULT '',
                unit_key TEXT NOT NULL DEFAULT '',
                plan_revision TEXT NOT NULL DEFAULT '',
                session_id INTEGER,
                idempotency_key TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE external_material_metadata (
                course_id INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                material_type TEXT NOT NULL CHECK(material_type IN (
                    'past_paper', 'student_notes', 'study_guide', 'textbook',
                    'exercise_solution', 'lecture_material', 'other'
                )),
                source_label TEXT,
                notes TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                PRIMARY KEY (course_id, source_path),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE global_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_key TEXT NOT NULL,
                announcement_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                description TEXT,
                pub_date TEXT,
                fetched_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                UNIQUE(feed_key, announcement_id)
            );

CREATE TABLE exercises (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                exercise_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                deadline TEXT,
                submission_status TEXT,
                grade TEXT,
                work_type TEXT,
                description TEXT,
                start_date TEXT,
                max_grade TEXT,
                assignment_file_name TEXT,
                assignment_file_url TEXT,
                grade_comments TEXT,
                submission_date TEXT,
                ignored INTEGER NOT NULL DEFAULT 0,
                fetched_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                UNIQUE(course_id, exercise_id),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE study_plan_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                scheduled_date TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('study', 'review')),
                completed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE study_review_overrides (
                course_id INTEGER NOT NULL,
                review_offset INTEGER NOT NULL,
                scheduled_date TEXT NOT NULL,
                PRIMARY KEY (course_id, review_offset),
                FOREIGN KEY (course_id) REFERENCES courses (id) ON DELETE CASCADE
            );

CREATE TABLE sync_status (
                job TEXT PRIMARY KEY,
                last_run_at TEXT,
                last_result TEXT,
                last_error TEXT,
                last_message TEXT
            );

CREATE INDEX idx_study_unit_events_action
            ON study_unit_events(plan_revision, action_id, created_at)
        ;

CREATE INDEX idx_practice_attempts_question
            ON practice_attempts(question_id, attempted_at)
        ;

CREATE INDEX idx_practice_attempts_course
            ON practice_attempts(course_id, unit_key, attempted_at)
        ;

CREATE UNIQUE INDEX idx_practice_attempts_idempotency
            ON practice_attempts(idempotency_key)
            WHERE idempotency_key IS NOT NULL
        ;

CREATE INDEX idx_chat_messages_conversation
            ON chat_messages(conversation_id, id)
        ;

CREATE INDEX idx_chat_conversations_updated
            ON chat_conversations(updated_at DESC, id DESC)
        ;

CREATE UNIQUE INDEX idx_workspace_sessions_key
            ON study_workspace_sessions(client_session_key)
        ;

CREATE INDEX idx_workspace_sessions_action
            ON study_workspace_sessions(course_id, action_id, started_at)
        ;

CREATE INDEX idx_reading_spans_document
            ON study_reading_spans(document_id, page_number)
        ;

CREATE INDEX idx_reading_spans_course_document
            ON study_reading_spans(course_id, document_id, page_number)
        ;

CREATE INDEX idx_reading_spans_action
            ON study_reading_spans(course_id, action_id)
        ;

CREATE INDEX idx_annotations_document
            ON study_annotations(document_id, page_number, status)
        ;

CREATE INDEX idx_annotations_course
            ON study_annotations(course_id, updated_at DESC)
        ;

CREATE UNIQUE INDEX idx_annotations_idempotency
            ON study_annotations(idempotency_key)
            WHERE idempotency_key IS NOT NULL
        ;

CREATE INDEX idx_external_material_metadata_course
            ON external_material_metadata(course_id, material_type, updated_at)
        ;

CREATE UNIQUE INDEX idx_study_unit_events_idempotency
            ON study_unit_events(idempotency_key)
            WHERE idempotency_key IS NOT NULL
        ;

INSERT INTO schema_version VALUES ('1','38','2026-09-07 18:34:08');

INSERT INTO study_planner_settings VALUES ('1','6','50','{"0":150,"1":150,"2":150,"3":150,"4":150,"5":400,"6":400}','[]','2');
