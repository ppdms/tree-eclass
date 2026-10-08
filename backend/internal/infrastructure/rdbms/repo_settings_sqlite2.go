package rdbms

import (
	"context"
	"fmt"

	"tree-eclass/internal/domain/database"
)

// settingsExportQueriesSQLite mirrors the PostgreSQL projection allowlist
// with unqualified tables and explicit ORDER BY columns.
var settingsExportQueriesSQLite = map[string]struct {
	query   string
	columns []string
}{
	"courses": {
		`SELECT id,name,webdav_folder,sort_order,hidden,short_name FROM courses ORDER BY id`,
		[]string{"id", "name", "webdav_folder", "sort_order", "hidden", "short_name"},
	},
	"exam_plans": {
		settingsExamPlanSelectSQLite + `ORDER BY c.sort_order NULLS LAST,c.id`,
		[]string{"course_id", "course_name", "exam_at", "remaining_blocks", "importance",
			"max_daily_blocks", "enabled", "short_name", "commitment", "target_grade", "planning_notes"},
	},
	"discord_course_channels": {
		`SELECT root_channel_id,course_id,updated_at FROM discord_course_channels ORDER BY root_channel_id`,
		[]string{"root_channel_id", "course_id", "updated_at"},
	},
	"study_sessions": {
		`SELECT id,course_id,timestamp,note FROM study_sessions ORDER BY id`,
		[]string{"id", "course_id", "timestamp", "note"},
	},
	"study_unit_events": {
		`SELECT id,course_id,plan_revision,action_id,unit_key,event_type,idempotency_key,confidence,` +
			`actual_minutes,score,note,created_at FROM study_unit_events ORDER BY id`,
		[]string{"id", "course_id", "plan_revision", "action_id", "unit_key", "event_type",
			"idempotency_key", "confidence", "actual_minutes", "score", "note", "created_at"},
	},
	"study_plan_items": {
		`SELECT id,course_id,scheduled_date,kind,completed,created_at FROM study_plan_items ORDER BY id`,
		[]string{"id", "course_id", "scheduled_date", "kind", "completed", "created_at"},
	},
	"study_review_overrides": {
		`SELECT course_id,review_offset,scheduled_date FROM study_review_overrides ORDER BY course_id`,
		[]string{"course_id", "review_offset", "scheduled_date"},
	},
	"file_study": {
		`SELECT course_id,file_path,level,last_updated FROM file_study ORDER BY course_id`,
		[]string{"course_id", "file_path", "level", "last_updated"},
	},
	"collapsed_course_folders": {
		`SELECT course_id,folder_key,collapsed,updated_at FROM collapsed_course_folders ORDER BY course_id`,
		[]string{"course_id", "folder_key", "collapsed", "updated_at"},
	},
	"practice_attempts": {
		`SELECT id,course_id,unit_key,question_id,question_key,set_hash,blueprint_revision_hash,outcome,` +
			`grading_mode,confidence,seconds,answer,note,idempotency_key,study_event_id,attempted_at` +
			` FROM practice_attempts ORDER BY id`,
		[]string{"id", "course_id", "unit_key", "question_id", "question_key", "set_hash",
			"blueprint_revision_hash", "outcome", "grading_mode", "confidence", "seconds",
			"answer", "note", "idempotency_key", "study_event_id", "attempted_at"},
	},
	"study_workspace_sessions": {
		`SELECT id,course_id,action_id,unit_key,plan_revision,client_session_key,planned_minutes,` +
			`active_seconds,visible_seconds,outcome,note,started_at,last_seen_at,ended_at,confidence,` +
			`study_event_id FROM study_workspace_sessions ORDER BY id`,
		[]string{"id", "course_id", "action_id", "unit_key", "plan_revision", "client_session_key",
			"planned_minutes", "active_seconds", "visible_seconds", "outcome", "note",
			"started_at", "last_seen_at", "ended_at", "confidence", "study_event_id"},
	},
	"study_reading_spans": {
		`SELECT id,session_id,course_id,document_id,source_hash,page_number,action_id,unit_key,` +
			`plan_revision,active_seconds,visible_seconds,started_at,ended_at` +
			` FROM study_reading_spans ORDER BY id`,
		[]string{"id", "session_id", "course_id", "document_id", "source_hash", "page_number",
			"action_id", "unit_key", "plan_revision", "active_seconds", "visible_seconds",
			"started_at", "ended_at"},
	},
	"study_reading_beats": {
		`SELECT session_id,sequence,recorded_at FROM study_reading_beats ORDER BY session_id`,
		[]string{"session_id", "sequence", "recorded_at"},
	},
	"study_annotations": {
		`SELECT id,course_id,document_id,source_hash,page_number,kind,origin,status,color,quote,prefix,` +
			`suffix,char_start,char_end,rects_json,chunk_id,body,tags_json,action_id,unit_key,` +
			`plan_revision,session_id,idempotency_key,created_at,updated_at` +
			` FROM study_annotations ORDER BY id`,
		[]string{"id", "course_id", "document_id", "source_hash", "page_number", "kind", "origin",
			"status", "color", "quote", "prefix", "suffix", "char_start", "char_end", "rects_json",
			"chunk_id", "body", "tags_json", "action_id", "unit_key", "plan_revision",
			"session_id", "idempotency_key", "created_at", "updated_at"},
	},
}

func (s sqliteSettings) ExportRows(
	ctx context.Context, table string,
) (database.Iterator[database.SettingsExportRow], error) {
	projection, ok := settingsExportQueriesSQLite[table]
	if !ok {
		return nil, fmt.Errorf("settings: unknown export table %q", table)
	}
	rows, err := s.db.Query(ctx, projection.query)
	if err != nil {
		return nil, err
	}
	columns := projection.columns
	return typedIterator(rows, func(native nativeRows) (database.SettingsExportRow, error) {
		raw := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range raw {
			pointers[i] = &raw[i]
		}
		if err := native.Scan(pointers...); err != nil {
			return nil, err
		}
		return decodeSettingsExportRow(columns, raw)
	}), nil
}
