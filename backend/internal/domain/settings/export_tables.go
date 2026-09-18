package settings

// Export columns are an allowlist. Never replace these projections with SELECT *:
// new internal fields must not silently become part of a portable learner export.
type exportTable struct{ Name, Columns string }

var learnerTables = []exportTable{
	{"study_sessions", "id,course_id,timestamp,note"},
	{
		"study_unit_events",
		"id,course_id,plan_revision,action_id,unit_key,event_type,idempotency_key,confidence,actual_minutes,score,note,created_at",
	},
	{"study_plan_items", "id,course_id,scheduled_date,kind,completed,created_at"},
	{"study_review_overrides", "course_id,review_offset,scheduled_date"},
	{"file_study", "course_id,file_path,level,last_updated"},
	{"collapsed_course_folders", "course_id,folder_key,collapsed,updated_at"},
	{
		"practice_attempts",
		"id,course_id,unit_key,question_id,question_key,set_hash,blueprint_revision_hash,outcome,grading_mode,confidence,seconds,answer,note,idempotency_key,study_event_id,attempted_at",
	},
	{
		"study_workspace_sessions",
		"id,course_id,action_id,unit_key,plan_revision,client_session_key,planned_minutes,active_seconds,visible_seconds,outcome,note,started_at,last_seen_at,ended_at,confidence,study_event_id",
	},
	{
		"study_reading_spans",
		"id,session_id,course_id,document_id,source_hash,page_number,action_id,unit_key,plan_revision,active_seconds,visible_seconds,started_at,ended_at",
	},
	{"study_reading_beats", "session_id,sequence,recorded_at"},
	{
		"study_annotations",
		"id,course_id,document_id,source_hash,page_number,kind,origin,status,color,quote,prefix,suffix,char_start,char_end,rects_json,chunk_id,body,tags_json,action_id,unit_key,plan_revision,session_id,idempotency_key,created_at,updated_at",
	},
}

const exportCourses = `SELECT id,name,webdav_folder,sort_order,hidden,short_name FROM app.courses ORDER BY id`

const examPlanSelect = `SELECT c.id AS course_id,c.name AS course_name,p.exam_at,coalesce(p.remaining_blocks,0) AS remaining_blocks,
coalesce(p.importance,1.0) AS importance,coalesce(p.max_daily_blocks,3) AS max_daily_blocks,coalesce(p.enabled,0)=1 AS enabled,c.short_name,
coalesce(p.commitment,'committed') AS commitment,coalesce(p.target_grade,5.0) AS target_grade,p.planning_notes
FROM app.courses c LEFT JOIN app.course_exam_plans p ON p.course_id=c.id `
const exportExamPlans = examPlanSelect + `ORDER BY c.sort_order,c.id`
