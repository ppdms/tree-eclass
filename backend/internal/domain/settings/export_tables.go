package settings

// Export tables are an allowlist. Never replace these projections with a
// wildcard: new internal fields must not silently become part of a portable
// learner export. Names match the Settings port projection keys.
var learnerTables = []string{
	"study_sessions",
	"study_unit_events",
	"study_plan_items",
	"study_review_overrides",
	"file_study",
	"collapsed_course_folders",
	"practice_attempts",
	"study_workspace_sessions",
	"study_reading_spans",
	"study_reading_beats",
	"study_annotations",
}
