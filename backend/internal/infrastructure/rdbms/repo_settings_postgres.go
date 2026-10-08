package rdbms

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

var errDiscordExportChanged = errors.New("Discord settings changed during export")

func boolToInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

// postgresSettings implements database.Settings with native PostgreSQL SQL:
// $N placeholders, schema-qualified tables, row locks and clock functions.
type postgresSettings struct{ db nativeDBTX }

// postgresSettings AI defaults live in settings.DefaultAI; RawAISettings
// exposes the stored JSON and the domain normalizes whitespace/models.
func (s postgresSettings) RawAISettings(ctx context.Context) ([]byte, error) {
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT value FROM app.native_settings WHERE key='ai'`).Scan(&raw)
	return raw, err
}

func (s postgresSettings) SaveAISettings(ctx context.Context, raw []byte) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.native_settings(key,value) VALUES('ai',$1)`+
		` ON CONFLICT(key) DO UPDATE SET value=$1,updated_at=now()`, raw)
	return err
}

func (s postgresSettings) LoadPreferences(ctx context.Context) (database.SettingsPreferences, error) {
	var prefs database.SettingsPreferences
	err := s.db.QueryRow(ctx, `SELECT check_interval_minutes,max_concurrent_downloads,request_timeout_seconds,`+
		`retry_attempts,notification_enabled=1,notification_on_error=1,global_feed_dept_enabled=1,`+
		`global_feed_undergrad_enabled=1,global_feed_rector_enabled=1,semester_start,semester_end,`+
		`download_base_path FROM app.preferences WHERE id=1`).Scan(
		&prefs.CheckInterval, &prefs.Downloads, &prefs.Timeout, &prefs.Retries,
		&prefs.Notifications, &prefs.NotifyErrors, &prefs.DepartmentFeed, &prefs.UndergradFeed,
		&prefs.RectorFeed, &prefs.SemesterStart, &prefs.SemesterEnd, &prefs.BasePath)
	return prefs, err
}

func (s postgresSettings) SavePreferences(ctx context.Context, prefs database.SettingsPreferences) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.preferences(id,check_interval_minutes,max_concurrent_downloads,`+
		`request_timeout_seconds,retry_attempts,notification_enabled,notification_on_error,`+
		`global_feed_dept_enabled,global_feed_undergrad_enabled,global_feed_rector_enabled,`+
		`semester_start,semester_end,download_base_path)`+
		` VALUES(1,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`+
		` ON CONFLICT(id) DO UPDATE SET check_interval_minutes=$1,max_concurrent_downloads=$2,`+
		`request_timeout_seconds=$3,retry_attempts=$4,notification_enabled=$5,notification_on_error=$6,`+
		`global_feed_dept_enabled=$7,global_feed_undergrad_enabled=$8,global_feed_rector_enabled=$9,`+
		`semester_start=$10,semester_end=$11,download_base_path=$12`,
		prefs.CheckInterval, prefs.Downloads, prefs.Timeout, prefs.Retries,
		boolToInt(prefs.Notifications), boolToInt(prefs.NotifyErrors), boolToInt(prefs.DepartmentFeed),
		boolToInt(prefs.UndergradFeed), boolToInt(prefs.RectorFeed),
		prefs.SemesterStart, prefs.SemesterEnd, prefs.BasePath)
	return err
}

func (s postgresSettings) LoadCredentials(ctx context.Context) (database.SettingsCredentials, error) {
	var creds database.SettingsCredentials
	err := s.db.QueryRow(ctx, `SELECT username,password FROM app.credentials WHERE id=1`).
		Scan(&creds.Username, &creds.Password)
	return creds, err
}

func (s postgresSettings) SaveCredentials(ctx context.Context, creds database.SettingsCredentials) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.credentials(id,username,password) VALUES(1,$1,$2)`+
		` ON CONFLICT(id) DO UPDATE SET username=$1,password=$2`, creds.Username, creds.Password)
	return err
}

func (s postgresSettings) ClearSessionCookie(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `DELETE FROM app.app_data WHERE key='session_cookie'`)
	return err
}

func (s postgresSettings) LoadWebhook(ctx context.Context) (string, error) {
	var value string
	err := s.db.QueryRow(ctx, `SELECT webhook_url FROM app.webhook_config WHERE id=1`).Scan(&value)
	return value, err
}

func (s postgresSettings) SaveWebhook(ctx context.Context, encoded string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.webhook_config(id,webhook_url) VALUES(1,$1)`+
		` ON CONFLICT(id) DO UPDATE SET webhook_url=$1`, encoded)
	return err
}

func (s postgresSettings) LoadDiscordSettings(ctx context.Context) (database.SettingsDiscord, error) {
	var row database.SettingsDiscord
	err := s.db.QueryRow(ctx, `SELECT enabled=1,token,interval_seconds,include_threads,media=1,parallel`+
		` FROM app.discord_export_settings WHERE id=1`).
		Scan(&row.Enabled, &row.Token, &row.Interval, &row.Threads, &row.Media, &row.Parallel)
	return row, err
}

func (s postgresSettings) SaveDiscordSettings(ctx context.Context, settings database.SettingsDiscord) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.discord_export_settings`+
		`(id,enabled,token,interval_seconds,include_threads,media,parallel)`+
		` VALUES(1,$1,$2,$3,$4,$5,$6)`+
		` ON CONFLICT(id) DO UPDATE SET enabled=$1,token=$2,interval_seconds=$3,include_threads=$4,`+
		`media=$5,parallel=$6,updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		boolToInt(settings.Enabled), settings.Token, settings.Interval, settings.Threads,
		boolToInt(settings.Media), settings.Parallel)
	return err
}

func (s postgresSettings) VerifyDiscordExport(
	ctx context.Context,
	expected database.DiscordExportExpectation,
) error {
	if _, err := advisoryLock(ctx, s.db, "settings:discord-export", true, false); err != nil {
		return err
	}
	var match bool
	err := s.db.QueryRow(ctx, `SELECT enabled=1 AND token=$1 AND interval_seconds=$2 `+
		`AND include_threads=$3 AND media=$4 AND parallel=$5 `+
		`FROM app.discord_export_settings WHERE id=1`,
		expected.Token,
		expected.Interval,
		expected.Threads,
		boolToInt(expected.Media),
		expected.Parallel,
	).Scan(&match)
	if err != nil {
		return err
	}
	if !match {
		return errDiscordExportChanged
	}
	return nil
}

func (s postgresSettings) ListDiscordChannels(ctx context.Context) ([]database.SettingsDiscordChannel, error) {
	rows, err := s.db.Query(ctx, `SELECT coalesce(r.root_channel_id,m.root_channel_id),`+
		`coalesce(r.name,'Unavailable channel'),m.course_id`+
		` FROM app.discord_root_channels r FULL JOIN app.discord_course_channels m`+
		` ON m.root_channel_id=r.root_channel_id`+
		` ORDER BY lower(coalesce(r.name,'Unavailable channel')),coalesce(r.root_channel_id,m.root_channel_id)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SettingsDiscordChannel{}
	for rows.Next() {
		var channel database.SettingsDiscordChannel
		if err := rows.Scan(&channel.RootID, &channel.Name, &channel.CourseID); err != nil {
			return nil, err
		}
		out = append(out, channel)
	}
	return out, rows.Err()
}

func (s postgresSettings) ListDiscordMappingRoots(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT root_channel_id FROM app.discord_root_channels`+
		` UNION SELECT root_channel_id FROM app.discord_course_channels`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var roots []string
	for rows.Next() {
		var root string
		if err := rows.Scan(&root); err != nil {
			return nil, err
		}
		roots = append(roots, root)
	}
	return roots, rows.Err()
}

func (s postgresSettings) ReplaceDiscordMapping(ctx context.Context, mapping map[string]int64) error {
	if _, err := advisoryLock(ctx, s.db, "settings:discord-map", true, false); err != nil {
		return err
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM app.discord_course_channels`); err != nil {
		return err
	}
	for root, course := range mapping {
		if _, err := s.db.Exec(ctx, `INSERT INTO app.discord_course_channels(root_channel_id,course_id)`+
			` VALUES($1,$2)`, root, course); err != nil {
			return err
		}
	}
	return nil
}

func (s postgresSettings) LoadPlannerSettings(ctx context.Context) (database.SettingsPlanner, error) {
	var planner database.SettingsPlanner
	err := s.db.QueryRow(ctx, `SELECT daily_blocks,block_minutes,weekly_minutes_json,blackout_dates_json,`+
		`max_courses_per_day FROM app.study_planner_settings WHERE id=1`).Scan(
		&planner.DailyBlocks, &planner.BlockMinutes, &planner.WeeklyJSON,
		&planner.BlackoutsJSON, &planner.MaxCourses)
	return planner, err
}

func (s postgresSettings) SavePlannerSettings(ctx context.Context, planner database.SettingsPlanner) error {
	_, err := s.db.Exec(ctx, `INSERT INTO app.study_planner_settings`+
		`(id,daily_blocks,block_minutes,weekly_minutes_json,blackout_dates_json,max_courses_per_day)`+
		` VALUES(1,$1,$2,$3,$4,$5)`+
		` ON CONFLICT(id) DO UPDATE SET daily_blocks=$1,block_minutes=$2,weekly_minutes_json=$3,`+
		`blackout_dates_json=$4,max_courses_per_day=$5`,
		planner.DailyBlocks, planner.BlockMinutes, planner.WeeklyJSON,
		planner.BlackoutsJSON, planner.MaxCourses)
	return err
}

func (s postgresSettings) LockCoursesForPlanner(ctx context.Context) error {
	if _, ok := s.db.(*postgresTx); !ok {
		return errors.New("transaction lock requires a transaction-bound repository")
	}
	_, err := s.db.Exec(ctx, `LOCK TABLE app.courses IN SHARE ROW EXCLUSIVE MODE`)
	return err
}

const settingsExamPlanSelectPG = `SELECT c.id,c.name,p.exam_at,coalesce(p.remaining_blocks,0),` +
	`coalesce(p.importance,1.0),coalesce(p.max_daily_blocks,3),coalesce(p.enabled,0)=1,c.short_name,` +
	`coalesce(p.commitment,'committed'),coalesce(p.target_grade,5.0),p.planning_notes` +
	` FROM app.courses c LEFT JOIN app.course_exam_plans p ON p.course_id=c.id `

func scanSettingsExamPlan(row nativeRows) (database.SettingsExamPlan, error) {
	var plan database.SettingsExamPlan
	err := row.Scan(
		&plan.CourseID, &plan.CourseName, &plan.ExamAt, &plan.Remaining, &plan.Importance,
		&plan.MaxBlocks, &plan.Enabled, &plan.ShortName, &plan.Commitment,
		&plan.TargetGrade, &plan.Notes)
	return plan, err
}

func (s postgresSettings) ListExamPlans(ctx context.Context) ([]database.SettingsExamPlan, error) {
	rows, err := s.db.Query(ctx, settingsExamPlanSelectPG+
		`WHERE c.hidden=0 OR coalesce(p.enabled,0)=1 ORDER BY c.sort_order,c.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	plans := []database.SettingsExamPlan{}
	for rows.Next() {
		plan, err := scanSettingsExamPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, rows.Err()
}

func (s postgresSettings) GetExamPlan(ctx context.Context, course int64) (database.SettingsExamPlan, error) {
	rows, err := s.db.Query(ctx, settingsExamPlanSelectPG+`WHERE c.id=$1`, course)
	if err != nil {
		return database.SettingsExamPlan{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return database.SettingsExamPlan{}, err
		}
		return database.SettingsExamPlan{}, database.ErrNoRows
	}
	plan, err := scanSettingsExamPlan(rows)
	if err != nil {
		return database.SettingsExamPlan{}, err
	}
	return plan, rows.Err()
}

func (s postgresSettings) SaveExamPlan(ctx context.Context, plan database.SettingsExamPlan) error {
	if _, err := s.db.Exec(ctx, `INSERT INTO app.course_exam_plans(course_id,exam_at,remaining_blocks,`+
		`importance,max_daily_blocks,enabled,commitment,target_grade,planning_notes)`+
		` VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`+
		` ON CONFLICT(course_id) DO UPDATE SET exam_at=$2,remaining_blocks=$3,importance=$4,`+
		`max_daily_blocks=$5,enabled=$6,commitment=$7,target_grade=$8,planning_notes=$9,`+
		`updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		plan.CourseID, plan.ExamAt, plan.Remaining, plan.Importance, plan.MaxBlocks,
		boolToInt(plan.Enabled), plan.Commitment, plan.TargetGrade, plan.Notes); err != nil {
		return err
	}
	_, err := s.db.Exec(ctx, `UPDATE app.courses SET short_name=$2 WHERE id=$1`,
		plan.CourseID, plan.ShortName)
	return err
}

func (s postgresSettings) LoadCheckStatus(ctx context.Context) (database.SettingsCheckStatus, error) {
	var status database.SettingsCheckStatus
	err := s.db.QueryRow(ctx, `SELECT cs.is_checking=1,cs.started_at,c.id,c.name,cs.last_check_at,`+
		`cs.last_check_result,cs.last_error,cs.last_files_added,cs.last_files_changed`+
		` FROM app.check_status cs LEFT JOIN app.courses c ON c.id=cs.current_course_id AND c.hidden=0`+
		` WHERE cs.id=1`).Scan(
		&status.IsChecking, &status.StartedAt, &status.CourseID, &status.CourseName,
		&status.LastCheckAt, &status.LastResult, &status.LastError,
		&status.FilesAdded, &status.FilesChanged)
	return status, err
}

func (s postgresSettings) ListSyncStatus(ctx context.Context) ([]database.SettingsSyncStatus, error) {
	rows, err := s.db.Query(ctx, `SELECT job,last_run_at,last_result,last_error,last_message`+
		` FROM app.sync_status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.SettingsSyncStatus{}
	for rows.Next() {
		var status database.SettingsSyncStatus
		if err := rows.Scan(&status.Job, &status.LastRunAt, &status.LastResult,
			&status.LastError, &status.LastMessage); err != nil {
			return nil, err
		}
		out = append(out, status)
	}
	return out, rows.Err()
}

func (s postgresSettings) SchemaVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRow(ctx, `SELECT coalesce(max(version),0) FROM public.tree_go_migrations`).
		Scan(&version)
	return version, err
}

func (s postgresSettings) LockSettingsSection(ctx context.Context, section string) error {
	_, err := advisoryLock(ctx, s.db, "settings:"+section, true, false)
	return err
}
