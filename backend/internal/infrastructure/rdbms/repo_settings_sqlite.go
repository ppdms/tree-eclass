package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// sqliteSettings implements database.Settings with native SQLite SQL:
// positional placeholders, unqualified tables, no row locks or table locks.
// The admitted writer owns every write until commit/rollback, which is
// stronger than the PostgreSQL advisory and table locks; lock methods only
// verify the caller holds a writer transaction.
type sqliteSettings struct{ db nativeDBTX }

func (s sqliteSettings) RawAISettings(ctx context.Context) ([]byte, error) {
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT value FROM native_settings WHERE key='ai'`).Scan(&raw)
	return raw, err
}

func (s sqliteSettings) SaveAISettings(ctx context.Context, raw []byte) error {
	_, err := s.db.Exec(ctx, `INSERT INTO native_settings(key,value) VALUES('ai',json(?))`+
		` ON CONFLICT(key) DO UPDATE SET value=excluded.value,`+
		`updated_at=strftime('%Y-%m-%d %H:%M:%S','now')`, string(raw))
	return err
}

func (s sqliteSettings) LoadPreferences(ctx context.Context) (database.SettingsPreferences, error) {
	var prefs database.SettingsPreferences
	var notifications, notify, dept, undergrad, rector int64
	err := s.db.QueryRow(ctx, `SELECT check_interval_minutes,max_concurrent_downloads,request_timeout_seconds,`+
		`retry_attempts,notification_enabled,notification_on_error,global_feed_dept_enabled,`+
		`global_feed_undergrad_enabled,global_feed_rector_enabled,semester_start,semester_end,`+
		`download_base_path FROM preferences WHERE id=1`).Scan(
		&prefs.CheckInterval, &prefs.Downloads, &prefs.Timeout, &prefs.Retries,
		&notifications, &notify, &dept, &undergrad, &rector,
		&prefs.SemesterStart, &prefs.SemesterEnd, &prefs.BasePath)
	if err != nil {
		return database.SettingsPreferences{}, err
	}
	prefs.Notifications = notifications == 1
	prefs.NotifyErrors = notify == 1
	prefs.DepartmentFeed = dept == 1
	prefs.UndergradFeed = undergrad == 1
	prefs.RectorFeed = rector == 1
	return prefs, nil
}

func (s sqliteSettings) SavePreferences(ctx context.Context, prefs database.SettingsPreferences) error {
	_, err := s.db.Exec(ctx, `INSERT INTO preferences(id,check_interval_minutes,max_concurrent_downloads,`+
		`request_timeout_seconds,retry_attempts,notification_enabled,notification_on_error,`+
		`global_feed_dept_enabled,global_feed_undergrad_enabled,global_feed_rector_enabled,`+
		`semester_start,semester_end,download_base_path)`+
		` VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?)`+
		` ON CONFLICT(id) DO UPDATE SET check_interval_minutes=excluded.check_interval_minutes,`+
		`max_concurrent_downloads=excluded.max_concurrent_downloads,`+
		`request_timeout_seconds=excluded.request_timeout_seconds,retry_attempts=excluded.retry_attempts,`+
		`notification_enabled=excluded.notification_enabled,`+
		`notification_on_error=excluded.notification_on_error,`+
		`global_feed_dept_enabled=excluded.global_feed_dept_enabled,`+
		`global_feed_undergrad_enabled=excluded.global_feed_undergrad_enabled,`+
		`global_feed_rector_enabled=excluded.global_feed_rector_enabled,`+
		`semester_start=excluded.semester_start,semester_end=excluded.semester_end,`+
		`download_base_path=excluded.download_base_path`,
		prefs.CheckInterval, prefs.Downloads, prefs.Timeout, prefs.Retries,
		boolToInt(prefs.Notifications), boolToInt(prefs.NotifyErrors), boolToInt(prefs.DepartmentFeed),
		boolToInt(prefs.UndergradFeed), boolToInt(prefs.RectorFeed),
		prefs.SemesterStart, prefs.SemesterEnd, prefs.BasePath)
	return err
}

func (s sqliteSettings) LoadCredentials(ctx context.Context) (database.SettingsCredentials, error) {
	var creds database.SettingsCredentials
	err := s.db.QueryRow(ctx, `SELECT username,password FROM credentials WHERE id=1`).
		Scan(&creds.Username, &creds.Password)
	return creds, err
}

func (s sqliteSettings) SaveCredentials(ctx context.Context, creds database.SettingsCredentials) error {
	_, err := s.db.Exec(ctx, `INSERT INTO credentials(id,username,password) VALUES(1,?,?)`+
		` ON CONFLICT(id) DO UPDATE SET username=excluded.username,password=excluded.password`,
		creds.Username, creds.Password)
	return err
}

func (s sqliteSettings) ClearSessionCookie(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `DELETE FROM app_data WHERE key='session_cookie'`)
	return err
}

func (s sqliteSettings) LoadWebhook(ctx context.Context) (string, error) {
	var value string
	err := s.db.QueryRow(ctx, `SELECT webhook_url FROM webhook_config WHERE id=1`).Scan(&value)
	return value, err
}

func (s sqliteSettings) SaveWebhook(ctx context.Context, encoded string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO webhook_config(id,webhook_url) VALUES(1,?)`+
		` ON CONFLICT(id) DO UPDATE SET webhook_url=excluded.webhook_url`, encoded)
	return err
}

func (s sqliteSettings) LoadDiscordSettings(ctx context.Context) (database.SettingsDiscord, error) {
	var row database.SettingsDiscord
	var enabled, media int64
	err := s.db.QueryRow(ctx, `SELECT enabled,token,interval_seconds,include_threads,media,parallel`+
		` FROM discord_export_settings WHERE id=1`).
		Scan(&enabled, &row.Token, &row.Interval, &row.Threads, &media, &row.Parallel)
	if err != nil {
		return database.SettingsDiscord{}, err
	}
	row.Enabled = enabled == 1
	row.Media = media == 1
	return row, nil
}

func (s sqliteSettings) SaveDiscordSettings(ctx context.Context, settings database.SettingsDiscord) error {
	_, err := s.db.Exec(ctx, `INSERT INTO discord_export_settings`+
		`(id,enabled,token,interval_seconds,include_threads,media,parallel)`+
		` VALUES(1,?,?,?,?,?,?)`+
		` ON CONFLICT(id) DO UPDATE SET enabled=excluded.enabled,token=excluded.token,`+
		`interval_seconds=excluded.interval_seconds,include_threads=excluded.include_threads,`+
		`media=excluded.media,parallel=excluded.parallel,`+
		`updated_at=strftime('%Y-%m-%d %H:%M:%S','now')`,
		boolToInt(settings.Enabled), settings.Token, settings.Interval, settings.Threads,
		boolToInt(settings.Media), settings.Parallel)
	return err
}

func (s sqliteSettings) VerifyDiscordExport(
	ctx context.Context,
	expected database.DiscordExportExpectation,
) error {
	if _, err := advisoryLock(ctx, s.db, "settings:discord-export", true, false); err != nil {
		return err
	}
	var match bool
	err := s.db.QueryRow(ctx, `SELECT enabled=1 AND token=? AND interval_seconds=? `+
		`AND include_threads=? AND media=? AND parallel=? `+
		`FROM discord_export_settings WHERE id=1`,
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

func (s sqliteSettings) ListDiscordChannels(ctx context.Context) ([]database.SettingsDiscordChannel, error) {
	// SQLite has no FULL JOIN. The union keeps the PostgreSQL shape:
	// roots without mappings, mappings without roots, and joined rows.
	rows, err := s.db.Query(ctx, `SELECT root_channel_id,name,course_id FROM (`+
		`SELECT r.root_channel_id AS root_channel_id,r.name AS name,m.course_id AS course_id`+
		` FROM discord_root_channels r LEFT JOIN discord_course_channels m`+
		` ON m.root_channel_id=r.root_channel_id`+
		` UNION ALL`+
		` SELECT m.root_channel_id AS root_channel_id,'Unavailable channel' AS name,m.course_id AS course_id`+
		` FROM discord_course_channels m WHERE NOT EXISTS`+
		` (SELECT 1 FROM discord_root_channels r WHERE r.root_channel_id=m.root_channel_id))`+
		` ORDER BY lower(name),root_channel_id`)
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

func (s sqliteSettings) ListDiscordMappingRoots(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT root_channel_id FROM discord_root_channels`+
		` UNION SELECT root_channel_id FROM discord_course_channels`)
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

func (s sqliteSettings) ReplaceDiscordMapping(ctx context.Context, mapping map[string]int64) error {
	if _, err := advisoryLock(ctx, s.db, "settings:discord-map", true, false); err != nil {
		return err
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM discord_course_channels`); err != nil {
		return err
	}
	for root, course := range mapping {
		if _, err := s.db.Exec(ctx, `INSERT INTO discord_course_channels(root_channel_id,course_id)`+
			` VALUES(?,?)`, root, course); err != nil {
			return err
		}
	}
	return nil
}

func (s sqliteSettings) LoadPlannerSettings(ctx context.Context) (database.SettingsPlanner, error) {
	var planner database.SettingsPlanner
	err := s.db.QueryRow(ctx, `SELECT daily_blocks,block_minutes,weekly_minutes_json,blackout_dates_json,`+
		`max_courses_per_day FROM study_planner_settings WHERE id=1`).Scan(
		&planner.DailyBlocks, &planner.BlockMinutes, &planner.WeeklyJSON,
		&planner.BlackoutsJSON, &planner.MaxCourses)
	return planner, err
}

func (s sqliteSettings) SavePlannerSettings(ctx context.Context, planner database.SettingsPlanner) error {
	_, err := s.db.Exec(ctx, `INSERT INTO study_planner_settings`+
		`(id,daily_blocks,block_minutes,weekly_minutes_json,blackout_dates_json,max_courses_per_day)`+
		` VALUES(1,?,?,?,?,?)`+
		` ON CONFLICT(id) DO UPDATE SET daily_blocks=excluded.daily_blocks,`+
		`block_minutes=excluded.block_minutes,weekly_minutes_json=excluded.weekly_minutes_json,`+
		`blackout_dates_json=excluded.blackout_dates_json,`+
		`max_courses_per_day=excluded.max_courses_per_day`,
		planner.DailyBlocks, planner.BlockMinutes, planner.WeeklyJSON,
		planner.BlackoutsJSON, planner.MaxCourses)
	return err
}

func (s sqliteSettings) LockCoursesForPlanner(ctx context.Context) error {
	// SQLite has no table locks. Writer admission already excludes
	// concurrent hide/delete transactions, and the guard verifies the
	// caller holds a writer transaction.
	_, err := advisoryLock(ctx, s.db, "settings:planner-courses", true, false)
	return err
}

const settingsExamPlanSelectSQLite = `SELECT c.id,c.name,p.exam_at,coalesce(p.remaining_blocks,0),` +
	`coalesce(p.importance,1.0),coalesce(p.max_daily_blocks,3),coalesce(p.enabled,0)=1,c.short_name,` +
	`coalesce(p.commitment,'committed'),coalesce(p.target_grade,5.0),p.planning_notes` +
	` FROM courses c LEFT JOIN course_exam_plans p ON p.course_id=c.id `

func scanSQLiteSettingsExamPlan(row nativeRows) (database.SettingsExamPlan, error) {
	var plan database.SettingsExamPlan
	var enabled int64
	err := row.Scan(
		&plan.CourseID, &plan.CourseName, &plan.ExamAt, &plan.Remaining, &plan.Importance,
		&plan.MaxBlocks, &enabled, &plan.ShortName, &plan.Commitment,
		&plan.TargetGrade, &plan.Notes)
	if err != nil {
		return database.SettingsExamPlan{}, err
	}
	plan.Enabled = enabled == 1
	return plan, nil
}

func (s sqliteSettings) ListExamPlans(ctx context.Context) ([]database.SettingsExamPlan, error) {
	rows, err := s.db.Query(ctx, settingsExamPlanSelectSQLite+
		`WHERE c.hidden=0 OR coalesce(p.enabled,0)=1 ORDER BY c.sort_order NULLS LAST,c.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	plans := []database.SettingsExamPlan{}
	for rows.Next() {
		plan, err := scanSQLiteSettingsExamPlan(rows)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, rows.Err()
}

func (s sqliteSettings) GetExamPlan(ctx context.Context, course int64) (database.SettingsExamPlan, error) {
	rows, err := s.db.Query(ctx, settingsExamPlanSelectSQLite+`WHERE c.id=?`, course)
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
	plan, err := scanSQLiteSettingsExamPlan(rows)
	if err != nil {
		return database.SettingsExamPlan{}, err
	}
	return plan, rows.Err()
}

func (s sqliteSettings) SaveExamPlan(ctx context.Context, plan database.SettingsExamPlan) error {
	if _, err := s.db.Exec(ctx, `INSERT INTO course_exam_plans(course_id,exam_at,remaining_blocks,`+
		`importance,max_daily_blocks,enabled,commitment,target_grade,planning_notes)`+
		` VALUES(?,?,?,?,?,?,?,?,?)`+
		` ON CONFLICT(course_id) DO UPDATE SET exam_at=excluded.exam_at,`+
		`remaining_blocks=excluded.remaining_blocks,importance=excluded.importance,`+
		`max_daily_blocks=excluded.max_daily_blocks,enabled=excluded.enabled,`+
		`commitment=excluded.commitment,target_grade=excluded.target_grade,`+
		`planning_notes=excluded.planning_notes,`+
		`updated_at=strftime('%Y-%m-%d %H:%M:%S','now')`,
		plan.CourseID, plan.ExamAt, plan.Remaining, plan.Importance, plan.MaxBlocks,
		boolToInt(plan.Enabled), plan.Commitment, plan.TargetGrade, plan.Notes); err != nil {
		return err
	}
	_, err := s.db.Exec(ctx, `UPDATE courses SET short_name=? WHERE id=?`,
		plan.ShortName, plan.CourseID)
	return err
}

func (s sqliteSettings) LoadCheckStatus(ctx context.Context) (database.SettingsCheckStatus, error) {
	var status database.SettingsCheckStatus
	var checking int64
	err := s.db.QueryRow(ctx, `SELECT cs.is_checking,cs.started_at,c.id,c.name,cs.last_check_at,`+
		`cs.last_check_result,cs.last_error,cs.last_files_added,cs.last_files_changed`+
		` FROM check_status cs LEFT JOIN courses c ON c.id=cs.current_course_id AND c.hidden=0`+
		` WHERE cs.id=1`).Scan(
		&checking, &status.StartedAt, &status.CourseID, &status.CourseName,
		&status.LastCheckAt, &status.LastResult, &status.LastError,
		&status.FilesAdded, &status.FilesChanged)
	if err != nil {
		return database.SettingsCheckStatus{}, err
	}
	status.IsChecking = checking == 1
	return status, nil
}

func (s sqliteSettings) ListSyncStatus(ctx context.Context) ([]database.SettingsSyncStatus, error) {
	rows, err := s.db.Query(ctx, `SELECT job,last_run_at,last_result,last_error,last_message`+
		` FROM sync_status`)
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

func (s sqliteSettings) SchemaVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.db.QueryRow(ctx, `SELECT coalesce(max(version),0) FROM tree_go_migrations`).
		Scan(&version)
	return version, err
}

func (s sqliteSettings) LockSettingsSection(ctx context.Context, section string) error {
	_, err := advisoryLock(ctx, s.db, "settings:"+section, true, false)
	return err
}
