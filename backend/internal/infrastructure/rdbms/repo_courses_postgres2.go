package rdbms

import (
	"context"
	"fmt"

	"tree-eclass/internal/domain/database"
)

// Courses learner state, coverage builds and destructive mutations on
// PostgreSQL. SQLite states NULLS LAST explicitly; PostgreSQL keeps its
// native ordering below.
func (c postgresCourses) SetStudyLevel(ctx context.Context, params database.SetStudyLevelParams) error {
	if params.LastUpdated == nil {
		_, err := c.db.Exec(ctx, `INSERT INTO app.file_study(course_id,file_path,level,last_updated)`+
			` VALUES($1,$2,$3,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'))`+
			` ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,`+
			`last_updated=excluded.last_updated`, params.CourseID, params.FilePath, params.Level)
		return err
	}
	_, err := c.db.Exec(ctx, `INSERT INTO app.file_study(course_id,file_path,level,last_updated)`+
		` VALUES($1,$2,$3,$4) ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,`+
		`last_updated=excluded.last_updated`, params.CourseID, params.FilePath, params.Level, params.LastUpdated)
	return err
}

func (c postgresCourses) NodeForFolder(ctx context.Context, courseID int64, key string) error {
	var found int64
	return c.db.QueryRow(ctx, `SELECT id FROM app.nodes WHERE course_id=$1 AND (url=$2 OR local_path=$2)`+
		` LIMIT 1 FOR SHARE`, courseID, key).Scan(&found)
}

func (c postgresCourses) SetFolderCollapsed(ctx context.Context, courseID int64, key string, collapsed int64) error {
	_, err := c.db.Exec(ctx, `INSERT INTO app.collapsed_course_folders(course_id,folder_key,collapsed)`+
		` VALUES($1,$2,$3) ON CONFLICT(course_id,folder_key) DO UPDATE SET collapsed=$3,`+
		`updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		courseID, key, collapsed)
	return err
}

func (c postgresCourses) StudyLevelsForCourse(
	ctx context.Context, courseID int64) ([]database.CourseStudyLevel, error) {
	rows, err := c.db.Query(ctx, `SELECT file_path,level FROM app.file_study WHERE course_id=$1`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.CourseStudyLevel{}
	for rows.Next() {
		var row database.CourseStudyLevel
		if err := rows.Scan(&row.FilePath, &row.Level); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c postgresCourses) StudyDistribution(
	ctx context.Context, courseID int64) ([]database.StudyDistributionBucket, error) {
	rows, err := c.db.Query(ctx, `WITH paths AS (`+
		` SELECT f.local_path path FROM app.files f JOIN app.nodes n ON n.id=f.node_id WHERE n.course_id=$1`+
		` UNION SELECT source_path FROM knowledge.documents`+
		` WHERE course_id=$1 AND is_current=1 AND source_origin='external' )`+
		` SELECT coalesce(s.level,0),count(*) FROM paths p`+
		` LEFT JOIN app.file_study s ON s.course_id=$1 AND s.file_path=p.path`+
		` GROUP BY coalesce(s.level,0)`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.StudyDistributionBucket{}
	for rows.Next() {
		var bucket database.StudyDistributionBucket
		if err := rows.Scan(&bucket.Level, &bucket.Count); err != nil {
			return nil, err
		}
		out = append(out, bucket)
	}
	return out, rows.Err()
}

func (c postgresCourses) CountReadyDocuments(ctx context.Context, courseID int64) (int64, error) {
	var count int64
	err := c.db.QueryRow(ctx, `SELECT count(*) FROM knowledge.documents`+
		` WHERE course_id=$1 AND is_current=1 AND status='ready'`, courseID).Scan(&count)
	return count, err
}

func (c postgresCourses) RecentMaterialsForCourse(
	ctx context.Context, courseID int64) ([]database.RecentMaterialRow, error) {
	rows, err := c.db.Query(ctx, `SELECT d.id,d.course_id,c.name,d.source_path,d.display_name,d.indexed_at`+
		` FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id`+
		` WHERE d.course_id=$1 AND d.is_current=1 AND d.status='ready'`+
		` ORDER BY coalesce(d.indexed_at,'') DESC,d.id DESC LIMIT 6`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.RecentMaterialRow{}
	for rows.Next() {
		var row database.RecentMaterialRow
		if err := rows.Scan(&row.ID, &row.CourseID, &row.CourseName,
			&row.SourcePath, &row.Display, &row.Indexed); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c postgresCourses) FolderPreferences(ctx context.Context, courseID int64) ([]database.FolderPreference, error) {
	rows, err := c.db.Query(ctx, `SELECT folder_key,collapsed FROM app.collapsed_course_folders`+
		` WHERE course_id=$1 ORDER BY folder_key`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.FolderPreference{}
	for rows.Next() {
		var row database.FolderPreference
		if err := rows.Scan(&row.FolderKey, &row.Collapsed); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c postgresCourses) FileVersionChanges(ctx context.Context, courseID int64) ([]database.FileVersionChange, error) {
	rows, err := c.db.Query(ctx, `SELECT DISTINCT file_path,change_type FROM app.file_versions`+
		` WHERE course_id=$1 AND change_type IN('modified','deleted') ORDER BY file_path`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.FileVersionChange{}
	for rows.Next() {
		var row database.FileVersionChange
		if err := rows.Scan(&row.FilePath, &row.ChangeType); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c postgresCourses) ShelfRows(ctx context.Context) ([]database.ShelfRow, error) {
	rows, err := c.db.Query(ctx, `SELECT c.id,c.name,c.webdav_folder,c.sort_order,c.hidden,c.short_name,`+
		`p.payload_json,p.recent_json,p.generated_at,`+
		` coalesce(p.generation<>g.generation OR p.learner_generation<>coalesce(l.generation,0),true),`+
		`coalesce(p.generation,0) FROM app.courses c`+
		` JOIN read_model.course_generation g ON g.course_id=c.id`+
		` LEFT JOIN read_model.learner_generation l ON l.course_id=c.id`+
		` LEFT JOIN read_model.course_coverage p ON p.course_id=c.id`+
		` WHERE c.hidden=0 ORDER BY c.sort_order,c.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ShelfRow{}
	for rows.Next() {
		var row database.ShelfRow
		if err := rows.Scan(&row.Course.ID, &row.Course.Name, &row.Course.WebdavFolder,
			&row.Course.SortOrder, &row.Course.Hidden, &row.Course.ShortName,
			&row.Payload, &row.Recent, &row.Generated, &row.Stale, &row.Generation); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c postgresCourses) StaleCoverageTarget(ctx context.Context) (database.StaleCoverageTarget, error) {
	var target database.StaleCoverageTarget
	err := c.db.QueryRow(ctx, `SELECT c.id,g.generation,coalesce(l.generation,0)`+
		` FROM app.courses c JOIN read_model.course_generation g ON g.course_id=c.id`+
		` LEFT JOIN read_model.learner_generation l ON l.course_id=c.id`+
		` LEFT JOIN read_model.course_coverage p ON p.course_id=c.id`+
		` WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans e`+
		` WHERE e.course_id=c.id AND e.enabled=1))`+
		` AND (p.course_id IS NULL OR p.generation<>g.generation OR`+
		` p.learner_generation<>coalesce(l.generation,0)) ORDER BY c.id LIMIT 1`).Scan(
		&target.CourseID, &target.Generation, &target.Learner)
	return target, err
}

func (c postgresCourses) PublishCoverage(ctx context.Context, params database.PublishCoverageParams) error {
	_, err := c.db.Exec(ctx, `INSERT INTO read_model.course_coverage`+
		`(course_id,payload_json,generated_at,generation,learner_generation,recent_json)`+
		` VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(course_id) DO UPDATE`+
		` SET payload_json=excluded.payload_json,generated_at=excluded.generated_at,`+
		`generation=excluded.generation,learner_generation=excluded.learner_generation,`+
		`recent_json=excluded.recent_json`, params.CourseID, params.Payload, params.Generated,
		params.Generation, params.Learner, params.Recent)
	return err
}

func (c postgresCourses) TryLockCourseForMutation(ctx context.Context, courseID int64) (bool, error) {
	return advisoryLock(ctx, c.db, fmt.Sprintf("eclass-sync:%d", courseID), true, true)
}

func (c postgresCourses) ClaimCourseMutation(
	ctx context.Context, action string, courseID int64, key string) (bool, error) {
	result, err := c.db.Exec(ctx, `INSERT INTO app.course_mutations(action,course_id,idempotency_key)`+
		` VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, action, courseID, key)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func (c postgresCourses) LockCourseForMutation(ctx context.Context, courseID int64) error {
	var found int64
	return c.db.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 FOR UPDATE`, courseID).Scan(&found)
}

func (c postgresCourses) ResetCourse(ctx context.Context, courseID int64) error {
	for _, table := range []string{"nodes", "change_history", "change_records", "announcements", "file_versions"} {
		if _, err := c.db.Exec(ctx, `DELETE FROM app.`+table+` WHERE course_id=$1`, courseID); err != nil {
			return err
		}
	}
	_, err := c.db.Exec(ctx, `UPDATE knowledge.documents SET is_current=0`+
		` WHERE course_id=$1 AND source_origin='eclass'`, courseID)
	return err
}

func (c postgresCourses) DeleteCourse(ctx context.Context, courseID int64) error {
	if _, err := c.db.Exec(ctx, `DELETE FROM app.control_commands WHERE payload->>'document_id' IN`+
		` (SELECT id FROM knowledge.documents WHERE course_id=$1)`, courseID); err != nil {
		return err
	}
	tables := []string{"practice_questions", "practice_question_sets", "course_blueprints", "index_jobs", "documents"}
	for _, table := range tables {
		if _, err := c.db.Exec(ctx, `DELETE FROM knowledge.`+table+` WHERE course_id=$1`, courseID); err != nil {
			return err
		}
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM app.control_commands WHERE payload->>'course_id'=$1`,
		fmt.Sprint(courseID)); err != nil {
		return err
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM read_model.course_coverage WHERE course_id=$1`, courseID); err != nil {
		return err
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM read_model.recent_materials WHERE course_id=$1`, courseID); err != nil {
		return err
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM read_model.study_metrics WHERE scope=$1`,
		fmt.Sprintf("course:%d", courseID)); err != nil {
		return err
	}
	_, err := c.db.Exec(ctx, `DELETE FROM app.courses WHERE id=$1`, courseID)
	return err
}
