package rdbms

import (
	"context"
	"fmt"

	"tree-eclass/internal/domain/database"
)

// Courses learner state, coverage builds and destructive mutations on
// SQLite. Native schema, placeholders and JSON semantics throughout; no
// PostgreSQL compatibility shims.
func (c sqliteCourses) SetStudyLevel(ctx context.Context, params database.SetStudyLevelParams) error {
	if params.LastUpdated == nil {
		_, err := c.db.Exec(ctx, `INSERT INTO file_study(course_id,file_path,level,last_updated)`+
			` VALUES(?,?,?,strftime('%Y-%m-%d %H:%M:%S','now'))`+
			` ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,`+
			`last_updated=excluded.last_updated`, params.CourseID, params.FilePath, params.Level)
		return err
	}
	_, err := c.db.Exec(ctx, `INSERT INTO file_study(course_id,file_path,level,last_updated)`+
		` VALUES(?,?,?,?) ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,`+
		`last_updated=excluded.last_updated`, params.CourseID, params.FilePath, params.Level, params.LastUpdated)
	return err
}

func (c sqliteCourses) NodeForFolder(ctx context.Context, courseID int64, key string) error {
	var found int64
	return c.db.QueryRow(ctx, `SELECT id FROM nodes WHERE course_id=? AND (url=? OR local_path=?)`+
		` LIMIT 1`, courseID, key, key).Scan(&found)
}

func (c sqliteCourses) SetFolderCollapsed(ctx context.Context, courseID int64, key string, collapsed int64) error {
	_, err := c.db.Exec(ctx, `INSERT INTO collapsed_course_folders(course_id,folder_key,collapsed)`+
		` VALUES(?,?,?) ON CONFLICT(course_id,folder_key) DO UPDATE SET collapsed=?,`+
		`updated_at=strftime('%Y-%m-%d %H:%M:%S','now')`, courseID, key, collapsed, collapsed)
	return err
}

func (c sqliteCourses) StudyLevelsForCourse(ctx context.Context, courseID int64) ([]database.CourseStudyLevel, error) {
	rows, err := c.db.Query(ctx, `SELECT file_path,level FROM file_study WHERE course_id=?`, courseID)
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

func (c sqliteCourses) StudyDistribution(
	ctx context.Context, courseID int64) ([]database.StudyDistributionBucket, error) {
	rows, err := c.db.Query(ctx, `WITH paths(path) AS (`+
		` SELECT f.local_path FROM files f JOIN nodes n ON n.id=f.node_id WHERE n.course_id=?`+
		` UNION SELECT source_path FROM documents`+
		` WHERE course_id=? AND is_current=1 AND source_origin='external' )`+
		` SELECT coalesce(s.level,0),count(*) FROM paths p`+
		` LEFT JOIN file_study s ON s.course_id=? AND s.file_path=p.path`+
		` GROUP BY coalesce(s.level,0)`, courseID, courseID, courseID)
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

func (c sqliteCourses) CountReadyDocuments(ctx context.Context, courseID int64) (int64, error) {
	var count int64
	err := c.db.QueryRow(ctx, `SELECT count(*) FROM documents`+
		` WHERE course_id=? AND is_current=1 AND status='ready'`, courseID).Scan(&count)
	return count, err
}

func (c sqliteCourses) RecentMaterialsForCourse(
	ctx context.Context, courseID int64) ([]database.RecentMaterialRow, error) {
	rows, err := c.db.Query(ctx, `SELECT d.id,d.course_id,c.name,d.source_path,d.display_name,d.indexed_at`+
		` FROM documents d JOIN courses c ON c.id=d.course_id`+
		` WHERE d.course_id=? AND d.is_current=1 AND d.status='ready'`+
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

func (c sqliteCourses) FolderPreferences(ctx context.Context, courseID int64) ([]database.FolderPreference, error) {
	rows, err := c.db.Query(ctx, `SELECT folder_key,collapsed FROM collapsed_course_folders`+
		` WHERE course_id=? ORDER BY folder_key`, courseID)
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

func (c sqliteCourses) FileVersionChanges(ctx context.Context, courseID int64) ([]database.FileVersionChange, error) {
	rows, err := c.db.Query(ctx, `SELECT DISTINCT file_path,change_type FROM file_versions`+
		` WHERE course_id=? AND change_type IN('modified','deleted') ORDER BY file_path`, courseID)
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

func (c sqliteCourses) ShelfRows(ctx context.Context) ([]database.ShelfRow, error) {
	rows, err := c.db.Query(ctx, `SELECT c.id,c.name,c.webdav_folder,c.sort_order,c.hidden,c.short_name,`+
		`p.payload_json,p.recent_json,p.generated_at,`+
		` coalesce(p.generation<>g.generation OR p.learner_generation<>coalesce(l.generation,0),1),`+
		`coalesce(p.generation,0) FROM courses c`+
		` JOIN course_generation g ON g.course_id=c.id`+
		` LEFT JOIN learner_generation l ON l.course_id=c.id`+
		` LEFT JOIN course_coverage p ON p.course_id=c.id`+
		` WHERE c.hidden=0 ORDER BY c.sort_order NULLS LAST,c.id`)
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

func (c sqliteCourses) StaleCoverageTarget(ctx context.Context) (database.StaleCoverageTarget, error) {
	var target database.StaleCoverageTarget
	err := c.db.QueryRow(ctx, `SELECT c.id,g.generation,coalesce(l.generation,0)`+
		` FROM courses c JOIN course_generation g ON g.course_id=c.id`+
		` LEFT JOIN learner_generation l ON l.course_id=c.id`+
		` LEFT JOIN course_coverage p ON p.course_id=c.id`+
		` WHERE (c.hidden=0 OR EXISTS(SELECT 1 FROM course_exam_plans e`+
		` WHERE e.course_id=c.id AND e.enabled=1))`+
		` AND (p.course_id IS NULL OR p.generation<>g.generation OR`+
		` p.learner_generation<>coalesce(l.generation,0)) ORDER BY c.id LIMIT 1`).Scan(
		&target.CourseID, &target.Generation, &target.Learner)
	return target, err
}

func (c sqliteCourses) PublishCoverage(ctx context.Context, params database.PublishCoverageParams) error {
	// Recent binds as TEXT so JSON1 readers see JSON text.
	_, err := c.db.Exec(ctx, `INSERT INTO course_coverage`+
		`(course_id,payload_json,generated_at,generation,learner_generation,recent_json)`+
		` VALUES(?,?,?,?,?,?) ON CONFLICT(course_id) DO UPDATE`+
		` SET payload_json=excluded.payload_json,generated_at=excluded.generated_at,`+
		`generation=excluded.generation,learner_generation=excluded.learner_generation,`+
		`recent_json=excluded.recent_json`, params.CourseID, params.Payload, params.Generated,
		params.Generation, params.Learner, string(params.Recent))
	return err
}

func (c sqliteCourses) TryLockCourseForMutation(ctx context.Context, courseID int64) (bool, error) {
	return advisoryLock(ctx, c.db, fmt.Sprintf("eclass-sync:%d", courseID), true, true)
}

func (c sqliteCourses) ClaimCourseMutation(
	ctx context.Context, action string, courseID int64, key string) (bool, error) {
	result, err := c.db.Exec(ctx, `INSERT INTO course_mutations(action,course_id,idempotency_key)`+
		` VALUES(?,?,?) ON CONFLICT DO NOTHING`, action, courseID, key)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func (c sqliteCourses) LockCourseForMutation(ctx context.Context, courseID int64) error {
	// The admitted writer transaction serializes stronger than FOR UPDATE.
	var found int64
	return c.db.QueryRow(ctx, `SELECT id FROM courses WHERE id=?`, courseID).Scan(&found)
}

func (c sqliteCourses) ResetCourse(ctx context.Context, courseID int64) error {
	for _, table := range []string{"nodes", "change_history", "change_records", "announcements", "file_versions"} {
		if _, err := c.db.Exec(ctx, `DELETE FROM `+table+` WHERE course_id=?`, courseID); err != nil {
			return err
		}
	}
	_, err := c.db.Exec(ctx, `UPDATE documents SET is_current=0`+
		` WHERE course_id=? AND source_origin='eclass'`, courseID)
	return err
}

func (c sqliteCourses) DeleteCourse(ctx context.Context, courseID int64) error {
	if _, err := c.db.Exec(ctx, `DELETE FROM control_commands WHERE`+
		` json_extract(payload,'$.document_id') IN(SELECT id FROM documents WHERE course_id=?)`,
		courseID); err != nil {
		return err
	}
	tables := []string{"practice_questions", "practice_question_sets", "course_blueprints", "index_jobs", "documents"}
	for _, table := range tables {
		if _, err := c.db.Exec(ctx, `DELETE FROM `+table+` WHERE course_id=?`, courseID); err != nil {
			return err
		}
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM control_commands WHERE`+
		` CAST(json_extract(payload,'$.course_id') AS TEXT)=?`, fmt.Sprint(courseID)); err != nil {
		return err
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM course_coverage WHERE course_id=?`, courseID); err != nil {
		return err
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM recent_materials WHERE course_id=?`, courseID); err != nil {
		return err
	}
	if _, err := c.db.Exec(ctx, `DELETE FROM study_metrics WHERE scope=?`,
		fmt.Sprintf("course:%d", courseID)); err != nil {
		return err
	}
	_, err := c.db.Exec(ctx, `DELETE FROM courses WHERE id=?`, courseID)
	return err
}
