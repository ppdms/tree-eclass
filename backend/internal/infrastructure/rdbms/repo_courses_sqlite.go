package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// Courses identity, shelf mutations and catalog projections on SQLite. Reads
// run on the caller's snapshot; writers already hold the admitted writer
// transaction, which serializes stronger than FOR SHARE and LOCK TABLE.
type sqliteCourses struct{ db nativeDBTX }

func scanSQLiteCourse(row nativeRow) (database.AppCourse, error) {
	var course database.AppCourse
	err := row.Scan(
		&course.ID,
		&course.Name,
		&course.WebdavFolder,
		&course.SortOrder,
		&course.Hidden,
		&course.ShortName,
	)
	return course, err
}

const sqliteCourseColumns = `id,name,webdav_folder,sort_order,hidden,short_name`

func (c sqliteCourses) Course(ctx context.Context, id int64) (database.AppCourse, error) {
	return scanSQLiteCourse(c.db.QueryRow(ctx,
		`SELECT `+sqliteCourseColumns+` FROM courses WHERE id=?`, id))
}

func (c sqliteCourses) VisibleCourse(ctx context.Context, id int64) (database.AppCourse, error) {
	return scanSQLiteCourse(c.db.QueryRow(ctx,
		`SELECT `+sqliteCourseColumns+` FROM courses WHERE id=? AND hidden=0`, id))
}

func (c sqliteCourses) LockCourseForWrite(ctx context.Context, id int64) (database.AppCourse, error) {
	return scanSQLiteCourse(c.db.QueryRow(ctx,
		`SELECT `+sqliteCourseColumns+` FROM courses WHERE id=? AND hidden=0`, id))
}

func (c sqliteCourses) ListCourses(ctx context.Context, includeHidden bool) ([]database.AppCourse, error) {
	// Added courses default sort_order 0; PostgreSQL orders explicitly
	// cleared NULL positions last by default, so state it explicitly and
	// keep both backends in agreement.
	rows, err := c.db.Query(ctx, `SELECT `+sqliteCourseColumns+` FROM courses`+
		` WHERE hidden=0 OR ? ORDER BY sort_order NULLS LAST,id`, includeHidden)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.AppCourse{}
	for rows.Next() {
		var course database.AppCourse
		if err := rows.Scan(&course.ID, &course.Name, &course.WebdavFolder,
			&course.SortOrder, &course.Hidden, &course.ShortName); err != nil {
			return nil, err
		}
		out = append(out, course)
	}
	return out, rows.Err()
}

func (c sqliteCourses) AddCourse(ctx context.Context, params database.AddCourseParams) error {
	_, err := c.db.Exec(ctx, `INSERT INTO courses(id,name,webdav_folder,short_name)`+
		` VALUES(?,?,?,?)`, params.ID, params.Name, params.WebdavFolder, params.ShortName)
	return err
}

func (c sqliteCourses) RenameCourse(ctx context.Context, params database.RenameCourseParams) (int64, error) {
	result, err := c.db.Exec(ctx, `UPDATE courses SET name=? WHERE id=?`, params.Name, params.ID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (c sqliteCourses) HideCourse(ctx context.Context, params database.HideCourseParams) (int64, error) {
	result, err := c.db.Exec(ctx, `UPDATE courses SET hidden=? WHERE id=?`, params.Hidden, params.ID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (c sqliteCourses) OrderCourse(ctx context.Context, params database.OrderCourseParams) error {
	_, err := c.db.Exec(ctx, `UPDATE courses SET sort_order=? WHERE id=?`, params.SortOrder, params.ID)
	return err
}

func (c sqliteCourses) LockCoursesForReorder(ctx context.Context) error {
	_, err := advisoryLock(ctx, c.db, "courses:reorder", true, false)
	return err
}

func (c sqliteCourses) ExamPlanEnabled(ctx context.Context, id int64) (bool, error) {
	var enabled bool
	err := c.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM course_exam_plans`+
		` WHERE course_id=? AND enabled=1)`, id).Scan(&enabled)
	return enabled, err
}

func (c sqliteCourses) TreeNodes(ctx context.Context, courseID int64) ([]database.TreeNode, error) {
	rows, err := c.db.Query(ctx, `SELECT id,parent_id,name,url,local_path FROM nodes`+
		` WHERE course_id=? ORDER BY id`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.TreeNode{}
	for rows.Next() {
		var node database.TreeNode
		if err := rows.Scan(&node.ID, &node.ParentID, &node.Name, &node.URL, &node.LocalPath); err != nil {
			return nil, err
		}
		out = append(out, node)
	}
	return out, rows.Err()
}

func (c sqliteCourses) TreeFiles(ctx context.Context, courseID int64) ([]database.TreeFile, error) {
	rows, err := c.db.Query(ctx, `SELECT f.node_id,f.url,f.name,f.md5_hash,f.etag,`+
		`f.last_updated,f.local_path,f.redirect_url FROM files f JOIN nodes n ON n.id=f.node_id`+
		` WHERE n.course_id=? ORDER BY f.id`, courseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.TreeFile{}
	for rows.Next() {
		var file database.TreeFile
		if err := rows.Scan(&file.NodeID, &file.URL, &file.Name, &file.MD5Hash,
			&file.Etag, &file.LastUpdated, &file.LocalPath, &file.RedirectURL); err != nil {
			return nil, err
		}
		out = append(out, file)
	}
	return out, rows.Err()
}

func (c sqliteCourses) CourseCoverage(ctx context.Context) ([]database.CourseCoverageRow, error) {
	rows, err := c.db.Query(ctx, `SELECT course_id,payload_json FROM course_coverage ORDER BY course_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.CourseCoverageRow{}
	for rows.Next() {
		var row database.CourseCoverageRow
		if err := rows.Scan(&row.CourseID, &row.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c sqliteCourses) RecentMaterials(ctx context.Context) ([]database.RecentMaterialsRow, error) {
	rows, err := c.db.Query(ctx, `SELECT course_id,payload_json FROM recent_materials ORDER BY ordinal`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.RecentMaterialsRow{}
	for rows.Next() {
		var row database.RecentMaterialsRow
		if err := rows.Scan(&row.CourseID, &row.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (c sqliteCourses) StudyLevels(ctx context.Context) ([]database.StudyLevelRow, error) {
	rows, err := c.db.Query(ctx, `SELECT course_id,file_path,level FROM file_study`+
		` ORDER BY course_id,file_path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.StudyLevelRow{}
	for rows.Next() {
		var row database.StudyLevelRow
		if err := rows.Scan(&row.CourseID, &row.FilePath, &row.Level); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
