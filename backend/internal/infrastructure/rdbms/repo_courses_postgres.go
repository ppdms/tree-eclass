package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// Courses identity, shelf mutations and catalog projections on PostgreSQL.
// Added courses default sort_order 0; explicitly cleared NULL positions
// order after every value (native ORDER BY sort_order,id), and SQLite
// states NULLS LAST explicitly.
type postgresCourses struct{ db nativeDBTX }

func scanCourse(row nativeRow) (database.AppCourse, error) {
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

const courseColumns = `id,name,webdav_folder,sort_order,hidden,short_name`

func (c postgresCourses) Course(ctx context.Context, id int64) (database.AppCourse, error) {
	return scanCourse(c.db.QueryRow(ctx,
		`SELECT `+courseColumns+` FROM app.courses WHERE id=$1`, id))
}

func (c postgresCourses) VisibleCourse(ctx context.Context, id int64) (database.AppCourse, error) {
	return scanCourse(c.db.QueryRow(ctx,
		`SELECT `+courseColumns+` FROM app.courses WHERE id=$1 AND hidden=0`, id))
}

func (c postgresCourses) LockCourseForWrite(ctx context.Context, id int64) (database.AppCourse, error) {
	return scanCourse(c.db.QueryRow(ctx,
		`SELECT `+courseColumns+` FROM app.courses WHERE id=$1 AND hidden=0 FOR SHARE`, id))
}

func (c postgresCourses) ListCourses(ctx context.Context, includeHidden bool) ([]database.AppCourse, error) {
	rows, err := c.db.Query(ctx, `SELECT `+courseColumns+` FROM app.courses`+
		` WHERE hidden=0 OR $1::boolean ORDER BY sort_order,id`, includeHidden)
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

func (c postgresCourses) AddCourse(ctx context.Context, params database.AddCourseParams) error {
	_, err := c.db.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder,short_name)`+
		` VALUES($1,$2,$3,$4)`, params.ID, params.Name, params.WebdavFolder, params.ShortName)
	return err
}

func (c postgresCourses) RenameCourse(ctx context.Context, params database.RenameCourseParams) (int64, error) {
	result, err := c.db.Exec(ctx, `UPDATE app.courses SET name=$2 WHERE id=$1`, params.ID, params.Name)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (c postgresCourses) HideCourse(ctx context.Context, params database.HideCourseParams) (int64, error) {
	result, err := c.db.Exec(ctx, `UPDATE app.courses SET hidden=$2 WHERE id=$1`, params.ID, params.Hidden)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (c postgresCourses) OrderCourse(ctx context.Context, params database.OrderCourseParams) error {
	_, err := c.db.Exec(ctx, `UPDATE app.courses SET sort_order=$2 WHERE id=$1`, params.ID, params.SortOrder)
	return err
}

func (c postgresCourses) LockCoursesForReorder(ctx context.Context) error {
	_, err := c.db.Exec(ctx, `LOCK TABLE app.courses IN SHARE ROW EXCLUSIVE MODE`)
	return err
}

func (c postgresCourses) ExamPlanEnabled(ctx context.Context, id int64) (bool, error) {
	var enabled bool
	err := c.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.course_exam_plans`+
		` WHERE course_id=$1 AND enabled=1)`, id).Scan(&enabled)
	return enabled, err
}

func (c postgresCourses) TreeNodes(ctx context.Context, courseID int64) ([]database.TreeNode, error) {
	rows, err := c.db.Query(ctx, `SELECT id,parent_id,name,url,local_path FROM app.nodes`+
		` WHERE course_id=$1 ORDER BY id`, courseID)
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

func (c postgresCourses) TreeFiles(ctx context.Context, courseID int64) ([]database.TreeFile, error) {
	rows, err := c.db.Query(ctx, `SELECT f.node_id,f.url,f.name,f.md5_hash,f.etag,`+
		`f.last_updated,f.local_path,f.redirect_url FROM app.files f JOIN app.nodes n ON n.id=f.node_id`+
		` WHERE n.course_id=$1 ORDER BY f.id`, courseID)
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

func (c postgresCourses) CourseCoverage(ctx context.Context) ([]database.CourseCoverageRow, error) {
	rows, err := c.db.Query(ctx, `SELECT course_id,payload_json FROM read_model.course_coverage`+
		` ORDER BY course_id`)
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

func (c postgresCourses) RecentMaterials(ctx context.Context) ([]database.RecentMaterialsRow, error) {
	rows, err := c.db.Query(ctx, `SELECT course_id,payload_json FROM read_model.recent_materials`+
		` ORDER BY ordinal`)
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

func (c postgresCourses) StudyLevels(ctx context.Context) ([]database.StudyLevelRow, error) {
	rows, err := c.db.Query(ctx, `SELECT course_id,file_path,level FROM app.file_study`+
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
