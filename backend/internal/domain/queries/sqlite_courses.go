package queries

import (
	"context"
)

// SQLite course + read-model methods over the driver-neutral rdbms.DBTX
// surface. SQL text is copied verbatim from the generated *.sql.go consts;
// the sqlite driver rewrites placeholders/casts at Exec/Query time.

const sqliteAddCourse = `-- name: AddCourse :exec
INSERT INTO app.courses(id,name,webdav_folder,short_name) VALUES($1,$2,$3,$4)
`

func (q *SQLiteQueries) AddCourse(ctx context.Context, arg AddCourseParams) error {
	_, err := q.db.Exec(ctx, sqliteAddCourse, arg.ID, arg.Name, arg.WebdavFolder, arg.ShortName)
	return err
}

const sqliteCourse = `-- name: Course :one
SELECT id,name,webdav_folder,sort_order,hidden,short_name FROM app.courses WHERE id=$1
`

func (q *SQLiteQueries) Course(ctx context.Context, id int64) (AppCourse, error) {
	row := q.db.QueryRow(ctx, sqliteCourse, id)
	var i AppCourse
	err := row.Scan(
		&i.ID,
		&i.Name,
		&i.WebdavFolder,
		&i.SortOrder,
		&i.Hidden,
		&i.ShortName,
	)
	return i, err
}

const sqliteCourseCoverage = `-- name: CourseCoverage :many
SELECT course_id,payload_json FROM read_model.course_coverage ORDER BY course_id
`

func (q *SQLiteQueries) CourseCoverage(ctx context.Context) ([]CourseCoverageRow, error) {
	rows, err := q.db.Query(ctx, sqliteCourseCoverage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []CourseCoverageRow{}
	for rows.Next() {
		var i CourseCoverageRow
		if err := rows.Scan(&i.CourseID, &i.PayloadJson); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const sqliteHideCourse = `-- name: HideCourse :execrows
UPDATE app.courses SET hidden=$2 WHERE id=$1
`

func (q *SQLiteQueries) HideCourse(ctx context.Context, arg HideCourseParams) (int64, error) {
	result, err := q.db.Exec(ctx, sqliteHideCourse, arg.ID, arg.Hidden)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const sqliteListCourses = `-- name: ListCourses :many
SELECT id,name,webdav_folder,sort_order,hidden,short_name FROM app.courses
WHERE hidden=0 OR $1::boolean ORDER BY sort_order,id
`

func (q *SQLiteQueries) ListCourses(ctx context.Context, includeHidden bool) ([]AppCourse, error) {
	rows, err := q.db.Query(ctx, sqliteListCourses, includeHidden)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []AppCourse{}
	for rows.Next() {
		var i AppCourse
		if err := rows.Scan(
			&i.ID,
			&i.Name,
			&i.WebdavFolder,
			&i.SortOrder,
			&i.Hidden,
			&i.ShortName,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const sqliteOrderCourse = `-- name: OrderCourse :exec
UPDATE app.courses SET sort_order=$2 WHERE id=$1
`

func (q *SQLiteQueries) OrderCourse(ctx context.Context, arg OrderCourseParams) error {
	_, err := q.db.Exec(ctx, sqliteOrderCourse, arg.ID, arg.SortOrder)
	return err
}

const sqliteRecentMaterials = `-- name: RecentMaterials :many
SELECT course_id,payload_json FROM read_model.recent_materials ORDER BY ordinal
`

func (q *SQLiteQueries) RecentMaterials(ctx context.Context) ([]RecentMaterialsRow, error) {
	rows, err := q.db.Query(ctx, sqliteRecentMaterials)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []RecentMaterialsRow{}
	for rows.Next() {
		var i RecentMaterialsRow
		if err := rows.Scan(&i.CourseID, &i.PayloadJson); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const sqliteRenameCourse = `-- name: RenameCourse :execrows
UPDATE app.courses SET name=$2 WHERE id=$1
`

func (q *SQLiteQueries) RenameCourse(ctx context.Context, arg RenameCourseParams) (int64, error) {
	result, err := q.db.Exec(ctx, sqliteRenameCourse, arg.ID, arg.Name)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const sqliteSetStudyLevel = `-- name: SetStudyLevel :exec
INSERT INTO app.file_study(course_id,file_path,level,last_updated) VALUES($1,$2,$3,$4)
ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,last_updated=excluded.last_updated
`

func (q *SQLiteQueries) SetStudyLevel(ctx context.Context, arg SetStudyLevelParams) error {
	_, err := q.db.Exec(ctx, sqliteSetStudyLevel,
		arg.CourseID,
		arg.FilePath,
		arg.Level,
		arg.LastUpdated,
	)
	return err
}

const sqliteStudyLevels = `-- name: StudyLevels :many
SELECT course_id,file_path,level FROM app.file_study ORDER BY course_id,file_path
`

func (q *SQLiteQueries) StudyLevels(ctx context.Context) ([]StudyLevelsRow, error) {
	rows, err := q.db.Query(ctx, sqliteStudyLevels)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []StudyLevelsRow{}
	for rows.Next() {
		var i StudyLevelsRow
		if err := rows.Scan(&i.CourseID, &i.FilePath, &i.Level); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
