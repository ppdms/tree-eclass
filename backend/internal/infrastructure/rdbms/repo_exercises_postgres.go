package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

// PostgreSQL exercises reads. Detail bodies bind only when requested; NULL
// columns stay NULL so both backends agree on present versus absent text.
type postgresExercises struct{ db nativeDBTX }

const exerciseSummaryColumns = `e.id,e.course_id,c.name,e.exercise_id,e.title,e.link,` +
	`e.deadline,e.submission_status,e.grade,e.max_grade,e.ignored,e.fetched_at`
const exerciseDetailColumns = `,e.description,e.work_type,e.start_date,e.assignment_file_name,` +
	`e.assignment_file_url,e.grade_comments,e.submission_date`

func scanExerciseSummary(rows nativeRows, details bool) (database.ExerciseRow, error) {
	var item database.ExerciseRow
	args := []any{
		&item.ID, &item.CourseID, &item.CourseName, &item.ExerciseID,
		&item.Title, &item.Link, &item.Deadline, &item.SubmissionStatus,
		&item.Grade, &item.MaxGrade, &item.Ignored, &item.FetchedAt,
	}
	if details {
		args = append(args,
			&item.Description, &item.WorkType, &item.StartDate,
			&item.AssignmentName, &item.AssignmentURL,
			&item.GradeComments, &item.SubmissionDate,
		)
	}
	return item, rows.Scan(args...)
}

func (e postgresExercises) List(ctx context.Context, includeIgnored, details bool) ([]database.ExerciseRow, error) {
	columns := exerciseSummaryColumns
	if details {
		columns += exerciseDetailColumns
	}
	rows, err := e.db.Query(ctx, `SELECT `+columns+` FROM app.exercises e JOIN app.courses c ON c.id=e.course_id`+
		` WHERE c.hidden=0 AND ($1::boolean OR e.ignored=0) ORDER BY e.course_id,e.id DESC LIMIT 200`, includeIgnored)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ExerciseRow{}
	for rows.Next() {
		item, err := scanExerciseSummary(rows, details)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (e postgresExercises) Get(ctx context.Context, courseID int64, exerciseID string) (database.ExerciseRow, error) {
	var item database.ExerciseRow
	err := e.db.QueryRow(ctx, `SELECT `+exerciseSummaryColumns+exerciseDetailColumns+
		` FROM app.exercises e JOIN app.courses c ON c.id=e.course_id`+
		` WHERE c.hidden=0 AND e.course_id=$1 AND e.exercise_id=$2`, courseID, exerciseID).Scan(
		&item.ID, &item.CourseID, &item.CourseName, &item.ExerciseID,
		&item.Title, &item.Link, &item.Deadline, &item.SubmissionStatus,
		&item.Grade, &item.MaxGrade, &item.Ignored, &item.FetchedAt,
		&item.Description, &item.WorkType, &item.StartDate,
		&item.AssignmentName, &item.AssignmentURL,
		&item.GradeComments, &item.SubmissionDate)
	return item, err
}

func (e postgresExercises) SetIgnored(ctx context.Context, courseID int64, exerciseID string, ignored int64) error {
	_, err := e.db.Exec(ctx, `UPDATE app.exercises SET ignored=$3 WHERE course_id=$1 AND exercise_id=$2`,
		courseID, exerciseID, ignored)
	return err
}
