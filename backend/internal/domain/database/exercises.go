package database

import "context"

// ExerciseRow is one stored exercise joined to its visible course name.
// CourseName, ExerciseID, Title and Link stay encoded exactly as stored;
// callers decode via identity.Decode. Detail fields are nil unless the
// caller requested details.
type ExerciseRow struct {
	ID               int64   `json:"id"`
	CourseID         int64   `json:"course_id"`
	CourseName       string  `json:"course_name"`
	ExerciseID       string  `json:"exercise_id"`
	Title            string  `json:"title"`
	Link             string  `json:"link"`
	Deadline         *string `json:"deadline"`
	SubmissionStatus *string `json:"submission_status"`
	Grade            *string `json:"grade"`
	MaxGrade         *string `json:"max_grade"`
	Ignored          int64   `json:"ignored"`
	FetchedAt        *string `json:"fetched_at"`
	Description      *string `json:"description"`
	WorkType         *string `json:"work_type"`
	StartDate        *string `json:"start_date"`
	AssignmentName   *string `json:"assignment_file_name"`
	AssignmentURL    *string `json:"assignment_file_url"`
	GradeComments    *string `json:"grade_comments"`
	SubmissionDate   *string `json:"submission_date"`
}

// Exercises is the typed port for assignment reads and ignore flags.
type Exercises interface {
	// List returns up to 200 exercises for visible courses ordered by
	// course then id descending. IncludeIgnored admits ignored rows;
	// details selects the large assignment bodies as well.
	List(ctx context.Context, includeIgnored, details bool) ([]ExerciseRow, error)
	// Get returns one exercise with its detail bodies for a visible
	// course. ExerciseID is already encoded by the caller. It reports
	// ErrNoRows when missing or the course is hidden.
	Get(ctx context.Context, courseID int64, exerciseID string) (ExerciseRow, error)
	// SetIgnored sets the ignored flag (0 or 1) for one exercise.
	// ExerciseID is already encoded by the caller.
	SetIgnored(ctx context.Context, courseID int64, exerciseID string, ignored int64) error
}
