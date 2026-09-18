package exercises

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool *pgxpool.Pool }

// Detail is a pointer so summary responses omit descriptions instead of fetching
// and retaining every potentially large assignment body.
type Exercise struct {
	ID                 int64   `json:"id"`
	CourseID           int64   `json:"course_id"`
	CourseName         string  `json:"course_name"`
	ExerciseID         string  `json:"exercise_id"`
	Title              string  `json:"title"`
	Link               string  `json:"link"`
	Deadline           *string `json:"deadline"`
	SubmissionStatus   *string `json:"submission_status"`
	Grade              *string `json:"grade"`
	MaxGrade           *string `json:"max_grade"`
	Ignored            int64   `json:"ignored"`
	FetchedAt          *string `json:"fetched_at"`
	Description        *string `json:"description,omitempty"`
	WorkType           *string `json:"work_type,omitempty"`
	StartDate          *string `json:"start_date,omitempty"`
	AssignmentFileName *string `json:"assignment_file_name,omitempty"`
	AssignmentFileURL  *string `json:"assignment_file_url,omitempty"`
	GradeComments      *string `json:"grade_comments,omitempty"`
	SubmissionDate     *string `json:"submission_date,omitempty"`
	Urgency            string  `json:"_urgency,omitempty"`
	TimeLabel          *string `json:"_time_label"`
	DeadlineShort      string  `json:"deadline_short,omitempty"`
	timestamp          int64
}

const summaryColumns = `e.id,e.course_id,c.name AS course_name,e.exercise_id,e.title,e.link,
 e.deadline,e.submission_status,e.grade,e.max_grade,e.ignored,e.fetched_at`
const detailColumns = `,e.description,e.work_type,e.start_date,e.assignment_file_name,
 e.assignment_file_url,e.grade_comments,e.submission_date`

func decode(raw []byte) (Exercise, error) {
	var result Exercise
	err := json.Unmarshal(raw, &result)
	for _, field := range []*string{&result.CourseName, &result.ExerciseID, &result.Title, &result.Link} {
		*field = identity.Decode(*field)
	}
	for _, field := range []*string{
		result.Deadline,
		result.SubmissionStatus,
		result.Grade,
		result.MaxGrade,
		result.FetchedAt,
		result.Description,
		result.WorkType,
		result.StartDate,
		result.AssignmentFileName,
		result.AssignmentFileURL,
		result.GradeComments,
		result.SubmissionDate,
	} {
		if field != nil {
			*field = identity.Decode(*field)
		}
	}
	return result, err
}

func (s Service) List(ctx context.Context, ignored, details bool, now time.Time) ([]Exercise, error) {
	columns := summaryColumns
	if details {
		columns += detailColumns
	}
	rows, err := s.Pool.Query(
		ctx,
		`SELECT row_to_json(x) FROM (SELECT `+columns+` FROM app.exercises e JOIN app.courses c ON c.id=e.course_id WHERE c.hidden=0 AND ($1 OR e.ignored=0) ORDER BY e.course_id,e.id DESC LIMIT 200) x`,
		ignored,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Exercise, 0)
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decode(raw)
		if err != nil {
			return nil, err
		}
		item.annotate(now)
		result = append(result, item)
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := urgencyOrder[result[i].Urgency], urgencyOrder[result[j].Urgency]
		if a != b {
			return a < b
		}
		return result[i].timestamp < result[j].timestamp
	})
	return result, rows.Err()
}

func (e *Exercise) annotate(now time.Time) {
	var deadline time.Time
	if e.Deadline != nil {
		deadline = Deadline(*e.Deadline, now)
	}
	e.Urgency, e.TimeLabel = Urgency(deadline, e.SubmissionStatus != nil && *e.SubmissionStatus == "submitted", now)
	e.timestamp = 9999999999
	if !deadline.IsZero() {
		e.timestamp = deadline.Unix()
		e.DeadlineShort = deadline.Format("2 Jan · 15:04")
	}
}

func (s Service) Get(ctx context.Context, course int64, id string) (Exercise, error) {
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT row_to_json(x) FROM (SELECT `+summaryColumns+detailColumns+` FROM app.exercises e JOIN app.courses c ON c.id=e.course_id WHERE c.hidden=0 AND e.course_id=$1 AND e.exercise_id=$2) x`, course, identity.Encode(id)).
		Scan(&raw)
	if err != nil {
		return Exercise{}, err
	}
	return decode(raw)
}

func (s Service) Ignore(ctx context.Context, course int64, id string, ignored bool) error {
	flag := 0
	if ignored {
		flag = 1
	}
	_, err := s.Pool.Exec(
		ctx,
		`UPDATE app.exercises SET ignored=$3 WHERE course_id=$1 AND exercise_id=$2`,
		course,
		identity.Encode(id),
		flag,
	)
	return err
}
