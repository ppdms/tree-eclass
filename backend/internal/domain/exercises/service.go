package exercises

import (
	"context"
	"sort"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool database.Store }

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

func exercise(row database.ExerciseRow) Exercise {
	result := Exercise{
		ID:                 row.ID,
		CourseID:           row.CourseID,
		CourseName:         identity.Decode(row.CourseName),
		ExerciseID:         identity.Decode(row.ExerciseID),
		Title:              identity.Decode(row.Title),
		Link:               identity.Decode(row.Link),
		Ignored:            row.Ignored,
		Deadline:           decodeField(row.Deadline),
		SubmissionStatus:   decodeField(row.SubmissionStatus),
		Grade:              decodeField(row.Grade),
		MaxGrade:           decodeField(row.MaxGrade),
		FetchedAt:          decodeField(row.FetchedAt),
		Description:        decodeField(row.Description),
		WorkType:           decodeField(row.WorkType),
		StartDate:          decodeField(row.StartDate),
		AssignmentFileName: decodeField(row.AssignmentName),
		AssignmentFileURL:  decodeField(row.AssignmentURL),
		GradeComments:      decodeField(row.GradeComments),
		SubmissionDate:     decodeField(row.SubmissionDate),
	}
	return result
}

func decodeField(value *string) *string {
	if value == nil {
		return nil
	}
	text := identity.Decode(*value)
	return &text
}

func (s Service) List(ctx context.Context, ignored, details bool, now time.Time) ([]Exercise, error) {
	rows, err := s.Pool.Exercises().List(ctx, ignored, details)
	if err != nil {
		return nil, err
	}
	result := make([]Exercise, 0, len(rows))
	for _, row := range rows {
		item := exercise(row)
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
	return result, nil
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
	row, err := s.Pool.Exercises().Get(ctx, course, identity.Encode(id))
	if err != nil {
		return Exercise{}, err
	}
	return exercise(row), nil
}

func (s Service) Ignore(ctx context.Context, course int64, id string, ignored bool) error {
	flag := int64(0)
	if ignored {
		flag = 1
	}
	return s.Pool.Exercises().SetIgnored(ctx, course, identity.Encode(id), flag)
}
