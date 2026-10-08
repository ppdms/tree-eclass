package settings

import (
	"context"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type ExamPlan struct {
	CourseID    int64   `json:"course_id"`
	CourseName  string  `json:"course_name"`
	ExamAt      *string `json:"exam_at"`
	Remaining   int64   `json:"remaining_blocks"`
	Importance  float64 `json:"importance"`
	MaxBlocks   int64   `json:"max_daily_blocks"`
	Enabled     bool    `json:"enabled"`
	ShortName   *string `json:"short_name"`
	Commitment  string  `json:"commitment"`
	TargetGrade float64 `json:"target_grade"`
	Notes       *string `json:"planning_notes"`
}

type rowsQueryer interface {
	Query(context.Context, string, ...any) (rdbms.Rows, error)
}

func ReadExamPlans(ctx context.Context, db rowsQueryer) ([]ExamPlan, error) {
	query := examPlanSelect + `WHERE c.hidden=0 OR coalesce(p.enabled,0)=1 ORDER BY c.sort_order,c.id`
	return readExamPlans(ctx, db, query)
}

func ReadExamPlan(ctx context.Context, db rowsQueryer, course int64) (ExamPlan, error) {
	rows, err := readExamPlans(ctx, db, examPlanSelect+`WHERE c.id=$1`, course)
	if err != nil {
		return ExamPlan{}, err
	}
	if len(rows) == 0 {
		return ExamPlan{}, rdbms.ErrNoRows
	}
	return rows[0], nil
}

func readExamPlans(ctx context.Context, db rowsQueryer, query string, args ...any) ([]ExamPlan, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	plans := []ExamPlan{}
	for rows.Next() {
		var p ExamPlan
		if err := rows.Scan(
			&p.CourseID,
			&p.CourseName,
			&p.ExamAt,
			&p.Remaining,
			&p.Importance,
			&p.MaxBlocks,
			&p.Enabled,
			&p.ShortName,
			&p.Commitment,
			&p.TargetGrade,
			&p.Notes,
		); err != nil {
			return nil, err
		}
		p.CourseName = identity.Decode(p.CourseName)
		for _, value := range []*string{p.ShortName, p.Notes} {
			if value != nil {
				*value = identity.Decode(*value)
			}
		}
		plans = append(plans, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return plans, nil
}

func (s Service) ExamPlans(ctx context.Context) ([]ExamPlan, error) {
	return ReadExamPlans(ctx, s.Pool)
}
func (s Service) Planner(ctx context.Context) (Planner, error) { return ReadPlanner(ctx, s.Pool) }
