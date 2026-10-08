package settings

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
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

func decodeExamPlan(plan database.SettingsExamPlan) ExamPlan {
	out := ExamPlan{
		CourseID:    plan.CourseID,
		CourseName:  identity.Decode(plan.CourseName),
		ExamAt:      plan.ExamAt,
		Remaining:   plan.Remaining,
		Importance:  plan.Importance,
		MaxBlocks:   plan.MaxBlocks,
		Enabled:     plan.Enabled,
		Commitment:  plan.Commitment,
		TargetGrade: plan.TargetGrade,
	}
	if plan.ShortName != nil {
		text := identity.Decode(*plan.ShortName)
		out.ShortName = &text
	}
	if plan.Notes != nil {
		text := identity.Decode(*plan.Notes)
		out.Notes = &text
	}
	return out
}

// ReadExamPlans accepts store or transaction operations so derived reads
// share their source snapshot.
func ReadExamPlans(ctx context.Context, db database.Operations) ([]ExamPlan, error) {
	rows, err := db.Settings().ListExamPlans(ctx)
	if err != nil {
		return nil, err
	}
	plans := make([]ExamPlan, 0, len(rows))
	for _, row := range rows {
		plans = append(plans, decodeExamPlan(row))
	}
	return plans, nil
}

// ReadExamPlan accepts store or transaction operations so derived reads
// share their source snapshot. It reports database.ErrNoRows when the
// course commitment is absent.
func ReadExamPlan(ctx context.Context, db database.Operations, course int64) (ExamPlan, error) {
	row, err := db.Settings().GetExamPlan(ctx, course)
	if err != nil {
		return ExamPlan{}, err
	}
	return decodeExamPlan(row), nil
}

func (s Service) ExamPlans(ctx context.Context) ([]ExamPlan, error) {
	return ReadExamPlans(ctx, s.Pool)
}
func (s Service) Planner(ctx context.Context) (Planner, error) { return ReadPlanner(ctx, s.Pool) }
