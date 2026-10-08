package settings

import (
	"context"
	"encoding/json"
	"net/url"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// SavePlanner commits the complete form and its projection debt together. The
// course lock prevents a concurrent hide/delete from changing the form's scope.
func (s Service) SavePlanner(ctx context.Context, form url.Values) error {
	return s.mutate(ctx, "planner", func(tx database.Tx) error {
		if err := tx.Settings().LockCoursesForPlanner(ctx); err != nil {
			return err
		}
		p, err := ReadPlanner(ctx, tx)
		if err != nil {
			return err
		}
		plans, err := ReadExamPlans(ctx, tx)
		if err != nil {
			return err
		}
		var issues PlannerErrors
		p.apply(form, &issues)
		for i := range plans {
			plans[i].apply(form, &issues)
		}
		if len(issues) > 0 {
			return issues
		}
		weekly, _ := json.Marshal(p.Weekly)
		blackouts, _ := json.Marshal(p.Blackouts)
		if err := tx.Settings().SavePlannerSettings(ctx, database.SettingsPlanner{
			DailyBlocks:   p.DailyBlocks,
			BlockMinutes:  p.BlockMinutes,
			WeeklyJSON:    string(weekly),
			BlackoutsJSON: string(blackouts),
			MaxCourses:    p.MaxCourses,
		}); err != nil {
			return err
		}
		for _, plan := range plans {
			if err = saveExamPlan(ctx, tx, plan); err != nil {
				return err
			}
		}
		_, err = commands.EnqueueTx(ctx, tx, "projection", "refresh_priorities", map[string]any{}, true)
		return err
	})
}

func encodedOptional(value *string) *string {
	if value == nil {
		return nil
	}
	text := identity.Encode(*value)
	return &text
}

func saveExamPlan(ctx context.Context, tx database.Tx, p ExamPlan) error {
	return tx.Settings().SaveExamPlan(ctx, database.SettingsExamPlan{
		CourseID:    p.CourseID,
		CourseName:  identity.Encode(p.CourseName),
		ExamAt:      p.ExamAt,
		Remaining:   p.Remaining,
		Importance:  p.Importance,
		MaxBlocks:   p.MaxBlocks,
		Enabled:     p.Enabled,
		ShortName:   encodedOptional(p.ShortName),
		Commitment:  p.Commitment,
		TargetGrade: p.TargetGrade,
		Notes:       encodedOptional(p.Notes),
	})
}
