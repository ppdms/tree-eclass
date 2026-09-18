package settings

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/jobs"
)

// SavePlanner commits the complete form and its projection debt together. The
// course lock prevents a concurrent hide/delete from changing the form's scope.
func (s Service) SavePlanner(ctx context.Context, form url.Values) error {
	return s.mutate(ctx, "planner", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `LOCK TABLE app.courses IN SHARE ROW EXCLUSIVE MODE`); err != nil {
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
		_, err = tx.Exec(
			ctx,
			`INSERT INTO app.study_planner_settings(id,daily_blocks,block_minutes,weekly_minutes_json,blackout_dates_json,max_courses_per_day)
VALUES(1,$1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET daily_blocks=$1,block_minutes=$2,weekly_minutes_json=$3,blackout_dates_json=$4,max_courses_per_day=$5`,
			p.DailyBlocks,
			p.BlockMinutes,
			string(weekly),
			string(blackouts),
			p.MaxCourses,
		)
		if err != nil {
			return err
		}
		for _, plan := range plans {
			if err = saveExamPlan(ctx, tx, plan); err != nil {
				return err
			}
		}
		_, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_priorities", map[string]any{}, true)
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

func saveExamPlan(ctx context.Context, tx pgx.Tx, p ExamPlan) error {
	_, err := tx.Exec(
		ctx,
		`INSERT INTO app.course_exam_plans(course_id,exam_at,remaining_blocks,importance,max_daily_blocks,enabled,commitment,target_grade,planning_notes)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(course_id) DO UPDATE SET exam_at=$2,remaining_blocks=$3,importance=$4,max_daily_blocks=$5,enabled=$6,commitment=$7,target_grade=$8,planning_notes=$9,updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')`,
		p.CourseID,
		p.ExamAt,
		p.Remaining,
		p.Importance,
		p.MaxBlocks,
		flag(p.Enabled),
		p.Commitment,
		p.TargetGrade,
		encodedOptional(p.Notes),
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE app.courses SET short_name=$2 WHERE id=$1`, p.CourseID, encodedOptional(p.ShortName))
	return err
}
