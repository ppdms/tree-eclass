// Package study reads learner planning state and materialized study decisions.
package study

import (
	"context"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/settings"
)

type Service struct{ Pool *pgxpool.Pool }
type Snapshot struct {
	Courses  []courses.Course    `json:"courses"`
	Inbox    []InboxItem         `json:"inbox"`
	Plans    []settings.ExamPlan `json:"planner_rows"`
	Settings settings.Planner    `json:"planner_settings"`
	Selected *courses.Course     `json:"selected_course"`
}

func (s Service) Snapshot(ctx context.Context, selected *int64, now time.Time) (Snapshot, error) {
	result := Snapshot{}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	result, err = snapshotTx(ctx, tx, selected, now)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func snapshotTx(ctx context.Context, tx pgx.Tx, selected *int64, now time.Time) (Snapshot, error) {
	result := Snapshot{}
	var err error
	result.Courses, result.Selected, err = courses.SnapshotCourses(ctx, tx, selected)
	if err != nil {
		return result, err
	}
	result.Plans, err = settings.ReadExamPlans(ctx, tx)
	if err != nil {
		return result, err
	}
	if selected != nil {
		result.Plans = slices.DeleteFunc(
			result.Plans,
			func(p settings.ExamPlan) bool { return p.CourseID != *selected },
		)
	}
	result.Settings, err = settings.ReadPlanner(ctx, tx)
	if err != nil {
		return result, err
	}
	result.Inbox, err = readInbox(ctx, tx, selected, now)
	if err != nil {
		return result, err
	}
	return result, nil
}
