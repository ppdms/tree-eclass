package navigation

import (
	"context"
	"strconv"

	"tree-eclass/internal/domain/activity"
	"tree-eclass/internal/domain/database"
)

// Detail retains the legacy bounded course response. Its blueprint is still
// read from the generation-checked projection, never synthesized on demand.
func (s Service) Detail(ctx context.Context, course int64) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	view, err := s.ReadTx(ctx, tx, Request{CourseID: course, Roadmap: true, IncludeActions: true})
	if err != nil {
		return nil, err
	}
	updates, err := activity.CoursePageTx(ctx, tx, course, 50, 0)
	if err != nil {
		return nil, err
	}
	distribution := map[string]int64{"0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
	counts, err := tx.Navigation().StudyDistribution(ctx, course)
	if err != nil {
		return nil, err
	}
	for level, count := range counts {
		distribution[strconv.FormatInt(level, 10)] = count
	}
	return map[string]any{
		"course":             view.Course,
		"course_blueprint":   view.Blueprint,
		"timeline":           updates.Timeline,
		"study_distribution": distribution,
	}, tx.Commit(ctx)
}
