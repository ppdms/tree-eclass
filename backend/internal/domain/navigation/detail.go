package navigation

import (
	"context"
	"strconv"

	"tree-eclass/internal/domain/activity"
	"tree-eclass/internal/infrastructure/rdbms"
)

// Detail retains the legacy bounded course response. Its blueprint is still
// read from the generation-checked projection, never synthesized on demand.
func (s Service) Detail(ctx context.Context, course int64) (map[string]any, error) {
	tx, err := s.Pool.BeginTx(ctx, rdbms.Options{Isolation: rdbms.RepeatableRead, AccessMode: rdbms.ReadOnly})
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
	rows, err := tx.Query(
		ctx,
		`SELECT greatest(0,least(5,level)),count(*) FROM app.file_study WHERE course_id=$1 GROUP BY 1`,
		course,
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var level, count int64
		if err = rows.Scan(&level, &count); err != nil {
			rows.Close()
			return nil, err
		}
		distribution[strconv.FormatInt(level, 10)] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"course":             view.Course,
		"course_blueprint":   view.Blueprint,
		"timeline":           updates.Timeline,
		"study_distribution": distribution,
	}, tx.Commit(
		ctx,
	)
}
