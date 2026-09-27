package courses

import (
	"context"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/queries"
)

func SnapshotCourses(ctx context.Context, tx pgx.Tx, selected *int64) ([]Course, *Course, error) {
	result := []Course{}
	if selected == nil {
		rows, err := queries.New(tx).ListCourses(ctx, false)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			result = append(result, course(row))
		}
		return result, nil, nil
	}
	row, err := queries.New(tx).Course(ctx, *selected)
	if err != nil {
		return nil, nil, err
	}
	if row.Hidden != 0 {
		var planned bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.course_exam_plans WHERE course_id=$1 AND enabled=1)`, *selected).Scan(&planned); err != nil {
			return nil, nil, err
		}
		if !planned {
			return nil, nil, pgx.ErrNoRows
		}
	}
	item := course(row)
	return []Course{item}, &item, nil
}
