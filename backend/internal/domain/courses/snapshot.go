package courses

import (
	"context"

	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
)

func SnapshotCourses(ctx context.Context, tx rdbms.Tx, selected *int64) ([]Course, *Course, error) {
	result := []Course{}
	q := queries.ForTx(tx)
	if selected == nil {
		rows, err := q.ListCourses(ctx, false)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			result = append(result, course(row))
		}
		return result, nil, nil
	}
	row, err := q.Course(ctx, *selected)
	if err != nil {
		return nil, nil, err
	}
	if row.Hidden != 0 {
		var planned bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.course_exam_plans WHERE course_id=$1 AND enabled=1)`, *selected).Scan(&planned); err != nil {
			return nil, nil, err
		}
		if !planned {
			return nil, nil, rdbms.ErrNoRows
		}
	}
	item := course(row)
	return []Course{item}, &item, nil
}
