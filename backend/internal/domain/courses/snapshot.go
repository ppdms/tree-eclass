package courses

import (
	"context"

	"tree-eclass/internal/domain/database"
)

func SnapshotCourses(ctx context.Context, ops database.Operations, selected *int64) ([]Course, *Course, error) {
	result := []Course{}
	if selected == nil {
		rows, err := ops.Courses().ListCourses(ctx, false)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			result = append(result, course(row))
		}
		return result, nil, nil
	}
	row, err := ops.Courses().Course(ctx, *selected)
	if err != nil {
		return nil, nil, err
	}
	if row.Hidden != 0 {
		planned, err := ops.Courses().ExamPlanEnabled(ctx, *selected)
		if err != nil {
			return nil, nil, err
		}
		if !planned {
			return nil, nil, database.ErrNoRows
		}
	}
	item := course(row)
	return []Course{item}, &item, nil
}
