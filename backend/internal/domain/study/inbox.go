package study

import (
	"context"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type InboxItem struct {
	Path       string  `json:"file_path"`
	Name       string  `json:"file_name"`
	URL        string  `json:"url"`
	Redirect   *string `json:"redirect_url"`
	Updated    *string `json:"last_updated"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	Prefix     string  `json:"storage_prefix"`
	Level      int64   `json:"level"`
	StudyLevel int64   `json:"study_level"`
	Priority   float64 `json:"priority"`
}

func readInbox(ctx context.Context, ops database.Operations, selected *int64, now time.Time) ([]InboxItem, error) {
	rows, err := ops.Study().ListInbox(ctx, selected, now)
	if err != nil {
		return []InboxItem{}, err
	}
	result := make([]InboxItem, 0, len(rows))
	for _, row := range rows {
		item := InboxItem{
			Path:       identity.Decode(row.Path),
			Name:       identity.Decode(row.Name),
			URL:        identity.Decode(row.URL),
			Updated:    row.Updated,
			CourseID:   row.CourseID,
			CourseName: identity.Decode(row.CourseName),
			Prefix:     identity.Decode(row.Prefix),
			Level:      row.Level,
			StudyLevel: row.Level,
			Priority:   row.Priority,
		}
		if row.Redirect != nil {
			redirect := identity.Decode(*row.Redirect)
			item.Redirect = &redirect
		}
		result = append(result, item)
	}
	return result, nil
}
