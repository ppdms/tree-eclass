package knowledge

import (
	"context"
	"time"

	"tree-eclass/internal/domain/identity"
)

func (s Reader) Recent(ctx context.Context, requested []int64, since string, limit int) (map[string]any, error) {
	ids, err := s.Visible(ctx, requested)
	if err != nil {
		return nil, err
	}
	stamp := time.Unix(0, 0)
	if since != "" {
		stamp, err = time.Parse(time.RFC3339, since)
		if err != nil {
			return nil, err
		}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	items, err := tx.Documents().RecentChanges(ctx, ids, stamp.UTC().Format(time.RFC3339Nano), min(200, max(1, limit)))
	if err != nil {
		return nil, err
	}
	rows := []map[string]any{}
	for _, item := range items {
		row := map[string]any{
			"course_id":         item.CourseID,
			"course_name":       item.CourseName,
			"timestamp":         item.Timestamp,
			"change_no":         item.ChangeNo,
			"change_type":       item.ChangeType,
			"file_path":         item.FilePath,
			"display_name":      item.DisplayName,
			"redirect_url":      item.RedirectURL,
			"diff_webdav_path":  item.DiffWebdav,
			"untrusted_content": true,
		}
		if item.DisplayName != nil {
			row["display_name"] = identity.Decode(*item.DisplayName)
		}
		for _, key := range []string{"file_path", "redirect_url", "diff_webdav_path"} {
			if text, ok := row[key].(*string); ok && text != nil {
				row[key] = identity.Decode(*text)
			}
		}
		rows = append(rows, row)
	}
	return map[string]any{"changes": rows, "untrusted_content_notice": UntrustedNotice}, tx.Commit(ctx)
}
