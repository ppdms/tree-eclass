package synchronization

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Version struct {
	ID          int64   `json:"id"`
	CourseID    int64   `json:"course_id"`
	Path        string  `json:"file_path"`
	StoragePath *string `json:"version_webdav_path"`
	Type        string  `json:"change_type"`
	Timestamp   string  `json:"timestamp"`
	Name        *string `json:"display_name"`
	Redirect    *string `json:"redirect_url"`
	Diff        *string `json:"diff_webdav_path"`
}
type ChangeRecord struct {
	ID         int64   `json:"id"`
	CourseID   int64   `json:"course_id"`
	CourseName string  `json:"course_name"`
	Number     string  `json:"change_no"`
	Timestamp  string  `json:"timestamp"`
	Message    *string `json:"message"`
	Count      int64   `json:"changes_count"`
}
type HistoryItem struct {
	Type     string  `json:"change_type"`
	Path     string  `json:"file_path"`
	Name     *string `json:"display_name"`
	Redirect *string `json:"redirect_url"`
	Diff     *string `json:"diff_webdav_path"`
}

func (s Service) Versions(ctx context.Context, id int64, kind string, file, folder *string) ([]Version, error) {
	visible, err := s.Pool.Sync().CourseVisible(ctx, id)
	if err != nil {
		return nil, err
	}
	if !visible {
		return nil, database.ErrNoRows
	}
	if file != nil {
		encoded := identity.Encode(*file)
		file = &encoded
	}
	if folder != nil {
		encoded := identity.Encode(*folder)
		folder = &encoded
	}
	rows, err := s.Pool.Sync().ListVersions(ctx, id, kind, file, folder)
	if err != nil {
		return nil, err
	}
	versions := []Version{}
	for _, row := range rows {
		v := Version{
			ID:          row.ID,
			CourseID:    row.CourseID,
			Path:        identity.Decode(row.Path),
			StoragePath: row.StoragePath,
			Type:        row.Type,
			Timestamp:   row.Timestamp,
			Name:        row.Name,
			Redirect:    row.Redirect,
			Diff:        row.Diff,
		}
		decodeText(v.Name, v.StoragePath, v.Diff)
		versions = append(versions, v)
	}
	return versions, nil
}

func (s Service) History(ctx context.Context, id int64, number string) (ChangeRecord, []HistoryItem, error) {
	var record ChangeRecord
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return record, nil, err
	}
	defer tx.Rollback(ctx)
	stored, course, err := tx.Sync().ChangeRecord(ctx, id, number)
	if err != nil {
		return record, nil, err
	}
	record = ChangeRecord{
		ID:         stored.ID,
		CourseID:   stored.CourseID,
		CourseName: identity.Decode(course),
		Number:     stored.Number,
		Timestamp:  stored.Timestamp,
		Message:    stored.Message,
		Count:      stored.Count,
	}
	decodeText(record.Message)
	rows, err := tx.Sync().ChangeRecordItems(ctx, stored.ID)
	if err != nil {
		return record, nil, err
	}
	items := []HistoryItem{}
	for _, row := range rows {
		item := HistoryItem{
			Type:     row.Type,
			Path:     identity.Decode(row.Path),
			Name:     row.Name,
			Redirect: row.Redirect,
			Diff:     row.Diff,
		}
		decodeText(item.Name, item.Diff)
		items = append(items, item)
	}
	return record, items, tx.Commit(ctx)
}

func decodeText(values ...*string) {
	for _, value := range values {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
}
