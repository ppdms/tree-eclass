package synchronization

import (
	"context"

	"github.com/jackc/pgx/v5"
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
	var exists bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.courses WHERE id=$1 AND hidden=0)`, id).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, pgx.ErrNoRows
	}
	if file != nil {
		encoded := identity.Encode(*file)
		file = &encoded
	}
	if folder != nil {
		encoded := identity.Encode(*folder)
		folder = &encoded
	}
	rows, err := s.Pool.Query(
		ctx,
		`SELECT id,course_id,file_path,version_webdav_path,change_type,timestamp,display_name,redirect_url,diff_webdav_path FROM app.file_versions
WHERE course_id=$1 AND change_type=$2 AND ($3::text IS NULL OR file_path=$3) AND ($4::text IS NULL OR $4='' OR file_path=$4 OR starts_with(file_path,rtrim($4,'/')||'/')) ORDER BY timestamp DESC,id DESC`,
		id,
		kind,
		file,
		folder,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := []Version{}
	for rows.Next() {
		var v Version
		if err = rows.Scan(
			&v.ID,
			&v.CourseID,
			&v.Path,
			&v.StoragePath,
			&v.Type,
			&v.Timestamp,
			&v.Name,
			&v.Redirect,
			&v.Diff,
		); err != nil {
			return nil, err
		}
		v.Path = identity.Decode(v.Path)
		decodeText(v.Name, v.StoragePath, v.Diff)
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

func (s Service) History(ctx context.Context, id int64, number string) (ChangeRecord, []HistoryItem, error) {
	var record ChangeRecord
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return record, nil, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT r.id,r.course_id,c.name,r.change_no,r.timestamp,r.message,r.changes_count FROM app.change_records r JOIN app.courses c ON c.id=r.course_id WHERE r.course_id=$1 AND r.change_no=$2 AND c.hidden=0`, id, number).
		Scan(
			&record.ID,
			&record.CourseID,
			&record.CourseName,
			&record.Number,
			&record.Timestamp,
			&record.Message,
			&record.Count,
		)
	if err != nil {
		return record, nil, err
	}
	record.CourseName = identity.Decode(record.CourseName)
	decodeText(record.Message)
	rows, err := tx.Query(
		ctx,
		`SELECT change_type,file_path,display_name,redirect_url,diff_webdav_path FROM app.change_record_items WHERE change_record_id=$1 ORDER BY id`,
		record.ID,
	)
	if err != nil {
		return record, nil, err
	}
	defer rows.Close()
	items := []HistoryItem{}
	for rows.Next() {
		var item HistoryItem
		if err = rows.Scan(&item.Type, &item.Path, &item.Name, &item.Redirect, &item.Diff); err != nil {
			return record, nil, err
		}
		item.Path = identity.Decode(item.Path)
		decodeText(item.Name, item.Diff)
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return record, nil, err
	}
	rows.Close()
	return record, items, tx.Commit(ctx)
}

func decodeText(values ...*string) {
	for _, value := range values {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
}
