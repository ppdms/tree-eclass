package settings

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

type CheckStatus struct {
	IsChecking   bool    `json:"is_checking"`
	StartedAt    *string `json:"started_at"`
	CourseID     *int64  `json:"current_course_id"`
	CourseName   *string `json:"course_name"`
	LastCheckAt  *string `json:"last_check_at"`
	LastResult   *string `json:"last_check_result"`
	LastError    *string `json:"last_error"`
	FilesAdded   *int64  `json:"last_files_added"`
	FilesChanged *int64  `json:"last_files_changed"`
}
type SyncStatus struct {
	LastRunAt   *string `json:"last_run_at"`
	LastResult  *string `json:"last_result"`
	LastError   *string `json:"last_error"`
	LastMessage *string `json:"last_message"`
}

func (s Service) Check(ctx context.Context) (CheckStatus, error) {
	var status CheckStatus
	err := s.Pool.QueryRow(ctx, `SELECT cs.is_checking=1,cs.started_at,c.id,c.name,cs.last_check_at,cs.last_check_result,cs.last_error,cs.last_files_added,cs.last_files_changed FROM app.check_status cs LEFT JOIN app.courses c ON c.id=cs.current_course_id AND c.hidden=0 WHERE cs.id=1`).
		Scan(
			&status.IsChecking,
			&status.StartedAt,
			&status.CourseID,
			&status.CourseName,
			&status.LastCheckAt,
			&status.LastResult,
			&status.LastError,
			&status.FilesAdded,
			&status.FilesChanged,
		)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	for _, text := range []*string{status.CourseName, status.LastError} {
		if text != nil {
			*text = identity.Decode(*text)
		}
	}
	return status, err
}
func (s Service) Sync(ctx context.Context) (map[string]SyncStatus, error) {
	rows, err := s.Pool.Query(ctx, `SELECT job,last_run_at,last_result,last_error,last_message FROM app.sync_status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]SyncStatus{}
	for rows.Next() {
		var name string
		var status SyncStatus
		if err = rows.Scan(&name, &status.LastRunAt, &status.LastResult, &status.LastError, &status.LastMessage); err != nil {
			return nil, err
		}
		for _, text := range []*string{status.LastError, status.LastMessage} {
			if text != nil {
				*text = identity.Decode(*text)
			}
		}
		result[name] = status
	}
	return result, rows.Err()
}
