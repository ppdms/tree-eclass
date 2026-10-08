package settings

import (
	"context"

	"tree-eclass/internal/domain/database"
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
	stored, err := s.Pool.Settings().LoadCheckStatus(ctx)
	if database.IsNoRows(err) {
		return CheckStatus{}, nil
	}
	if err != nil {
		return CheckStatus{}, err
	}
	status := CheckStatus{
		IsChecking:   stored.IsChecking,
		StartedAt:    stored.StartedAt,
		CourseID:     stored.CourseID,
		CourseName:   stored.CourseName,
		LastCheckAt:  stored.LastCheckAt,
		LastResult:   stored.LastResult,
		LastError:    stored.LastError,
		FilesAdded:   stored.FilesAdded,
		FilesChanged: stored.FilesChanged,
	}
	for _, text := range []*string{status.CourseName, status.LastError} {
		if text != nil {
			*text = identity.Decode(*text)
		}
	}
	return status, nil
}
func (s Service) Sync(ctx context.Context) (map[string]SyncStatus, error) {
	rows, err := s.Pool.Settings().ListSyncStatus(ctx)
	if err != nil {
		return nil, err
	}
	result := map[string]SyncStatus{}
	for _, row := range rows {
		status := SyncStatus{
			LastRunAt:   row.LastRunAt,
			LastResult:  row.LastResult,
			LastError:   row.LastError,
			LastMessage: row.LastMessage,
		}
		for _, text := range []*string{status.LastError, status.LastMessage} {
			if text != nil {
				*text = identity.Decode(*text)
			}
		}
		result[row.Job] = status
	}
	return result, nil
}
