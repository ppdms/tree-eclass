package notifications

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
)

type Status struct {
	Counts    map[string]int64 `json:"counts"`
	LastError *string          `json:"last_error"`
}

func (s Service) Status(ctx context.Context) (Status, error) {
	status := Status{Counts: map[string]int64{}}
	counts, err := s.Pool.Notifications().StatusCounts(ctx)
	if err != nil {
		return status, err
	}
	for _, count := range counts {
		status.Counts[count.Status] = count.Count
	}
	message, err := s.Pool.Notifications().LastFailure(ctx)
	if errors.Is(err, database.ErrNoRows) {
		return status, nil
	}
	if err != nil {
		return status, err
	}
	status.LastError = &message
	return status, nil
}
func (s Service) Retry(ctx context.Context) (int64, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err = lockConfig(ctx, tx); err != nil {
		return 0, err
	}
	cfg, err := readConfig(ctx, tx)
	if err != nil {
		return 0, err
	}
	if !cfg.Enabled || cfg.Target == "" {
		return 0, nil
	}
	count, err := tx.Notifications().RetryFailed(ctx, targetHash(cfg.Target))
	if err != nil {
		return 0, err
	}
	return count, tx.Commit(ctx)
}
