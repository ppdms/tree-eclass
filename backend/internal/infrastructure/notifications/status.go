package notifications

import (
	"context"
	"errors"

	"tree-eclass/internal/infrastructure/rdbms"
)

type Status struct {
	Counts    map[string]int64 `json:"counts"`
	LastError *string          `json:"last_error"`
}

func (s Service) Status(ctx context.Context) (Status, error) {
	status := Status{Counts: map[string]int64{}}
	rows, err := s.Pool.Query(ctx, `SELECT status,count(*) FROM app.notification_messages GROUP BY status`)
	if err != nil {
		return status, err
	}
	for rows.Next() {
		var name string
		var count int64
		if err = rows.Scan(&name, &count); err != nil {
			rows.Close()
			return status, err
		}
		status.Counts[name] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return status, err
	}
	err = s.Pool.QueryRow(ctx, `SELECT error FROM app.notification_messages WHERE status='failed' ORDER BY created_at DESC LIMIT 1`).
		Scan(&status.LastError)
	if errors.Is(err, rdbms.ErrNoRows) {
		err = nil
	}
	return status, err
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
	result, err := tx.Exec(
		ctx,
		`UPDATE app.notification_messages SET status='pending',attempts=0,available_at=clock_timestamp(),error=NULL WHERE status='failed' AND target_hash=$1`,
		targetHash(cfg.Target),
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), tx.Commit(ctx)
}
