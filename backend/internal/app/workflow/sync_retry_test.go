package workflow

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/services/synchronization"
)

func syncRetryChecks(t *testing.T, pool *pgxpool.Pool, service synchronization.Service) {
	t.Helper()
	ctx := t.Context()
	var original string
	if err := pool.QueryRow(ctx, `UPDATE app.control_commands SET attempts=4,error='login failed',available_at=clock_timestamp()+interval '2 hours'
WHERE queue='sync' AND status='pending' RETURNING id`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(ctx, new(int64(101))); !errors.Is(err, synchronization.ErrBusy) {
		t.Fatal("scheduled admission bypassed backoff", err)
	}
	if _, err := service.EnqueueManual(ctx, nil); !errors.Is(err, synchronization.ErrBusy) {
		t.Fatal("retry changed the pending course scope", err)
	}
	id, err := service.EnqueueManual(ctx, new(int64(101)))
	if err != nil || id != original {
		t.Fatal("manual retry did not reuse the failed pending command", id, err)
	}
	status, err := (settings.Service{Pool: pool}).Check(ctx)
	if err != nil || !status.IsChecking {
		t.Fatal("manual retry did not publish checking status", status, err)
	}
	if _, err := service.EnqueueManual(ctx, new(int64(101))); !errors.Is(err, synchronization.ErrBusy) {
		t.Fatal("fresh pending retry was admitted twice", err)
	}
	claimed, err := (jobs.Queue{Pool: pool}).Claim(ctx, "sync")
	if err != nil || len(claimed) != 1 || claimed[0].ID != original || claimed[0].Attempts != 1 ||
		claimed[0].Error != nil {
		t.Fatal("retry was not immediately claimable with a fresh budget", claimed, err)
	}
	if _, err := service.EnqueueManual(ctx, new(int64(101))); !errors.Is(err, synchronization.ErrBusy) {
		t.Fatal("running command was replaced by manual retry", err)
	}
}
