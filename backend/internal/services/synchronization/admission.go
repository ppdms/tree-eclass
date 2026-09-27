package synchronization

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/domain/settings"
)

type Request struct {
	CourseID *int64 `json:"course_id"`
}

func (s Service) Enqueue(ctx context.Context, id *int64) (string, error) {
	return s.enqueue(ctx, id, false)
}

// EnqueueManual can retry the same failed pending check immediately. Scheduled
// admission preserves backoff, and running checks always retain their inputs.
func (s Service) EnqueueManual(ctx context.Context, id *int64) (string, error) {
	return s.enqueue(ctx, id, true)
}

func (s Service) enqueue(ctx context.Context, id *int64, retry bool) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	q := queries.New(tx)
	if err = q.QueueLock(ctx, "eclass-check"); err != nil {
		return "", err
	}
	if id != nil {
		var hidden int64
		if err = tx.QueryRow(ctx, `SELECT hidden FROM app.courses WHERE id=$1 FOR SHARE`, *id).Scan(&hidden); err != nil {
			return "", err
		}
		if hidden != 0 {
			return "", pgx.ErrNoRows
		}
	}
	command, err := admitCheck(ctx, tx, id, retry)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO app.check_status(id,is_checking,started_at,current_course_id) VALUES(1,1,$1,$2) ON CONFLICT(id) DO UPDATE SET is_checking=1,started_at=$1,current_course_id=$2`,
		time.Now().UTC().Format(time.RFC3339),
		id,
	)
	if err != nil {
		return "", err
	}
	return command, tx.Commit(ctx)
}

func (s Service) Schedule(ctx context.Context, now time.Time) error {
	service := settings.Service{Pool: s.Pool}
	credentials, err := service.Credentials(ctx)
	if err != nil {
		return err
	}
	if credentials == nil || credentials.Username == "" || credentials.Password == "" {
		return nil
	}
	prefs, err := service.Preferences(ctx)
	if err != nil {
		return err
	}
	status, err := service.Check(ctx)
	if err != nil {
		return err
	}
	if status.LastCheckAt != nil {
		last, err := time.Parse(time.RFC3339, *status.LastCheckAt)
		if err != nil {
			last, err = time.Parse("2006-01-02 15:04:05", *status.LastCheckAt)
		}
		if err == nil && now.Sub(last) < time.Duration(prefs.CheckInterval)*time.Minute {
			return nil
		}
	}
	_, err = s.Enqueue(ctx, nil)
	if errors.Is(err, ErrBusy) {
		return nil
	}
	return err
}
