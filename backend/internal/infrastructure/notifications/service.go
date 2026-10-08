package notifications

import (
	"context"
	"errors"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Service struct {
	Pool   database.Store
	Sender Sender
}
type delivery struct {
	ID, Content, Target string
	Attempts            int
}

func (s Service) Recover(ctx context.Context) error {
	return s.Pool.Notifications().RecoverInterrupted(ctx)
}
func (s Service) claim(ctx context.Context) (delivery, error) {
	var d delivery
	// Fast path: no webhook configured means no claimable work. Skip the
	// serializing write transaction entirely so an idle notifier never
	// contends with crawls and projections on a small sqlite pool.
	target, err := s.Pool.Notifications().WebhookTarget(ctx)
	if err != nil {
		return d, err
	}
	if target == "" {
		return d, nil
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback(ctx)
	if err = lockConfig(ctx, tx); err != nil {
		return d, err
	}
	cfg, err := readConfig(ctx, tx)
	if err != nil {
		return d, err
	}
	obsolete := cfg.Enabled && cfg.Target != ""
	if err = tx.Notifications().CancelObsoleteTargets(ctx, obsolete, targetHash(cfg.Target)); err != nil {
		return d, err
	}
	if !cfg.Enabled || cfg.Target == "" {
		return d, tx.Commit(ctx)
	}
	paused, err := tx.Notifications().DestinationPaused(ctx, targetHash(cfg.Target))
	if err != nil {
		return d, err
	}
	if paused {
		return d, tx.Commit(ctx)
	}
	claim, err := tx.Notifications().ClaimMessage(ctx, targetHash(cfg.Target))
	if err != nil && !errors.Is(err, database.ErrNoRows) {
		return d, err
	}
	if err == nil {
		d.ID, d.Content, d.Attempts = claim.ID, claim.Content, claim.Attempts
	}
	d.Target, d.Content = cfg.Target, identity.Decode(d.Content)
	return d, tx.Commit(ctx)
}
func (s Service) Tick(ctx context.Context) error {
	d, err := s.claim(ctx)
	if err != nil || d.ID == "" {
		return err
	}
	retry, sendErr := s.Sender.Send(ctx, d.Target, d.Content)
	// Shutdown leaves the committed claim for recovery, rather than writing with
	// a canceled context and pretending that acknowledgement was persisted.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if sendErr == nil {
		if retry > 0 {
			if err = s.pause(ctx, d.Target, retry); err != nil {
				return err
			}
		}
		return s.Pool.Notifications().AckMessage(ctx, d.ID)
	}
	status := "pending"
	var failure Failure
	quota := errors.As(sendErr, &failure) && failure.Status == 429
	if quota {
		retry = max(time.Second, retry)
		if err = s.pause(ctx, d.Target, retry); err != nil {
			return err
		}
	}
	if !quota && (d.Attempts >= 5 || (failure.Status >= 300 && failure.Status < 500)) {
		status = "failed"
	}
	if retry == 0 {
		delays := []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, time.Hour, 6 * time.Hour}
		retry = delays[min(max(d.Attempts-1, 0), 4)]
	}
	message := "Webhook delivery failed before acknowledgement; retry may repeat a delivered message"
	if failure.Status != 0 {
		message = failure.Error()
	}
	return s.Pool.Notifications().FailMessage(
		ctx,
		database.NotificationFailure{
			ID:           d.ID,
			Status:       status,
			Error:        message,
			DelaySeconds: retry.Seconds(),
			Quota:        quota,
		},
	)
}

func (s Service) pause(ctx context.Context, target string, delay time.Duration) error {
	return s.Pool.Notifications().PauseDestination(ctx, targetHash(target), delay)
}
