package notifications

import (
	"context"
	"errors"
	"time"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Service struct {
	Pool   rdbms.Pool
	Sender Sender
}
type delivery struct {
	ID, Content, Target string
	Attempts            int
}

func (s Service) Recover(ctx context.Context) error {
	_, err := s.Pool.Exec(
		ctx,
		`UPDATE app.notification_messages SET status=CASE WHEN attempts>=5 THEN 'failed' ELSE 'pending' END,error='Delivery interrupted before acknowledgement' WHERE status='running'`,
	)
	return err
}
func (s Service) claim(ctx context.Context) (delivery, error) {
	var d delivery
	// Fast path: no webhook configured means no claimable work. Skip the
	// serializing write transaction entirely so an idle notifier never
	// contends with crawls and projections on a small sqlite pool.
	var target string
	if err := s.Pool.QueryRow(ctx, `SELECT coalesce((SELECT webhook_url FROM app.webhook_config WHERE id=1),'')`).Scan(&target); err != nil {
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
	if _, err = tx.Exec(ctx, `UPDATE app.notification_messages SET status='canceled',error='Webhook destination changed or notifications disabled' WHERE status='pending' AND (NOT $1 OR target_hash<>$2)`, cfg.Enabled && cfg.Target != "", targetHash(cfg.Target)); err != nil {
		return d, err
	}
	if !cfg.Enabled || cfg.Target == "" {
		return d, tx.Commit(ctx)
	}
	var paused bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.notification_limits WHERE target_hash=$1 AND next_at>clock_timestamp())`, targetHash(cfg.Target)).Scan(&paused); err != nil {
		return d, err
	}
	if paused {
		return d, tx.Commit(ctx)
	}
	err = tx.QueryRow(ctx, `WITH next AS(SELECT id FROM app.notification_messages WHERE status='pending' AND target_hash=$1 AND available_at<=clock_timestamp() ORDER BY created_at,event_key,position FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE app.notification_messages m SET status='running',attempts=attempts+1 FROM next WHERE m.id=next.id RETURNING m.id,m.content,m.attempts`, targetHash(cfg.Target)).
		Scan(&d.ID, &d.Content, &d.Attempts)
	if err != nil && !errors.Is(err, rdbms.ErrNoRows) {
		return d, err
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
		_, err = s.Pool.Exec(
			ctx,
			`UPDATE app.notification_messages SET status='sent',sent_at=clock_timestamp(),error=NULL WHERE id=$1 AND status='running'`,
			d.ID,
		)
		return err
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
	_, err = s.Pool.Exec(
		ctx,
		`UPDATE app.notification_messages SET status=$2,error=$3,available_at=clock_timestamp()+$4*interval '1 second',attempts=attempts-$5 WHERE id=$1 AND status='running'`,
		d.ID,
		status,
		message,
		retry.Seconds(),
		flag(quota),
	)
	return err
}
func flag(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s Service) pause(ctx context.Context, target string, delay time.Duration) error {
	_, err := s.Pool.Exec(
		ctx,
		`INSERT INTO app.notification_limits(target_hash,next_at) VALUES($1,clock_timestamp()+$2*interval '1 second') ON CONFLICT(target_hash) DO UPDATE SET next_at=greatest(app.notification_limits.next_at,excluded.next_at)`,
		targetHash(target),
		delay.Seconds(),
	)
	return err
}
