package rdbms

import (
	"context"
	"time"

	"tree-eclass/internal/domain/database"
)

type postgresNotifications struct{ db nativeDBTX }

func (n postgresNotifications) LockDeliveryConfig(ctx context.Context) error {
	for _, name := range []string{"webhook", "preferences"} {
		if _, err := advisoryLock(ctx, n.db, "settings:"+name, true, false); err != nil {
			return err
		}
	}
	return nil
}

func (n postgresNotifications) DeliveryConfig(ctx context.Context) (database.NotificationConfiguration, error) {
	var config database.NotificationConfiguration
	err := n.db.QueryRow(ctx, `SELECT coalesce((SELECT webhook_url FROM app.webhook_config WHERE id=1),''),`+
		`coalesce((SELECT notification_enabled=1 FROM app.preferences WHERE id=1),true),`+
		`coalesce((SELECT notification_on_error=1 FROM app.preferences WHERE id=1),true)`).
		Scan(&config.Target, &config.Enabled, &config.NotifyOnError)
	return config, err
}

func (n postgresNotifications) WebhookTarget(ctx context.Context) (string, error) {
	var target string
	err := n.db.QueryRow(ctx, `SELECT coalesce((SELECT webhook_url FROM app.webhook_config WHERE id=1),'')`).
		Scan(&target)
	return target, err
}

func (n postgresNotifications) EnqueueMessages(ctx context.Context, messages []database.NotificationMessage) error {
	for _, message := range messages {
		if _, err := n.db.Exec(ctx, `INSERT INTO app.notification_messages(id,event_key,position,target_hash,content)`+
			` VALUES($1,$2,$3,$4,$5) ON CONFLICT(event_key,position) DO NOTHING`,
			message.ID, message.EventKey, message.Position, message.TargetHash, message.Content); err != nil {
			return err
		}
	}
	return nil
}

func (n postgresNotifications) CancelObsoleteTargets(ctx context.Context, enabled bool, targetHash string) error {
	_, err := n.db.Exec(ctx, `UPDATE app.notification_messages SET status='canceled',`+
		`error='Webhook destination changed or notifications disabled' WHERE status='pending' AND `+
		`(NOT $1 OR target_hash<>$2)`, enabled, targetHash)
	return err
}

func (n postgresNotifications) DestinationPaused(ctx context.Context, targetHash string) (bool, error) {
	var paused bool
	err := n.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.notification_limits`+
		` WHERE target_hash=$1 AND next_at>clock_timestamp())`, targetHash).Scan(&paused)
	return paused, err
}

func (n postgresNotifications) ClaimMessage(ctx context.Context, targetHash string) (database.NotificationClaim,
	error) {
	var claim database.NotificationClaim
	err := n.db.QueryRow(ctx, `WITH next AS(SELECT id FROM app.notification_messages`+
		` WHERE status='pending' AND target_hash=$1 AND available_at<=clock_timestamp()`+
		` ORDER BY created_at,event_key,position FOR UPDATE SKIP LOCKED LIMIT 1)`+
		` UPDATE app.notification_messages m SET status='running',attempts=attempts+1 FROM next`+
		` WHERE m.id=next.id RETURNING m.id,m.content,m.attempts`, targetHash).
		Scan(&claim.ID, &claim.Content, &claim.Attempts)
	return claim, err
}

func (n postgresNotifications) AckMessage(ctx context.Context, id string) error {
	_, err := n.db.Exec(ctx, `UPDATE app.notification_messages SET status='sent',sent_at=clock_timestamp(),`+
		`error=NULL WHERE id=$1 AND status='running'`, id)
	return err
}

func (n postgresNotifications) FailMessage(ctx context.Context, failure database.NotificationFailure) error {
	quota := 0
	if failure.Quota {
		quota = 1
	}
	_, err := n.db.Exec(ctx, `UPDATE app.notification_messages SET status=$2,error=$3,`+
		`available_at=clock_timestamp()+$4*interval '1 second',attempts=attempts-$5`+
		` WHERE id=$1 AND status='running'`,
		failure.ID, failure.Status, failure.Error, failure.DelaySeconds, quota)
	return err
}

func (n postgresNotifications) PauseDestination(ctx context.Context, targetHash string, delay time.Duration) error {
	_, err := n.db.Exec(ctx, `INSERT INTO app.notification_limits(target_hash,next_at)`+
		` VALUES($1,clock_timestamp()+$2*interval '1 second')`+
		` ON CONFLICT(target_hash) DO UPDATE SET next_at=greatest(app.notification_limits.next_at,excluded.next_at)`,
		targetHash, delay.Seconds())
	return err
}

func (n postgresNotifications) StatusCounts(ctx context.Context) ([]database.NotificationStatusCount, error) {
	rows, err := n.db.Query(ctx, `SELECT status,count(*) FROM app.notification_messages GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.NotificationStatusCount{}
	for rows.Next() {
		var count database.NotificationStatusCount
		if err := rows.Scan(&count.Status, &count.Count); err != nil {
			return nil, err
		}
		out = append(out, count)
	}
	return out, rows.Err()
}

func (n postgresNotifications) LastFailure(ctx context.Context) (string, error) {
	var message string
	err := n.db.QueryRow(ctx, `SELECT coalesce(error,'') FROM app.notification_messages`+
		` WHERE status='failed' ORDER BY created_at DESC LIMIT 1`).Scan(&message)
	return message, err
}

func (n postgresNotifications) RetryFailed(ctx context.Context, targetHash string) (int64, error) {
	result, err := n.db.Exec(ctx, `UPDATE app.notification_messages SET status='pending',attempts=0,`+
		`available_at=clock_timestamp(),error=NULL WHERE status='failed' AND target_hash=$1`, targetHash)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (n postgresNotifications) RecoverInterrupted(ctx context.Context) error {
	_, err := n.db.Exec(ctx, `UPDATE app.notification_messages SET`+
		` status=CASE WHEN attempts>=5 THEN 'failed' ELSE 'pending' END,`+
		`error='Delivery interrupted before acknowledgement' WHERE status='running'`)
	return err
}
