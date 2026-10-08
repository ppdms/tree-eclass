package rdbms

import (
	"context"
	"errors"
	"time"

	"tree-eclass/internal/domain/database"
)

type sqliteNotifications struct{ db nativeDBTX }

func (n sqliteNotifications) LockDeliveryConfig(ctx context.Context) error {
	for _, name := range []string{"webhook", "preferences"} {
		if _, err := advisoryLock(ctx, n.db, "settings:"+name, true, false); err != nil {
			return err
		}
	}
	return nil
}

func (n sqliteNotifications) DeliveryConfig(ctx context.Context) (database.NotificationConfiguration, error) {
	var config database.NotificationConfiguration
	var enabled, notify int64
	var target *string
	var enabledNull, notifyNull *int64
	err := n.db.QueryRow(ctx, `SELECT (SELECT webhook_url FROM webhook_config WHERE id=1),`+
		`(SELECT notification_enabled FROM preferences WHERE id=1),`+
		`(SELECT notification_on_error FROM preferences WHERE id=1)`).
		Scan(&target, &enabledNull, &notifyNull)
	if err != nil {
		return database.NotificationConfiguration{}, err
	}
	if target != nil {
		config.Target = *target
	}
	enabled, notify = 1, 1
	if enabledNull != nil {
		enabled = *enabledNull
	}
	if notifyNull != nil {
		notify = *notifyNull
	}
	config.Enabled = enabled == 1
	config.NotifyOnError = notify == 1
	return config, nil
}

func (n sqliteNotifications) WebhookTarget(ctx context.Context) (string, error) {
	var target *string
	err := n.db.QueryRow(ctx, `SELECT webhook_url FROM webhook_config WHERE id=1`).Scan(&target)
	if err != nil {
		if errors.Is(err, database.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if target == nil {
		return "", nil
	}
	return *target, nil
}

func (n sqliteNotifications) EnqueueMessages(ctx context.Context, messages []database.NotificationMessage) error {
	for _, message := range messages {
		if _, err := n.db.Exec(ctx, `INSERT INTO notification_messages(id,event_key,position,target_hash,content)`+
			` VALUES(?,?,?,?,?) ON CONFLICT(event_key,position) DO NOTHING`,
			message.ID, message.EventKey, message.Position, message.TargetHash, message.Content); err != nil {
			return err
		}
	}
	return nil
}

func (n sqliteNotifications) CancelObsoleteTargets(ctx context.Context, enabled bool, targetHash string) error {
	active := 0
	if enabled {
		active = 1
	}
	_, err := n.db.Exec(ctx, `UPDATE notification_messages SET status='canceled',`+
		`error='Webhook destination changed or notifications disabled'`+
		` WHERE status='pending' AND (NOT ? OR target_hash<>?)`, active, targetHash)
	return err
}

func (n sqliteNotifications) DestinationPaused(ctx context.Context, targetHash string) (bool, error) {
	var paused bool
	err := n.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM notification_limits`+
		` WHERE target_hash=? AND datetime(next_at)>datetime('now'))`, targetHash).Scan(&paused)
	return paused, err
}

func (n sqliteNotifications) ClaimMessage(ctx context.Context, targetHash string) (database.NotificationClaim, error) {
	// The admitted writer owns the claim until commit/rollback, so the
	// ordered update is atomic without row locks.
	var claim database.NotificationClaim
	err := n.db.QueryRow(ctx, `UPDATE notification_messages SET status='running',attempts=attempts+1`+
		` WHERE id=(SELECT id FROM notification_messages`+
		` WHERE status='pending' AND target_hash=? AND datetime(available_at)<=datetime('now')`+
		` ORDER BY created_at,event_key,position LIMIT 1)`+
		` RETURNING id,content,attempts`, targetHash).
		Scan(&claim.ID, &claim.Content, &claim.Attempts)
	return claim, err
}

func (n sqliteNotifications) AckMessage(ctx context.Context, id string) error {
	_, err := n.db.Exec(ctx, `UPDATE notification_messages SET status='sent',`+
		`sent_at=strftime('%Y-%m-%d %H:%M:%S','now'),error=NULL WHERE id=? AND status='running'`, id)
	return err
}

func (n sqliteNotifications) FailMessage(ctx context.Context, failure database.NotificationFailure) error {
	quota := 0
	if failure.Quota {
		quota = 1
	}
	_, err := n.db.Exec(ctx, `UPDATE notification_messages SET status=?,error=?,`+
		`available_at=datetime('now','+'||?||' seconds'),attempts=attempts-?`+
		` WHERE id=? AND status='running'`,
		failure.Status, failure.Error, int64(failure.DelaySeconds), quota, failure.ID)
	return err
}

func (n sqliteNotifications) PauseDestination(ctx context.Context, targetHash string, delay time.Duration) error {
	seconds := int64(delay / time.Second)
	_, err := n.db.Exec(ctx, `INSERT INTO notification_limits(target_hash,next_at)`+
		` VALUES(?,datetime('now','+'||?||' seconds'))`+
		` ON CONFLICT(target_hash) DO UPDATE SET next_at=max(notification_limits.next_at,excluded.next_at)`,
		targetHash, seconds)
	return err
}

func (n sqliteNotifications) StatusCounts(ctx context.Context) ([]database.NotificationStatusCount, error) {
	rows, err := n.db.Query(ctx, `SELECT status,count(*) FROM notification_messages GROUP BY status`)
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

func (n sqliteNotifications) LastFailure(ctx context.Context) (string, error) {
	var message *string
	err := n.db.QueryRow(ctx, `SELECT error FROM notification_messages`+
		` WHERE status='failed' ORDER BY created_at DESC LIMIT 1`).Scan(&message)
	if err != nil {
		return "", err
	}
	if message == nil {
		return "", nil
	}
	return *message, nil
}

func (n sqliteNotifications) RetryFailed(ctx context.Context, targetHash string) (int64, error) {
	result, err := n.db.Exec(ctx, `UPDATE notification_messages SET status='pending',attempts=0,`+
		`available_at=strftime('%Y-%m-%d %H:%M:%S','now'),error=NULL`+
		` WHERE status='failed' AND target_hash=?`, targetHash)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (n sqliteNotifications) RecoverInterrupted(ctx context.Context) error {
	_, err := n.db.Exec(ctx, `UPDATE notification_messages SET`+
		` status=CASE WHEN attempts>=5 THEN 'failed' ELSE 'pending' END,`+
		`error='Delivery interrupted before acknowledgement' WHERE status='running'`)
	return err
}
