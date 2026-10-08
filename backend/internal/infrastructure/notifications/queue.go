package notifications

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type configuration struct {
	Target          string
	Enabled, Errors bool
}

func readConfig(ctx context.Context, tx rdbms.Tx) (configuration, error) {
	var c configuration
	err := tx.QueryRow(ctx, `SELECT coalesce((SELECT webhook_url FROM app.webhook_config WHERE id=1),''),coalesce((SELECT notification_enabled=1 FROM app.preferences WHERE id=1),true),coalesce((SELECT notification_on_error=1 FROM app.preferences WHERE id=1),true)`).
		Scan(&c.Target, &c.Enabled, &c.Errors)
	c.Target = identity.Decode(c.Target)
	return c, err
}
func targetHash(target string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(target))) }

// EnqueueTx binds delivery to the webhook selected when the source observation
// commits. A changed destination must never receive queued private course data.
func EnqueueTx(ctx context.Context, tx rdbms.Tx, event Event) error {
	if event.Key == "" || len(event.Key) > 512 {
		return errors.New("notification event identity is required")
	}
	cfg, err := readConfig(ctx, tx)
	if err != nil {
		return err
	}
	if !cfg.Enabled || cfg.Target == "" || (event.Error && !cfg.Errors) {
		return nil
	}
	if err = ValidURL(cfg.Target); err != nil {
		// A preserved legacy setting must not prevent source publication.
		return nil
	}
	messages, err := Batch(event.Header, event.Lines)
	if err != nil {
		return err
	}
	stmts := []string{}
	args := [][]any{}
	for index, content := range messages {
		stmts = append(
			stmts,
			`INSERT INTO app.notification_messages(id,event_key,position,target_hash,content) VALUES($1,$2,$3,$4,$5) ON CONFLICT(event_key,position) DO NOTHING`,
		)
		args = append(
			args,
			[]any{
				identity.Stable("notify", event.Key, fmt.Sprint(index)),
				event.Key,
				index,
				targetHash(cfg.Target),
				identity.Encode(content),
			},
		)
	}
	return rdbms.Batch(ctx, tx, stmts, args)
}

func lockConfig(ctx context.Context, tx rdbms.Tx) error {
	for _, name := range []string{"webhook", "preferences"} {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('settings:'||$1,0))`, name); err != nil {
			return err
		}
	}
	return nil
}
