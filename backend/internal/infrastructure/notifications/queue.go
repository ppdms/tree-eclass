package notifications

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type configuration struct {
	Target          string
	Enabled, Errors bool
}

func readConfig(ctx context.Context, ops database.Operations) (configuration, error) {
	config, err := ops.Notifications().DeliveryConfig(ctx)
	if err != nil {
		return configuration{}, err
	}
	return configuration{
		Target:  identity.Decode(config.Target),
		Enabled: config.Enabled,
		Errors:  config.NotifyOnError,
	}, nil
}
func targetHash(target string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(target))) }

// EnqueueTx binds delivery to the webhook selected when the source observation
// commits. A changed destination must never receive queued private course data.
func EnqueueTx(ctx context.Context, tx database.Tx, event Event) error {
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
	out := make([]database.NotificationMessage, 0, len(messages))
	for index, content := range messages {
		out = append(
			out,
			database.NotificationMessage{
				ID:         identity.Stable("notify", event.Key, fmt.Sprint(index)),
				EventKey:   event.Key,
				Position:   index,
				TargetHash: targetHash(cfg.Target),
				Content:    identity.Encode(content),
			},
		)
	}
	return tx.Notifications().EnqueueMessages(ctx, out)
}

func lockConfig(ctx context.Context, tx database.Tx) error {
	return tx.Notifications().LockDeliveryConfig(ctx)
}
