// Package commands defines the transactional work-queue contract used by
// domain services. It owns no queue implementation.
package commands

import (
	"context"
	"crypto/rand"
	"encoding/json"

	"tree-eclass/internal/domain/database"
)

// Enqueuer persists queue actions on the caller's transaction so enqueues
// commit or roll back together with the surrounding work.
type Enqueuer interface {
	EnqueueTx(ctx context.Context, tx database.Tx, queue, action string, payload any, coalesce bool) (string, error)
}

// EnqueueTx lets the domain publish data and its follow-up work atomically.
// Coalescing reuses only an unclaimed command. A running job already has its
// input snapshot, so a later mutation must leave another pending command.
func EnqueueTx(ctx context.Context, tx database.Tx, queue, action string, payload any, coalesce bool) (string, error) {
	if coalesce {
		if err := tx.Jobs().QueueLock(ctx, queue); err != nil {
			return "", err
		}
		id, err := tx.Jobs().PendingCommand(ctx, queue, action)
		if err == nil {
			return id, nil
		}
		if !database.IsNoRows(err) {
			return "", err
		}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	id := rand.Text()
	err = tx.Jobs().EnqueueCommand(ctx, database.EnqueueCommandParams{ID: id, Queue: queue, Action: action, Payload: data})
	return id, err
}
