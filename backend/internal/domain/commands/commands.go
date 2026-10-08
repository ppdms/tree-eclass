// Package commands defines the transactional work-queue contract used by
// domain services. It owns no queue implementation.
package commands

import (
	"context"
	"crypto/rand"
	"encoding/json"

	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
)

// Enqueuer persists queue actions on the caller's transaction so enqueues
// commit or roll back together with the surrounding work.
type Enqueuer interface {
	EnqueueTx(ctx context.Context, tx rdbms.Tx, queue, action string, payload any, coalesce bool) (string, error)
}

// EnqueueTx lets the domain publish data and its follow-up work atomically.
// Coalescing reuses only an unclaimed command. A running job already has its
// input snapshot, so a later mutation must leave another pending command.
func EnqueueTx(ctx context.Context, tx rdbms.Tx, queue, action string, payload any, coalesce bool) (string, error) {
	q := queries.ForTx(tx)
	if coalesce {
		if err := q.QueueLock(ctx, queue); err != nil {
			return "", err
		}
		id, err := q.PendingCommand(ctx, queries.PendingCommandParams{Queue: queue, Action: action})
		if err == nil {
			return id, nil
		}
		// The query runs on the unwrapped native handle, so a miss surfaces
		// as the raw driver sentinel; IsNoRows covers either driver.
		if !rdbms.IsNoRows(err) {
			return "", err
		}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	id := rand.Text()
	err = q.EnqueueCommand(ctx, queries.EnqueueCommandParams{ID: id, Queue: queue, Action: action, Payload: data})
	return id, err
}
