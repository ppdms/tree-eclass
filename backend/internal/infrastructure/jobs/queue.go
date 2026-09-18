// Package jobs owns durable command admission, claiming and bounded retries.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/infrastructure/storage/queries"
)

type Queue struct{ Pool *pgxpool.Pool }

// EnqueueTx lets the domain publish data and its follow-up work atomically.
// Coalescing reuses only an unclaimed command. A running job already has its
// input snapshot, so a later mutation must leave another pending command.
func EnqueueTx(ctx context.Context, tx pgx.Tx, queue, action string, payload any, coalesce bool) (string, error) {
	q := queries.New(tx)
	if coalesce {
		if err := q.QueueLock(ctx, queue); err != nil {
			return "", err
		}
		id, err := q.PendingCommand(ctx, queries.PendingCommandParams{Queue: queue, Action: action})
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
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

func (q Queue) Enqueue(ctx context.Context, queue, action string, payload any, coalesce bool) (string, error) {
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := EnqueueTx(ctx, tx, queue, action, payload, coalesce)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}
func (q Queue) Claim(ctx context.Context, queue string) ([]queries.AppControlCommand, error) {
	return queries.New(q.Pool).ClaimCommands(ctx, queries.ClaimCommandsParams{Queue: queue, Limit: 1})
}
func (q Queue) Complete(ctx context.Context, id string) error {
	return queries.New(q.Pool).CompleteCommand(ctx, id)
}
func (q Queue) Recover(ctx context.Context) error { return queries.New(q.Pool).RecoverCommands(ctx) }
func (q Queue) Fail(ctx context.Context, command queries.AppControlCommand, failure error) error {
	delays := []int64{30, 300, 1800, 7200, 43200}
	status := "pending"
	if command.Attempts >= 5 {
		status = "failed"
	}
	message := strings.ToValidUTF8(failure.Error(), "�")
	if len([]rune(message)) > 1000 {
		message = string([]rune(message)[:1000])
	}
	return queries.New(q.Pool).
		FailCommand(
			ctx,
			queries.FailCommandParams{
				ID:           command.ID,
				Status:       status,
				Error:        &message,
				DelaySeconds: delays[min(max(int(command.Attempts)-1, 0), 4)],
			},
		)
}
