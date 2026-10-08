// Package jobs owns durable command admission, claiming and bounded retries.
package jobs

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Queue struct{ Pool rdbms.Pool }

// EnqueueTx lets the domain publish data and its follow-up work atomically.
// Coalescing reuses only an unclaimed command. A running job already has its
// input snapshot, so a later mutation must leave another pending command.
func EnqueueTx(ctx context.Context, tx rdbms.Tx, queue, action string, payload any, coalesce bool) (string, error) {
	return commands.EnqueueTx(ctx, tx, queue, action, payload, coalesce)
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
	// Claim inside a writer transaction, not a pool autocommit: the
	// UPDATE...RETURNING must serialize with other writers otherwise its
	// commit invalidates their DEFERRED snapshots (SQLITE_BUSY_SNAPSHOT).
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	commands, err := queries.ForTx(tx).ClaimCommands(ctx, queries.ClaimCommandsParams{Queue: queue, Limit: 1})
	if err != nil {
		return nil, err
	}
	return commands, tx.Commit(ctx)
}
func (q Queue) Complete(ctx context.Context, id string) error {
	return queries.ForPool(q.Pool).CompleteCommand(ctx, id)
}
func (q Queue) Recover(ctx context.Context) error {
	return queries.ForPool(q.Pool).RecoverCommands(ctx)
}
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
	return queries.ForPool(q.Pool).FailCommand(
		ctx,
		queries.FailCommandParams{
			ID:           command.ID,
			Status:       status,
			Error:        &message,
			DelaySeconds: delays[min(max(int(command.Attempts)-1, 0), 4)],
		},
	)
}
