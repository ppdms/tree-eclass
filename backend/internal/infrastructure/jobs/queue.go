// Package jobs owns durable command admission, claiming and bounded retries.
package jobs

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
)

type Queue struct{ Pool database.Store }

// EnqueueTx lets the domain publish data and its follow-up work atomically.
// Coalescing reuses only an unclaimed command. A running job already has its
// input snapshot, so a later mutation must leave another pending command.
func EnqueueTx(ctx context.Context, tx database.Tx, queue, action string, payload any, coalesce bool) (string, error) {
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
func (q Queue) Claim(ctx context.Context, queue string) ([]database.AppControlCommand, error) {
	// Claim inside a writer transaction and commit before doing the work:
	// the claim must serialize with concurrent publishers, and workers
	// must observe the running state before acting on the command.
	tx, err := q.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	cmds, err := tx.Jobs().ClaimCommands(ctx, queue, 1)
	if err != nil {
		return nil, err
	}
	return cmds, tx.Commit(ctx)
}
func (q Queue) Complete(ctx context.Context, id string) error {
	return q.Pool.Jobs().CompleteCommand(ctx, id)
}
func (q Queue) Recover(ctx context.Context) error {
	return q.Pool.Jobs().RecoverCommands(ctx)
}
func (q Queue) Fail(ctx context.Context, command database.AppControlCommand, failure error) error {
	delays := []int64{30, 300, 1800, 7200, 43200}
	status := "pending"
	if command.Attempts >= 5 {
		status = "failed"
	}
	message := strings.ToValidUTF8(failure.Error(), "�")
	if len([]rune(message)) > 1000 {
		message = string([]rune(message)[:1000])
	}
	return q.Pool.Jobs().FailCommand(
		ctx,
		database.FailCommandParams{
			ID:           command.ID,
			Status:       status,
			Error:        &message,
			DelaySeconds: delays[min(max(int(command.Attempts)-1, 0), 4)],
		},
	)
}
