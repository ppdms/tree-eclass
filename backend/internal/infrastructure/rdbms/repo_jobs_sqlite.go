package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqliteJobs struct{ db nativeDBTX }

func (j sqliteJobs) QueueLock(ctx context.Context, queue string) error {
	// The admitted writer owns every write until commit/rollback, which is
	// stronger than the keyed advisory lock; requires a writer transaction.
	_, err := advisoryLock(ctx, j.db, queue, false, false)
	return err
}

func (j sqliteJobs) PendingCommand(ctx context.Context, queue, action string) (string, error) {
	var id string
	err := j.db.QueryRow(ctx, `SELECT id FROM control_commands
		WHERE queue=? AND action=? AND status='pending'
		ORDER BY available_at,id LIMIT 1`, queue, action).Scan(&id)
	return id, err
}

func (j sqliteJobs) EnqueueCommand(ctx context.Context, params database.EnqueueCommandParams) error {
	// Payload binds as TEXT so JSON1 reads (json_extract) see JSON text.
	_, err := j.db.Exec(ctx, `INSERT INTO control_commands(id,queue,action,payload)
		VALUES(?,?,?,?)`, params.ID, params.Queue, params.Action, string(params.Payload))
	return err
}

func (j sqliteJobs) ClaimCommands(
	ctx context.Context, queue string, limit int32,
) ([]database.AppControlCommand, error) {
	// The single-writer connection makes the claim atomic without row locks.
	// Clock reads/writes use strftime in the schema TEXT format.
	rows, err := j.db.Query(ctx, `UPDATE control_commands
		SET status='running',attempts=attempts+1,claimed_at=strftime('%Y-%m-%d %H:%M:%S','now')
		WHERE id IN (
			SELECT pending.id
			FROM control_commands pending
			WHERE pending.queue=? AND pending.status='pending'
				AND pending.available_at<=strftime('%Y-%m-%d %H:%M:%S','now')
			ORDER BY pending.available_at,pending.id
			LIMIT ?
		) RETURNING id,queue,action,payload,status,attempts,available_at,claimed_at,error`, queue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.AppControlCommand{}
	for rows.Next() {
		var cmd database.AppControlCommand
		if err := rows.Scan(&cmd.ID, &cmd.Queue, &cmd.Action, &cmd.Payload, &cmd.Status,
			&cmd.Attempts, (*nativeTime)(&cmd.AvailableAt), (*nativeTime)(&cmd.ClaimedAt), &cmd.Error); err != nil {
			return nil, err
		}
		out = append(out, cmd)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (j sqliteJobs) CompleteCommand(ctx context.Context, id string) error {
	_, err := j.db.Exec(ctx, `DELETE FROM control_commands WHERE id=?`, id)
	return err
}

func (j sqliteJobs) FailCommand(ctx context.Context, params database.FailCommandParams) error {
	// The delay binds into a '+N seconds' modifier, matching the previous
	// datetime('now','+N seconds') availability shift.
	_, err := j.db.Exec(ctx, `UPDATE control_commands
		SET status=?,error=?,available_at=datetime('now','+'||?||' seconds'),claimed_at=NULL
		WHERE id=?`, params.Status, params.Error, params.DelaySeconds, params.ID)
	return err
}

func (j sqliteJobs) RecoverCommands(ctx context.Context) error {
	_, err := j.db.Exec(ctx, `UPDATE control_commands SET status='pending',claimed_at=NULL WHERE status='running'`)
	return err
}
