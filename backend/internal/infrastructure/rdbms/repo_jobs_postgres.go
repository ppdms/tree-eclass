package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresJobs struct{ db nativeDBTX }

func (j postgresJobs) QueueLock(ctx context.Context, queue string) error {
	// Raw hashtext namespace, distinct from the hashtextextended mapping locks.
	_, err := advisoryLock(ctx, j.db, queue, false, false)
	return err
}

func (j postgresJobs) PendingCommand(ctx context.Context, queue, action string) (string, error) {
	var id string
	err := j.db.QueryRow(ctx, `SELECT id FROM app.control_commands
		WHERE queue=$1 AND action=$2 AND status='pending'
		ORDER BY available_at,id LIMIT 1 FOR UPDATE`, queue, action).Scan(&id)
	return id, err
}

func (j postgresJobs) EnqueueCommand(ctx context.Context, params database.EnqueueCommandParams) error {
	_, err := j.db.Exec(ctx, `INSERT INTO app.control_commands(id,queue,action,payload)
		VALUES($1,$2,$3,$4)`, params.ID, params.Queue, params.Action, params.Payload)
	return err
}

func (j postgresJobs) ClaimCommands(
	ctx context.Context, queue string, limit int32,
) ([]database.AppControlCommand, error) {
	rows, err := j.db.Query(ctx, `UPDATE app.control_commands
		SET status='running',attempts=attempts+1,claimed_at=now()
		WHERE id IN (
			SELECT pending.id
			FROM app.control_commands pending
			WHERE pending.queue=$1 AND pending.status='pending' AND pending.available_at<=now()
			ORDER BY pending.available_at,pending.id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
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

func (j postgresJobs) CompleteCommand(ctx context.Context, id string) error {
	_, err := j.db.Exec(ctx, `DELETE FROM app.control_commands WHERE id=$1`, id)
	return err
}

func (j postgresJobs) FailCommand(ctx context.Context, params database.FailCommandParams) error {
	_, err := j.db.Exec(ctx, `UPDATE app.control_commands
		SET status=$2,error=$3,available_at=now()+$4::bigint*interval '1 second',claimed_at=NULL
		WHERE id=$1`, params.ID, params.Status, params.Error, params.DelaySeconds)
	return err
}

func (j postgresJobs) RecoverCommands(ctx context.Context) error {
	_, err := j.db.Exec(ctx, `UPDATE app.control_commands SET status='pending',claimed_at=NULL WHERE status='running'`)
	return err
}
