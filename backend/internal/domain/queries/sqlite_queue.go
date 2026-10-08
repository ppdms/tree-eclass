package queries

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLite queue commands. QueueLock/PendingCommand/EnqueueCommand/
// CompleteCommand/RecoverCommands reuse the generated SQL verbatim; the
// sqlite driver rewrites $N placeholders, ::casts and row locks at
// Exec/Query time. ClaimCommands and FailCommand carry sqlite-specific SQL
// (see below). Timestamp columns scan via the shared parseTimestamptz helper
// in sqlite_core.go.

const sqliteQueueLock = `-- name: QueueLock :exec
SELECT pg_advisory_xact_lock(hashtext($1)::bigint)
`

func (q *SQLiteQueries) QueueLock(ctx context.Context, hashtext string) error {
	_, err := q.db.Exec(ctx, sqliteQueueLock, hashtext)
	return err
}

const sqlitePendingCommand = `-- name: PendingCommand :one
SELECT id FROM app.control_commands WHERE queue=$1 AND action=$2 AND status='pending'
ORDER BY available_at,id LIMIT 1 FOR UPDATE
`

func (q *SQLiteQueries) PendingCommand(ctx context.Context, arg PendingCommandParams) (string, error) {
	row := q.db.QueryRow(ctx, sqlitePendingCommand, arg.Queue, arg.Action)
	var id string
	err := row.Scan(&id)
	return id, err
}

const sqliteEnqueueCommand = `-- name: EnqueueCommand :exec
INSERT INTO app.control_commands(id,queue,action,payload) VALUES($1,$2,$3,$4)
`

func (q *SQLiteQueries) EnqueueCommand(ctx context.Context, arg EnqueueCommandParams) error {
	// Payload binds as TEXT so JSON1 reads (json_extract) see JSON text.
	_, err := q.db.Exec(ctx, sqliteEnqueueCommand,
		arg.ID,
		arg.Queue,
		arg.Action,
		string(arg.Payload),
	)
	return err
}

// sqliteClaimCommands replaces the postgres FOR UPDATE SKIP LOCKED claim with
// a single UPDATE..WHERE id IN (SELECT..LIMIT) RETURNING. The single-writer
// sqlite connection makes the claim atomic without row locking. now() is not
// a sqlite function, so clock reads/writes use strftime in the schema's TEXT
// format (YYYY-MM-DD HH24:MI:SS).
const sqliteClaimCommands = `-- name: ClaimCommands :many
UPDATE app.control_commands SET status='running',attempts=attempts+1,claimed_at=strftime('%Y-%m-%d %H:%M:%S','now')
WHERE id IN (SELECT pending.id FROM app.control_commands pending WHERE pending.queue=$1 AND pending.status='pending' AND pending.available_at<=strftime('%Y-%m-%d %H:%M:%S','now')
ORDER BY pending.available_at,pending.id LIMIT $2) RETURNING id, queue, action, payload, status, attempts, available_at, claimed_at, error
`

func (q *SQLiteQueries) ClaimCommands(ctx context.Context, arg ClaimCommandsParams) ([]AppControlCommand, error) {
	rows, err := q.db.Query(ctx, sqliteClaimCommands, arg.Queue, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []AppControlCommand{}
	for rows.Next() {
		var i AppControlCommand
		var availableAt, claimedAt sql.NullString
		if err := rows.Scan(
			&i.ID,
			&i.Queue,
			&i.Action,
			&i.Payload,
			&i.Status,
			&i.Attempts,
			&availableAt,
			&claimedAt,
			&i.Error,
		); err != nil {
			return nil, err
		}
		i.AvailableAt = parseTimestamptz(availableAt)
		i.ClaimedAt = parseTimestamptz(claimedAt)
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const sqliteCompleteCommand = `-- name: CompleteCommand :exec
DELETE FROM app.control_commands WHERE id=$1
`

func (q *SQLiteQueries) CompleteCommand(ctx context.Context, id string) error {
	_, err := q.db.Exec(ctx, sqliteCompleteCommand, id)
	return err
}

// sqliteFailCommandFmt inlines the retry delay with datetime('now','+N
// seconds'): sqlite has no interval arithmetic, and %d over an int64 delay is
// injection-safe. Status/error/id stay bound ($1..$3 map to ID, status,
// error, matching the generated argument order minus the folded delay).
const sqliteFailCommandFmt = `UPDATE app.control_commands SET status=$2,error=$3,available_at=datetime('now','+%d seconds'),claimed_at=NULL WHERE id=$1`

func (q *SQLiteQueries) FailCommand(ctx context.Context, arg FailCommandParams) error {
	_, err := q.db.Exec(ctx, fmt.Sprintf(sqliteFailCommandFmt, arg.DelaySeconds),
		arg.ID,
		arg.Status,
		arg.Error,
	)
	return err
}

const sqliteRecoverCommands = `-- name: RecoverCommands :exec
UPDATE app.control_commands SET status='pending',claimed_at=NULL WHERE status='running'
`

func (q *SQLiteQueries) RecoverCommands(ctx context.Context) error {
	_, err := q.db.Exec(ctx, sqliteRecoverCommands)
	return err
}
