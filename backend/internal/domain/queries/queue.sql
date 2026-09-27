-- name: QueueLock :exec
SELECT pg_advisory_xact_lock(hashtext($1)::bigint);

-- name: PendingCommand :one
SELECT id FROM app.control_commands WHERE queue=$1 AND action=$2 AND status='pending'
ORDER BY available_at,id LIMIT 1 FOR UPDATE;

-- name: EnqueueCommand :exec
INSERT INTO app.control_commands(id,queue,action,payload) VALUES($1,$2,$3,$4);

-- name: ClaimCommands :many
UPDATE app.control_commands
SET status='running',attempts=attempts+1,claimed_at=now()
WHERE id IN (
  SELECT pending.id
  FROM app.control_commands pending
  WHERE pending.queue=$1 AND pending.status='pending' AND pending.available_at<=now()
  ORDER BY pending.available_at,pending.id
  FOR UPDATE SKIP LOCKED
  LIMIT $2
) RETURNING *;

-- name: CompleteCommand :exec
DELETE FROM app.control_commands WHERE id=$1;

-- name: FailCommand :exec
UPDATE app.control_commands
SET status=$2,error=$3,available_at=now()+sqlc.arg(delay_seconds)::bigint*interval '1 second',claimed_at=NULL
WHERE id=$1;

-- name: RecoverCommands :exec
UPDATE app.control_commands SET status='pending',claimed_at=NULL WHERE status='running';
