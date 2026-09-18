package synchronization

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/jobs"
)

func admitCheck(ctx context.Context, tx pgx.Tx, course *int64, retry bool) (string, error) {
	var id, status string
	var payload []byte
	var failure *string
	// Claimers must wait or skip this row before capturing its retry inputs.
	err := tx.QueryRow(ctx, `SELECT id,status,payload,error FROM app.control_commands
WHERE queue='sync' AND status IN ('pending','running') LIMIT 1 FOR UPDATE`).Scan(&id, &status, &payload, &failure)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.EnqueueTx(ctx, tx, "sync", "check", Request{CourseID: course}, false)
	}
	if err != nil {
		return "", err
	}
	if !retry || status != "pending" || failure == nil {
		return "", ErrBusy
	}
	var previous Request
	if err := json.Unmarshal(payload, &previous); err != nil {
		return "", err
	}
	if (previous.CourseID == nil) != (course == nil) || (course != nil && *previous.CourseID != *course) {
		return "", ErrBusy
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE app.control_commands SET attempts=0,available_at=clock_timestamp(),claimed_at=NULL,error=NULL WHERE id=$1`,
		id,
	)
	return id, err
}
