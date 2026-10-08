package synchronization

import (
	"context"
	"encoding/json"
	"errors"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/jobs"
)

func admitCheck(ctx context.Context, tx database.Tx, course *int64, retry bool) (string, error) {
	// Claimers must wait or skip this row before capturing its retry inputs.
	claim, err := tx.Sync().FindSyncClaim(ctx)
	if errors.Is(err, database.ErrNoRows) {
		return jobs.EnqueueTx(ctx, tx, "sync", "check", Request{CourseID: course}, false)
	}
	if err != nil {
		return "", err
	}
	if !retry || claim.Status != "pending" || claim.Failure == nil {
		return "", ErrBusy
	}
	var previous Request
	if err := json.Unmarshal(claim.Payload, &previous); err != nil {
		return "", err
	}
	if (previous.CourseID == nil) != (course == nil) || (course != nil && *previous.CourseID != *course) {
		return "", ErrBusy
	}
	return claim.ID, tx.Sync().ResetSyncClaim(ctx, claim.ID)
}
