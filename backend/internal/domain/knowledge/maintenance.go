package knowledge

import (
	"context"
)

// Maintain runs on the same serial queue as extraction. Rebuilding replaces
// derived chunks during successful indexing; document IDs, revisions, source
// bytes, annotations, AI evidence and learner history remain intact.
func (s Reader) Maintain(ctx context.Context, action string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Match domain write ordering before taking document/derived-generation locks.
	if err = tx.Indexing().LockCoursesForMaintenance(ctx); err != nil {
		return err
	}
	if err = tx.Jobs().QueueLock(ctx, "index"); err != nil {
		return err
	}
	if err = tx.Indexing().MaintainIndex(ctx, action); err != nil {
		return err
	}
	if action == "retry_failed" {
		if err = tx.Indexing().RetryFailedAnalyses(ctx); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
