package knowledge

import (
	"context"
	"strings"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

func (i Indexer) recordFailure(ctx context.Context, id, hash string, failure error) error {
	status, reason := "failed", "extraction_failed"
	if ctx.Err() != nil {
		status, reason = "pending", "extraction_interrupted"
	}
	message := []rune(strings.ToValidUTF8(failure.Error(), "�"))
	message = message[:min(1000, len(message))]
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	return i.Pool.Indexing().RecordIndexFailure(cleanup, database.RecordIndexFailureParams{
		ID:     id,
		Hash:   hash,
		Status: status,
		Error:  identity.Encode(string(message)),
		Reason: reason,
	})
}
