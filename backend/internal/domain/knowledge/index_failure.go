package knowledge

import (
	"context"
	"strings"
	"time"

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
	_, err := i.Pool.Exec(
		cleanup,
		`UPDATE knowledge.documents SET status=$3,error=$4,diagnostic_reason=$5 WHERE id=$1 AND source_hash=$2 AND is_current=1 AND status='running'`,
		id,
		hash,
		status,
		identity.Encode(string(message)),
		reason,
	)
	return err
}
