package navigation

import (
	"context"
	"errors"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/domain/settings"
)

func freshCommunity(ctx context.Context, tx database.Tx, course int64, packet map[string]any) (string, error) {
	source, _ := packet["source_snapshot"].(map[string]any)
	captured := map[string]string{}
	entries, _ := source["conversations"].([]any)
	if len(entries) > 100 {
		return "cached_community_snapshot_invalid", nil
	}
	for _, raw := range entries {
		item, ok := raw.(map[string]any)
		if !ok {
			return "cached_community_snapshot_invalid", nil
		}
		id, _ := item["conversation_id"].(string)
		hash, _ := item["content_hash"].(string)
		if id == "" || hash == "" || captured[id] != "" {
			return "cached_community_snapshot_invalid", nil
		}
		captured[id] = hash
	}
	evidence, _ := packet["evidence"].([]any)
	for _, raw := range evidence {
		item, _ := raw.(map[string]any)
		ref, _ := item["evidence_ref"].(string)
		if !strings.HasPrefix(ref, "discord:") {
			continue
		}
		id := strings.TrimPrefix(ref, "discord:")
		if captured[id] == "" {
			return "cached_community_snapshot_missing", nil
		}
		hash, err := messages.Snapshot(ctx, tx, course, id)
		if errors.Is(err, database.ErrNoRows) {
			return "cached_community_mapping_stale", nil
		}
		if err != nil {
			return "", err
		}
		if captured[id] != hash {
			return "cached_community_evidence_stale", nil
		}
	}
	return "", nil
}

// ValidateEvidence is shared by synthesis publication and navigation projection.
// A newly added source can queue a successor; every captured source must still
// match exactly before a completed model result becomes usable.
func ValidateEvidence(
	ctx context.Context,
	tx database.Tx,
	course int64,
	a settings.AI,
	packet map[string]any,
) (string, error) {
	_, reason, err := freshEvidence(ctx, tx, course, a, packet)
	return reason, err
}
