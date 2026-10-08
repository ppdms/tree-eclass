package workflow

import (
	"testing"
	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/settings"
)

func communityFreshnessChecks(t *testing.T, pool *fixtureStore) {
	t.Helper()
	ctx := t.Context()
	tx, err := pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := messages.Snapshot(ctx, tx, 901, "conversation")
	if err != nil {
		t.Fatal(err)
	}
	packet := map[string]any{
		"evidence": []any{map[string]any{"evidence_ref": "discord:conversation"}},
		"source_snapshot": map[string]any{
			"documents":     []any{},
			"conversations": []any{map[string]any{"conversation_id": "conversation", "content_hash": hash}},
		},
	}
	if reason, err := navigation.ValidateEvidence(ctx, tx, 901, settings.DefaultAI(), packet); err != nil ||
		reason != "" {
		t.Fatal("current community snapshot", reason, err)
	}
	tx.Rollback(ctx)
	var before, after int64
	if err = pool.Native.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=901`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Native.Exec(ctx, `UPDATE messages.conversation_messages SET position=position+1 WHERE conversation_id='conversation'`); err != nil {
		t.Fatal(err)
	}
	if err = pool.Native.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=901`).Scan(&after); err != nil ||
		after <= before {
		t.Fatal("membership did not invalidate navigation", before, after, err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if reason, err := navigation.ValidateEvidence(ctx, tx, 901, settings.DefaultAI(), packet); err != nil ||
		reason != "cached_community_evidence_stale" {
		t.Fatal("changed community remained admitted", reason, err)
	}
	packet["source_snapshot"].(map[string]any)["conversations"] = []any{}
	if reason, err := navigation.ValidateEvidence(ctx, tx, 901, settings.DefaultAI(), packet); err != nil ||
		reason != "cached_community_snapshot_missing" {
		t.Fatal("unbound community admitted", reason, err)
	}
}
