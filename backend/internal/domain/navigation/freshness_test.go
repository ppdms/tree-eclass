package navigation

import (
	"testing"
	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/settings"
)

func TestFallbackServingModelRetainsRequestedGeneration(t *testing.T) {
	a := settings.DefaultAI()
	d := evidenceDocument{
		ID:             "document",
		Hash:           "hash",
		Kind:           "text",
		AnalysisStatus: "ready",
		AnalysisHash:   "hash",
		Version:        settings.DocumentVersion("text"),
		Model:          "serving-fallback",
		RequestedModel: a.Model,
		Payload:        `{"summary":"source-bound guide"}`,
	}
	hash, err := blueprints.PayloadHash([]byte(d.Payload))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := evidenceSnapshot{
		ID:             d.ID,
		Hash:           d.Hash,
		AnalysisHash:   d.Hash,
		Version:        d.Version,
		Model:          d.Model,
		RequestedModel: a.Model,
		PayloadHash:    hash,
	}
	if reason := checkSnapshot(d, snapshot, a); reason != "" {
		t.Fatal("valid fallback marked stale", reason)
	}
	snapshot.RequestedModel = "another-generation"
	if reason := checkSnapshot(d, snapshot, a); reason == "" {
		t.Fatal("wrong requested generation accepted")
	}
}
