package rdbms_test

import (
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
)

func TestMaterialHintsUseExactGenerationAndRejectMalformedSummaries(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if err := (courses.Service{Pool: store}).Add(t.Context(), 77, "contract"); err != nil {
				t.Fatal(err)
			}
			seedSearchDocument(t, store, "doc_a", "first", "", "alpha")
			publishContractGuide(t, store, "document-v1", `{"summary":"usable summary"}`)
			assertMaterialHint(t, store, "document-v1", true)
			assertMaterialHint(t, store, "stale-document", false)
			assertSummaryAvailable(t, store, "document-v1", true)
			publishContractGuide(t, store, "document-v2", `{"summary":`)
			assertSummaryAvailable(t, store, "document-v2", false)
			assertMalformedSynthesisEvidence(t, store)
		})
	}
}

func publishContractGuide(t *testing.T, store database.Store, version, payload string) {
	t.Helper()
	tx, err := store.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	doc, err := tx.Analysis().CurrentDocument(t.Context(), "doc_a", false)
	if err != nil {
		t.Fatal(err)
	}
	contextHash := database.AnalysisContextHash(doc.ID, doc.CourseID, doc.SourceHash, doc.Kind,
		doc.Name, doc.CourseName, doc.Path, doc.Origin)
	claim := "2020-01-01T00:00:00.123456789Z"
	if err := tx.Analysis().QueueDocument(t.Context(), database.QueueDocumentParams{
		DocumentID: doc.ID, SourceHash: doc.SourceHash, ContextHash: contextHash, Version: version,
		Model: "contract-model", AvailableAt: claim,
	}); err != nil {
		t.Fatal(err)
	}
	if attempts, err := tx.Analysis().ClaimDocument(t.Context(), doc.ID, claim); err != nil || attempts != 1 {
		t.Fatalf("analysis claim = %d, %v", attempts, err)
	}
	if err := tx.Analysis().PublishReady(t.Context(), database.AnalysisPublishParams{
		DocumentID: doc.ID, ClaimedAt: claim, Model: "contract-model", Requested: "contract-model",
		Payload: payload, GeneratedAt: claim, Hash: doc.SourceHash, ContextHash: contextHash, Version: version,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func assertMaterialHint(t *testing.T, store database.Store, version string, ready bool) {
	t.Helper()
	rows, err := store.Study().ListPriorityMaterials(t.Context(), database.StudyIntelligenceParams{
		Model: "contract-model", DocumentVersion: version, PageVersion: "page-v3", Included: []int64{77},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("missing priority material: %v", rows.Err())
	}
	row := rows.Value()
	if row.ID != "doc_a" || (row.Enrichment != nil) != ready {
		t.Fatalf("priority hint for %s = %+v", version, row)
	}
	if ready && *row.Enrichment != `{"summary":"usable summary"}` {
		t.Fatalf("priority hint payload = %s", *row.Enrichment)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected trailing material or iteration error: %v", rows.Err())
	}
}

func assertSummaryAvailable(t *testing.T, store database.Store, version string, ready bool) {
	t.Helper()
	rows, err := store.Documents().FileMetadataRows(t.Context(), database.FileMetadataParams{
		Course: 77, Model: "contract-model", DocumentVersion: version,
		PageVersion: "page-v3", SynthesisVersion: "visual-v4",
	})
	if err != nil || len(rows) != 1 || rows[0].ID != "doc_a" || rows[0].Guide != ready {
		t.Fatalf("summary availability for %s = %+v, %v", version, rows, err)
	}
}

func assertMalformedSynthesisEvidence(t *testing.T, store database.Store) {
	t.Helper()
	params := database.SynthesisEvidenceParams{
		CourseID: 77, Model: "contract-model", DocVersion: "document-v2", PageVersion: "page-v3", Limit: 10,
	}
	counts, err := store.Synthesis().EvidenceCounts(t.Context(), params)
	if err != nil || counts.Total != 1 || counts.Ready != 0 {
		t.Fatalf("malformed synthesis readiness = %+v, %v", counts, err)
	}
	docs, err := store.Synthesis().EvidenceDocuments(t.Context(), params)
	if err != nil || len(docs) != 0 {
		t.Fatalf("malformed synthesis documents = %+v, %v", docs, err)
	}
}
