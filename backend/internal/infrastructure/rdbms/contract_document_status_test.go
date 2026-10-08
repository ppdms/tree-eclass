package rdbms_test

import (
	"reflect"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/knowledge"
)

func TestDocumentStatusUsesTheRequestedCourseAndGeneration(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if err := (courses.Service{Pool: store}).Add(t.Context(), 77, "contract"); err != nil {
				t.Fatal(err)
			}
			seedSearchDocument(t, store, "doc_a", "ΠΡΏΤΟ", "", "alpha")
			seedSearchDocument(t, store, "doc_b", "other", "", "beta")
			rows, err := store.Documents().ListAdminDocuments(t.Context(), []int64{77}, "ready", "other", 10)
			if err != nil || len(rows) != 1 || rows[0].ID != "doc_b" || rows[0].ChunkCount != 1 {
				t.Fatalf("filtered document status = %+v, %v", rows, err)
			}
			rows, err = store.Documents().ListAdminDocuments(t.Context(), []int64{77}, "ready", "πρώτο", 10)
			if err != nil || len(rows) != 1 || rows[0].ID != "doc_a" {
				t.Fatalf("Unicode document status filter = %+v, %v", rows, err)
			}
			chunks, embedded, err := store.Documents().EmbeddingCounts(t.Context(), []int64{77},
				knowledge.LocalEmbeddingModel)
			if err != nil || chunks != 2 || embedded != 2 {
				t.Fatalf("embedding coverage = %d/%d, %v", embedded, chunks, err)
			}
			assertGuideStatus(t, store)
		})
	}
}

func assertGuideStatus(t *testing.T, store database.Store) {
	t.Helper()
	params := database.GuideFreshnessParams{Courses: []int64{77}, Model: "contract-model",
		DocumentVersion: "document-v1", SynthesisVersion: "visual-v2"}
	counts, err := store.Documents().GuideSummary(t.Context(), params)
	if err != nil || !reflect.DeepEqual(counts, []database.StatusCount{{Status: "not_queued", Count: 2}}) {
		t.Fatalf("guide status for exact course = %+v, %v", counts, err)
	}
	rows, err := store.Documents().GuideDiagnostics(t.Context(), params, 10)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, row := range rows {
		ids = append(ids, row.DocumentID)
		if row.CourseID != 77 || row.Status != "not_queued" || row.Reason != "not_queued" ||
			row.Model != nil || row.Error != nil {
			t.Fatalf("guide diagnostic = %+v", row)
		}
	}
	if !reflect.DeepEqual(ids, []string{"doc_a", "doc_b"}) {
		t.Fatalf("guide diagnostics order = %v", ids)
	}
}
