package rdbms_test

import (
	"reflect"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
)

func TestMaterialCursorComparesInstantsAcrossFractionalTimestamps(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if err := (courses.Service{Pool: store}).Add(t.Context(), 77, "contract"); err != nil {
				t.Fatal(err)
			}
			seedSearchDocument(t, store, "doc_a", "first", "", "alpha")
			seedSearchDocument(t, store, "doc_b", "other", "", "beta")
			for id, stamp := range map[string]string{
				"doc_a": "2020-01-01T00:00:00.500Z", "doc_b": "2020-01-01T00:00:00Z",
			} {
				if err := store.Indexing().MarkIndexed(t.Context(), database.MarkIndexedParams{
					ID: id, IndexedAt: &stamp, WarningsJSON: "[]",
				}); err != nil {
					t.Fatal(err)
				}
			}
			for _, scenario := range []struct {
				since string
				ids   []string
			}{
				{"2020-01-01T00:00:00Z", []string{"doc_a", "doc_b"}},
				{"2020-01-01T00:00:00.250Z", []string{"doc_a"}},
				{"2020-01-01T02:00:00+02:00", []string{"doc_a", "doc_b"}},
			} {
				rows, err := store.Documents().ListMaterials(t.Context(), 77, "", "/external/", "text", &scenario.since, 10)
				if err != nil {
					t.Fatal(err)
				}
				ids := []string{}
				for _, row := range rows {
					ids = append(ids, row.ID)
				}
				if !reflect.DeepEqual(ids, scenario.ids) {
					t.Fatalf("materials since %s = %v, want %v", scenario.since, ids, scenario.ids)
				}
			}
		})
	}
}
