package rdbms_test

import (
	"testing"

	"tree-eclass/internal/domain/annotations"
	"tree-eclass/internal/domain/courses"
)

func TestDeletedBookmarkDoesNotHideItsReplacement(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if err := (courses.Service{Pool: store}).Add(t.Context(), 77, "contract"); err != nil {
				t.Fatal(err)
			}
			seedSearchDocument(t, store, "doc_a", "first", "", "alpha")
			service := annotations.Service{Pool: store}
			first := createContractBookmark(t, service, "bookmark:first")
			deleted := "deleted"
			if _, err := service.Update(t.Context(), first.ID, annotations.Update{Status: &deleted}); err != nil {
				t.Fatal(err)
			}
			second := createContractBookmark(t, service, "bookmark:second")
			if second.ID == first.ID || second.Status != "active" || second.Body == nil ||
				*second.Body != "σημείωση\x00tail" {
				t.Fatalf("replacement bookmark = %+v", second)
			}
			replay := createContractBookmark(t, service, "bookmark:third")
			if replay.ID != second.ID {
				t.Fatalf("live page bookmark duplicated: %d != %d", replay.ID, second.ID)
			}
		})
	}
}

func createContractBookmark(t *testing.T, service annotations.Service, key string) annotations.Annotation {
	t.Helper()
	body := "σημείωση\x00tail"
	result, err := service.Create(t.Context(), annotations.Create{
		Annotation: annotations.Annotation{CourseID: 77, DocumentID: "doc_a", PageNumber: 1,
			Kind: "bookmark", Body: &body}, IdempotencyKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
