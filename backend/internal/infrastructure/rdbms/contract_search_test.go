package rdbms_test

import (
	"reflect"
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
)

func TestDocumentCandidatesMatchUnicodeMetadataAndNotVectorPositions(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			if err := (courses.Service{Pool: store}).Add(t.Context(), 77, "contract"); err != nil {
				t.Fatal(err)
			}
			seedSearchDocument(t, store, "doc_a", "ΠΕΡΙΓΡΑΦΉ", "ΣΥΣΤΉΜΑΤΑ", "alpha laboratory")
			seedSearchDocument(t, store, "doc_b", "other", "", "alpha Ελληνικά\x00tail")
			filter := database.DocumentFilter{CourseIDs: []int64{77}}
			for _, scenario := range []struct {
				term  string
				limit int
				ids   []string
			}{
				{"περιγραφη", 10, []string{"doc_a"}}, {"συστηματα", 10, []string{"doc_a"}},
				{"ελληνικα", 10, []string{"doc_b"}}, {"1", 10, []string{}},
				{"alpha", 1, []string{"doc_a"}}, {"alpha", 10, []string{"doc_a", "doc_b"}},
			} {
				rows, err := store.Documents().LexicalCandidates(t.Context(), filter, []string{scenario.term}, scenario.limit)
				if err != nil {
					t.Fatal(err)
				}
				ids := make([]string, 0, len(rows))
				for _, row := range rows {
					ids = append(ids, row.Document.ID)
				}
				if !reflect.DeepEqual(ids, scenario.ids) {
					t.Fatalf("term %q: got %v, want %v", scenario.term, ids, scenario.ids)
				}
			}
			assertEmbeddedCandidates(t, store, filter)
		})
	}
}

func assertEmbeddedCandidates(t *testing.T, store database.Store, filter database.DocumentFilter) {
	t.Helper()
	rows, err := store.Documents().EmbeddedCandidates(t.Context(), filter,
		knowledge.LocalEmbeddingModel, knowledge.EmbeddingDimensions)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		row := rows.Value()
		ids = append(ids, row.Document.ID)
		text := "alpha laboratory"
		if row.Document.ID == "doc_b" {
			text = "alpha Ελληνικά\x00tail"
		}
		if row.Text != identity.Encode(text) ||
			!reflect.DeepEqual(row.Vector, knowledge.Pack(knowledge.Embed(text))) {
			t.Fatalf("source or packed vector changed: %+v", row)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []string{"doc_a", "doc_b"}) {
		t.Fatalf("embedded source order = %v", ids)
	}
}

func seedSearchDocument(t *testing.T, store database.Store, id, name, heading, text string) {
	t.Helper()
	tx, err := store.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	path, hash := "/external/"+id+".txt", identity.TextHash(id+text)
	if err := tx.Indexing().ObserveDocument(t.Context(), database.ObserveDocumentParams{
		ID: id, CourseID: 77, CourseName: "contract", SourcePath: path, NormalizedPath: path,
		DisplayName: identity.Encode(name), SourceHash: hash, DocumentKind: "text",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Objects().RegisterObject(t.Context(), database.ObjectReference{
		Bucket: "materials", Key: hash, VersionID: hash, SHA256: hash, Bytes: int64(len(text)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Objects().RegisterRevision(t.Context(), database.RegisterRevisionParams{
		ID: "revision_" + id, DocumentID: id, CourseID: 77, LogicalPath: path, ObjectID: hash,
	}); err != nil {
		t.Fatal(err)
	}
	seedSearchChunk(t, tx, id, name, heading, text, path)
	if err := tx.Indexing().MarkIndexed(t.Context(), database.MarkIndexedParams{ID: id, WarningsJSON: "[]"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func seedSearchChunk(t *testing.T, tx database.Tx, id, name, heading, text, path string) {
	t.Helper()
	chunk, encoded, normalized := "chunk_"+id, identity.Encode(text), identity.Encode(identity.Search(text))
	heading, name = identity.Encode(heading), identity.Encode(name)
	if err := tx.Indexing().InsertChunk(t.Context(), database.InsertChunkParams{
		ID: chunk, DocumentID: id, Ordinal: 0, LocatorType: "section", Heading: &heading,
		Text: encoded, NormalizedText: normalized, ContentHash: identity.TextHash(text), MetadataJSON: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Indexing().IndexChunkSearch(t.Context(), database.IndexChunkSearchParams{
		ChunkID: chunk, Text: &encoded, NormalizedText: &normalized, Heading: &heading,
		DisplayName: &name, SourcePath: &path,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Indexing().IndexEmbedding(t.Context(), database.IndexEmbeddingParams{
		ChunkID: chunk, Model: knowledge.LocalEmbeddingModel,
		Vector: knowledge.Pack(knowledge.Embed(text)), Dimensions: knowledge.EmbeddingDimensions,
	}); err != nil {
		t.Fatal(err)
	}
}
