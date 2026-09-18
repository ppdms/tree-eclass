package workflow

import (
	"context"
	"net/url"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/knowledge"
)

func searchChecks(t *testing.T, db *pgx.Conn, base, document string) {
	t.Helper()
	for _, mode := range []string{"lexical", "semantic", "hybrid"} {
		var result knowledge.SearchResponse
		apiJSON(t, "GET", base+"/api/knowledge/search?q="+url.QueryEscape("δενδρα")+"&mode="+mode, nil, 200, &result)
		if len(result.Results) != 1 || result.Results[0].DocumentID != document ||
			result.Results[0].CourseName != "Συνθετικό μάθημα" {
			t.Fatalf("%s search: %+v", mode, result)
		}
		if !result.Results[0].UntrustedContent || result.Results[0].ResourceURI == "" {
			t.Fatal("retrieval lost evidence provenance")
		}
	}
	var documents struct {
		Documents []knowledge.AdminDocument `json:"documents"`
	}
	apiJSON(t, "GET", base+"/api/knowledge/documents", nil, 200, &documents)
	if len(documents.Documents) != 1 || documents.Documents[0].ChunkCount != 1 ||
		documents.Documents[0].EmbeddingCount != 1 {
		t.Fatalf("index diagnostics: %+v", documents)
	}
	if _, err := db.Exec(context.Background(), `INSERT INTO knowledge.page_enrichments(document_id,page_number,source_hash,analysis_version,status,model,requested_model,payload_json,available_at) SELECT id,1,source_hash,'test','ready','synthetic','synthetic','{"summary":"Synthetic page insight","secret_internal_field":"must not leave"}','now' FROM knowledge.documents WHERE id=$1`, document); err != nil {
		t.Fatal(err)
	}
	var pages knowledge.PageInsights
	apiJSON(t, "GET", base+"/api/study/document/"+document+"/pages?course_id=101&last=1000", nil, 200, &pages)
	if pages.LastPage > 24 || len(pages.Pages) != 1 || pages.Pages[0].Insight["secret_internal_field"] != nil {
		t.Fatal("page read bounds or payload boundary failed")
	}
	apiJSON(t, "GET", base+"/api/knowledge/search?q=tree&course_id=102", nil, 400, nil)
	apiJSON(t, "GET", base+"/api/knowledge/search?q=tree&mode=invalid", nil, 400, nil)
}

func readChecks(t *testing.T, pool *pgxpool.Pool, document string) {
	t.Helper()
	reader := knowledge.Reader{Pool: pool}
	ctx := context.Background()
	result, err := reader.Read(ctx, knowledge.ReadRequest{DocumentID: document, MaxCharacters: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || result.Characters != 5 || len(result.Units) != 1 ||
		utf8.RuneCountInString(result.Units[0].Text) != 5 {
		t.Fatal("material read did not preserve the Unicode character limit")
	}
	listing, err := reader.Materials(ctx, knowledge.ListRequest{CourseID: 101, IncludeInsights: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Materials) != 1 || listing.Materials[0].ResourceURI != "eclass://documents/"+document ||
		!listing.Materials[0].UntrustedContent {
		t.Fatal("material catalog lost source provenance")
	}
}
