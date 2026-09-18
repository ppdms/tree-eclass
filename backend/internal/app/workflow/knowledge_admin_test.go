package workflow

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/infrastructure/jobs"
)

func knowledgeAdminChecks(t *testing.T, pool *pgxpool.Pool, base, document string, indexer knowledge.Indexer) {
	t.Helper()
	ctx := t.Context()
	service := knowledge.Reader{Pool: pool}
	var view map[string]any
	apiJSON(t, "GET", base+"/api/knowledge/status?course_id=101", nil, 200, &view)
	if len(view["roadmap_diagnostics"].([]any)) != 1 ||
		view["embedding"].(map[string]any)["missing_chunks"] != float64(0) {
		t.Fatal("native status lost scope or embedding completeness", view)
	}
	apiJSON(t, "GET", base+"/api/knowledge/overview?course_id=101", nil, 200, &view)
	if len(view["documents"].([]any)) != 1 || view["status"] == nil {
		t.Fatal("native admin overview missing catalog", view)
	}
	apiJSON(t, "GET", base+"/api/knowledge/status?course_id=999999", nil, 400, nil)
	// Repair a derived-index hole using the real queued maintenance contract.
	if _, err := pool.Exec(ctx, `DELETE FROM app.control_commands WHERE queue='index'; DELETE FROM knowledge.chunks_fts`); err != nil {
		t.Fatal(err)
	}
	var first, second map[string]any
	apiJSON(t, "POST", base+"/api/knowledge/reconcile", nil, 200, &first)
	apiJSON(t, "POST", base+"/api/knowledge/reconcile", nil, 200, &second)
	if first["command_id"] != second["command_id"] {
		t.Fatal("repeated maintenance was not coalesced")
	}
	queue := jobs.Queue{Pool: pool}
	commands, err := queue.Claim(ctx, "index")
	if err != nil || len(commands) != 1 || commands[0].Action != "reconcile" {
		t.Fatal("maintenance command missing", commands, err)
	}
	if err = service.Maintain(ctx, commands[0].Action); err != nil {
		t.Fatal(err)
	}
	if err = queue.Complete(ctx, commands[0].ID); err != nil {
		t.Fatal(err)
	}
	if err = service.Maintain(ctx, "rebuild"); err != nil {
		t.Fatal(err)
	}
	assertIndexQueue(t, pool, document, 1)
	var revisions, docs int64
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.document_revisions WHERE document_id=$1),(SELECT count(*) FROM knowledge.documents WHERE id=$1)`, document).Scan(&revisions, &docs); err != nil ||
		revisions != 1 ||
		docs != 1 {
		t.Fatal("rebuild replaced authoritative document identity", revisions, docs, err)
	}
	if err = indexer.Index(ctx, document); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE app.control_commands SET status='failed',attempts=5,error='synthetic failure' WHERE queue='index' AND payload->>'document_id'=$1`, document); err != nil {
		t.Fatal(err)
	}
	if err = service.Maintain(ctx, "retry_failed"); err != nil {
		t.Fatal(err)
	}
	assertIndexQueue(t, pool, document, 1)
	var attempts int
	if err = pool.QueryRow(ctx, `SELECT attempts FROM app.control_commands WHERE queue='index' AND payload->>'document_id'=$1`, document).Scan(&attempts); err != nil ||
		attempts != 0 {
		t.Fatal("manual retry retained exhausted attempt budget", attempts, err)
	}
	if err = indexer.Index(ctx, document); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `DELETE FROM app.control_commands WHERE queue='index'`); err != nil {
		t.Fatal(err)
	}
	if err = service.Maintain(ctx, "reconcile"); err != nil {
		t.Fatal(err)
	}
	assertIndexQueue(t, pool, document, 0)
}

func assertIndexQueue(t *testing.T, pool *pgxpool.Pool, document string, wanted int) {
	t.Helper()
	var count int
	err := pool.QueryRow(t.Context(), `SELECT count(*) FROM app.control_commands WHERE queue='index' AND status='pending' AND payload->>'document_id'=$1`, document).
		Scan(&count)
	if err != nil || count != wanted {
		t.Fatal("index repair admission", count, wanted, err)
	}
}
