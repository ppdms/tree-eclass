package workspace

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/storage"
)

// Pending documents report ErrDocumentPending (the reader waits) instead of
// ErrNoRows (the reader reports the document gone). Scratch sqlite only.
// Fixtures use a side native handle (tests only); the service under test
// receives the typed Store.
func TestPendingDocumentReportsPending(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := storage.OpenConfig(ctx, storage.Config{SQLitePath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seed, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = seed.ExecContext(ctx, `INSERT INTO courses(id,name,webdav_folder)`+
		` VALUES(245,'x','/Courses/245')`); err != nil {
		_ = seed.Close()
		t.Fatal(err)
	}
	if _, err = seed.ExecContext(ctx, `INSERT INTO documents(id,course_id,course_name,source_path,`+
		`normalized_path,display_name,source_hash,source_url,source_origin,document_kind,status,is_current)`+
		` VALUES('doc_pending',245,'x','/p','/p','notes.pdf','h','u','eclass','pdf','pending',1)`); err != nil {
		_ = seed.Close()
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	service := Service{Pool: db.Pool}
	if _, err = service.Context(ctx, ContextRequest{CourseID: 245, Document: "doc_pending"}); !errors.Is(
		err, ErrDocumentPending,
	) {
		t.Fatal("pending document did not report pending:", err)
	}
	if _, err = service.Context(ctx, ContextRequest{CourseID: 245, Document: "doc_missing"}); !errors.Is(
		err, database.ErrNoRows,
	) {
		t.Fatal("missing document did not report not found:", err)
	}
}
