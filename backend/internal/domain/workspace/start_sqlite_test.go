package workspace

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"tree-eclass/internal/infrastructure/storage"
)

// Session start is the reader's clock-in: a placeholder/argument mismatch
// here 500s every study session on every backend. Scratch sqlite only.
// Fixtures use a side native handle (tests only); the service under test
// receives the typed Store.
func TestSessionStartAdmitsDocumentSitting(t *testing.T) {
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
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	service := Service{Pool: db.Pool}
	session, err := service.Start(ctx, Start{CourseID: 245, Key: "synthetic-session-001"})
	if err != nil {
		t.Fatal(err)
	}
	if session.Session.ID < 1 || session.Session.CourseID != 245 ||
		session.Session.Key != "synthetic-session-001" || session.NextSequence != 0 {
		t.Fatalf("session not journaled: %+v", session)
	}
	again, err := service.Start(ctx, Start{CourseID: 245, Key: "synthetic-session-001"})
	if err != nil || again.Session.ID != session.Session.ID || again.NextSequence != 0 {
		t.Fatal("reload did not re-adopt the sitting:", again, err)
	}
	checkResumedSequence(t, service, session.Session.ID)
}

func checkResumedSequence(t *testing.T, service Service, id int64) {
	t.Helper()
	ctx := t.Context()
	tx, err := service.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Workspace().LockSession(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []int64{7, 9} {
		if err := tx.Workspace().InsertBeat(ctx, id, sequence, "synthetic-request"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err := service.Start(ctx, Start{CourseID: 245, Key: "synthetic-session-001"})
	if err != nil || resumed.Session.ID != id || resumed.NextSequence != 10 {
		t.Fatal("resume used beat count instead of highest sequence", resumed, err)
	}
}
