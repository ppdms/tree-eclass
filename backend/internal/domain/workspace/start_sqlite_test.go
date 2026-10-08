package workspace

import (
	"context"
	"path/filepath"
	"testing"

	"tree-eclass/internal/infrastructure/storage"
)

// Session start is the reader's clock-in: a placeholder/argument mismatch
// here 500s every study session on every backend. Scratch sqlite only.
func TestSessionStartAdmitsDocumentSitting(t *testing.T) {
	ctx := context.Background()
	db, err := storage.OpenConfig(ctx, storage.Config{SQLitePath: filepath.Join(t.TempDir(), "t.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pool := db.Pool
	if _, err = pool.Exec(ctx, `INSERT INTO courses(id,name,webdav_folder) VALUES(245,'x','/Courses/245')`); err != nil {
		t.Fatal(err)
	}
	service := Service{Pool: pool}
	session, err := service.Start(ctx, Start{CourseID: 245, Key: "synthetic-session-001"})
	if err != nil {
		t.Fatal(err)
	}
	if session.ID < 1 || session.CourseID != 245 || session.Key != "synthetic-session-001" {
		t.Fatalf("session not journaled: %+v", session)
	}
	again, err := service.Start(ctx, Start{CourseID: 245, Key: "synthetic-session-001"})
	if err != nil || again.ID != session.ID {
		t.Fatal("reload did not re-adopt the sitting:", again, err)
	}
}
