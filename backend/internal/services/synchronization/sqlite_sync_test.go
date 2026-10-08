package synchronization

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
	"tree-eclass/internal/integrations/eclass"
)

// TestSQLiteSyncPublishesDocuments is the permanent regression test for the
// sqlite sync path: full course sync (crawl, object registration, revision
// link, document upsert, change records) plus announcement and global-feed
// upserts against a scratch sqlite database with a synthetic upstream. It
// covers the exact prod failures: the dropped revision ObjectID (FK 787),
// the missing AT TIME ZONE rewrite, and the UPDATE-alias syntax gap.
func TestSQLiteSyncPublishesDocuments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := storage.OpenConfig(ctx, storage.Config{SQLitePath: filepath.Join(dir, "t.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pool := db.Pool
	if err = pool.Courses().AddCourse(ctx, database.AddCourseParams{
		ID: 161, Name: "x", WebdavFolder: "/Courses/161",
	}); err != nil {
		t.Fatal(err)
	}
	if err = (settings.Service{Pool: pool}).SaveCredentials(ctx, "u", "p", false); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "s", Path: "/"})
			fmt.Fprint(w, "Welcome")
			return
		}
		if r.URL.Path == "/modules/document/index.php" {
			fmt.Fprint(w, "<title>Έγγραφα</title>"+
				`<a href="/modules/document/file.php?course=INF161&amp;download=/notes.txt">notes</a>`)
			return
		}
		if r.URL.Path == "/modules/document/file.php" {
			w.Header().Set("ETag", `"A"`)
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Disposition", `attachment; filename="notes.txt"`)
			fmt.Fprint(w, "hello notes")
			return
		}
		fmt.Fprint(w, "Welcome")
	}))
	t.Cleanup(upstream.Close)
	objects, err := blob.New(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if err = objects.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	source, err := eclass.New(upstream.URL, "u", "p")
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Login(ctx); err != nil {
		t.Fatal(err)
	}
	svc := Service{Pool: pool, Objects: objects, Temp: t.TempDir()}
	result, err := svc.Sync(ctx, 161, source, upstream.URL+"/modules/document/index.php?course=INF161")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if result.Added != 1 || result.FilesAdded != 1 {
		t.Fatalf("sync published nothing: %+v", result)
	}
	published := "2026-10-06"
	if err = svc.SaveAnnouncements(ctx, 161, []eclass.Announcement{{
		ID: "a1", Title: "T", Link: "http://example.invalid/1",
		Description: "D", Published: &published,
	}}); err != nil {
		t.Fatalf("announcements: %v", err)
	}
	if err = svc.SaveGlobalAnnouncements(ctx, "dept", []eclass.Announcement{{
		ID: "g1", Title: "G", Link: "http://example.invalid/2",
		Description: "D", Published: &published,
	}}); err != nil {
		t.Fatalf("global: %v", err)
	}
}
