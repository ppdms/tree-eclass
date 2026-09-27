package workflow

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/eclass"
	"tree-eclass/internal/services/synchronization"
)

type syncFixture struct {
	ctx         context.Context
	c           *Controller
	conn        *pgx.Conn
	pool        *pgxpool.Pool
	objects     *blob.Store
	generation  *atomic.Int32
	downloads   *atomic.Int32
	service     synchronization.Service
	source      *eclass.Client
	root        string
	upstreamURL string
	document    string
	firstObject queries.DocumentObjectRow
}

func TestNativeSynchronizationRevisionAndFailure(t *testing.T) {
	fixture := newSyncFixture(t)
	syncRevisionChecks(t, fixture)
	syncArchiveChecks(t, fixture)
	syncMetadataChecks(t, fixture.pool, fixture.service)
	_, err := fixture.service.Run(
		fixture.ctx, synchronization.Request{CourseID: new(int64(101))}, fixture.upstreamURL,
	)
	if err != nil {
		t.Fatal("complete check orchestration", err)
	}
	syncAPIChecks(t, fixture.c)
}

func newSyncFixture(t *testing.T) *syncFixture {
	t.Helper()
	c := nativeSharedController(t)
	ctx := context.Background()
	conn, objects := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	if _, err := conn.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(101,'Συνθετικό','/Courses/101')`); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var generation, downloads atomic.Int32
	upstreamURL := newSyncUpstream(t, &generation, &downloads)
	source, err := eclass.New(upstreamURL, "student", "secret")
	if err != nil {
		t.Fatal(err)
	}
	service := synchronization.Service{Pool: pool, Objects: objects, MirrorObjects: objects, Temp: t.TempDir()}
	if _, err = conn.Exec(ctx, `INSERT INTO app.credentials(id,username,password) VALUES(1,'synthetic','synthetic')`); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	if _, err = conn.Exec(ctx, `INSERT INTO app.preferences(id,download_base_path) VALUES(1,'/University')`); err != nil {
		t.Fatal("configure automatic mirror", err)
	}
	if _, err = (materials.Service{Pool: pool, Objects: objects, Temp: t.TempDir()}).Upload(ctx, materials.Upload{
		CourseID: 101, Name: "June.pdf", Type: "past_paper", MediaType: "application/pdf",
		Body: strings.NewReader("synthetic past paper"),
	}); err != nil {
		t.Fatal("publish external material", err)
	}
	courseID := int64(101)
	first, err := service.Run(ctx, synchronization.Request{CourseID: &courseID}, upstreamURL)
	if err != nil || first.Added != 1 {
		t.Fatalf("initial automatic check: %#v %v", first, err)
	}
	mirrored, err := os.ReadFile(filepath.Join(os.Getenv("HOME"), "University", "Συνθετικό", "eclass", "notes.txt"))
	if err != nil || string(mirrored) != "Ελληνικές σημειώσεις A" {
		t.Fatalf("course check did not refresh configured mirror: %q %v", mirrored, err)
	}
	external, err := os.ReadFile(filepath.Join(
		os.Getenv("HOME"), "University", "Συνθετικό", "external", "past-papers", "June.pdf",
	))
	if err != nil || string(external) != "synthetic past paper" {
		t.Fatalf("course check did not refresh external mirror: %q %v", external, err)
	}
	var document string
	if err = pool.QueryRow(
		ctx,
		`SELECT id FROM knowledge.documents WHERE course_id=101 AND source_origin='eclass'`,
	).Scan(&document); err != nil {
		t.Fatal(err)
	}
	firstObject, err := queries.New(pool).DocumentObject(ctx, queries.DocumentObjectParams{DocumentID: document})
	if err != nil {
		t.Fatal(err)
	}
	return &syncFixture{
		ctx: ctx, c: c, conn: conn, pool: pool, objects: objects, generation: &generation,
		downloads: &downloads, service: service, source: source,
		root: upstreamURL + "/modules/document/index.php?course=INF101", upstreamURL: upstreamURL,
		document: document, firstObject: firstObject,
	}
}

func newSyncUpstream(t *testing.T, generation, downloads *atomic.Int32) string {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "synthetic", Path: "/"})
			fmt.Fprint(w, "Welcome")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/modules/announcements/") {
			syncAnnouncementResponse(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/modules/work/") {
			fmt.Fprint(w, "Δεν υπάρχουν εργασίες")
			return
		}
		if syncDocumentIndex(w, r, generation.Load()) {
			return
		}
		if r.URL.Path != "/modules/document/file.php" {
			fmt.Fprint(w, "Welcome")
			return
		}
		syncFileResponse(w, r, generation, downloads)
	}))
	t.Cleanup(upstream.Close)
	return upstream.URL
}

func syncAnnouncementResponse(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "rss.php") {
		fmt.Fprint(
			w,
			`<rss><channel><item><title>Feed</title><link>https://example.invalid/feed</link></item></channel></rss>`,
		)
		return
	}
	fmt.Fprint(w, `<a class="tiny-icon-rss" href="/modules/announcements/rss.php?token=synthetic">RSS</a>`)
}

func syncDocumentIndex(w http.ResponseWriter, r *http.Request, generation int32) bool {
	if r.URL.Path != "/modules/document/index.php" {
		return false
	}
	fmt.Fprint(w, "<title>Έγγραφα</title>")
	if generation != 4 {
		fmt.Fprint(
			w,
			`<a href="/modules/document/file.php?course=INF101&amp;download=/notes.txt">Σημειώσεις</a>`,
		)
	}
	if generation == 3 {
		fmt.Fprint(w, `<a href="/modules/document/index.php?course=INF101&amp;openDir=/broken">broken</a>`)
	}
	if r.URL.Query().Get("openDir") == "/broken" {
		fmt.Fprint(w, strings.Repeat("x", 8<<20))
	}
	return true
}

func syncFileResponse(w http.ResponseWriter, _ *http.Request, generation, downloads *atomic.Int32) {
	etag, body := `"A"`, "Ελληνικές σημειώσεις A"
	if generation.Load() == 1 || generation.Load() == 3 {
		etag, body = `"B"`, "Ελληνικές σημειώσεις B"
	}
	downloads.Add(1)
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Disposition", `attachment; filename="notes.txt"`)
	fmt.Fprint(w, body)
}

func syncRevisionChecks(t *testing.T, fixture *syncFixture) {
	t.Helper()
	ctx, service := fixture.ctx, fixture.service
	unchanged, err := service.Sync(ctx, 101, fixture.source, fixture.root)
	if err != nil || len(unchanged.Changes) != 0 || fixture.downloads.Load() != 2 {
		t.Fatalf("conditional sync: %#v %v downloads=%d", unchanged, err, fixture.downloads.Load())
	}
	fixture.generation.Store(1)
	changed, err := service.Sync(ctx, 101, fixture.source, fixture.root)
	if err != nil || changed.Modified != 1 {
		t.Fatalf("modified: %#v %v", changed, err)
	}
	q := queries.New(fixture.pool)
	b, err := q.DocumentObject(ctx, queries.DocumentObjectParams{DocumentID: fixture.document})
	if err != nil || b.Sha256 == fixture.firstObject.Sha256 {
		t.Fatalf("new revision: %#v %v", b, err)
	}
	fixture.generation.Store(2)
	if _, err = service.Sync(ctx, 101, fixture.source, fixture.root); err != nil {
		t.Fatal(err)
	}
	returned, err := q.DocumentObject(ctx, queries.DocumentObjectParams{DocumentID: fixture.document})
	if err != nil || returned.Sha256 != fixture.firstObject.Sha256 || returned.VersionID != fixture.firstObject.VersionID {
		t.Fatalf("A→B→A served wrong revision: %#v %v", returned, err)
	}
	fixture.generation.Store(3)
	if _, err = service.Sync(ctx, 101, fixture.source, fixture.root); err == nil {
		t.Fatal("incomplete crawl was committed")
	}
	retained, err := q.DocumentObject(ctx, queries.DocumentObjectParams{DocumentID: fixture.document})
	if err != nil || retained.Sha256 != fixture.firstObject.Sha256 {
		t.Fatal("failed crawl changed active catalog", err)
	}
	fixture.generation.Store(4)
	deleted, err := service.Sync(ctx, 101, fixture.source, fixture.root)
	if err != nil || deleted.Deleted != 1 {
		t.Fatalf("delete: %#v %v", deleted, err)
	}
	if _, err = q.DocumentObject(ctx, queries.DocumentObjectParams{DocumentID: fixture.document}); err == nil {
		t.Fatal("deleted source remained current")
	}
}

func syncArchiveChecks(t *testing.T, fixture *syncFixture) {
	t.Helper()
	ctx := fixture.ctx
	var alias string
	if err := fixture.conn.QueryRow(ctx, `SELECT version_webdav_path FROM app.file_versions WHERE change_type='deleted'`).Scan(&alias); err != nil {
		t.Fatal(err)
	}
	archive, err := (knowledge.Reader{Pool: fixture.pool}).LogicalContent(ctx, alias)
	if err != nil || archive.Object.VersionID != fixture.firstObject.VersionID {
		t.Fatal("deleted archive lost original version", err)
	}
	content, err := fixture.objects.Get(ctx, archive.Object, "")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(content.Body)
	_ = content.Body.Close()
	if err != nil || string(body) != "Ελληνικές σημειώσεις A" {
		t.Fatal("archive bytes", string(body), err)
	}
}
