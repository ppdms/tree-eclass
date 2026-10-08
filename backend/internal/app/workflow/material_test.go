package workflow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/integrations/parser"
)

type materialPublicationFixture struct {
	c       *Controller
	ctx     context.Context
	conn    *pgx.Conn
	pool    rdbms.Pool
	api     *server.Server
	service materials.Service
	indexer knowledge.Indexer
	temp    string
	result  materials.Result
}

func TestNativeMaterialPublicationAndReader(t *testing.T) {
	fixture := newMaterialPublicationFixture(t)
	materialIndexChecks(t, fixture)
	materialReaderChecks(t, fixture)
}

func newMaterialPublicationFixture(t *testing.T) *materialPublicationFixture {
	t.Helper()
	c := nativeSharedController(t)
	ctx := context.Background()
	conn, objects := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	if _, err := conn.Exec(ctx, "INSERT INTO app.courses(id,name,webdav_folder) VALUES(101,'Συνθετικό μάθημα','/Courses/101')"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	if _, err := conn.Exec(ctx, `INSERT INTO app.preferences(id,download_base_path) VALUES(1,'/University')`); err != nil {
		t.Fatal(err)
	}
	api, err := server.New(ctx, server.Config{
		DatabaseURL: c.databaseURL(), ObjectsRoot: c.testObjectsRoot(),
		Mode:            "test",
		ExternalWorkers: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(api.Close)
	nativePool, err := pgxpool.New(ctx, c.databaseURL())
	pool := rdbms.WrapPostgres(nativePool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	service := materials.Service{Pool: pool, Objects: objects, Temp: t.TempDir()}
	result, err := service.Upload(ctx, materials.Upload{
		CourseID: 101, Name: "notes.txt", MediaType: "text/plain", Type: "student_notes",
		Body: strings.NewReader("Ελληνικές σημειώσεις για τα Δένδρα. A second line."),
	})
	if err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	indexer := knowledge.Indexer{
		Pool: pool, Objects: objects, Temp: temp,
		Parser: parser.New(filepath.Join(c.Repo, ".venv/bin/python"), c.Repo, temp),
	}
	return &materialPublicationFixture{
		c: c, ctx: ctx, conn: conn, pool: pool, api: api, service: service,
		indexer: indexer, temp: temp, result: result,
	}
}

func materialIndexChecks(t *testing.T, fixture *materialPublicationFixture) {
	t.Helper()
	ctx, service, result := fixture.ctx, fixture.service, fixture.result
	_, err := service.Upload(ctx, materials.Upload{
		CourseID: 101, Name: "notes.txt", MediaType: "text/plain", Type: "student_notes",
		Body: strings.NewReader("Ελληνικές σημειώσεις για τα Δένδρα. A second line."),
	})
	if !errors.Is(err, materials.ErrDuplicate) {
		t.Fatal("duplicate publication", err)
	}
	badIndexer := fixture.indexer
	badIndexer.Parser = parser.New(filepath.Join(fixture.temp, "missing-parser"), fixture.c.Repo, fixture.temp)
	if err = badIndexer.Index(ctx, result.DocumentID); err == nil {
		t.Fatal("missing parser succeeded")
	}
	var failedStatus, reason string
	if err = fixture.pool.QueryRow(ctx, `SELECT status,diagnostic_reason FROM knowledge.documents WHERE id=$1`, result.DocumentID).Scan(&failedStatus, &reason); err != nil ||
		failedStatus != "failed" || reason != "extraction_failed" {
		t.Fatal("extraction failure remained pending", failedStatus, reason, err)
	}
	if err = fixture.indexer.Index(ctx, result.DocumentID); err != nil {
		t.Fatal(err)
	}
	var count int
	err = fixture.conn.QueryRow(ctx, "SELECT count(*) FROM knowledge.chunks_fts WHERE search_vector @@ public.tree_query('δενδρα')").Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("Greek full-text index: %d %v", count, err)
	}
}

func materialReaderChecks(t *testing.T, fixture *materialPublicationFixture) {
	t.Helper()
	api, result := fixture.api, fixture.result
	httpServer := httptest.NewServer(api)
	defer httpServer.Close()
	contentURL := httpServer.URL + "/api/study/document/" + result.DocumentID + "/content?course_id=101"
	response := request(t, contentURL, map[string]string{"Range": "bytes=0-3"})
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 206 || len(body) != 4 {
		t.Fatalf("reader range: %d %q", response.StatusCode, body)
	}
	response = request(t, contentURL, map[string]string{"Range": "bytes=0-3", "If-Range": "\"stale\""})
	body, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || len(body) != int(result.Object.Bytes) {
		t.Fatal("If-Range served a partial stale revision")
	}
	response = request(t, httpServer.URL+"/files/etc/passwd", nil)
	response.Body.Close()
	if response.StatusCode != 404 {
		t.Fatal("unregistered logical path was readable")
	}
	materialPresentationChecks(t, fixture.pool, httpServer.URL, result.DocumentID)
	searchChecks(t, fixture.conn, httpServer.URL, result.DocumentID)
	fileViewChecks(t, fixture.pool, httpServer.URL, result.DocumentID)
	archiveContentScopeChecks(t, fixture.pool, httpServer.URL, result.DocumentID)
	navigationBuildChecks(t, fixture.pool, httpServer.URL, result.DocumentID)
	workspaceChecks(t, fixture.pool, httpServer.URL, result.DocumentID)
	readChecks(t, fixture.pool, result.DocumentID)
	annotationChecks(t, fixture.conn, httpServer.URL, result.DocumentID)
	exerciseChecks(t, fixture.conn, httpServer.URL)
	settingsChecks(t, fixture.pool, httpServer.URL)
	chatChecks(t, fixture.pool, httpServer.URL)
	plannerChecks(t, fixture.pool, httpServer.URL)
	studySnapshotChecks(t, fixture.pool, httpServer.URL)
	knowledgeAdminChecks(t, fixture.pool, httpServer.URL, result.DocumentID, fixture.indexer)
	libraryChecks(t, fixture.pool, httpServer.URL, result.DocumentID)
	activityChecks(t, fixture.pool, httpServer.URL)
	queueInvalidationChecks(t, fixture.pool)
	destructiveChecks(t, fixture.pool, httpServer.URL)
	coverageScopeChecks(t, fixture.pool)
	navigationChecks(t, fixture.pool)
	materialUploadMirrorCheck(t, fixture.pool, httpServer.URL)
}

func materialUploadMirrorCheck(t *testing.T, pool rdbms.Pool, base string) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), `UPDATE app.preferences SET download_base_path='/University' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "mirror.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = part.Write([]byte("mirrored upload")); err != nil {
		t.Fatal(err)
	}
	if err = writer.WriteField("material_type", "student_notes"); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", base+"/api/v1/courses/101/materials", &body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("upload mirror request: %d", response.StatusCode)
	}
	path := filepath.Join(os.Getenv("HOME"), "University", "Συνθετικό μάθημα", "external", "student-notes", "mirror.txt")
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "mirrored upload" {
		t.Fatalf("uploaded material mirror: %q %v", content, err)
	}
}

func request(t *testing.T, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
