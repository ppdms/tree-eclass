package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/eclass"
	"tree-eclass/internal/integrations/pdfdiff"
	"tree-eclass/internal/services/synchronization"
)

func tinyPDF(label string) []byte {
	stream := "BT /F1 20 Tf 30 150 Td (" + label + ") Tj ET\n"
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(stream), stream),
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
	}
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	for i, object := range objects {
		offsets = append(offsets, out.Len())
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	start := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(offsets))
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), start)
	return out.Bytes()
}

type pdfSource struct{ Data []byte }

func (s pdfSource) Page(context.Context, string) ([]byte, error) {
	return []byte(
		`<title>Έγγραφα</title><a href="/modules/document/file.php?course=INF743&amp;download=/notes.pdf">notes.pdf</a>`,
	), nil
}
func (s pdfSource) Download(context.Context, string, string, string) (eclass.Download, error) {
	return eclass.Download{
		Name:      "notes.pdf",
		MediaType: "application/pdf",
		Body:      io.NopCloser(bytes.NewReader(s.Data)),
	}, nil
}
func (s pdfSource) Drive(context.Context, string, string) (eclass.Download, error) {
	return eclass.Download{}, errors.New("unexpected Drive request")
}

type countDiff struct {
	Runner pdfdiff.Runner
	Calls  int
}

func (c *countDiff) Run(ctx context.Context, dir, old, next string) (bool, error) {
	c.Calls++
	return c.Runner.Run(ctx, dir, old, next)
}

type pdfDifferenceFixture struct {
	c            *Controller
	ctx          context.Context
	pool         *pgxpool.Pool
	objects      *blob.Store
	synchronizer synchronization.Service
	runner       *countDiff
	service      pdfdiff.Service
	first        []byte
	second       []byte
	id           string
	alias        string
	temp         string
}

func TestNativePDFDifferences(t *testing.T) {
	t.Parallel()
	fixture := newPDFDifferenceFixture(t)
	pdfDifferenceRouteChecks(t, fixture)
	pdfDifferenceRunnerChecks(t, fixture)
}

func newPDFDifferenceFixture(t *testing.T) *pdfDifferenceFixture {
	t.Helper()
	c := nativeSharedController(t)
	conn, objects := startTestStorage(t, c)
	ctx := t.Context()
	t.Cleanup(func() { conn.Close(ctx) })
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err = pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(743,'Visual revisions','/Courses/743')`); err != nil {
		t.Fatal(err)
	}
	synchronizer := synchronization.Service{Pool: pool, Objects: objects, Temp: t.TempDir()}
	first, second := tinyPDF("First revision"), tinyPDF("Second revision")
	for _, data := range [][]byte{first, second} {
		if _, err = synchronizer.Sync(ctx, 743, pdfSource{data}, "https://example.invalid/modules/document/index.php?course=INF743"); err != nil {
			t.Fatal(err)
		}
	}
	var id string
	if err = pool.QueryRow(ctx, `SELECT id FROM app.pdf_differences`).Scan(&id); err != nil {
		t.Fatal("modified PDF was not queued", err)
	}
	binary, hash, err := pdfTool()
	if err != nil {
		t.Fatal(err)
	}
	runner := &countDiff{Runner: pdfdiff.NativeRunner{Binary: binary, SHA256: hash}}
	temp := t.TempDir()
	service := pdfdiff.Service{Pool: pool, Objects: objects, Temp: temp, Runner: runner}
	if err = service.Process(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = service.Process(ctx, id); err != nil || runner.Calls != 1 {
		t.Fatal("completed diff reran", runner.Calls, err)
	}
	var alias string
	if err = pool.QueryRow(ctx, `SELECT diff_webdav_path FROM app.file_versions WHERE pdf_difference_id=$1 LIMIT 1`, id).Scan(&alias); err != nil ||
		alias != pdfdiff.Alias(id) {
		t.Fatal("history link", alias, err)
	}
	return &pdfDifferenceFixture{
		c: c, ctx: ctx, pool: pool, objects: objects, synchronizer: synchronizer, runner: runner,
		service: service, first: first, second: second, id: id, alias: alias, temp: temp,
	}
}

func pdfDifferenceRouteChecks(t *testing.T, fixture *pdfDifferenceFixture) {
	t.Helper()
	ctx, c, alias := fixture.ctx, fixture.c, fixture.alias
	object, err := fixture.service.Content(ctx, pdfdiff.Alias(fixture.id))
	if err != nil {
		t.Fatal(err)
	}
	if object.Bytes == 0 || object.MediaType != "application/pdf" {
		t.Fatal("invalid output catalog", object)
	}
	api, err := server.New(ctx, server.Config{
		Mode: "test", DatabaseURL: c.databaseURL(), S3Endpoint: c.endpoint(),
		S3Access: c.Config.S3Access, S3Secret: c.Config.S3Secret, Temp: fixture.temp,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(api.Close)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest("GET", "http://127.0.0.1/files"+alias, nil))
	if response.Code != 200 || !strings.HasPrefix(response.Body.String(), "%PDF-") {
		t.Fatal("difference content route", response.Code, response.Body.String())
	}
	for _, data := range [][]byte{fixture.first, fixture.second} {
		if _, err = fixture.synchronizer.Sync(ctx, 743, pdfSource{data}, "https://example.invalid/modules/document/index.php?course=INF743"); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err = fixture.pool.QueryRow(ctx, `SELECT count(*) FROM app.pdf_differences`).Scan(&count); err != nil || count != 2 {
		t.Fatal("A-B-A-B diff identity", count, err)
	}
	if err = fixture.pool.QueryRow(ctx, `SELECT count(*) FROM app.file_versions WHERE pdf_difference_id=$1 AND diff_webdav_path=$2`, fixture.id, alias).Scan(&count); err != nil ||
		count != 2 {
		t.Fatal("reused diff link", count, err)
	}
	if _, err = fixture.pool.Exec(ctx, `UPDATE app.courses SET hidden=1 WHERE id=743`); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.service.Content(ctx, alias); err == nil {
		t.Fatal("hidden course difference exposed")
	}
	entries, err := os.ReadDir(fixture.temp)
	if err != nil || len(entries) != 0 {
		t.Fatal("difference temporaries retained", entries, err)
	}
}

func pdfDifferenceRunnerChecks(t *testing.T, fixture *pdfDifferenceFixture) {
	t.Helper()
	identical := t.TempDir()
	old, next := filepath.Join(identical, "old.pdf"), filepath.Join(identical, "next.pdf")
	if err := os.WriteFile(old, fixture.first, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(next, fixture.first, 0600); err != nil {
		t.Fatal(err)
	}
	if different, err := fixture.runner.Run(fixture.ctx, identical, old, next); err != nil || different {
		t.Fatal("identical PDFs", different, err)
	}
	if err := os.WriteFile(next, []byte("not a PDF"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.runner.Run(fixture.ctx, identical, old, next); err == nil {
		t.Fatal("invalid PDF accepted")
	}
}
