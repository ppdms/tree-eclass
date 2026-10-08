package workflow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/eclass"
	"tree-eclass/internal/services/synchronization"
)

type archiveSourceFixture struct {
	Data  []byte
	Empty bool
}

func (s *archiveSourceFixture) Page(context.Context, string) ([]byte, error) {
	if s.Empty {
		return []byte("<title>Έγγραφα</title>"), nil
	}
	return []byte(
		`<title>Έγγραφα</title><a href="/modules/document/file.php?course=INF792&amp;download=/bundle.zip">bundle.zip</a>`,
	), nil
}
func (s *archiveSourceFixture) Download(_ context.Context, _, _, etag string) (eclass.Download, error) {
	if etag != "" {
		return eclass.Download{Unchanged: true}, nil
	}
	return eclass.Download{
		Name:      "bundle.zip",
		ETag:      "fixture",
		MediaType: "application/zip",
		Body:      io.NopCloser(bytes.NewReader(s.Data)),
	}, nil
}
func (s *archiveSourceFixture) Drive(context.Context, string, string) (eclass.Download, error) {
	return eclass.Download{}, errors.New("unexpected Drive request")
}
func archiveSyncChecks(t *testing.T, pool *fixtureStore, indexer knowledge.Indexer) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Native.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(792,'Official archive','/Courses/792')`); err != nil {
		t.Fatal(err)
	}
	source := &archiveSourceFixture{
		Data: fixtureZIP(t, map[string][]byte{"notes.txt": []byte("Official leaf content")}),
	}
	service := synchronization.Service{Pool: pool, Objects: indexer.Objects, Temp: indexer.Temp}
	root := "https://example.invalid/modules/document/index.php?course=INF792"
	if _, err := service.Sync(ctx, 792, source, root); err != nil {
		t.Fatal(err)
	}
	var parent, child string
	if err := pool.Native.QueryRow(ctx, `SELECT id FROM knowledge.documents WHERE course_id=792`).Scan(&parent); err != nil {
		t.Fatal(err)
	}
	if err := indexer.Index(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if err := pool.Native.QueryRow(ctx, `SELECT child_document_id FROM knowledge.archive_members WHERE parent_document_id=$1`, parent).Scan(&child); err != nil {
		t.Fatal(err)
	}
	if err := indexer.Index(ctx, child); err != nil {
		t.Fatal(err)
	}
	reader := knowledge.Reader{Pool: pool}
	if _, err := service.Sync(ctx, 792, source, root); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Content(ctx, 792, child, ""); err != nil {
		t.Fatal("unchanged sync removed admitted archive child", err)
	}
	source.Empty = true
	if _, err := service.Sync(ctx, 792, source, root); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Content(ctx, 792, child, ""); err == nil {
		t.Fatal("removed official parent left child readable")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	readiness, err := knowledge.Readiness(ctx, tx, 792, settings.DefaultAI())
	tx.Rollback(ctx)
	if err != nil || readiness["ready_documents"] != int64(0) || readiness["ready"] != false {
		t.Fatal("orphaned child inflated readiness", readiness, err)
	}
	source.Empty = false
	if _, err := service.Sync(ctx, 792, source, root); err != nil {
		t.Fatal(err)
	}
	if err := indexer.Index(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Content(ctx, 792, child, ""); err != nil {
		t.Fatal("reappearing parent lost unchanged child", err)
	}
}
