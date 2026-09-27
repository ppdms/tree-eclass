package workflow

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
	"tree-eclass/internal/integrations/parser"
)

func fixtureZIP(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var out bytes.Buffer
	z := zip.NewWriter(&out)
	for name, data := range files {
		w, err := z.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func TestNativeArchiveMemberPublication(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"zip", "rar"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			archiveMemberPublication(t, format)
		})
	}
}

type archivePublicationFixture struct {
	ctx          context.Context
	temp         string
	pool         *pgxpool.Pool
	objects      *blob.Store
	indexer      knowledge.Indexer
	reader       knowledge.Reader
	parent       materials.Result
	notes        []byte
	mediaType    string
	archiveBytes func(map[string][]byte) []byte
}

func archiveMemberPublication(t *testing.T, format string) {
	fixture := newArchivePublicationFixture(t, format)
	_, unchanged, removed := archiveChildren(t, fixture)
	archiveReplacementChecks(t, fixture, unchanged, removed)
	archiveUnsafeChecks(t, fixture, unchanged)
	archiveSyncChecks(t, fixture.indexer)
	files, err := os.ReadDir(fixture.temp)
	if err != nil || len(files) != 0 {
		t.Fatal("archive parser leaked local artifacts", files, err)
	}
}

func newArchivePublicationFixture(t *testing.T, format string) archivePublicationFixture {
	t.Helper()
	c := nativeSharedController(t)
	ctx := t.Context()
	conn, objects := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err = pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(791,'Archive fixture','/Courses/791')`); err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	runner := parser.New(filepath.Join(c.Repo, ".venv/bin/python"), c.Repo, temp)
	indexer := knowledge.Indexer{Pool: pool, Objects: objects, Parser: runner, Temp: temp}
	notes := []byte("Δένδρα και αλγόριθμοι. A connected graph without cycles.")
	nested := fixtureZIP(t, map[string][]byte{"extra.txt": []byte("Nested original source")})
	mediaType := "application/zip"
	if format == "rar" {
		mediaType = "application/vnd.rar"
	}
	archive := archiveFixtureBytes(t, format, notes, nested)
	parent, err := (materials.Service{Pool: pool, Objects: objects, Temp: temp}).Upload(
		ctx,
		materials.Upload{CourseID: 791, Name: "bundle." + format, MediaType: mediaType, Body: bytes.NewReader(archive)},
	)
	if err != nil {
		t.Fatal(err)
	}
	return archivePublicationFixture{
		ctx: ctx, temp: temp, pool: pool, objects: objects, indexer: indexer,
		reader: knowledge.Reader{Pool: pool}, parent: parent, notes: notes, mediaType: mediaType,
		archiveBytes: func(files map[string][]byte) []byte {
			if format == "rar" {
				return fixtureRAR(files)
			}
			return fixtureZIP(t, files)
		},
	}
}

func archiveFixtureBytes(t *testing.T, format string, notes, nested []byte) []byte {
	files := map[string][]byte{"σημειώσεις.txt": notes, "nested.zip": nested}
	if format == "rar" {
		return fixtureRAR(files)
	}
	return fixtureZIP(t, files)
}

func archiveChildren(t *testing.T, fixture archivePublicationFixture) ([]string, string, string) {
	t.Helper()
	ctx, pool, parent := fixture.ctx, fixture.pool, fixture.parent
	if err := fixture.indexer.Index(ctx, parent.DocumentID); err != nil {
		t.Fatal("archive indexing", err)
	}
	var parentChunks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM knowledge.chunks WHERE document_id=$1`, parent.DocumentID).Scan(&parentChunks); err != nil ||
		parentChunks != 0 {
		t.Fatal("archive source was flattened outside its admitted leaves", parentChunks)
	}
	rows, err := pool.Query(
		ctx,
		`SELECT d.id FROM knowledge.archive_members m JOIN knowledge.documents d ON d.id=m.child_document_id WHERE m.parent_document_id=$1 ORDER BY d.id`,
		parent.DocumentID,
	)
	if err != nil {
		t.Fatal(err)
	}
	children, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(children) != 2 {
		t.Fatal("admitted children", children, err)
	}
	var unchanged, removed string
	for _, id := range children {
		if err = fixture.indexer.Index(ctx, id); err != nil {
			t.Fatal("child indexing", err)
		}
		content, err := fixture.reader.Content(ctx, 791, id, "")
		if err != nil {
			t.Fatal(err)
		}
		source, err := fixture.objects.Open(ctx, content.Object)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(source)
		source.Close()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(data, fixture.notes) {
			unchanged = id
		} else if string(data) == "Nested original source" {
			removed = id
		} else {
			t.Fatal("member bytes changed", string(data))
		}
	}
	if unchanged == "" || removed == "" {
		t.Fatal("missing leaf identity")
	}
	return children, unchanged, removed
}

func archiveReplacementChecks(t *testing.T, fixture archivePublicationFixture, unchanged, removed string) {
	t.Helper()
	ctx, parent := fixture.ctx, fixture.parent
	next := fixture.archiveBytes(map[string][]byte{"σημειώσεις.txt": fixture.notes, "replacement.txt": []byte("Replacement leaf")})
	replaceArchiveFixture(t, fixture.pool, fixture.objects, fixture.temp, parent.DocumentID, next, fixture.mediaType)
	if _, err := fixture.reader.Content(ctx, 791, unchanged, ""); err == nil {
		t.Fatal("child remained readable while parent revision was pending")
	}
	if err := fixture.indexer.Index(ctx, parent.DocumentID); err != nil {
		t.Fatal("changed archive", err)
	}
	if _, err := fixture.reader.Content(ctx, 791, unchanged, ""); err != nil {
		t.Fatal("unchanged leaf lost its identity or ready index", err)
	}
	if _, err := fixture.reader.Content(ctx, 791, removed, ""); err == nil {
		t.Fatal("removed leaf remained current")
	}
	var current int64
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM knowledge.archive_members m JOIN knowledge.documents d ON d.id=m.child_document_id WHERE m.parent_document_id=$1 AND d.is_current=1`, parent.DocumentID).Scan(&current); err != nil ||
		current != 2 {
		t.Fatal("member replacement was not atomic", current, err)
	}
}

func archiveUnsafeChecks(t *testing.T, fixture archivePublicationFixture, unchanged string) {
	t.Helper()
	ctx, parent := fixture.ctx, fixture.parent
	unsafe := fixture.archiveBytes(map[string][]byte{"../escape.txt": []byte("unsafe")})
	replaceArchiveFixture(t, fixture.pool, fixture.objects, fixture.temp, parent.DocumentID, unsafe, fixture.mediaType)
	if err := fixture.indexer.Index(ctx, parent.DocumentID); err == nil {
		t.Fatal("unsafe archive admitted")
	}
	if _, err := fixture.reader.Content(ctx, 791, unchanged, ""); err == nil {
		t.Fatal("failed parent authorized old child")
	}
	var current int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM knowledge.archive_members WHERE parent_document_id=$1`, parent.DocumentID).Scan(&current); err != nil ||
		current != 3 {
		t.Fatal("failed scan overwrote prior membership history", current, err)
	}
}

func replaceArchiveFixture(
	t *testing.T,
	pool *pgxpool.Pool,
	objects *blob.Store,
	temp, document string,
	data []byte,
	mediaType string,
) {
	t.Helper()
	ctx := t.Context()
	ref, err := objects.Put(ctx, bytes.NewReader(data), mediaType, temp)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	q := queries.New(tx)
	d, err := q.IndexDocument(ctx, document)
	if err != nil {
		t.Fatal(err)
	}
	if err = storage.RegisterObject(ctx, tx, ref); err != nil {
		t.Fatal(err)
	}
	if err = q.RegisterRevision(
		ctx,
		queries.RegisterRevisionParams{
			ID:          identity.Stable("rev", document, ref.SHA256),
			DocumentID:  document,
			CourseID:    d.CourseID,
			LogicalPath: d.NormalizedPath,
			ObjectID:    ref.SHA256,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE knowledge.documents SET source_hash=$2,source_size_bytes=$3,status='pending' WHERE id=$1`, document, ref.SHA256, ref.Bytes); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
