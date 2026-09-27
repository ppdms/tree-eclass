package workflow

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
)

type objectCollectionFixture struct {
	c     *Controller
	ctx   context.Context
	conn  *pgx.Conn
	store *blob.Store
	refs  []blob.Reference
}

func TestNativeObjectCollectionPreservesHistory(t *testing.T) {
	t.Parallel()
	fixture := newObjectCollectionFixture(t)
	c, ctx := fixture.c, fixture.ctx
	conn, refs := fixture.conn, fixture.refs
	t.Cleanup(func() { conn.Close(ctx) })
	seedCollectionReferences(t, ctx, conn, refs)
	addOrphanObjects(t, fixture)
	conn.Close(ctx)
	if err := c.stopAll(); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("c", 40)
	fixtureRelease(t, c, id, false)
	c.State.Release = id
	c.State.Mode = "development"
	c.State.Baseline = "protected-session"
	if err := c.Collect(ctx); err == nil {
		t.Fatal("collection admitted during development")
	}
	c.State.Mode = "stopped"
	c.State.Baseline = ""
	if err := c.Collect(ctx); err != nil {
		_ = c.Logs("collection")
		t.Fatal(err)
	}
	stoppedFixture(t, c)
	fixture.conn, fixture.store = startTestStorage(t, c)
	t.Cleanup(func() { fixture.conn.Close(ctx) })
	verifyCollection(t, fixture)
}

func newObjectCollectionFixture(t *testing.T) *objectCollectionFixture {
	t.Helper()
	c := nativeController(t)
	ctx := t.Context()
	conn, store := startTestStorage(t, c)
	return &objectCollectionFixture{
		c: c, ctx: ctx, conn: conn, store: store, refs: historicalObjects(t, ctx, conn, store),
	}
}

func historicalObjects(t *testing.T, ctx context.Context, conn *pgx.Conn, store *blob.Store) []blob.Reference {
	t.Helper()
	refs := make([]blob.Reference, 8)
	for i := range refs {
		var err error
		refs[i], err = store.Put(
			ctx, strings.NewReader(fmt.Sprintf("retained historical fixture %d", i)), "text/plain", t.TempDir(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = storage.RegisterObject(ctx, conn, refs[i]); err != nil {
			t.Fatal(err)
		}
	}
	return refs
}

func seedCollectionReferences(t *testing.T, ctx context.Context, conn *pgx.Conn, refs []blob.Reference) {
	t.Helper()
	statements := []struct {
		sql  string
		args []any
	}{
		{
			`INSERT INTO app.courses(id,name,webdav_folder,hidden) VALUES(991,'Hidden historical fixture','/Courses/991',1)`,
			nil,
		},
		{
			`INSERT INTO app.document_revisions(id,document_id,course_id,logical_path,object_id,deleted_at) VALUES('removed','removed',991,'deleted.pdf',$1,now())`,
			[]any{refs[0].SHA256},
		},
		{
			`INSERT INTO messages.archive_sources(path,root_id,course_id,fingerprint,channel_id,status,indexed_at,object_id) VALUES('removed-export','123',991,'fixture',123,'ready','fixture',$1)`,
			[]any{refs[1].SHA256},
		},
		{
			`INSERT INTO messages.archive_media(source_path,relative_path,object_id) VALUES('removed-export','media/attachment',$1)`,
			[]any{refs[2].SHA256},
		},
		{
			`INSERT INTO app.pdf_differences(id,course_id,old_object_id,new_object_id,object_id,tool_version,status) VALUES('old-comparison',991,$1,$2,$3,'fixture','ready')`,
			[]any{refs[3].SHA256, refs[4].SHA256, refs[5].SHA256},
		},
		{`CREATE TABLE app.fixture_future_reference(object_id text REFERENCES app.objects(id) ON DELETE CASCADE)`, nil},
		{`INSERT INTO app.fixture_future_reference VALUES($1)`, []any{refs[6].SHA256}},
	}
	for _, s := range statements {
		if _, err := conn.Exec(ctx, s.sql, s.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func addOrphanObjects(t *testing.T, fixture *objectCollectionFixture) {
	t.Helper()
	root := fixture.c.testObjectsRoot()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	// Hundreds of unregistered content-addressed files exercise the bulk sweep.
	for i := 0; i < 505; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%064x", i)), []byte("orphan"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Foreign namespaces: a non-hex file and a nested directory must survive every sweep.
	if err := os.WriteFile(filepath.Join(root, "foreign-file"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "foreign-dir", "nested")
	if err := os.MkdirAll(filepath.Dir(nested), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nested, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
}

func verifyCollection(t *testing.T, fixture *objectCollectionFixture) {
	t.Helper()
	ctx, conn, store, refs := fixture.ctx, fixture.conn, fixture.store, fixture.refs
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM app.objects`).Scan(&count); err != nil || count != 7 {
		t.Fatal("catalog lost history or retained abandoned entry", count, err)
	}
	for _, ref := range refs[:7] {
		object, err := store.Open(ctx, ref)
		if err != nil {
			t.Fatal("referenced historical object deleted", err)
		}
		b, err := io.ReadAll(object)
		object.Close()
		if err != nil || len(b) == 0 {
			t.Fatal("historical bytes unavailable", err)
		}
	}
	if object, err := store.Open(ctx, refs[7]); err == nil {
		object.Close()
		t.Fatal("unreferenced catalog object survived")
	}
	root := fixture.c.testObjectsRoot()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 9 {
		t.Fatal("orphan sweep or namespace preservation failed", len(entries))
	}
	kept := make(map[string]bool, len(entries))
	for _, entry := range entries {
		kept[entry.Name()] = true
	}
	for _, ref := range refs[:7] {
		if !kept[ref.SHA256] {
			t.Fatal("referenced object file missing", ref.SHA256)
		}
	}
	if !kept["foreign-file"] {
		t.Fatal("foreign object deleted")
	}
	nested, err := os.ReadFile(filepath.Join(root, "foreign-dir", "nested"))
	if err != nil || string(nested) != "preserve" {
		t.Fatal("foreign directory deleted", err)
	}
}
