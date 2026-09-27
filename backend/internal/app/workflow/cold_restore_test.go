package workflow

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/storage"
)

type coldRestoreFixture struct {
	c        *Controller
	ctx      context.Context
	conn     *pgx.Conn
	store    *blob.Store
	ref      blob.Reference
	snapshot checkpoint.Manifest
}

func TestNativeColdRestore(t *testing.T) {
	t.Parallel()
	fixture := prepareColdRestore(t)
	mutateColdDevelopment(t, fixture)
	restoreColdBaseline(t, fixture)
}

func prepareColdRestore(t *testing.T) coldRestoreFixture {
	t.Helper()
	c := nativeController(t)
	ctx := context.Background()
	conn, store := startTestStorage(t, c)
	ref := seedColdBaseline(t, c, ctx, conn, store)
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.stopAll(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.snapshots().
		Create(checkpoint.Manifest{Reason: "development", Versions: c.versions()})
	if err != nil {
		t.Fatal(err)
	}
	conn, store = startTestStorage(t, c)
	return coldRestoreFixture{c: c, ctx: ctx, conn: conn, store: store, ref: ref, snapshot: snapshot}
}

func seedColdBaseline(
	t *testing.T,
	c *Controller,
	ctx context.Context,
	conn *pgx.Conn,
	store *blob.Store,
) blob.Reference {
	t.Helper()
	if _, err := conn.Exec(ctx, "INSERT INTO app.courses(id,name,webdav_folder) VALUES(101,'Σταθερό μάθημα','/Courses/101')"); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(ctx, strings.NewReader("stable document"), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Idempotent uploads resolve the same content-addressed object rather than duplicating it.
	same, err := store.Put(ctx, strings.NewReader("stable document"), "text/plain", t.TempDir())
	if err != nil || same.VersionID != ref.VersionID || same.MediaType != ref.MediaType {
		t.Fatalf("immutable retry: %#v %v", same, err)
	}
	if err = storage.RegisterObject(ctx, conn, ref); err != nil {
		t.Fatal(err)
	}
	if err = storage.RegisterObject(ctx, conn, same); err != nil {
		t.Fatal("idempotent catalog registration", err)
	}
	changed := ref
	changed.VersionID = "must-not-relabel-a-revision"
	if err = storage.RegisterObject(ctx, conn, changed); err == nil {
		t.Fatal("object ID silently relabeled to a different object version")
	}
	if err = storage.Migrate(ctx, c.databaseURL()); err != nil {
		t.Fatal("idempotent migration", err)
	}
	return ref
}

func mutateColdDevelopment(t *testing.T, fixture coldRestoreFixture) {
	t.Helper()
	ctx, c, conn, store, ref := fixture.ctx, fixture.c, fixture.conn, fixture.store, fixture.ref
	if _, err := conn.Exec(ctx, "ALTER TABLE app.courses DROP COLUMN name CASCADE; CREATE TABLE app.dev_only(id int)"); err != nil {
		t.Fatal(err)
	}
	// Out-of-band loss: the content-addressed file vanishes after the snapshot was taken.
	if err := os.Remove(filepath.Join(c.testObjectsRoot(), ref.SHA256)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, strings.NewReader("dev-only document"), "text/plain", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.active(), "settings", "dev-only"), []byte("discard"), 0600); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	if err := c.stopAll(); err != nil {
		t.Fatal(err)
	}
	if err := c.snapshots().Restore(fixture.snapshot.ID); err != nil {
		t.Fatal(err)
	}
}

func restoreColdBaseline(t *testing.T, fixture coldRestoreFixture) {
	t.Helper()
	ctx, c := fixture.ctx, fixture.c
	conn, store := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	var name string
	if err := conn.QueryRow(ctx, "SELECT name FROM app.courses WHERE id=101").Scan(&name); err != nil ||
		name != "Σταθερό μάθημα" {
		t.Fatalf("relational restore: %q %v", name, err)
	}
	var extra *string
	if err := conn.QueryRow(ctx, "SELECT to_regclass('app.dev_only')::text").Scan(&extra); err != nil || extra != nil {
		t.Fatalf("development schema survived: %v %v", extra, err)
	}
	object, err := store.Open(ctx, fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(object)
	object.Close()
	if err != nil || string(content) != "stable document" {
		t.Fatalf("object restore: %q %v", content, err)
	}
	if _, err = os.Stat(filepath.Join(c.active(), "settings", "dev-only")); !os.IsNotExist(err) {
		t.Fatal("development settings survived")
	}
	entries, err := os.ReadDir(filepath.Join(c.testObjectsRoot()))
	if err != nil || len(entries) != 1 || entries[0].Name() != fixture.ref.SHA256 {
		t.Fatalf("object inventory: %v %v", entries, err)
	}
	t.Logf("restored database, incompatible schema, deleted object file and settings from %s", fixture.snapshot.ID)
}
