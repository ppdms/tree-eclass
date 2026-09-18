package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/infrastructure/checkpoint"
)

func TestNativeDevelopmentMigrationCheckpointIsDisposable(t *testing.T) {
	c := nativeController(t)
	development := seedDisposableMigrationFixture(t, c)
	rejectDisposableMigrationPromotion(t, c, development)
	verifyDevelopmentExitRestoresBaseline(t, c, development)
}

func seedDisposableMigrationFixture(t *testing.T, c *Controller) string {
	t.Helper()
	ctx := t.Context()
	conn, _ := startTestStorage(t, c)
	if _, err := conn.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(771,'Stable fixture','/Courses/771')`); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	if err := c.stopAll(); err != nil {
		t.Fatal(err)
	}
	baseline, err := c.snapshots().Create(checkpoint.Manifest{Reason: "development", Versions: c.versions()})
	if err != nil {
		t.Fatal(err)
	}
	c.State.Mode = "development"
	c.State.Baseline = baseline.ID
	conn, _ = startTestStorage(t, c)
	if _, err = conn.Exec(ctx, `UPDATE app.courses SET name='Development edit' WHERE id=771`); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	broken := strings.Repeat("d", 40)
	fixtureRelease(t, c, broken, true)
	candidate := filepath.Join(c.Root, "releases", broken, "tree-eclass")
	if err = c.prepareDevelopmentMigration(ctx, candidate); err == nil {
		t.Fatal("failed migration reported success")
	}
	if err = c.datasetStopped(); err != nil {
		t.Fatal("failed migration leaked writers", err)
	}
	snapshots, err := c.snapshots().List()
	if err != nil {
		t.Fatal(err)
	}
	development := ""
	for _, s := range snapshots {
		if s.Development == baseline.ID {
			development = s.ID
		}
	}
	if development == "" {
		t.Fatal("schema mutation had no development checkpoint")
	}
	return development
}

func rejectDisposableMigrationPromotion(t *testing.T, c *Controller, development string) {
	t.Helper()
	ctx := t.Context()
	if err := c.SnapshotRestore(development); err == nil {
		t.Fatal("development checkpoint promoted to stable")
	}
	// The failed migration was not silently rolled back; only explicit development
	// exit restores the baseline and removes all checkpoints of development writes.
	conn, _ := startTestStorage(t, c)
	var extra *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('app.partially_migrated')::text`).Scan(&extra); err != nil ||
		extra == nil {
		t.Fatal("failed migration silently restored data", err)
	}
	conn.Close(ctx)
}

func verifyDevelopmentExitRestoresBaseline(t *testing.T, c *Controller, development string) {
	t.Helper()
	ctx := t.Context()
	if err := c.Down(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.Root, "checkpoints", development)); !os.IsNotExist(err) {
		t.Fatal("development checkpoint survived exit", err)
	}
	conn, _ := startTestStorage(t, c)
	defer conn.Close(ctx)
	var name string
	if err := conn.QueryRow(ctx, `SELECT name FROM app.courses WHERE id=771`).Scan(&name); err != nil ||
		name != "Stable fixture" {
		t.Fatal("stable baseline not restored", name, err)
	}
	var extra *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('app.partially_migrated')::text`).Scan(&extra); err != nil ||
		extra != nil {
		t.Fatal("development schema survived exit", extra, err)
	}
}
