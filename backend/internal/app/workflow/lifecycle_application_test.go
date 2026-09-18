package workflow

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/process"
)

func TestNativeApplicationLifecycleAndFailedActivation(t *testing.T) {
	t.Parallel()
	c := nativeController(t)
	requireFixtureConfig(t, c)
	ctx := t.Context()
	first, broken := strings.Repeat("a", 40), strings.Repeat("b", 40)
	fixtureRelease(t, c, first, false)
	c.State.Release = first
	if err := c.Up(ctx); err != nil {
		_ = c.Logs("api")
		_ = c.Logs("frontend")
		t.Fatal("stable startup", err)
	}
	before := c.State.Session
	if err := c.Up(ctx); err != nil || c.State.Session != before {
		t.Fatal("idempotent startup changed session", err)
	}
	conn, err := pgx.Connect(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(991,'Stable fixture','/Courses/991')`); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	if err = c.Down(); err != nil {
		t.Fatal(err)
	}
	stoppedFixture(t, c)
	if err = c.Down(); err != nil {
		t.Fatal("repeated shutdown", err)
	}
	fixtureRelease(t, c, broken, true)
	var migrationExit *process.ExitError
	if err = c.ReleaseUse(ctx, broken); !errors.As(err, &migrationExit) || migrationExit.Code != 1 {
		t.Fatal("migration failure was not exercised", err)
	}
	if c.State.Release != first {
		t.Fatal("failed activation changed selected release", c.State)
	}
	stoppedFixture(t, c)
	if err = c.Up(ctx); err != nil {
		t.Fatal("resume after failed activation", err)
	}
	conn, err = pgx.Connect(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	verifyStableData(t, conn)
	conn.Close(ctx)
	if c.State.Session == before {
		t.Fatal("restart reused browser write authority")
	}
	if err = c.Down(); err != nil {
		t.Fatal(err)
	}
	failedDevelopmentStartChecks(t, c)
}

func verifyStableData(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx := t.Context()
	var name string
	var extra *string
	if err := conn.QueryRow(ctx, `SELECT name FROM app.courses WHERE id=991`).Scan(&name); err != nil ||
		name != "Stable fixture" {
		t.Fatal("stable data lost", name, err)
	}
	if err := conn.QueryRow(ctx, `SELECT to_regclass('app.partially_migrated')::text`).Scan(&extra); err != nil ||
		extra != nil {
		t.Fatal("failed activation schema survived", extra, err)
	}
}

func failedDevelopmentStartChecks(t *testing.T, c *Controller) {
	t.Helper()
	ctx := t.Context()
	// The isolated source directory has no go.mod: compilation must fail without
	// losing the development baseline or leaving any application writer running.
	if err := c.DevUp(ctx); err == nil {
		t.Fatal("invalid editable checkout started")
	}
	if c.State.Baseline == "" || c.State.Mode != "development" {
		t.Fatal("failed development start lost its checkpoint", c.State)
	}
	if err := c.stopped(); err != nil {
		t.Fatal("failed start leaked services", err)
	}
	baseline := c.State.Baseline
	if _, err := c.snapshots().Read(baseline); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.active(), "settings/dev-only"), []byte("discard"), 0600); err != nil {
		t.Fatal(err)
	}
	fixtureCLI(t, c, "dev", "down")
	stoppedFixture(t, c)
	if _, err := os.Stat(filepath.Join(c.active(), "settings/dev-only")); !os.IsNotExist(err) {
		t.Fatal("development settings survived exit", err)
	}
	if _, err := c.snapshots().Read(baseline); err != nil {
		t.Fatal("baseline disappeared before restore completed", err)
	}
	// Ensure the restored snapshot still carries the stable code association.
	var m checkpoint.Manifest
	m, err := c.snapshots().Read(baseline)
	if err != nil || m.Release != c.State.Release {
		t.Fatal("restored code/data association", m.Release, err)
	}
}
