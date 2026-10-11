package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/infrastructure/storage"
)

func TestMain(m *testing.M) {
	if filepath.Base(os.Args[0]) == "controller" && os.Getenv("TREE_NATIVE_TESTS") == "1" {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := Run(ctx, os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) == 5 && os.Args[1] == "_watch" {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := Watch(ctx, os.Args[2], os.Args[3], os.Args[4]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if filepath.Base(os.Args[0]) == "tree-eclass" && os.Getenv("TREE_NATIVE_TESTS") == "1" {
		os.Exit(fixtureApplication())
	}
	if len(os.Args) == 4 && os.Args[1] == "_exec" {
		if err := process.Execute(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) == 4 && os.Args[1] == "_supervise" {
		if err := process.Supervise(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func nativeController(t *testing.T) *Controller {
	t.Helper()
	if os.Getenv("TREE_NATIVE_TESTS") != "1" {
		t.Skip("set TREE_NATIVE_TESTS=1 for disposable native checks")
	}
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{
		Root:       t.TempDir(),
		Repo:       repo,
		Executable: executable,
		Config: Config{
			Format: 1,
			Ports:  nativePorts(t),
		},
	}
	c.Processes = process.Manager{Root: filepath.Join(c.Root, "processes"), Executable: executable}
	t.Cleanup(func() {
		if err := c.stopAll(); err != nil {
			t.Errorf("native cleanup: %v", err)
		}
	})
	if err = c.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

// sqliteDSN opens the same WAL database the runtime uses. Fixture handles are
// plain readers/writers; ownership admission stays with storage.OpenConfig.
func sqliteDSN(path string) string {
	return "file:" + path + "?_pragma=busy_timeout%3D30000&_pragma=journal_mode%3DWAL&_pragma=foreign_keys%3D1"
}

// sqliteTestDB opens a fixture handle on the controller's sqlite database.
// The handle closes with the test; explicit mid-test Close calls stay safe.
func sqliteTestDB(t *testing.T, c *Controller) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", sqliteDSN(c.storageConfig().SQLitePath))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.PingContext(t.Context()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func startTestStorage(t *testing.T, c *Controller) (*sql.DB, *blob.Store) {
	t.Helper()
	ctx := context.Background()
	if err := c.infrastructure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.migrate(ctx); err != nil {
		t.Fatal(err)
	}
	store, err := blob.New(c.testObjectsRoot())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	return sqliteTestDB(t, c), store
}

func TestNativeMigrationTamperAndOwnership(t *testing.T) {
	t.Parallel()
	c := nativeController(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, _ := startTestStorage(t, c)
	defer conn.Close()
	cfg := c.storageConfig()
	first, err := storage.OpenConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := storage.OpenConfig(ctx, cfg); err == nil {
		second.Close()
		t.Fatal("second runtime admitted")
	}
	if err = storage.MigrateConfig(ctx, cfg); err == nil {
		t.Fatal("migration admitted while application owns database")
	}
	first.Close()
	if err = storage.MigrateConfig(ctx, cfg); err != nil {
		t.Fatal("migration refused after writer stopped", err)
	}
	if _, err = conn.ExecContext(ctx, "UPDATE tree_go_migrations SET sha256='tampered' WHERE version=1"); err != nil {
		t.Fatal(err)
	}
	if err = storage.MigrateConfig(ctx, cfg); err == nil {
		t.Fatal("tampered migration accepted")
	}
}

func (c *Controller) testObjectsRoot() string {
	return c.objectsRoot()
}
