package workflow

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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
	if os.Getenv("TREE_NATIVE_TESTS") != "1" {
		os.Exit(m.Run())
	}
	code := m.Run()
	if err := closeSharedNativeEnvironment(); err != nil {
		fmt.Fprintln(os.Stderr, "native shared fixture cleanup:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func nativeController(t *testing.T) *Controller {
	t.Helper()
	if os.Getenv("TREE_NATIVE_TESTS") != "1" {
		t.Skip("set TREE_NATIVE_TESTS=1 for disposable native PostgreSQL checks")
	}
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	pg, err := nativePostgresBin()
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
			Format:          1,
			PostgresBin:     pg,
			PostgresVersion: "postgres (PostgreSQL) 18.6",
			Password:        "synthetic-password",
			Ports:           nativePorts(t),
		},
	}
	version, err := exec.Command(filepath.Join(pg, "postgres"), "--version").Output()
	if err != nil {
		t.Fatal(err)
	}
	c.Config.PostgresVersion = strings.TrimSpace(string(version))
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

func nativePostgresBin() (string, error) {
	path := os.Getenv("TREE_POSTGRES_BIN")
	if path == "" {
		path = "/opt/homebrew/opt/postgresql@18/bin"
	}
	return filepath.Abs(path)
}

func startTestStorage(t *testing.T, c *Controller) (*pgx.Conn, *blob.Store) {
	t.Helper()
	ctx := context.Background()
	if c.database == "" {
		if err := c.infrastructure(ctx); err != nil {
			_ = c.Logs("postgres")
			t.Fatal(err)
		}
		if err := c.migrate(ctx); err != nil {
			t.Fatal(err)
		}
	} else if err := c.migrate(ctx); err != nil {
		t.Fatal("shared test database migration", err)
	}
	conn, err := pgx.Connect(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	store, err := blob.New(c.testObjectsRoot())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	return conn, store
}

func TestNativeMigrationTamperAndOwnership(t *testing.T) {
	t.Parallel()
	c := nativeController(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, _ := startTestStorage(t, c)
	defer conn.Close(ctx)
	first, err := storage.Open(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	if second, err := storage.Open(ctx, c.databaseURL()); err == nil {
		second.Close()
		t.Fatal("second runtime admitted")
	}
	if err = storage.Migrate(ctx, c.databaseURL()); err == nil {
		t.Fatal("migration admitted while application owns database")
	}
	first.Close()
	if err = storage.Migrate(ctx, c.databaseURL()); err != nil {
		t.Fatal("migration refused after writer stopped", err)
	}
	if _, err = conn.Exec(ctx, "UPDATE public.tree_go_migrations SET sha256='tampered' WHERE version=1"); err != nil {
		t.Fatal(err)
	}
	if err = storage.Migrate(ctx, c.databaseURL()); err == nil {
		t.Fatal("tampered migration accepted")
	}
}

func (c *Controller) testObjectsRoot() string {
	if c.database != "" {
		return sharedNative.objects
	}
	return c.objectsRoot()
}
