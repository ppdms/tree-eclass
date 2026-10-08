package rdbms_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/infrastructure/rdbms"
)

type contractBackend struct {
	name string
	cfg  rdbms.Config
}

type contractPostgres struct {
	root    string
	url     string
	manager process.Manager
	next    atomic.Uint64
}

var (
	contractOnce sync.Once
	contractPG   *contractPostgres
	contractErr  error
)

func TestMain(m *testing.M) {
	if len(os.Args) == 4 {
		var err error
		switch os.Args[1] {
		case "_exec":
			err = process.Execute(os.Args[2], os.Args[3])
		case "_supervise":
			err = process.Supervise(os.Args[2], os.Args[3])
		default:
			os.Exit(m.Run())
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	code := m.Run()
	if contractPG != nil {
		if err := contractPG.manager.Stop("postgres"); err != nil {
			fmt.Fprintln(os.Stderr, "contract PostgreSQL cleanup:", err)
			code = 1
		} else if err = os.RemoveAll(contractPG.root); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}
	os.Exit(code)
}

func contractBackends(t *testing.T) []contractBackend {
	t.Helper()
	backends := []contractBackend{{name: "sqlite", cfg: rdbms.Config{
		SQLitePath: filepath.Join(t.TempDir(), "contract.db"),
	}}}
	if os.Getenv("TREE_NATIVE_TESTS") != "1" {
		return backends
	}
	contractOnce.Do(func() { contractPG, contractErr = startContractPostgres() })
	if contractErr != nil {
		t.Fatal(contractErr)
	}
	name := "contract_" + strconv.FormatUint(contractPG.next.Add(1), 10)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := rdbms.EnsurePostgresDatabase(ctx, contractPG.url, name); err != nil {
		t.Fatal(err)
	}
	cfg := rdbms.Config{PostgresURL: contractPG.url + name}
	if err := rdbms.Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	return append(backends, contractBackend{name: "postgres", cfg: cfg})
}

func startContractPostgres() (_ *contractPostgres, err error) {
	bin := os.Getenv("TREE_POSTGRES_BIN")
	if bin == "" {
		path, lookupErr := exec.LookPath("postgres")
		if lookupErr != nil {
			return nil, lookupErr
		}
		bin = filepath.Dir(path)
	}
	root, err := os.MkdirTemp("", "tree-database-contract-")
	if err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	fixture := &contractPostgres{root: root, manager: process.Manager{
		Root: filepath.Join(root, "processes"), Executable: executable,
	}}
	defer func() {
		if err != nil {
			if stopErr := fixture.manager.Stop("postgres"); stopErr == nil {
				_ = os.RemoveAll(root)
			}
		}
	}()
	if err = fixture.initialize(bin); err != nil {
		return nil, err
	}
	return fixture, nil
}

func (f *contractPostgres) initialize(bin string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	data := filepath.Join(f.root, "postgres")
	if err := f.manager.Run(ctx, process.Spec{
		Name: "initdb", Token: "contract-initdb", Dir: f.root,
		Command: []string{filepath.Join(bin, "initdb"), "-D", data,
			"-U", "fixture", "--auth=trust", "--encoding=UTF8", "--locale=C.UTF-8"},
	}); err != nil {
		return err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	f.url = "postgresql://fixture@" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + "/"
	if err = f.manager.Start(ctx, process.Spec{
		Name: "postgres", Token: "contract-postgres", Dir: f.root, StopSignal: int(syscall.SIGINT),
		Command: []string{filepath.Join(bin, "postgres"), "-D", data, "-h", "127.0.0.1",
			"-p", strconv.Itoa(port), "-k", "", "-c", "shared_buffers=16MB", "-c", "max_connections=20"},
	}); err != nil {
		return err
	}
	return f.wait(ctx)
}

func (f *contractPostgres) wait(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := rdbms.EnsurePostgresDatabase(ctx, f.url, "fixture_template")
		if !errors.Is(err, database.ErrUnavailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func openContractStore(t *testing.T, cfg rdbms.Config) database.Store {
	t.Helper()
	store, err := rdbms.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}
