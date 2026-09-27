package workflow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/process"
)

type sharedNativeEnvironment struct {
	controller *Controller
	root       string
	objects    string
	next       atomic.Uint64
}

var (
	sharedNativeOnce sync.Once
	sharedNative     *sharedNativeEnvironment
	sharedNativeErr  error
)

func nativePorts(t *testing.T) Ports {
	t.Helper()
	ports, err := reserveNativePorts()
	if err != nil {
		t.Fatal(err)
	}
	return ports
}

func reserveNativePorts() (ports Ports, err error) {
	used := map[int]bool{}
	listeners := []net.Listener{}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	pick := func() (int, error) {
		return reserveNativePort(used, &listeners)
	}
	if ports.Postgres, err = pick(); err != nil {
		return Ports{}, err
	}
	if ports.HTTP, err = pick(); err != nil {
		return Ports{}, err
	}
	return ports, nil
}

func reserveNativePort(used map[int]bool, listeners *[]net.Listener) (int, error) {
	for range 100 {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			continue
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if port < 1024 || used[port] {
			_ = listener.Close()
			continue
		}
		*listeners = append(*listeners, listener)
		used[port] = true
		return port, nil
	}
	return 0, fmt.Errorf("could not reserve native fixture ports")
}

// Service tests share these native processes and receive isolated databases.
// Lifecycle and destructive-storage tests keep their fully isolated fixtures.
func nativeSharedController(t *testing.T) *Controller {
	t.Helper()
	if os.Getenv("TREE_NATIVE_TESTS") != "1" {
		t.Skip("set TREE_NATIVE_TESTS=1 for disposable native PostgreSQL checks")
	}
	sharedNativeOnce.Do(func() {
		sharedNative, sharedNativeErr = setupSharedNativeEnvironment()
	})
	if sharedNativeErr != nil {
		t.Fatal(sharedNativeErr)
	}
	name := fmt.Sprintf("tree_test_%x", sharedNative.next.Add(1))
	if err := sharedNative.createDatabase(t.Context(), name); err != nil {
		t.Fatal("create shared test database:", err)
	}
	c := &Controller{
		Root: t.TempDir(), Repo: sharedNative.controller.Repo, Executable: sharedNative.controller.Executable,
		Config: sharedNative.controller.Config, database: name,
	}
	if err := os.MkdirAll(c.objectsRoot(), 0o700); err != nil {
		t.Fatal(err)
	}
	c.Processes = process.Manager{Root: filepath.Join(c.Root, "processes"), Executable: c.Executable}
	t.Cleanup(func() {
		if err := sharedNative.dropDatabase(context.Background(), name); err != nil {
			t.Errorf("drop shared test database: %v", err)
		}
	})
	return c
}

func setupSharedNativeEnvironment() (*sharedNativeEnvironment, error) {
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		return nil, err
	}
	pg, err := nativePostgresBin()
	if err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	ports, err := reserveNativePorts()
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "tree-eclass-shared-native-*")
	if err != nil {
		return nil, err
	}
	c := &Controller{
		Root: root, Repo: repo, Executable: executable,
		Config: Config{
			Format: 1, PostgresBin: pg, PostgresVersion: "postgres (PostgreSQL) 18.6",
			Password: "synthetic-password", Ports: ports,
		},
	}
	c.Processes = process.Manager{Root: filepath.Join(root, "processes"), Executable: executable}
	if err := detectPostgresVersion(pg, root, &c.Config); err != nil {
		return nil, err
	}
	ctx := context.Background()
	cleanup := true
	defer func() {
		if cleanup {
			_ = c.stopAll()
			_ = os.RemoveAll(root)
		}
	}()
	if err = c.initialize(ctx); err != nil {
		return nil, err
	}
	if err = c.infrastructure(ctx); err != nil {
		return nil, err
	}
	if err = c.migrate(ctx); err != nil {
		return nil, err
	}
	objects, err := os.MkdirTemp("", "tree-eclass-shared-objects-*")
	if err != nil {
		_ = c.stopAll()
		_ = os.RemoveAll(root)
		return nil, err
	}
	cleanup = false
	return &sharedNativeEnvironment{controller: c, root: root, objects: objects}, nil
}

func closeSharedNativeEnvironment() error {
	if sharedNative == nil {
		return nil
	}
	err := sharedNative.controller.stopAll()
	removeErr := os.RemoveAll(sharedNative.root)
	removeErr = errors.Join(removeErr, os.RemoveAll(sharedNative.objects))
	if err != nil {
		return err
	}
	return removeErr
}

func (s *sharedNativeEnvironment) createDatabase(ctx context.Context, name string) error {
	conn, err := s.admin(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE tree")
	return err
}

func (s *sharedNativeEnvironment) dropDatabase(ctx context.Context, name string) error {
	conn, err := s.admin(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize())
	return err
}

func (s *sharedNativeEnvironment) admin(ctx context.Context) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(s.controller.databaseURL())
	if err != nil {
		return nil, err
	}
	config.Database = "postgres"
	return pgx.ConnectConfig(ctx, config)
}

func detectPostgresVersion(pg, root string, cfg *Config) error {
	version, err := exec.Command(filepath.Join(pg, "postgres"), "--version").Output()
	if err != nil {
		_ = os.RemoveAll(root)
		return err
	}
	cfg.PostgresVersion = strings.TrimSpace(string(version))
	return nil
}
