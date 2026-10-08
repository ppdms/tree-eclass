package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"tree-eclass/internal/domain/platform"
)

func TestMain(m *testing.M) {
	// Tests never depend on host tool locations (/bin, /usr/bin): every child
	// is the test binary itself in fixture mode, so NixOS and containers agree.
	if len(os.Args) == 3 && os.Args[1] == "_helper-parent" {
		if err := helperParent(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) >= 3 && os.Args[1] == "_fixture" {
		os.Exit(fixtureMain(os.Args[2:]))
	}
	if len(os.Args) == 4 {
		var err error
		switch os.Args[1] {
		case "_supervise":
			err = Supervise(os.Args[2], os.Args[3])
		case "_exec":
			err = Execute(os.Args[2], os.Args[3])
		default:
			os.Exit(m.Run())
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fixtureMain implements the tiny child behaviors tests need: sleep blocks,
// exit reports a status, env prints one variable, touch creates a marker. All
// output goes to stdout; the exit code is the only status channel.
func fixtureMain(args []string) int {
	if len(args) == 0 {
		return 2
	}
	switch args[0] {
	case "sleep":
		time.Sleep(24 * time.Hour)
		return 0
	case "exit":
		if len(args) == 2 && args[1] == "0" {
			return 0
		}
		return 1
	case "env":
		if len(args) == 2 {
			fmt.Println(os.Getenv(args[1]))
			return 0
		}
	case "env-write":
		if len(args) == 3 {
			if err := os.WriteFile(args[2], []byte(os.Getenv(args[1])), 0600); err != nil {
				return 1
			}
			return 0
		}
	case "touch":
		if len(args) == 2 {
			file, err := os.OpenFile(args[1], os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				return 1
			}
			_ = file.Close()
			return 0
		}
	}
	return 2
}

func manager(t *testing.T) Manager {
	t.Helper()
	return Manager{Root: t.TempDir(), Executable: fixtureBinary(t)}
}

func fixtureBinary(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

func fixtureCommand(t *testing.T, args ...string) []string {
	t.Helper()
	return append([]string{fixtureBinary(t), "_fixture"}, args...)
}
func TestSupervisorCrashRecoversVerifiedChild(t *testing.T) {
	m := manager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	spec := Spec{Name: "fixture", Token: "synthetic-token", Command: fixtureCommand(t, "sleep")}
	if err := m.Start(ctx, spec); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Stop(spec.Name); err != nil {
			t.Error(err)
		}
	})
	var record Record
	if err := platform.ReadJSON(filepath.Join(m.dir(spec.Name), "process.json"), &record); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(record.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(spec.Name); err != nil {
		t.Fatal(err)
	}
	if syscall.Kill(-record.ChildPID, 0) == nil {
		t.Fatal("writer survived supervisor crash recovery")
	}
	// Revocation is idempotent, and a subsequent start uses fresh ownership.
	spec.Token = "synthetic-next-token"
	if err := m.Start(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(spec.Name); err != nil {
		t.Fatal(err)
	}
}

func TestUncommittedChildCannotExecute(t *testing.T) {
	m := manager(t)
	dir := m.dir("fixture")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "should-not-exist")
	spec := Spec{Name: "fixture", Token: "synthetic-token", Command: fixtureCommand(t, "touch", marker)}
	if err := platform.WriteJSON(filepath.Join(dir, "intent.json"), spec); err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(m.Executable, "_exec", dir, spec.Token)
	cmd.ExtraFiles = []*os.File{reader}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	reader.Close()
	writer.Close() // Simulates supervisor death before committing the child.
	if err = cmd.Wait(); err == nil {
		t.Fatal("uncommitted bootstrap reported success")
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("unrecorded child executed writer")
	}
}

func TestRecycledIdentityIsNeverSignaled(t *testing.T) {
	m := manager(t)
	dir := m.dir("fixture")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(fixtureBinary(t), "_fixture", "sleep")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	spec := Spec{Name: "fixture", Token: "old-token"}
	if err := platform.WriteJSON(filepath.Join(dir, "intent.json"), spec); err != nil {
		t.Fatal(err)
	}
	record := Record{Token: spec.Token, ChildPID: cmd.Process.Pid, ChildBirth: "different-process"}
	if err := platform.WriteJSON(filepath.Join(dir, "process.json"), record); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(spec.Name); err == nil {
		t.Fatal("accepted recycled identity")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("unrelated process was killed")
	}
}
