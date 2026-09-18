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

	"tree-eclass/internal/infrastructure/platform"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "_helper-parent" {
		if err := helperParent(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
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

func manager(t *testing.T) Manager {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Manager{Root: t.TempDir(), Executable: exe}
}
func TestSupervisorCrashRecoversVerifiedChild(t *testing.T) {
	m := manager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	spec := Spec{Name: "fixture", Token: "synthetic-token", Command: []string{"/bin/sleep", "120"}}
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
	spec := Spec{Name: "fixture", Token: "synthetic-token", Command: []string{"/usr/bin/touch", marker}}
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
	cmd := exec.Command("/bin/sleep", "120")
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
