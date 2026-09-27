package process

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"tree-eclass/internal/domain/platform"
)

func helperParent(registry string) error {
	cmd := exec.Command("/bin/sleep", "120")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	finish, err := StartHelper(cmd, registry)
	if err != nil {
		return err
	}
	defer finish()
	return cmd.Wait()
}
func TestHelperSurvivesParentCrashOnlyUntilRecovery(t *testing.T) {
	m := manager(t)
	registry := filepath.Join(m.Root, "helpers")
	parent := exec.Command(m.Executable, "_helper-parent", registry)
	parent.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := parent.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Process.Kill(); _ = StopHelpers(registry) })
	deadline := time.Now().Add(5 * time.Second)
	var record Record
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(registry)
		if len(entries) == 1 &&
			platform.ReadJSON(filepath.Join(registry, entries[0].Name(), "process.json"), &record) == nil &&
			record.ChildPID > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if record.ChildPID == 0 {
		t.Fatal("helper identity was not committed")
	}
	if err := HelpersStopped(registry); err == nil {
		t.Fatal("live helper accepted as cold storage")
	}
	if err := parent.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = parent.Wait()
	if err := StopHelpers(registry); err != nil {
		t.Fatal(err)
	}
	if syscall.Kill(-record.ChildPID, 0) == nil {
		t.Fatal("helper group survived recovery after parent death")
	}
	if err := HelpersStopped(registry); err != nil {
		t.Fatal(err)
	}
}
func TestHelperStreamingAndEnvironmentIsolation(t *testing.T) {
	t.Setenv("SYNTHETIC_PARENT_SECRET", "must-not-reach-helper")
	registry := filepath.Join(t.TempDir(), "helpers")
	cmd := exec.Command("/usr/bin/env")
	cmd.Env = []string{"ONLY=fixture"}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	finish, err := StartHelper(cmd, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	data, err := io.ReadAll(stdout)
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "ONLY=fixture" {
		t.Fatal("helper inherited parent environment", string(data))
	}
	if err = finish(); err != nil {
		t.Fatal(err)
	}
	if err = HelpersStopped(registry); err != nil {
		t.Fatal(err)
	}
}
