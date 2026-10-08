package process

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"tree-eclass/internal/domain/platform"
)

// Execute is an internal bootstrap. It must not start a writer until its parent
// has durably recorded its identity. Parent death closes the pipe, including the
// otherwise unjournaled interval between fork and publishing the child PID.
func Execute(dir, token string) error {
	gate := os.NewFile(3, "commit-gate")
	if gate == nil {
		return errors.New("missing child commit gate")
	}
	var permit [1]byte
	_, err := io.ReadFull(gate, permit[:])
	gate.Close()
	if err != nil || permit[0] != 1 {
		return errors.New("child start was not committed")
	}
	var spec Spec
	var record Record
	if err = platform.ReadJSON(filepath.Join(dir, "intent.json"), &spec); err != nil {
		return err
	}
	if err = platform.ReadJSON(filepath.Join(dir, "process.json"), &record); err != nil {
		return err
	}
	birth, err := Birth(os.Getpid())
	if err != nil {
		return err
	}
	if revoked(dir) || spec.Token != token || record.Token != token || record.ChildPID != os.Getpid() ||
		record.ChildBirth != birth {
		return errors.New("child ownership changed before execution")
	}
	if len(spec.Command) == 0 {
		return errors.New("empty child command")
	}
	binary, err := exec.LookPath(spec.Command[0])
	if err != nil {
		return err
	}
	if spec.Dir != "" {
		if err = os.Chdir(spec.Dir); err != nil {
			return err
		}
	}
	env := spec.Env
	if !spec.ReplaceEnv {
		env = append(dedupEnv(os.Environ(), spec.Env), spec.Env...)
	}
	return syscall.Exec(binary, spec.Command, env)
}

// dedupEnv drops base entries shadowed by overrides: with duplicate keys the
// child runtime may honor either entry, so the override must be unambiguous.
func dedupEnv(base, overrides []string) []string {
	shadowed := make(map[string]bool, len(overrides))
	for _, entry := range overrides {
		if name, _, ok := strings.Cut(entry, "="); ok {
			shadowed[name] = true
		}
	}
	kept := base[:0]
	for _, entry := range base {
		if name, _, ok := strings.Cut(entry, "="); ok && shadowed[name] {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}

func startCommitted(dir string, spec Spec, record *Record, output *rotatingLog) (*exec.Cmd, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	defer writer.Close()
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(executable, "_exec", dir, spec.Token)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.ExtraFiles = []*os.File{reader}
	cmd.Stdout, cmd.Stderr = output, output
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	reader.Close()
	record.ChildPID = cmd.Process.Pid
	record.ChildBirth, err = Birth(record.ChildPID)
	if err == nil {
		err = platform.WriteJSON(filepath.Join(dir, "process.json"), record)
	}
	if err == nil {
		_, err = writer.Write([]byte{1})
	}
	if err != nil {
		writer.Close()
		_ = cmd.Wait()
		return nil, err
	}
	return cmd, nil
}
