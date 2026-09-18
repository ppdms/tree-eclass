package process

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"tree-eclass/internal/infrastructure/platform"
)

// StartHelper preserves the caller's streaming pipes and cancellation while
// requiring durable ownership before a parser/exporter can execute. The registry
// is separate from all document-controlled filenames. StopHelpers is called only
// after its API parent has stopped, including recovery from a killed API.
func StartHelper(cmd *exec.Cmd, registry string) (func() error, error) {
	if registry == "" {
		return func() error { return nil }, cmd.Start()
	}
	dir, spec, err := stageIntent(cmd, registry)
	if err != nil {
		return nil, err
	}
	cleanup := func() error { return os.RemoveAll(dir) }
	fail := true
	defer func() {
		if fail {
			_ = cleanup()
		}
	}()
	reader, writer, err := bootstrapCmd(cmd, dir, spec)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	defer writer.Close()
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	reader.Close()
	birth, err := Birth(cmd.Process.Pid)
	if err == nil {
		err = platform.WriteJSON(
			filepath.Join(dir, "process.json"),
			Record{Token: spec.Token, ChildPID: cmd.Process.Pid, ChildBirth: birth},
		)
	}
	if err == nil {
		_, err = writer.Write([]byte{1})
	}
	if err != nil {
		writer.Close()
		_ = cmd.Wait()
		return nil, err
	}
	fail = false
	return func() error {
		if err := drain(cmd.Process.Pid, nil); err != nil {
			return err
		}
		return cleanup()
	}, nil
}

// stageIntent validates the child configuration, creates the private helper
// directory and journals the intent record before any process is created.
func stageIntent(cmd *exec.Cmd, registry string) (string, Spec, error) {
	if len(cmd.ExtraFiles) != 0 || cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		return "", Spec{}, errors.New("helper requires an exclusive process group and commit gate")
	}
	if err := os.MkdirAll(registry, 0700); err != nil {
		return "", Spec{}, err
	}
	dir, err := os.MkdirTemp(registry, "helper-*")
	if err != nil {
		return "", Spec{}, err
	}
	spec := Spec{
		Name:       filepath.Base(dir),
		Token:      filepath.Base(dir),
		Command:    append([]string{cmd.Path}, cmd.Args[1:]...),
		Dir:        cmd.Dir,
		Env:        cmd.Env,
		ReplaceEnv: cmd.Env != nil,
		StopSignal: int(syscall.SIGKILL),
	}
	if err = platform.WriteJSON(filepath.Join(dir, "intent.json"), spec); err != nil {
		_ = os.RemoveAll(dir)
		return "", Spec{}, err
	}
	return dir, spec, nil
}

func bootstrapCmd(cmd *exec.Cmd, dir string, spec Spec) (*os.File, *os.File, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		reader.Close()
		writer.Close()
		return nil, nil, err
	}
	cmd.Path = executable
	cmd.Args = []string{executable, "_exec", dir, spec.Token}
	cmd.ExtraFiles = []*os.File{reader}
	return reader, writer, nil
}

func StopHelpers(registry string) error {
	entries, err := os.ReadDir(registry)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	manager := Manager{Root: registry}
	for _, entry := range entries {
		if !entry.IsDir() {
			return errors.New("unexpected helper ownership registry entry")
		}
		if err = manager.Stop(entry.Name()); err != nil {
			return err
		}
		if err = os.RemoveAll(filepath.Join(registry, entry.Name())); err != nil {
			return err
		}
	}
	return platform.SyncDir(registry)
}

// HelpersStopped is a positive checkpoint boundary, not an attempt to infer
// safety from a missing API parent. Recover leftover records through StopHelpers.
func HelpersStopped(registry string) error {
	entries, err := os.ReadDir(registry)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return errors.New("helper ownership records remain; stop and recover all helpers before checkpointing")
	}
	return nil
}
