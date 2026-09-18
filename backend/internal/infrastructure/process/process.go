// Package process supervises native children without login services. Each child
// has a private intent file, an OS lock and a persistent supervisor identity.
package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"tree-eclass/internal/infrastructure/platform"
)

type Spec struct {
	Name       string   `json:"name"`
	Token      string   `json:"token"`
	Command    []string `json:"command"`
	Dir        string   `json:"dir"`
	Env        []string `json:"env,omitempty"`
	StopSignal int      `json:"stop_signal"`
	Finite     bool     `json:"finite,omitempty"`
	ReplaceEnv bool     `json:"replace_env,omitempty"`
}
type Record struct {
	Token      string `json:"token"`
	PID        int    `json:"pid"`
	Birth      string `json:"birth"`
	ChildPID   int    `json:"child_pid"`
	ChildBirth string `json:"child_birth"`
	Completed  bool   `json:"completed,omitempty"`
	ExitCode   int    `json:"exit_code"`
}
type Manager struct{ Root, Executable string }

func (m Manager) dir(name string) string { return filepath.Join(m.Root, name) }
func (m Manager) Alive(name string) bool {
	lock, err := platform.Lock(filepath.Join(m.dir(name), "owner.lock"))
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	platform.Unlock(lock)
	return false
}

func (m Manager) Start(ctx context.Context, spec Spec) error {
	spec.Finite = false
	return m.launch(ctx, spec)
}

// Run journals finite writers exactly like services, including durable completion.
// A lost supervisor or missing completion record can never imply success.
func (m Manager) Run(ctx context.Context, spec Spec) error {
	spec.Finite = true
	return m.launch(ctx, spec)
}

func (m Manager) launch(ctx context.Context, spec Spec) error {
	dir := m.dir(spec.Name)
	cmd, log, err := m.startSupervisor(dir, spec)
	if err != nil {
		return err
	}
	defer log.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	poll := time.NewTimer(0)
	if !poll.Stop() {
		<-poll.C
	}
	defer poll.Stop()
	for {
		var r Record
		if platform.ReadJSON(filepath.Join(dir, "process.json"), &r) == nil && r.Token == spec.Token {
			if spec.Finite && r.Completed && !m.Alive(spec.Name) {
				return completion(spec.Name, r)
			}
			if !spec.Finite && r.ChildPID > 0 && m.Alive(spec.Name) {
				return nil
			}
		}
		poll.Reset(50 * time.Millisecond)
		select {
		case err := <-done:
			if spec.Finite && platform.ReadJSON(filepath.Join(dir, "process.json"), &r) == nil &&
				r.Token == spec.Token &&
				r.Completed {
				return completion(spec.Name, r)
			}
			return fmt.Errorf("%s supervisor exited without confirmed completion (see %s): %v", spec.Name, dir, err)
		case <-ctx.Done():
			return errors.Join(ctx.Err(), m.Stop(spec.Name))
		case <-poll.C:
		}
	}
}

// startSupervisor journals the intent, clears any stop marker and spawns the
// supervisor child. Intent journaling always precedes child creation.
func (m Manager) startSupervisor(dir string, spec Spec) (*exec.Cmd, *os.File, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, nil, err
	}
	if m.Alive(spec.Name) {
		return nil, nil, fmt.Errorf("%s is already running", spec.Name)
	}
	if err := m.Stop(spec.Name); err != nil {
		return nil, nil, err
	}
	if err := platform.WriteJSON(filepath.Join(dir, "intent.json"), spec); err != nil {
		return nil, nil, err
	}
	_ = os.Remove(filepath.Join(dir, "stop"))
	cmd := exec.Command(m.Executable, "_supervise", dir, spec.Token)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	log, err := os.OpenFile(filepath.Join(dir, "supervisor.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, nil, err
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, nil, err
	}
	return cmd, log, nil
}

// Stop revokes intent before reading a PID. Even a delayed supervisor cannot
// start a child after this returns. Never signal a process from a bare stale PID.
func (m Manager) Stop(name string) error {
	dir := m.dir(name)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "stop"), nil, 0600); err != nil {
		return err
	}
	if err := platform.SyncDir(dir); err != nil {
		return err
	}
	deadline := time.Now().Add(45 * time.Second)
	for m.Alive(name) {
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not stop; checkpoint refused (see %s)", name, dir)
		}
		time.Sleep(50 * time.Millisecond)
	}
	var r Record
	if err := platform.ReadJSON(filepath.Join(dir, "process.json"), &r); err == nil && r.ChildPID > 0 {
		if syscall.Kill(-r.ChildPID, 0) == nil {
			return m.stopOrphan(name, r)
		}
	}
	return nil
}

func completion(name string, r Record) error {
	if r.ExitCode != 0 {
		return &ExitError{Name: name, Code: r.ExitCode}
	}
	return nil
}

// ExitError reports an observed unsuccessful command, distinct from missing
// completion after a supervisor crash.
type ExitError struct {
	Name string
	Code int
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("%s exited with status %d; see ./tree logs %s", e.Name, e.Code, e.Name)
}
