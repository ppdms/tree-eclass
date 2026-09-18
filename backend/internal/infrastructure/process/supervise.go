package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"tree-eclass/internal/infrastructure/platform"
)

func revoked(dir string) bool { _, err := os.Stat(filepath.Join(dir, "stop")); return err == nil }

// Supervise is an internal CLI entry point. The child cannot be spawned before
// the ownership record is durably written. Cancellation drains the entire group.
func Supervise(dir, token string) error {
	lock, err := platform.Lock(filepath.Join(dir, "owner.lock"))
	if err != nil {
		return err
	}
	defer platform.Unlock(lock)
	var spec Spec
	if err = platform.ReadJSON(filepath.Join(dir, "intent.json"), &spec); err != nil {
		return err
	}
	if spec.Token != token || revoked(dir) {
		return nil
	}
	if len(spec.Command) == 0 {
		return errors.New("empty native command")
	}
	r, path, err := ownRecord(dir, token)
	if err != nil {
		return err
	}
	log, err := openLog(filepath.Join(dir, "output.log"))
	if err != nil {
		return err
	}
	defer log.Close()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	if revoked(dir) {
		return nil
	}
	cmd, err := startCommitted(dir, spec, &r, log)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	err = monitor(dir, spec, r, signals, done)
	// The supervisor remains the owner until all descendants have exited.
	if syscall.Kill(-r.ChildPID, 0) == nil {
		return fmt.Errorf("child group %d survived shutdown: %w", r.ChildPID, err)
	}
	r = finalize(r, spec, err)
	return errors.Join(err, platform.WriteJSON(path, r))
}

// ownRecord durably publishes the supervisor's ownership record before any
// child can be spawned.
func ownRecord(dir, token string) (Record, string, error) {
	birth, err := Birth(os.Getpid())
	if err != nil {
		return Record{}, "", err
	}
	r := Record{Token: token, PID: os.Getpid(), Birth: birth}
	path := filepath.Join(dir, "process.json")
	if err = platform.WriteJSON(path, r); err != nil {
		return Record{}, "", err
	}
	return r, path, nil
}

// finalize clears the live-child markers once the whole group has exited and
// records the observed exit status.
func finalize(r Record, spec Spec, err error) Record {
	r.ChildPID = 0
	r.ChildBirth = ""
	r.Completed = spec.Finite
	if err != nil {
		r.ExitCode = -1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			r.ExitCode = exit.ExitCode()
		}
	}
	return r
}

func monitor(dir string, spec Spec, r Record, signals <-chan os.Signal, done <-chan error) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return drain(r.ChildPID, err)
		case <-signals:
			return terminate(spec, r.ChildPID, done)
		case <-ticker.C:
			if revoked(dir) {
				return terminate(spec, r.ChildPID, done)
			}
		}
	}
}

func terminate(spec Spec, pid int, done <-chan error) error {
	sig := syscall.Signal(spec.StopSignal)
	if sig == 0 {
		sig = syscall.SIGTERM
	}
	_ = syscall.Kill(-pid, sig)
	select {
	case <-done:
		var result error
		if spec.Finite {
			result = context.Canceled
		}
		return drain(pid, result)
	case <-time.After(30 * time.Second):
		// Do not force-kill storage. Its dirty state is not eligible for a checkpoint.
		return fmt.Errorf("%s exceeded graceful shutdown deadline", spec.Name)
	}
}

func drain(pid int, result error) error {
	if syscall.Kill(-pid, 0) != nil {
		return result
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(-pid, 0) != nil {
			return result
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.Join(result, fmt.Errorf("descendants in process group %d still running", pid))
}
