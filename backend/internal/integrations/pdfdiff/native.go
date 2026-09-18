package pdfdiff

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/process"
)

type NativeRunner struct{ Binary, SHA256, Root, ToolsRoot, Registry string }

func (r NativeRunner) Run(ctx context.Context, dir, old, next string) (changed bool, err error) {
	if err = r.verifyBinary(); err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	target := filepath.Join(dir, "difference.pdf")
	if _, err = os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		return false, errors.New("visual difference output already exists")
	}
	if err = validateInputs(old, next); err != nil {
		return false, err
	}
	cmd := r.command(ctx, dir, target, old, next)
	finish, err := process.StartHelper(cmd, r.Registry)
	if err != nil {
		return false, errors.New("could not start visual PDF comparison")
	}
	defer func() { err = errors.Join(err, finish()) }()
	watch, stop := context.WithCancel(ctx)
	memory := make(chan error, 1)
	output := make(chan error, 1)
	go func() { memory <- process.WatchMemory(watch, cmd.Process.Pid, 512*1024*1024) }()
	go func() { output <- watchOutput(watch, target, cmd.Cancel) }()
	runErr := cmd.Wait()
	stop()
	if err = errors.Join(<-memory, <-output); err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if runErr == nil {
		return false, nil
	}
	var exit *exec.ExitError
	if !errors.As(runErr, &exit) || exit.ExitCode() != 1 {
		return false, errors.New("visual PDF comparison failed")
	}
	return artifactChanged(target)
}

func (r NativeRunner) verifyBinary() error {
	if r.Binary == "" || r.SHA256 == "" {
		return errors.New("visual PDF tool is not configured; run ./tree setup")
	}
	executable, err := os.Open(r.Binary)
	if err != nil {
		return err
	}
	hash := sha256.New()
	_, err = io.Copy(hash, executable)
	executable.Close()
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != r.SHA256 {
		return errors.New("visual PDF executable differs from the configured checksum")
	}
	return nil
}

func (r NativeRunner) command(ctx context.Context, dir, target, old, next string) *exec.Cmd {
	cmd := exec.CommandContext(
		ctx,
		r.Binary,
		"--output-diff="+target,
		"--skip-identical",
		"--mark-differences",
		old,
		next,
	)
	cmd.Env = process.DocumentEnvironment(dir, r.ToolsRoot)
	cmd.Dir = dir
	if r.Root != "" {
		cmd.Dir = r.Root
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	return cmd
}

func validateInputs(old, next string) error {
	for _, input := range []string{old, next} {
		if err := pdfHead(input, "visual comparison input is not a PDF"); err != nil {
			return err
		}
	}
	return nil
}

func artifactChanged(target string) (bool, error) {
	info, err := os.Lstat(target)
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() > blob.MaxSourceBytes || info.Size() < 8 {
		return false, errors.New("invalid visual difference artifact")
	}
	if err := pdfHead(target, "visual difference artifact is not a PDF"); err != nil {
		return false, err
	}
	return true, nil
}

func pdfHead(path, message string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	head := make([]byte, 5)
	_, readErr := io.ReadFull(file, head)
	file.Close()
	if readErr != nil || string(head) != "%PDF-" {
		return errors.New(message)
	}
	return nil
}
func watchOutput(ctx context.Context, target string, kill func() error) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		info, err := os.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			_ = kill()
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > blob.MaxSourceBytes {
			_ = kill()
			return errors.New("visual difference output exceeds 50 MiB or is not regular")
		}
	}
}
