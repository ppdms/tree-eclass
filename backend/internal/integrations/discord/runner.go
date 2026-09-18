// Package discord orchestrates short-lived native DiscordChatExporter commands.
// Tokens enter only the helper environment, never command arguments or logs.
package discord

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"tree-eclass/internal/infrastructure/platform"
	"tree-eclass/internal/infrastructure/process"
)

type Runner interface {
	Run(context.Context, string, string, ...string) (string, error)
}
type NativeRunner struct{ Binary, Registry string }
type limitedOutput struct{ bytes.Buffer }

func (b *limitedOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 2*1024*1024 {
		return 0, errors.New("Discord exporter output exceeded 2 MiB")
	}
	return b.Buffer.Write(p)
}
func (r NativeRunner) Run(ctx context.Context, dir, token string, args ...string) (result string, err error) {
	if r.Binary == "" {
		return "", errors.New("native Discord exporter is not configured; run ./tree setup")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, r.Binary, args...)
	command.Dir = dir
	command.Env = []string{
		"LANG=C.UTF-8",
		"TMPDIR=" + dir,
		"HOME=" + dir,
		"DISCORD_TOKEN=" + token,
		"DOTNET_GCHeapHardLimit=0x10000000",
		"DOTNET_EnableDiagnostics=0",
		"NO_COLOR=1",
		"TERM=dumb",
	}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	command.WaitDelay = 2 * time.Second
	output := &limitedOutput{}
	command.Stdout = output
	command.Stderr = output
	finish, err := process.StartHelper(command, r.Registry)
	if err != nil {
		return "", errors.New("could not start native Discord exporter")
	}
	defer func() { err = errors.Join(err, finish()) }()
	watch, stop := context.WithCancel(ctx)
	memory := make(chan error, 1)
	disk := make(chan error, 1)
	go func() { memory <- process.WatchMemory(watch, command.Process.Pid, 384*1024*1024) }()
	go func() { disk <- watchDisk(watch, dir, command.Cancel) }()
	runErr := command.Wait()
	stop()
	memoryErr, diskErr := <-memory, <-disk
	if memoryErr != nil {
		return "", memoryErr
	}
	if diskErr != nil {
		return "", diskErr
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	// Exporter diagnostics can echo remote content or credentials. Publish only a
	// fixed error; validated listing data is handled separately by the caller.
	if runErr != nil {
		return "", errors.New("Discord exporter failed; check the token, channel access and export settings")
	}
	return output.String(), nil
}
func watchDisk(ctx context.Context, dir string, kill func() error) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if err := checkDirectory(dir); err != nil {
			_ = kill()
			return err
		}
	}
}
func checkDirectory(dir string) error {
	var total int64
	count := 0
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("Discord exporter produced a nonregular file")
		}
		count++
		total += info.Size()
		if count > 2000 || total > 256*1024*1024 || info.Size() > 50*1024*1024 {
			return errors.New("Discord export exceeds the 256 MiB interval, 50 MiB file or 2000-file limit")
		}
		return nil
	})
	if err != nil {
		return err
	}
	free, err := platform.Available(dir)
	if err != nil {
		return err
	}
	if free < 5*1024*1024*1024 {
		return errors.New("Discord export stopped to preserve 5 GiB free disk")
	}
	return nil
}

var _ Runner = NativeRunner{}
