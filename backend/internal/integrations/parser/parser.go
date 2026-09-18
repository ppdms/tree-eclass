// Package parser runs bounded, short-lived Python document helpers. Database,
// object-store and provider credentials never enter the helper environment.
package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"tree-eclass/internal/infrastructure/process"
)

type Source struct {
	CourseID        int64   `json:"course_id"`
	CourseName      string  `json:"course_name"`
	CourseShortName *string `json:"course_short_name"`
	SourcePath      string  `json:"source_path"`
	SourceURL       *string `json:"source_url"`
	DisplayName     string  `json:"display_name"`
	SourceHash      string  `json:"source_hash"`
	MIMEType        string  `json:"mime_type"`
}
type Request struct {
	ArchiveFormat string         `json:"archive_format,omitempty"`
	Operation     string         `json:"operation"`
	Path          string         `json:"path"`
	Output        string         `json:"output"`
	Kind          string         `json:"kind,omitempty"`
	Source        *Source        `json:"source,omitempty"`
	Limits        map[string]any `json:"limits,omitempty"`
	Options       map[string]any `json:"options,omitempty"`
	Pages         []int          `json:"pages,omitempty"`
	MemberChain   []string       `json:"member_chain,omitempty"`
}
type Record struct {
	Type           string         `json:"type"`
	Title          string         `json:"title"`
	Kind           string         `json:"kind"`
	Text           string         `json:"text"`
	LocatorType    string         `json:"locator_type"`
	LocatorStart   string         `json:"locator_start"`
	LocatorEnd     *string        `json:"locator_end"`
	Heading        *string        `json:"heading"`
	Metadata       map[string]any `json:"metadata"`
	Warnings       []string       `json:"warnings"`
	Path           string         `json:"path"`
	Bytes          int64          `json:"bytes"`
	Page           int            `json:"page"`
	Reason         string         `json:"reason"`
	Message        string         `json:"message"`
	MemberPath     string         `json:"member_path"`
	MemberChain    []string       `json:"member_chain"`
	ContentHash    string         `json:"content_hash"`
	CRC32          uint32         `json:"crc32"`
	ExpandedSize   int64          `json:"expanded_size"`
	CompressedSize int64          `json:"compressed_size"`
	MIMEType       string         `json:"mime_type"`
	Depth          int            `json:"depth"`
	ArchiveFormat  string         `json:"archive_format"`
}
type Runner struct {
	Python, Root, Temp string
	Tessdata           string
	ToolsRoot          string
	Registry           string
	Timeout            time.Duration
	gate               chan struct{}
}

func New(python, root, temp string) *Runner {
	return &Runner{Python: python, Root: root, Temp: temp, Timeout: 15 * time.Minute, gate: make(chan struct{}, 1)}
}

// Run calls consume synchronously. Artifact paths exist only during this call;
// the caller must stream them into durable storage before consume returns.
func (r *Runner) Run(ctx context.Context, request Request, consume func(Record) error) (err error) {
	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	dir, err := os.MkdirTemp(r.Temp, "parse-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	request.Output = dir
	input, err := json.Marshal(request)
	if err != nil {
		return err
	}
	cmd := r.command(ctx, dir, input)
	stderr := &boundedError{}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	finish, err := process.StartHelper(cmd, r.Registry)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, finish()) }()
	watchCtx, stopWatching := context.WithCancel(ctx)
	memoryDone := make(chan error, 1)
	go func() { memoryDone <- process.WatchMemory(watchCtx, cmd.Process.Pid, 512*1024*1024) }()
	readErr := records(stdout, dir, consume)
	if readErr != nil {
		_ = cmd.Cancel()
	}
	waitErr := cmd.Wait()
	stopWatching()
	memoryErr := <-memoryDone
	if memoryErr != nil {
		return memoryErr
	}
	return runFailure(ctx, waitErr, readErr, stderr)
}

func runFailure(ctx context.Context, waitErr, readErr error, stderr *boundedError) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if readErr != nil {
		return fmt.Errorf("%w: %s", readErr, stderr.String())
	}
	if waitErr != nil {
		return fmt.Errorf("parser failed: %w: %s", waitErr, stderr.String())
	}
	return nil
}

func (r *Runner) command(ctx context.Context, dir string, input []byte) *exec.Cmd {
	cmd := exec.CommandContext(ctx, r.Python, "-m", "parser.parser_helper")
	cmd.Dir = r.Root
	cmd.Env = environmentWithTools(dir, r.ToolsRoot)
	if r.Tessdata != "" {
		cmd.Env = append(cmd.Env, "TESSDATA_PREFIX="+r.Tessdata)
	}
	cmd.Stdin = bytes.NewReader(input)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	return cmd
}

func environment(temp string) []string { return environmentWithTools(temp, "") }
func environmentWithTools(temp, tools string) []string {
	values := append(
		process.DocumentEnvironment(temp, tools),
		"PYTHONUNBUFFERED=1",
		"PYTHONDONTWRITEBYTECODE=1",
		"PYTHONNOUSERSITE=1",
		"TREE_PARSER_ISOLATED=1",
	)
	if value, ok := os.LookupEnv("SYSTEMROOT"); ok {
		values = append(values, "SYSTEMROOT="+value)
	}
	return values
}

func records(input io.Reader, dir string, consume func(Record) error) error {
	decoder := json.NewDecoder(io.LimitReader(input, 32*1024*1024+1))
	complete := false
	for count := 0; count < 12000; count++ {
		var record Record
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			if complete {
				return nil
			}
			return errors.New("parser exited without completing its output")
		}
		if err != nil {
			return err
		}
		if complete {
			return errors.New("parser emitted data after completion")
		}
		if record.Type == "error" {
			return fmt.Errorf("%s: %s", record.Reason, record.Message)
		}
		if record.Type == "artifact" {
			if filepath.Base(record.Path) != record.Path || record.Path == "." {
				return errors.New("unsafe parser artifact path")
			}
			record.Path = filepath.Join(dir, record.Path)
			info, err := os.Lstat(record.Path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("parser artifact is not a regular file")
			}
		}
		if err = consume(record); err != nil {
			return err
		}
		complete = record.Type == "complete"
	}
	return errors.New("parser output exceeded the record limit")
}

type boundedError struct{ strings.Builder }

func (b *boundedError) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 4096 - b.Len()
	if remaining > 0 {
		_, _ = b.Builder.Write(p[:min(n, remaining)])
	}
	return n, nil
}
