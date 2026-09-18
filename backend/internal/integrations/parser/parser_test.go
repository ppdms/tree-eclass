package parser

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractionAndCredentialIsolation(t *testing.T) {
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(repo, ".venv", "bin", "python")
	if _, err = os.Stat(python); err != nil {
		t.Skip("parser environment is not installed")
	}
	temp := t.TempDir()
	path := filepath.Join(temp, "notes.txt")
	if err = os.WriteFile(path, []byte("Ελληνικές σημειώσεις\nA second line"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", "must-not-reach-parser")
	t.Setenv("OPENAI_API_KEY", "must-not-reach-parser")
	for _, entry := range environment(temp) {
		if strings.Contains(entry, "must-not-reach-parser") {
			t.Fatal("credential leaked")
		}
	}
	runner := New(python, repo, temp)
	var text strings.Builder
	completed := false
	err = runner.Run(
		context.Background(),
		Request{
			Operation: "extract",
			Path:      path,
			Source: &Source{
				CourseID:    101,
				CourseName:  "Synthetic",
				DisplayName: "notes.txt",
				SourcePath:  "/notes.txt",
				SourceHash:  "hash",
				MIMEType:    "text/plain",
			},
		},
		func(r Record) error {
			if r.Type == "unit" {
				text.WriteString(r.Text)
			}
			completed = r.Type == "complete"
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !completed || !strings.Contains(text.String(), "Ελληνικές σημειώσεις") {
		t.Fatal("extraction lost text", text.String())
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 1 {
		t.Fatal("parser leaked temporary files", entries, err)
	}
}

func TestCancellationStopsNativeDescendants(t *testing.T) {
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(repo, ".venv/bin/python")
	if _, err = os.Stat(python); err != nil {
		t.Skip("parser environment is not installed")
	}
	root := t.TempDir()
	module := filepath.Join(root, "parser")
	if err = os.MkdirAll(module, 0700); err != nil {
		t.Fatal(err)
	}
	script := "import json,subprocess,sys,time\nr=json.load(sys.stdin)\np=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])\nopen(r['path'],'w').write(str(p.pid))\nprint(json.dumps({'type':'document'}),flush=True)\ntime.sleep(60)\n"
	if err = os.WriteFile(filepath.Join(module, "parser_helper.py"), []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(root, "child.pid")
	jobs := t.TempDir()
	runner := New(python, root, jobs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = runner.Run(
		ctx,
		Request{Operation: "extract", Path: pidFile},
		func(record Record) error { cancel(); return nil },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	pid, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	state, _ := exec.Command("ps", "-p", string(pid), "-o", "state=").Output()
	if value := strings.TrimSpace(string(state)); value != "" && !strings.HasPrefix(value, "Z") {
		t.Fatalf("native descendant survived cancellation: %s", state)
	}
	entries, err := os.ReadDir(jobs)
	if err != nil || len(entries) != 0 {
		t.Fatal("cancelled parser leaked files", entries, err)
	}
}
