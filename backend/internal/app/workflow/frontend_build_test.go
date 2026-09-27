package workflow

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tree-eclass/internal/infrastructure/process"
)

func TestNativeFrontendBuildWatch(t *testing.T) {
	if os.Getenv("TREE_TEST_FRONTEND_BUILD") == "" {
		t.Skip("enable the prepared browser build fixture")
	}
	t.Parallel()
	c, main, entry := newFrontendWatchFixture(t)
	t.Cleanup(func() {
		if err := c.Processes.Stop("frontend-build"); err != nil {
			t.Error(err)
		}
	})
	if err := c.startFrontendBuild(t.Context(), c.Repo); err != nil {
		_ = c.Logs("frontend-build")
		t.Fatal(err)
	}
	first, err := os.ReadFile(entry)
	if err != nil {
		t.Fatal(err)
	}
	front := filepath.Join(c.Repo, "frontend")
	writeFrontendTheme(t, front, "green")
	waitBrowserBuild(t, c, func() bool { b, _ := os.ReadFile(entry); return string(b) != string(first) })
	second, _ := os.ReadFile(entry)
	logPath := filepath.Join(c.Processes.Root, "frontend-build/output.log")
	prior, _ := os.ReadFile(logPath)
	writeFrontendFile(t, front, "src/main.tsx", main+"\n<<< invalid source")
	waitBrowserBuild(t, c, func() bool {
		b, _ := os.ReadFile(logPath)
		return len(b) > len(prior) && strings.Contains(strings.ToLower(string(b[len(prior):])), "error")
	})
	failed, _ := os.ReadFile(entry)
	if string(failed) != string(second) {
		t.Fatal("failed build replaced the working entry")
	}
	writeFrontendFile(t, front, "src/main.tsx", strings.ReplaceAll(main, "Browser fixture", "Repaired browser fixture"))
	waitBrowserBuild(t, c, func() bool { b, _ := os.ReadFile(entry); return string(b) != string(second) })
	if err = c.Processes.Stop("frontend-build"); err != nil {
		t.Fatal(err)
	}
	if c.Processes.Alive("frontend-build") {
		t.Fatal("compiler survived shutdown")
	}
}

func newFrontendWatchFixture(t *testing.T) (*Controller, string, string) {
	t.Helper()
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{
		Root:       t.TempDir(),
		Repo:       t.TempDir(),
		Executable: binary,
		Config:     Config{Bun: bun},
		State:      Selection{Mode: "development"},
	}
	c.Processes = process.Manager{Root: filepath.Join(c.Root, "processes"), Executable: binary}
	front := filepath.Join(c.Repo, "frontend")
	if err = os.MkdirAll(filepath.Join(front, "src"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"vite.config.ts", "postcss.config.cjs", "index.html"} {
		b, err := os.ReadFile(filepath.Join(repo, "frontend", file))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(front, file), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Symlink(filepath.Join(repo, "frontend/node_modules"), filepath.Join(front, "node_modules")); err != nil {
		t.Fatal(err)
	}
	writeFrontendFile(t, front, "package.json", `{"type":"module","scripts":{"dev":"vite build --watch"}}`)
	main := `import * as stylex from '@stylexjs/stylex';import { createRoot } from 'react-dom/client';import { theme } from './theme.stylex';import './entry.css';const s=stylex.create({title:{color:theme.foreground}});createRoot(document.getElementById('tree-root')).render(<h1 {...stylex.props(s.title)}>Browser fixture</h1>);`
	writeFrontendFile(t, front, "src/main.tsx", main)
	writeFrontendFile(t, front, "src/entry.css", "@stylex;")
	writeFrontendTheme(t, front, "red")
	return c, main, filepath.Join(c.Root, "build/frontend/index.html")
}

func writeFrontendFile(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeFrontendTheme(t *testing.T, root, color string) {
	t.Helper()
	writeFrontendFile(
		t,
		root,
		"src/theme.stylex.ts",
		fmt.Sprintf(
			"import * as stylex from '@stylexjs/stylex';export const theme=stylex.defineConsts({foreground:%q});",
			color,
		),
	)
}

func waitBrowserBuild(t *testing.T, c *Controller, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = c.Logs("frontend-build")
	t.Fatal("browser rebuild did not reach expected state")
}

func TestSingleHTTPPortReadsPreviousConfiguration(t *testing.T) {
	var p Ports
	if err := p.UnmarshalJSON([]byte(`{"Frontend":8000,"API":8001,"Postgres":15432}`)); err != nil || p.HTTP != 8000 ||
		p.Postgres != 15432 {
		t.Fatal(p, err)
	}
	if err := p.UnmarshalJSON([]byte(`{"HTTP":8123,"Frontend":8000}`)); err != nil || p.HTTP != 8123 {
		t.Fatal(p, err)
	}
}
