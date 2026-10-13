package workflow

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	c, entry := newFrontendWatchFixture(t)
	t.Cleanup(func() {
		if err := c.Processes.Stop("frontend-build"); err != nil {
			t.Error(err)
		}
	})
	if err := c.startFrontendBuild(t.Context(), c.Repo); err != nil {
		_ = c.Logs("frontend-build")
		t.Fatal(err)
	}
	first := frontendPublication(t, entry)
	requireChangedFrontendModules(t, nil, first.files)
	front := filepath.Join(c.Repo, "frontend")
	writeFrontendPalette(t, front, "green")
	second := waitFrontendPublication(t, c, entry, first)
	requirePreservedFrontendAssets(t, first.files, second.files)
	requireChangedFrontendModules(t, first.files, second.files)
	requireFailedFrontendBuild(t, c, front, entry, second)
	writeFrontendPalette(t, front, "blue")
	repaired := waitFrontendPublication(t, c, entry, second)
	requirePreservedFrontendAssets(t, second.files, repaired.files)
	requireChangedFrontendModules(t, second.files, repaired.files)
	if err := c.Processes.Stop("frontend-build"); err != nil {
		t.Fatal(err)
	}
	if c.Processes.Alive("frontend-build") {
		t.Fatal("compiler survived shutdown")
	}
}

func requireFailedFrontendBuild(t *testing.T, c *Controller, front, entry string, working frontendBuildPublication) {
	t.Helper()
	logPath := filepath.Join(c.Processes.Root, "frontend-build/output.log")
	prior := readFrontendFile(t, logPath)
	writeFrontendFile(t, front, "src/canvas/app/palette.ts", "<<< invalid source")
	waitBrowserBuild(t, c, func() bool {
		b, _ := os.ReadFile(logPath)
		return len(b) > len(prior) && strings.Contains(strings.ToLower(string(b[len(prior):])), "error")
	})
	if !c.Processes.Alive("frontend-build") {
		t.Fatal("compiler exited after a failed dependency rebuild")
	}
	failed := frontendPublication(t, entry)
	if !bytes.Equal(failed.entry, working.entry) || !maps.Equal(failed.files, working.files) {
		t.Fatal("failed build changed the working entry or published asset inventory")
	}
}

func newFrontendWatchFixture(t *testing.T) (*Controller, string) {
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
	prepareFrontendWatchFixture(t, repo, filepath.Join(c.Repo, "frontend"))
	return c, filepath.Join(c.Root, "build/frontend/index.html")
}

func prepareFrontendWatchFixture(t *testing.T, repo, front string) {
	t.Helper()
	for _, dir := range []string{"src/canvas/app", "tools"} {
		if err := os.MkdirAll(filepath.Join(front, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{"vite.config.ts", "index.html", "tools/releaseNotices.ts"} {
		b := readFrontendFile(t, filepath.Join(repo, "frontend", file))
		if err := os.WriteFile(filepath.Join(front, file), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(repo, "frontend/node_modules"), filepath.Join(front, "node_modules")); err != nil {
		t.Fatal(err)
	}
	writeFrontendFile(t, front, "package.json", `{"type":"module","scripts":{"dev":"vite build --watch"}}`)
	// This gate exercises the production entry and worker build graph, not the
	// application renderer: its browser module transfers a real canvas to a worker.
	writeFrontendFile(t, front, "src/canvas/app/startup.ts", frontendCanvasStartup)
	writeFrontendFile(t, front, "src/canvas/app/worker.ts", frontendCanvasWorker)
	writeFrontendFile(t, front, "src/canvas/app/paint.ts", frontendCanvasPaint)
	writeFrontendPalette(t, front, "red")
}

const frontendCanvasStartup = `
const canvas = document.getElementById('stage') as HTMLCanvasElement;
canvas.width = 320;
canvas.height = 180;
const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' });
const surface = canvas.transferControlToOffscreen();
worker.postMessage({ canvas: surface }, [surface]);
`

const frontendCanvasWorker = `
self.addEventListener('message', async (event: MessageEvent<{ canvas: OffscreenCanvas }>) => {
  const { paint } = await import('./paint');
  paint(event.data.canvas);
});
`

const frontendCanvasPaint = `
import { foreground } from './palette';
export function paint(canvas: OffscreenCanvas): void {
  const context = canvas.getContext('2d');
  if (!context) throw new Error('Canvas 2D context unavailable');
  context.fillStyle = foreground;
  context.fillRect(0, 0, canvas.width, canvas.height);
  context.fillStyle = 'white';
  context.font = '20px sans-serif';
  context.fillText('Native canvas build fixture', 16, 40);
}
`

func readFrontendFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeFrontendFile(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeFrontendPalette(t *testing.T, root, color string) {
	t.Helper()
	writeFrontendFile(t, root, "src/canvas/app/palette.ts", fmt.Sprintf("export const foreground = %q;", color))
}

type frontendBuildPublication struct {
	entry []byte
	files map[string]string
}

var frontendPublishedAsset = regexp.MustCompile(`(?:src|href)="(/assets/[^"]+)"`)

func frontendPublication(t *testing.T, entry string) frontendBuildPublication {
	t.Helper()
	b := readFrontendFile(t, entry)
	dir := filepath.Dir(entry)
	references := frontendPublishedAsset.FindAllSubmatch(b, -1)
	if len(references) == 0 {
		t.Fatal("published entry has no compiled asset references")
	}
	// Observe assets as soon as the new entry becomes visible, not after a
	// separate wait that could hide publishing the entry before its dependencies.
	for _, reference := range references {
		path := filepath.Join(dir, strings.TrimPrefix(string(reference[1]), "/"))
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			t.Fatal("published entry references an unavailable asset", path, err)
		}
	}
	files, err := releaseFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := files[".index.tmp"]; exists {
		t.Fatal("published entry left an unfinished atomic replacement")
	}
	return frontendBuildPublication{entry: b, files: files}
}

func waitFrontendPublication(
	t *testing.T, c *Controller, entry string, previous frontendBuildPublication,
) frontendBuildPublication {
	t.Helper()
	var next frontendBuildPublication
	waitBrowserBuild(t, c, func() bool {
		b, err := os.ReadFile(entry)
		if err != nil || bytes.Equal(b, previous.entry) {
			return false
		}
		next = frontendPublication(t, entry)
		return true
	})
	return next
}

func requirePreservedFrontendAssets(t *testing.T, before, after map[string]string) {
	t.Helper()
	for path, hash := range before {
		if path != "index.html" && after[path] != hash {
			t.Fatal("successful rebuild removed or changed a previously published asset", path)
		}
	}
}

func requireChangedFrontendModules(t *testing.T, before, after map[string]string) {
	t.Helper()
	for _, module := range []string{"worker", "paint"} {
		added := 0
		for path := range after {
			if strings.HasPrefix(path, "assets/"+module+"-") && strings.HasSuffix(path, ".js") && before[path] == "" {
				added++
			}
		}
		if added != 1 {
			t.Fatalf("expected one new hashed %s module, got %d", module, added)
		}
	}
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
