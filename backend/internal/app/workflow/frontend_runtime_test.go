package workflow

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/process"
)

func TestNativeProductionFrontend(t *testing.T) {
	fixture := newProductionFrontendFixture(t)
	productionRouteChecks(t, fixture)
	rss := productionMemoryChecks(t, fixture)
	writeBrowserHold(t, fixture, rss)
	if err := fixture.c.Down(); err != nil {
		t.Fatal(err)
	}
	productionShutdownChecks(t, fixture)
}

type productionFrontendFixture struct {
	c         *Controller
	id        string
	base      string
	optimized bool
}

func newProductionFrontendFixture(t *testing.T) productionFrontendFixture {
	t.Helper()
	build := os.Getenv("TREE_TEST_FRONTEND_BUILD")
	if build == "" {
		t.Skip("build the browser app and set TREE_TEST_FRONTEND_BUILD to its output directory")
	}
	c := nativeController(t)
	requireFixtureConfig(t, c)
	id := strings.Repeat("e", 40)
	fixtureRelease(t, c, id, false)
	root := filepath.Join(c.Root, "releases", id)
	optimized := fixtureOptimizedRuntime(t, root)
	front := filepath.Join(root, "frontend")
	if err := os.RemoveAll(front); err != nil {
		t.Fatal(err)
	}
	if err := cloneArtifact(build, front); err != nil {
		t.Fatal(err)
	}
	// A stable release must start with no Bun executable available.
	c.Config.Bun = filepath.Join(t.TempDir(), "missing-bun")
	var release Release
	if err := platform.ReadJSON(filepath.Join(root, "release.json"), &release); err != nil {
		t.Fatal(err)
	}
	var err error
	release.Files, err = releaseFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = platform.WriteJSON(filepath.Join(root, "release.json"), release); err != nil {
		t.Fatal(err)
	}
	c.State.Release = id
	if err := c.Up(t.Context()); err != nil {
		_ = c.Logs("frontend")
		_ = c.Logs("api")
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Down(); err != nil {
			t.Error(err)
		}
	})
	return productionFrontendFixture{
		c: c, id: id, base: fmt.Sprintf("http://127.0.0.1:%d", c.Config.Ports.HTTP), optimized: optimized,
	}
}

func productionRouteChecks(t *testing.T, fixture productionFrontendFixture) {
	t.Helper()
	client := http.Client{Timeout: 20 * time.Second}
	for _, path := range []string{"/", "/courses", "/activity", "/announcements", "/exercises", "/history", "/timeline", "/study", "/ask", "/settings"} {
		response, err := client.Get(fixture.base + path)
		if err != nil {
			t.Fatal(path, err)
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || !strings.Contains(string(body), "<html") {
			_ = fixture.c.Logs("frontend")
			t.Fatal("Go-served browser entry", path, response.StatusCode, err)
		}
	}
	if fixture.c.Processes.Alive("frontend") || fixture.c.Processes.Alive("frontend-build") {
		t.Fatal("stable release started a JavaScript process")
	}
}

func productionMemoryChecks(t *testing.T, fixture productionFrontendFixture) map[string]int64 {
	t.Helper()
	c := fixture.c
	rss := map[string]int64{}
	var err error
	for _, name := range []string{"api", "postgres"} {
		var record process.Record
		if err := platform.ReadJSON(filepath.Join(c.Processes.Root, name, "process.json"), &record); err != nil {
			t.Fatal(err)
		}
		rss[name], err = process.DescendantRSS(t.Context(), record.PID)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Summed RSS by complete supervised process tree, bytes: %v", rss)
	if fixture.optimized {
		fixtureExtractionMemory(t, c, rss)
	}
	return rss
}

func writeBrowserHold(t *testing.T, fixture productionFrontendFixture, rss map[string]int64) {
	t.Helper()
	hold := os.Getenv("TREE_BROWSER_HOLD_FILE")
	if hold == "" {
		return
	}
	c := fixture.c
	info, _ := json.Marshal(map[string]any{"url": fixture.base, "root": c.Root, "rss": rss})
	if err := os.WriteFile(hold, info, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(hold); os.IsNotExist(err) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	_ = os.Remove(hold)
}

func productionShutdownChecks(t *testing.T, fixture productionFrontendFixture) {
	t.Helper()
	c := fixture.c
	if _, err := c.release(fixture.id); err != nil {
		t.Fatal("static runtime mutated sealed release", err)
	}
	if _, err := os.Stat(filepath.Join(c.Root, "run/frontend")); !os.IsNotExist(err) {
		t.Fatal("unexpected frontend runtime directory", err)
	}
	stoppedFixture(t, c)
}
