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

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/storage"
)

// Only the HTTP payload is a small fixture. Compilation, automatic detection,
// process replacement, native storage, checkpoint restoration and shutdown are real.
func TestNativeAutomaticGoReload(t *testing.T) {
	t.Parallel()
	c, session, write := newReloadFixture(t)
	t.Cleanup(func() {
		if err := c.Down(); err != nil {
			t.Error(err)
		}
	})
	reloadDuringDevelopment(t, c, session, write)
	verifyReloadExit(t, c)
}

func newReloadFixture(t *testing.T) (*Controller, string, func(string)) {
	t.Helper()
	c := nativeController(t)
	requireFixtureConfig(t, c)
	c.Repo = sourceFixture(t)
	c.Config.SourceRoot = c.Repo
	if err := platform.WriteJSON(filepath.Join(c.Root, "config.json"), c.Config); err != nil {
		t.Fatal(err)
	}
	frontend := filepath.Join(c.Repo, "frontend")
	if err := os.WriteFile(filepath.Join(frontend, "package.json"), []byte(`{"scripts":{"dev":"bun build.js"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	front := "await Bun.write(process.env.TREE_FRONTEND_OUT+'/index.html'," + fmt.Sprintf(
		"%q",
		fixtureHTML,
	) + ");setInterval(()=>{},1000);"

	if err := os.WriteFile(filepath.Join(frontend, "build.js"), []byte(front), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := storage.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.Repo, "backend/cmd/tree-eclass/main.go")
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	write := func(version string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(reloadProgram(version, string(encoded))), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("first")
	conn, _ := startTestStorage(t, c)
	if _, err = conn.Exec(t.Context(), `INSERT INTO app.courses(id,name,webdav_folder) VALUES(771,'Stable fixture','/Courses/771')`); err != nil {
		t.Fatal(err)
	}
	conn.Close(t.Context())
	if err = c.stopAll(); err != nil {
		t.Fatal(err)
	}
	if err = c.DevUp(t.Context()); err != nil {
		_ = c.Logs("api")
		_ = c.Logs("watcher")
		t.Fatal(err)
	}
	session := c.State.Session
	if !c.Processes.Alive("watcher") {
		t.Fatal("development watcher missing")
	}
	return c, session, write
}

func reloadDuringDevelopment(t *testing.T, c *Controller, session string, write func(string)) {
	t.Helper()
	path := filepath.Join(c.Repo, "backend/cmd/tree-eclass/main.go")
	conn, err := pgx.Connect(t.Context(), c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `UPDATE app.courses SET name='Development edit' WHERE id=771`); err != nil {
		t.Fatal(err)
	}
	conn.Close(t.Context())
	write("second")
	waitReloadVersion(t, c, "second:"+session)
	if err = os.WriteFile(path, []byte("this cannot compile"), 0600); err != nil {
		t.Fatal(err)
	}
	// Give the two-scan debounce and compiler time to reject this edit.
	time.Sleep(4 * time.Second)
	waitReloadVersion(t, c, "second:"+session)
	write("third")
	waitReloadVersion(t, c, "third:"+session)
	developmentWritesSurviveReload(t, c)
}

func developmentWritesSurviveReload(t *testing.T, c *Controller) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	var name string
	if err := conn.QueryRow(t.Context(), `SELECT name FROM app.courses WHERE id=771`).Scan(&name); err != nil ||
		name != "Development edit" {
		t.Fatal("reload lost development writes", name, err)
	}
	conn.Close(t.Context())
}

func verifyReloadExit(t *testing.T, c *Controller) {
	t.Helper()
	var name string
	if err := c.Down(); err != nil {
		t.Fatal(err)
	}
	stoppedFixture(t, c)
	if c.Processes.Alive("watcher") {
		t.Fatal("watcher survived shutdown")
	}
	conn, _ := startTestStorage(t, c)
	defer conn.Close(t.Context())
	if err := conn.QueryRow(t.Context(), `SELECT name FROM app.courses WHERE id=771`).Scan(&name); err != nil ||
		name != "Stable fixture" {
		t.Fatal("exit failed to restore stable data", name, err)
	}
	if _, err := os.Stat(filepath.Join(c.Root, "build")); !os.IsNotExist(err) {
		t.Fatal("compiler output survived shutdown", err)
	}
}
func reloadProgram(version, manifest string) string {
	return fmt.Sprintf(`package main
import("encoding/json";"fmt";"net/http";"os")
func main(){
 if len(os.Args)>1 { if os.Args[1]=="manifest"{fmt.Println(%q)};return }
 var cfg struct{Address string;Session string};b,_:=os.ReadFile(os.Getenv("TREE_RUNTIME_CONFIG"));if json.Unmarshal(b,&cfg)!=nil{os.Exit(2)}
 http.HandleFunc("/api/health",func(w http.ResponseWriter,r *http.Request){fmt.Fprint(w,%q+":"+cfg.Session)})
 if http.ListenAndServe(cfg.Address,nil)!=nil{os.Exit(3)}
}`, manifest, version)
}
func waitReloadVersion(t *testing.T, c *Controller, want string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	client := http.Client{Timeout: time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d/api/health", c.Config.Ports.HTTP)
	for time.Now().Before(deadline) {
		res, err := client.Get(url)
		if err == nil {
			b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
			res.Body.Close()
			if strings.TrimSpace(string(b)) == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	_ = c.Logs("watcher")
	_ = c.Logs("api")
	t.Fatal("automatic reload did not serve", want)
}
