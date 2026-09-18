package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/platform"
	"tree-eclass/internal/infrastructure/process"
)

func fixtureOptimizedRuntime(t *testing.T, root string) bool {
	t.Helper()
	application := os.Getenv("TREE_TEST_APPLICATION")
	if application == "" {
		return false
	}
	base := os.Getenv("TREE_TEST_PARSER_BASE")
	var meta parserDistribution
	if err := platform.ReadJSON(filepath.Join(base, "distribution.json"), &meta); err != nil {
		t.Fatal(err)
	}
	if err := verifyParserDistribution(base, meta.RequirementsSHA); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "runtime")); err != nil {
		t.Fatal(err)
	}
	packageFixtureHelpers(t, root)
	if err := cloneArtifact(filepath.Join(base, "python"), filepath.Join(root, ".python")); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err = copyParserSource(repo, root); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "optimized-api")
	if err = platform.CloneFile(application, target); err != nil {
		t.Fatal(err)
	}
	// The fixture bootstrap disables external work before exec of the actual
	// optimized application; no synthetic setting can accidentally contact eClass.
	t.Setenv("TREE_FIXTURE_APPLICATION", target)
	return true
}

func fixtureExtractionMemory(t *testing.T, c *Controller, before map[string]int64) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	if _, err = conn.Exec(t.Context(), `INSERT INTO app.courses(id,name,webdav_folder) VALUES(991002,'Memory fixture','/Courses/991002')`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		id := uploadMemoryPDF(t, c, i)
		deadline := time.Now().Add(time.Minute)
		for {
			var status, diagnostic string
			err = conn.QueryRow(t.Context(), `SELECT status,COALESCE(error,'') FROM knowledge.documents WHERE id=$1`, id).
				Scan(&status, &diagnostic)
			if err != nil {
				t.Fatal(err)
			}
			if status == "ready" {
				break
			}
			if status == "failed" || time.Now().After(deadline) {
				_ = c.Logs("api")
				t.Fatal("live extraction", status, diagnostic)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if err = process.HelpersStopped(filepath.Join(c.Root, "run/jobs/.helpers")); err != nil {
		t.Fatal(err)
	}
	after := map[string]int64{}
	var total int64
	for _, name := range []string{"api", "postgres", "seaweed"} {
		var record process.Record
		if err = platform.ReadJSON(filepath.Join(c.Processes.Root, name, "process.json"), &record); err != nil {
			t.Fatal(err)
		}
		after[name], err = process.DescendantRSS(t.Context(), record.PID)
		if err != nil {
			t.Fatal(err)
		}
		total += after[name]
	}
	t.Logf(
		"Optimized Go API serving the static browser app and packaged helpers; summed tree RSS before=%v after six PDFs=%v total=%d bytes (supervisors remain instrumented; shared pages counted per process)",
		before,
		after,
		total,
	)
	if after["api"] > 128<<20 || total > 1<<30 {
		t.Fatal("idle runtime memory budget exceeded", after, total)
	}
	if after["api"]-before["api"] > 32<<20 {
		t.Fatal("API retained excessive memory after repeated extraction", before["api"], after["api"])
	}
}

func uploadMemoryPDF(t *testing.T, c *Controller, index int) string {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", fmt.Sprintf("memory-%d.pdf", index))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = part.Write(tinyPDF(strings.Repeat("Synthetic parser memory fixture. ", 2048) + fmt.Sprint(index))); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(
		t.Context(),
		"POST",
		fmt.Sprintf("http://127.0.0.1:%d/api/v1/courses/991002/materials", c.Config.Ports.HTTP),
		&body,
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Tree-Runtime", c.State.Session)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil || response.StatusCode != 202 {
		t.Fatal("synthetic upload", response.StatusCode, string(raw), err)
	}
	var payload struct {
		Material struct {
			ID string `json:"id"`
		} `json:"material"`
	}
	if err = json.Unmarshal(raw, &payload); err != nil || payload.Material.ID == "" {
		t.Fatal("upload identity", string(raw), err)
	}
	return payload.Material.ID
}
