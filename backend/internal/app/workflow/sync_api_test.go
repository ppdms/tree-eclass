package workflow

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/services/synchronization"
)

func syncAPIChecks(t *testing.T, c *Controller) {
	t.Helper()
	api, err := server.New(
		t.Context(),
		server.Config{
			DatabaseURL:     c.databaseURL(),
			ObjectsRoot:     c.testObjectsRoot(),
			Mode:            "test",
			ExternalWorkers: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	host := httptest.NewServer(api)
	defer host.Close()
	var tree struct {
		Tree *courses.Node `json:"tree"`
	}
	apiJSON(t, "GET", host.URL+"/api/v1/courses/101/tree", nil, 200, &tree)
	if tree.Tree == nil || tree.Tree.Path != "/Courses/101/eclass" || tree.Tree.Files == nil ||
		tree.Tree.Children == nil {
		t.Fatal("synchronized tree API", tree.Tree)
	}
	apiJSON(t, "GET", host.URL+"/api/v1/courses/999/tree", nil, 404, nil)
	response := request(t, host.URL+"/api/courses/101/file-versions?file_path=notes.txt", nil)
	var result struct {
		Versions []synchronization.Version `json:"versions"`
	}
	err = json.NewDecoder(response.Body).Decode(&result)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(result.Versions) != 2 {
		t.Fatal("version API", result, err)
	}
	response = request(t, host.URL+"/files"+*result.Versions[0].StoragePath, nil)
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(body) == 0 {
		t.Fatal("version download", response.StatusCode, err)
	}
	response, err = http.Post(host.URL+"/api/run-check", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 409 {
		t.Fatal("parallel check API admitted", response.StatusCode)
	}
}
