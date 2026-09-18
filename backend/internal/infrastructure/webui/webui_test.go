package webui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T, mode string) (*Handler, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "assets"), 0700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"index.html":            `<html data-tree-runtime="__TREE_RUNTIME__" data-tree-storage="__TREE_STORAGE__" data-tree-mode="__TREE_MODE__" data-tree-build="__TREE_BUILD__"><script src="/assets/app-fixture.js"></script></html>`,
		"assets/app-fixture.js": "console.log('fixture')",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	h, err := New(root, `session"<`, "development:fixture:", mode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, root
}

func TestBrowserPagesAndAssetBoundaries(t *testing.T) {
	h, root := fixture(t, "stable")
	outside := filepath.Join(t.TempDir(), "private.txt")
	os.WriteFile(outside, []byte("private"), 0600)
	if err := os.Symlink(outside, filepath.Join(root, "assets/escape.js")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/courses", "/courses/42", "/courses/42/changes/a%3Ab", "/study/session?course_id=42", "/ask?c=fixture", "/settings"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" ||
			!strings.Contains(w.Body.String(), `session&#34;&lt;`) ||
			strings.Contains(w.Body.String(), "__TREE_") {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/assets/escape.js", "/assets/", "/assets/missing.js", "/assets/.secret", "/api/missing", "/files/missing", "/mcp/missing", "/src/main.tsx"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 || strings.Contains(w.Body.String(), "private") {
			t.Fatal(path, w.Code, w.Body.String())
		}
		if strings.HasPrefix(path, "/api") && strings.Contains(w.Body.String(), "<html") {
			t.Fatal("API miss became HTML")
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/courses", strings.NewReader("test")))
	if w.Code != 405 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("HEAD", "/courses", nil))
	if w.Code != 200 || w.Body.Len() != 0 {
		t.Fatal("HEAD", w.Code, w.Body.String())
	}
	ts := httptest.NewServer(h)
	defer ts.Close()
	req, _ := http.NewRequest("GET", ts.URL+"/assets/app-fixture.js", nil)
	req.Header.Set("Range", "bytes=0-6")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 206 || string(body) != "console" ||
		!strings.Contains(response.Header.Get("Cache-Control"), "immutable") {
		t.Fatal(response.StatusCode, string(body))
	}
}

func TestDevelopmentBuildCannotRefreshRuntimeAuthority(t *testing.T) {
	h, root := fixture(t, "development")
	read := func(token string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/_app/build", nil)
		r.Header.Set("X-Tree-Runtime", token)
		h.ServeHTTP(w, r)
		return w
	}
	if read("old-session").Code != 409 || read("").Code != 409 {
		t.Fatal("stale page can refresh")
	}
	first := read(h.session)
	if first.Code != 200 || first.Body.Len() != 64 {
		t.Fatal(first.Code, first.Body.String())
	}
	p := filepath.Join(root, "index.html")
	b, _ := os.ReadFile(p)
	if err := os.WriteFile(p, append(b, []byte("<!-- changed -->")...), 0600); err != nil {
		t.Fatal(err)
	}
	next := read(h.session)
	if next.Code != 200 || next.Body.String() == first.Body.String() {
		t.Fatal("successful build was not detected")
	}
	h.session = "next-session"
	if read(`session"<`).Code != 409 {
		t.Fatal("mode switch authorized old tab")
	}
}
