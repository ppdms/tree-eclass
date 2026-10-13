package blob

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSourceAdmissionPrecedesCachedContent(t *testing.T) {
	store, _ := newTestStore(t)
	ref, err := store.Put(t.Context(), strings.NewReader("new revision"), "text/plain", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	current := "\"sha256-" + ref.SHA256 + "\""
	old := "\"sha256-" + strings.Repeat("0", 64) + "\""
	tests := []struct {
		name, method, match, none string
		status                    int
		body                      string
	}{
		{"stale source with cached current", "GET", old, current, http.StatusPreconditionFailed, ""},
		{"weak source cannot admit", "GET", "W/" + current, current, http.StatusPreconditionFailed, ""},
		{"head also admits source first", "HEAD", old, current, http.StatusPreconditionFailed, ""},
		{"admitted source still validates cache", "GET", current, current, http.StatusNotModified, ""},
		{"admitted source serves requested range", "GET", current, old, http.StatusPartialContent, "new"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "/document", nil)
			request.Header.Set("If-Match", test.match)
			request.Header.Set("If-None-Match", test.none)
			request.Header.Set("Range", "bytes=0-2")
			response := httptest.NewRecorder()
			store.Serve(response, request, ref, "source.txt")
			if response.Code != test.status || response.Body.String() != test.body {
				t.Fatalf("source-bound response = %d %q, want %d %q",
					response.Code, response.Body.String(), test.status, test.body)
			}
			if response.Header().Get("ETag") != current {
				t.Fatalf("validator changed: %q", response.Header().Get("ETag"))
			}
		})
	}
}
