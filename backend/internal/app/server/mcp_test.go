package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tree-eclass/internal/services/library"
)

func TestMCPTrustAndRuntimeBoundary(t *testing.T) {
	registry, err := library.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{config: Config{Mode: "development", Session: "current"}, mux: http.NewServeMux()}
	s.mux.Handle("/mcp", registry.HTTP())
	s.mux.HandleFunc(
		"POST /api/write",
		func(w http.ResponseWriter, r *http.Request) { t.Fatal("stale write reached handler") },
	)
	for _, test := range []struct {
		path, host, origin string
		status             int
	}{
		{"/mcp", "127.0.0.1", "", 200},
		{"/mcp", "attacker.invalid", "", 403},
		{"/mcp", "127.0.0.1", "https://attacker.invalid", 403},
		{"/mcp", "127.0.0.1", "null", 403},
		{"/api/write", "127.0.0.1", "", 409},
		{"/mcp/child", "127.0.0.1", "", 409},
	} {
		req := httptest.NewRequest(
			"POST",
			"http://"+test.host+test.path,
			strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`),
		)
		req.Header.Set("Origin", test.origin)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		out := httptest.NewRecorder()
		s.ServeHTTP(out, req)
		if out.Code != test.status {
			t.Fatal(test, out.Code, out.Body.String())
		}
	}
}
