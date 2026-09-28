package server

import (
	"net/http"
)

// The page's generation travels with its writes. A cookie or a token inserted by
// the current frontend would silently authorize a stale tab after a mode switch.
// This is a freshness check, not user authentication.
func (s *Server) currentRuntime(w http.ResponseWriter, r *http.Request) bool {
	// MCP POST carries only the fixed read-only registry, never application writes.
	if r.Method == "POST" && (r.URL.Path == "/mcp" || r.URL.Path == "/mcp/") {
		return true
	}
	if r.Method == "GET" || r.Method == "HEAD" || r.Method == "OPTIONS" {
		return true
	}
	if s.config.Mode == "test" && s.config.Session == "" {
		return true
	}
	token := r.Header.Get("X-Tree-Runtime")
	if s.config.Session != "" && token == s.config.Session {
		return true
	}
	writeJSON(
		w,
		http.StatusConflict,
		map[string]string{
			"code":   "runtime_changed",
			"detail": "Tree-eClass has restarted or changed modes. Reload this page before saving; this request made no changes.",
		},
	)
	return false
}
