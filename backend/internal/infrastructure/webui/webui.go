// Package webui serves the compiled browser application without a JavaScript server.
package webui

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
)

type Handler struct {
	root                     *os.Root
	files                    http.Handler
	session, namespace, mode string
}

func New(dir, session, namespace, mode string) (*Handler, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	h := &Handler{root: root, files: http.FileServerFS(root.FS()), session: session, namespace: namespace, mode: mode}
	if _, _, err = h.entry(); err != nil {
		root.Close()
		return nil, err
	}
	return h, nil
}

func (h *Handler) Close() error { return h.root.Close() }

func (h *Handler) entry() ([]byte, string, error) {
	f, err := h.root.Open("index.html")
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 2<<20))
	if err != nil {
		return nil, "", err
	}
	if len(b) >= 2<<20 {
		return nil, "", fmt.Errorf("frontend entry exceeds size limit")
	}
	for _, marker := range []string{"__TREE_RUNTIME__", "__TREE_STORAGE__", "__TREE_MODE__", "__TREE_BUILD__"} {
		if !bytes.Contains(b, []byte(marker)) {
			return nil, "", fmt.Errorf("frontend entry missing %s", marker)
		}
	}
	return b, fmt.Sprintf("%x", sha256.Sum256(b)), nil
}

var page = regexp.MustCompile(
	`^/(?:activity|announcements|timeline|history|courses(?:/[^/]+(?:/changes/[^/]+)?)?|study(?:/session)?|exercises|settings|ask)?/?$`,
)

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD")
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Method not allowed", 405)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path == "/_app/build" {
		h.build(w, r)
		return
	}
	if r.URL.Path == "/index.html" || page.MatchString(r.URL.Path) {
		h.document(w, r, http.StatusOK)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/assets/") || strings.HasPrefix(r.URL.Path, "/fonts/") ||
		strings.HasPrefix(r.URL.Path, "/media/") ||
		strings.HasPrefix(r.URL.Path, "/favicon/") {
		h.asset(w, r)
		return
	}
	// API/file misses must never become a successful HTML response.
	if strings.HasPrefix(r.URL.Path, "/api") || strings.HasPrefix(r.URL.Path, "/files") ||
		strings.HasPrefix(r.URL.Path, "/mcp") ||
		strings.HasPrefix(r.URL.Path, "/_app") {
		miss(w, r)
		return
	}
	h.document(w, r, http.StatusNotFound)
}

// miss answers 404 without a cacheable body: a stale hashed asset must never
// poison an edge cache with a wrong-MIME miss.
func miss(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	http.NotFound(w, r)
}

func (h *Handler) document(w http.ResponseWriter, r *http.Request, status int) {
	w.Header().Set("Cache-Control", "no-store")
	b, digest, err := h.entry()
	if err != nil {
		http.Error(w, "Frontend build unavailable", http.StatusServiceUnavailable)
		return
	}
	replacements := strings.NewReplacer(
		"__TREE_RUNTIME__",
		html.EscapeString(h.session),
		"__TREE_STORAGE__",
		html.EscapeString(h.namespace),
		"__TREE_MODE__",
		html.EscapeString(h.mode),
		"__TREE_BUILD__",
		digest,
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if r.Method != "HEAD" {
		_, _ = io.WriteString(w, replacements.Replace(string(b)))
	}
}

func (h *Handler) asset(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	for _, part := range strings.Split(name, "/") {
		if strings.HasPrefix(part, ".") || part == "" {
			miss(w, r)
			return
		}
	}
	info, err := h.root.Stat(name)
	if err != nil || !info.Mode().IsRegular() {
		miss(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	h.files.ServeHTTP(w, r)
}

func (h *Handler) build(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if h.mode != "development" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("X-Tree-Runtime") != h.session {
		http.Error(w, "Runtime changed; reload before saving", http.StatusConflict)
		return
	}
	_, digest, err := h.entry()
	if err != nil {
		http.Error(w, "Build unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if r.Method != "HEAD" {
		_, _ = io.WriteString(w, digest)
	}
}
