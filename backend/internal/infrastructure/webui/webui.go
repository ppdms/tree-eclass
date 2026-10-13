// Package webui serves the compiled browser application without a JavaScript server.
package webui

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"html"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
)

type Handler struct {
	root                     *os.Root
	session, namespace, mode string
}

func New(dir, session, namespace, mode string) (*Handler, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	h := &Handler{root: root, session: session, namespace: namespace, mode: mode}
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
	file, info, err := h.openAsset(name)
	if err != nil {
		miss(w, r)
		return
	}
	defer file.Close()
	w.Header().Set("Cache-Control", "no-cache")
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Header().Add("Vary", "Accept-Encoding")
		if len(r.Header.Values("Range")) == 0 && acceptsGzip(r.Header.Values("Accept-Encoding")) {
			compressed, _, err := h.openAsset(name + ".gz")
			if err == nil {
				defer compressed.Close()
				contentType := mime.TypeByExtension(path.Ext(name))
				if contentType == "" {
					var sample [512]byte
					n, _ := file.Read(sample[:])
					contentType = http.DetectContentType(sample[:n])
				}
				w.Header().Set("Content-Type", contentType)
				file = compressed
				w = gzipAssetWriter{w}
			}
		}
	}
	http.ServeContent(w, r, name, info.ModTime(), file)
}

// Original files and sidecars share a rooted regular-file boundary. Reject leaf
// symlinks; parent resolution retains os.Root's existing in-root semantics.
// Stat the opened handle too, rather than reopening for serving.
func (h *Handler) openAsset(name string) (*os.File, os.FileInfo, error) {
	info, err := h.root.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, os.ErrNotExist
	}
	file, err := h.root.Open(name)
	if err != nil {
		return nil, nil, err
	}
	info, err = file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = os.ErrNotExist
	}
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	return file, info, nil
}

// Delay the encoding header until ServeContent has handled preconditions and
// set the selected file's Content-Length. It otherwise assumes gzip middleware
// changes the bytes and omits the length. No request-time compression occurs.
type gzipAssetWriter struct{ http.ResponseWriter }

func (w gzipAssetWriter) WriteHeader(status int) {
	if status == http.StatusOK {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w gzipAssetWriter) ReadFrom(r io.Reader) (int64, error) {
	if reader, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return reader.ReadFrom(r)
	}
	return io.Copy(w.ResponseWriter, r)
}

// An explicit gzip entry overrides wildcard, including a zero or malformed
// quality. Conflicting duplicates are conservatively treated as unacceptable.
func acceptsGzip(values []string) bool {
	gzipSeen, wildcardSeen := false, false
	gzipAllowed, wildcardAllowed := true, true
	for _, value := range values {
		for item := range strings.SplitSeq(value, ",") {
			coding, parameters, hasParameters := strings.Cut(item, ";")
			coding = strings.Trim(coding, " \t")
			if !strings.EqualFold(coding, "gzip") && coding != "*" {
				continue
			}
			allowed := true
			if hasParameters {
				key, quality, found := strings.Cut(parameters, "=")
				allowed = found && strings.EqualFold(strings.Trim(key, " \t"), "q") &&
					positiveQuality(strings.Trim(quality, " \t"))
			}
			if strings.EqualFold(coding, "gzip") {
				gzipSeen = true
				gzipAllowed = gzipAllowed && allowed
			} else {
				wildcardSeen = true
				wildcardAllowed = wildcardAllowed && allowed
			}
		}
	}
	if gzipSeen {
		return gzipAllowed
	}
	return wildcardSeen && wildcardAllowed
}

// RFC 9110 qvalue: 0 or 1, optionally a decimal point and up to three digits;
// fractions of 1 must be zero. Invalid values never opt a client into gzip.
func positiveQuality(value string) bool {
	if len(value) == 0 || (value[0] != '0' && value[0] != '1') {
		return false
	}
	positive := value[0] == '1'
	if len(value) == 1 {
		return positive
	}
	if len(value) > 5 || value[1] != '.' {
		return false
	}
	for i := 2; i < len(value); i++ {
		digit := value[i]
		if digit < '0' || digit > '9' || (value[0] == '1' && digit != '0') {
			return false
		}
		positive = positive || digit != '0'
	}
	return positive
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
