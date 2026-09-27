package blob

import (
	"fmt"
	"mime"
	"net/http"
	"os"
	"strings"
	"time"
)

// Serve and ServeDownload keep the immutable SHA-256 validator contract: a
// revision is pinned by its digest, so cached copies stay valid and range
// reads always address the same bytes.
func (s *Store) Serve(w http.ResponseWriter, r *http.Request, ref Reference, filename string) {
	s.serve(w, r, ref, filename, "inline")
}
func (s *Store) ServeDownload(w http.ResponseWriter, r *http.Request, ref Reference, filename string) {
	s.serve(w, r, ref, filename, "attachment")
}
func (s *Store) serve(w http.ResponseWriter, r *http.Request, ref Reference, filename, disposition string) {
	tag := "\"sha256-" + ref.SHA256 + "\""
	w.Header().Set("ETag", tag)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", ref.MediaType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": filename}))
	w.Header().Set("Cache-Control", "private, no-cache")
	if matches(r.Header.Get("If-None-Match"), tag, true) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if value := r.Header.Get("If-Match"); value != "" && !matches(value, tag, false) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	requested := r.Header.Get("Range")
	if validator := r.Header.Get("If-Range"); validator != "" && validator != tag {
		requested = ""
	}
	if strings.Contains(requested, ",") {
		requested = ""
	} // A full response is legal for multipart ranges.
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", fmt.Sprint(ref.Bytes))
		return
	}
	object, err := os.Open(s.path(ref.SHA256))
	if err != nil {
		http.Error(w, "Stored document is unavailable", http.StatusBadGateway)
		return
	}
	defer object.Close()
	info, err := object.Stat()
	if err == nil && info.Size() != ref.Bytes {
		err = fmt.Errorf("stored object %s has %d bytes, expected %d", ref.Key, info.Size(), ref.Bytes)
	}
	if err != nil {
		http.Error(w, "Stored document is unavailable", http.StatusBadGateway)
		return
	}
	if requested == "" {
		r.Header.Del("Range")
	}
	http.ServeContent(w, r, "", time.Time{}, object)
}

func matches(values, tag string, weak bool) bool {
	for _, value := range strings.Split(values, ",") {
		value = strings.TrimSpace(value)
		if weak {
			value = strings.TrimPrefix(value, "W/")
		}
		if value == tag || value == "*" {
			return true
		}
	}
	return false
}
