package blob

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Serve implements If-Range itself: SeaweedFS does not. A SHA-256 validator is
// tied to the catalog revision, and range reads always pin that object's version.
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
	object, err := s.Get(r.Context(), ref, requested)
	if err != nil {
		if isCode(err, "InvalidRange") {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", ref.Bytes))
			w.WriteHeader(416)
			return
		}
		http.Error(w, "Stored document is unavailable", http.StatusBadGateway)
		return
	}
	defer object.Body.Close()
	w.Header().Set("Content-Length", fmt.Sprint(aws.ToInt64(object.ContentLength)))
	if object.ContentRange != nil {
		w.Header().Set("Content-Range", *object.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = io.Copy(w, object.Body)
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
