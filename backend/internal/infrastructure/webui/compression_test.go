package webui

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func writeCompressedAsset(t *testing.T, root, name string, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string][]byte{name: body, name + ".gz": compressed.Bytes()} {
		if err := os.WriteFile(filepath.Join(root, file), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return compressed.Bytes()
}

func decodeAsset(t *testing.T, body []byte) []byte {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestAssetEncodingNegotiation(t *testing.T) {
	h, root := fixture(t, "stable")
	body := []byte(strings.Repeat("console.log('fixture');\n", 100))
	compressed := writeCompressedAsset(t, root, "assets/app-fixture.js", body)
	request := func(values []string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/assets/app-fixture.js", nil)
		for _, value := range values {
			r.Header.Add("Accept-Encoding", value)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	originalType := request(nil).Header().Get("Content-Type")
	type negotiation struct {
		name   string
		values []string
		gzip   bool
	}
	cases := []negotiation{
		{"absent", nil, false},
		{"empty", []string{""}, false},
		{"gzip", []string{"gzip"}, true},
		{"case and whitespace", []string{" br , GZip ; Q = 0.5 "}, true},
		{"small positive quality", []string{"gzip;q=0.001"}, true},
		{"one with point", []string{"gzip;q=1."}, true},
		{"one with zeros", []string{"gzip;q=1.000"}, true},
		{"multiple fields", []string{"br", "gzip;q=0.7"}, true},
		{"wildcard", []string{"br, *;q=0.5"}, true},
		{"explicit zero before wildcard", []string{"gzip;q=0, *;q=1"}, false},
		{"explicit zero after wildcard", []string{"*;q=1, gzip;q=0"}, false},
		{"explicit overrides wildcard zero", []string{"*;q=0, gzip;q=0.5"}, true},
		{"explicit invalid overrides wildcard", []string{"*;q=1, gzip;q=invalid"}, false},
		{"zero with point", []string{"gzip;q=0."}, false},
		{"zero with zeros", []string{"gzip;q=0.000"}, false},
		{"wildcard zero", []string{"*;q=0"}, false},
		{"unsupported", []string{"br, deflate, identity"}, false},
		{"unsupported alias", []string{"x-gzip"}, false},
		{"invalid coding whitespace", []string{"\u00a0gzip"}, false},
		{"unsupported member does not disable gzip", []string{"br;q=invalid, gzip"}, true},
		{"duplicate zero", []string{"gzip, gzip;q=0"}, false},
		{"duplicate invalid", []string{"gzip;q=no, gzip"}, false},
		{"duplicate wildcard zero", []string{"*, *;q=0"}, false},
		{"duplicate allowed", []string{"gzip;q=0.2, gzip;q=1"}, true},
	}
	for _, value := range []string{"", "-1", "1.1", "2", "NaN", "Inf", ".5", "01", "0.0001", "+0.5", "1e0", "\"0.5\"", "0. 5", "0.5;q=1", "\u00a01"} {
		cases = append(cases, negotiation{"invalid quality " + value, []string{"gzip;q=" + value}, false})
	}
	cases = append(cases, negotiation{"unsupported parameter", []string{"gzip;level=9"}, false})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			w := request(test.values)
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != originalType ||
				w.Header().Get("Vary") != "Accept-Encoding" ||
				w.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" {
				t.Fatal(w.Code, w.Header())
			}
			got, length := w.Body.Bytes(), len(body)
			if test.gzip {
				if w.Header().Get("Content-Encoding") != "gzip" {
					t.Fatal("gzip was not selected", w.Header())
				}
				got, length = decodeAsset(t, got), len(compressed)
			} else if w.Header().Get("Content-Encoding") != "" {
				t.Fatal("unacceptable encoding selected", w.Header())
			}
			if !bytes.Equal(got, body) || w.Header().Get("Content-Length") != strconv.Itoa(length) {
				t.Fatal("representation bytes or length changed", w.Header(), len(got))
			}
		})
	}
}

func TestCompressedAssetMIMEAndDocumentBoundaries(t *testing.T) {
	h, root := fixture(t, "stable")
	if err := os.Mkdir(filepath.Join(root, "media"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"assets/module.wasm", "assets/data.unknown", "media/script.js"} {
		body := []byte(strings.Repeat("plain data\n", 100))
		if strings.HasSuffix(name, ".wasm") {
			body = append([]byte{'\x00', 'a', 's', 'm', '\x01', '\x00', '\x00', '\x00'}, body...)
		}
		writeCompressedAsset(t, root, name, body)
		var originalType string
		for _, encoding := range []string{"identity", "gzip"} {
			r := httptest.NewRequest("GET", "/"+name, nil)
			r.Header.Set("Accept-Encoding", encoding)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatal(name, w.Code)
			}
			if encoding == "identity" {
				originalType = w.Header().Get("Content-Type")
			} else if w.Header().Get("Content-Type") != originalType {
				t.Fatal(name, "compressed MIME changed", originalType, w.Header())
			}
			if strings.HasSuffix(name, ".wasm") && originalType != "application/wasm" {
				t.Fatal("WASM MIME", originalType)
			}
			got := w.Body.Bytes()
			if encoding == "gzip" && strings.HasPrefix(name, "assets/") {
				if w.Header().Get("Content-Encoding") != "gzip" {
					t.Fatal(name, w.Header())
				}
				got = decodeAsset(t, got)
			} else if w.Header().Get("Content-Encoding") != "" {
				t.Fatal("non-asset was negotiated", name, w.Header())
			}
			if !bytes.Equal(got, body) {
				t.Fatal(name, "decoded representation changed")
			}
		}
	}
	r := httptest.NewRequest("GET", "/courses", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Header().Get("Content-Encoding") != "" || w.Header().Get("Cache-Control") != "no-store" ||
		strings.Contains(w.Body.String(), "__TREE_") {
		t.Fatal("dynamic document was compressed or not substituted", w.Header(), w.Body.String())
	}
}

func TestCompressedAssetHEADRangeAndConditional(t *testing.T) {
	h, root := fixture(t, "stable")
	body := []byte(strings.Repeat("console.log('fixture');\n", 100))
	compressed := writeCompressedAsset(t, root, "assets/app-fixture.js", body)
	modified := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(root, "assets/app-fixture.js"), modified, modified); err != nil {
		t.Fatal(err)
	}
	request := func(method string, headers map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/assets/app-fixture.js", nil)
		r.Header.Set("Accept-Encoding", "gzip")
		for key, value := range headers {
			r.Header.Set(key, value)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	lastModified := modified.Format(http.TimeFormat)
	for _, method := range []string{"GET", "HEAD"} {
		w := request(method, nil)
		if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "gzip" ||
			w.Header().Get("Content-Length") != strconv.Itoa(len(compressed)) ||
			w.Header().Get("Last-Modified") != lastModified || (method == "HEAD" && w.Body.Len() != 0) {
			t.Fatal(method, w.Code, w.Header(), w.Body.Len())
		}
		w = request(method, map[string]string{"Accept-Encoding": "gzip;q=0"})
		if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "" ||
			w.Header().Get("Content-Length") != strconv.Itoa(len(body)) || (method == "HEAD" && w.Body.Len() != 0) {
			t.Fatal(method, "identity", w.Code, w.Header())
		}
		w = request(method, map[string]string{"Range": "bytes=0-6"})
		if w.Code != http.StatusPartialContent || w.Header().Get("Content-Encoding") != "" ||
			w.Header().Get("Content-Length") != "7" ||
			w.Header().Get("Content-Range") != "bytes 0-6/"+strconv.Itoa(len(body)) ||
			w.Header().Get("Vary") != "Accept-Encoding" ||
			(method == "GET" && w.Body.String() != "console") || (method == "HEAD" && w.Body.Len() != 0) {
			t.Fatal(method, "range", w.Code, w.Header(), w.Body.String())
		}
		for _, headers := range []map[string]string{
			{"If-Modified-Since": lastModified}, {"If-None-Match": "*"},
		} {
			w = request(method, headers)
			if w.Code != http.StatusNotModified || w.Body.Len() != 0 ||
				w.Header().Get("Content-Encoding") != "" || w.Header().Get("Content-Length") != "" ||
				w.Header().Get("Vary") != "Accept-Encoding" ||
				!strings.Contains(w.Header().Get("Cache-Control"), "immutable") {
				t.Fatal(method, "conditional", w.Code, w.Header(), w.Body.Len())
			}
		}
		for _, headers := range []map[string]string{
			{"If-Unmodified-Since": modified.Add(-time.Hour).Format(http.TimeFormat)}, {"If-Match": "\"other\""},
		} {
			w = request(method, headers)
			if w.Code != http.StatusPreconditionFailed || w.Body.Len() != 0 ||
				w.Header().Get("Content-Encoding") != "" || w.Header().Get("Content-Length") != "" {
				t.Fatal(method, "failed precondition", w.Code, w.Header(), w.Body.Len())
			}
		}
	}
	w := request("GET", map[string]string{"Range": "bytes=0-6", "If-Range": modified.Add(-time.Hour).Format(http.TimeFormat)})
	if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "" || !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatal("If-Range changed original behavior", w.Code, w.Header())
	}
	w = request("GET", map[string]string{"Range": ""})
	if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "" || !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatal("empty Range header did not preserve original representation", w.Code, w.Header())
	}
	w = request("GET", map[string]string{"Range": "bytes=99999-"})
	if w.Code != http.StatusRequestedRangeNotSatisfiable || w.Header().Get("Content-Encoding") != "" {
		t.Fatal("invalid range", w.Code, w.Header())
	}
}

func TestCompressedAssetPathBoundaries(t *testing.T) {
	h, root := fixture(t, "stable")
	outside := filepath.Join(t.TempDir(), "private.txt")
	if err := os.WriteFile(outside, []byte("outside private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".private"), []byte("inside private"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "directory", "escape", "private"} {
		if err := os.WriteFile(filepath.Join(root, "assets", name+".js"), []byte("public original"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "assets/directory.js.gz"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"escape": outside, "private": "../.private"} {
		if err := os.Symlink(target, filepath.Join(root, "assets", name+".js.gz")); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"missing", "directory", "escape", "private"} {
		r := httptest.NewRequest("GET", "/assets/"+name+".js", nil)
		r.Header.Set("Accept-Encoding", "gzip")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "" || w.Body.String() != "public original" {
			t.Fatal(name, "unsafe sidecar did not fall back", w.Code, w.Header(), w.Body.String())
		}
	}
	if err := os.WriteFile(filepath.Join(root, "assets/orphan.js.gz"), []byte("private orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "assets/.hidden.js.gz"), []byte("hidden private"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/assets/orphan.js", "/assets/.hidden.js", "/assets/.hidden.js.gz", "/assets/directory.js.gz",
		"/assets/escape.js.gz", "/assets/private.js.gz", "/assets/../.private", "/assets/%2e%2e/.private", "/assets//private.js",
	} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Accept-Encoding", "gzip")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound || w.Header().Get("Cache-Control") != "no-store" ||
			strings.Contains(w.Body.String(), "private") {
			t.Fatal(path, "unsafe or cacheable miss", w.Code, w.Header(), w.Body.String())
		}
	}
}

func largestEmittedAsset(t *testing.T, assets []os.DirEntry, extension, prefix string) (string, int64) {
	t.Helper()
	name, size := "", int64(0)
	for _, asset := range assets {
		if !strings.HasSuffix(asset.Name(), extension) || !strings.HasPrefix(asset.Name(), prefix) {
			continue
		}
		info, err := asset.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().IsRegular() && info.Size() > size {
			name, size = asset.Name(), info.Size()
		}
	}
	if name == "" {
		t.Fatal("production build has no", prefix, extension, "asset")
	}
	return name, size
}

// The native check workflow already supplies this production build directory.
// Exercise real emitted worker JS and WASM over HTTP without browser profiling.
func TestEmittedAssetGzipHTTP(t *testing.T) {
	root := os.Getenv("TREE_TEST_FRONTEND_BUILD")
	if root == "" {
		t.Skip("enable the prepared production browser build")
	}
	h, err := New(root, "gzip-smoke", "verification:", "stable")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	server := httptest.NewServer(h)
	defer server.Close()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	assets, err := os.ReadDir(filepath.Join(root, "assets"))
	if err != nil {
		t.Fatal(err)
	}
	for _, selection := range []struct{ extension, prefix string }{
		{".js", "worker-"}, {".mjs", ""}, {".wasm", ""},
	} {
		name, size := largestEmittedAsset(t, assets, selection.extension, selection.prefix)
		var original []byte
		var originalType string
		for _, encoding := range []string{"identity", "gzip"} {
			r, err := http.NewRequest("GET", server.URL+"/assets/"+name, nil)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Accept-Encoding", encoding)
			response, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatal(readErr, closeErr)
			}
			if response.StatusCode != http.StatusOK || response.ContentLength != int64(len(body)) ||
				response.Header.Get("Vary") != "Accept-Encoding" ||
				response.Header.Get("Cache-Control") != "public, max-age=31536000, immutable" {
				t.Fatal(name, encoding, response.StatusCode, response.Header, len(body))
			}
			if encoding == "identity" {
				original, originalType = body, response.Header.Get("Content-Type")
				if response.Header.Get("Content-Encoding") != "" || int64(len(body)) != size {
					t.Fatal(name, "original representation changed", response.Header, len(body))
				}
			} else {
				if response.Header.Get("Content-Encoding") != "gzip" || response.Header.Get("Content-Type") != originalType ||
					len(body) >= len(original) || !bytes.Equal(decodeAsset(t, body), original) {
					t.Fatal(name, "compressed representation changed", response.Header, len(body))
				}
				t.Logf("emitted asset %s: original=%d gzip=%d headers=%v", name, len(original), len(body), response.Header)
			}
		}
	}
}
