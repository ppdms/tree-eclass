package server

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeSwitchRejectsStaleWriters(t *testing.T) {
	writes := 0
	s := &Server{config: Config{Mode: "development", Session: "development-one"}, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/write", func(w http.ResponseWriter, r *http.Request) { writes++; w.WriteHeader(204) })
	for _, token := range []string{"", "stable-one", "development-one"} {
		r := httptest.NewRequest("POST", "http://127.0.0.1/api/write", strings.NewReader("payload"))
		r.Header.Set("X-Tree-Runtime", token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if (w.Code == 204) != (token == "development-one") {
			t.Fatal("incorrect generation admission", token, w.Code)
		}
	}
	s.config.Mode, s.config.Session = "stable", "stable-two"
	r := httptest.NewRequest("POST", "http://127.0.0.1/api/write", nil)
	r.Header.Set("X-Tree-Runtime", "development-one")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 409 || writes != 1 || !strings.Contains(w.Body.String(), "runtime_changed") {
		t.Fatal("development tab wrote stable data")
	}
}

func TestRuntimeFormTokenPreservesBodyAndIgnoresQuery(t *testing.T) {
	s := &Server{config: Config{Mode: "stable", Session: "stable-two"}, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /settings/preferences", func(w http.ResponseWriter, r *http.Request) {
		form, ok := formBody(w, r)
		if !ok || form.Get("check_interval_minutes") != "90" {
			t.Error("guard consumed form fields")
		}
		w.WriteHeader(204)
	})
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("_tree_runtime", "stable-two")
	_ = writer.WriteField("check_interval_minutes", "90")
	_ = writer.Close()
	r := httptest.NewRequest("POST", "http://127.0.0.1/settings/preferences", &body)
	r.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal("current form rejected", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(
		"POST",
		"http://127.0.0.1/settings/preferences?_tree_runtime=stable-two",
		strings.NewReader("check_interval_minutes=90"),
	)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 409 {
		t.Fatal("query blessed a form with no page generation", w.Code)
	}
}
