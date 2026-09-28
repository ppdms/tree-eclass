package server

import (
	"bytes"
	"encoding/json"
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

func TestRuntimeHeaderAdmitsCurrentGenerationAndIgnoresQuery(t *testing.T) {
	s := &Server{config: Config{Mode: "stable", Session: "stable-two"}, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/v1/settings/preferences", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if !bodyJSON(w, r, &body) {
			t.Error("guard consumed JSON body")
			return
		}
		if body["check_interval_minutes"] != "90" {
			t.Error("guard consumed form fields")
		}
		w.WriteHeader(204)
	})
	encoded, _ := json.Marshal(map[string]any{"check_interval_minutes": "90"})
	r := httptest.NewRequest("POST", "http://127.0.0.1/api/v1/settings/preferences", bytes.NewReader(encoded))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Tree-Runtime", "stable-two")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal("current request rejected", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(
		"POST",
		"http://127.0.0.1/api/v1/settings/preferences?_tree_runtime=stable-two",
		strings.NewReader("check_interval_minutes=90"),
	)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 409 {
		t.Fatal("query blessed a request with no page generation", w.Code)
	}
}
