package server

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJSONFieldsPreservesPresenceAndRejectsNested(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"username": "synthetic-student",
		"enabled":  true,
		"interval": 30.0,
		"tags":     []any{"a", "b"},
		"empty":    nil,
	})
	req := httptest.NewRequest("POST", "/api/v1/settings/credentials", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	form, ok := jsonFields(httptest.NewRecorder(), req)
	if !ok || form.Get("username") != "synthetic-student" || form.Get("enabled") != "on" ||
		form.Get("interval") != "30" || form["tags"][0] != "a" || form["tags"][1] != "b" ||
		!form.Has("empty") {
		t.Fatal("JSON field mapping failed", form)
	}
	raw, _ = json.Marshal(map[string]any{"nested": map[string]any{"a": "b"}})
	req = httptest.NewRequest("POST", "/api/v1/settings/credentials", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if _, ok := jsonFields(httptest.NewRecorder(), req); ok {
		t.Fatal("nested JSON object accepted")
	}
	req = httptest.NewRequest(
		"POST",
		"/api/v1/settings/credentials",
		strings.NewReader(`{"username":`+strings.Repeat(`"x"`, 512*1024)+`}`),
	)
	req.Header.Set("Content-Type", "application/json")
	if _, ok := jsonFields(httptest.NewRecorder(), req); ok {
		t.Fatal("unbounded JSON accepted")
	}
}
