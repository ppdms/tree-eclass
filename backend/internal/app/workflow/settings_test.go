package workflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/settings"
)

func settingsChecks(t *testing.T, pool *pgxpool.Pool, base string) {
	t.Helper()
	ctx := context.Background()
	service := settings.Service{Pool: pool}
	postForm(
		t,
		base+"/settings/preferences",
		url.Values{"semester_start": {"2026-09-01"}, "download_base_path": {"/Σπουδές"}},
		303,
	)
	postForm(t, base+"/settings/preferences", url.Values{"check_interval_minutes": {"90"}}, 303)
	prefs, err := service.Preferences(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if prefs.CheckInterval != 90 || prefs.SemesterStart == nil || *prefs.SemesterStart != "2026-09-01" ||
		prefs.BasePath != "/Σπουδές" {
		t.Fatal("partial preferences overwrite previous fields")
	}
	postForm(t, base+"/settings/preferences", url.Values{"download_base_path": {""}}, 303)
	prefs, err = service.Preferences(ctx)
	if err != nil || prefs.BasePath != "" {
		t.Fatal("empty mirror path did not disable mirroring", prefs, err)
	}
	postForm(t, base+"/settings/preferences", url.Values{"download_base_path": {"/"}}, 422)
	prefs, err = service.Preferences(ctx)
	if err != nil || prefs.BasePath != "" {
		t.Fatal("invalid root mirror path changed preferences", prefs, err)
	}
	postForm(
		t,
		base+"/settings/preferences",
		url.Values{"semester_start": {"must-not-save"}, "retry_attempts": {"11"}},
		422,
	)
	prefs, err = service.Preferences(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if *prefs.SemesterStart != "2026-09-01" {
		t.Fatal("failed preferences save partially committed")
	}
	postForm(
		t,
		base+"/settings/credentials",
		url.Values{"username": {"synthetic-student"}, "password": {"synthetic-password"}},
		303,
	)
	postForm(t, base+"/settings/credentials", url.Values{"username": {"synthetic-other"}}, 400)
	credentials, err := service.Credentials(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Username != "synthetic-student" || credentials.Password != "synthetic-password" {
		t.Fatal("username change retained a mismatched password")
	}
	encoded, _ := json.Marshal(credentials)
	if strings.Contains(string(encoded), "synthetic-password") {
		t.Fatal("credential DTO serializes a secret")
	}
	postForm(
		t,
		base+"/settings/credentials",
		url.Values{"username": {"synthetic-student"}, "clear_password": {"on"}},
		303,
	)
	credentials, err = service.Credentials(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Password != "" {
		t.Fatal("explicit password clear did not persist")
	}
	apiJSON(t, "GET", base+"/api/settings/sync-status", nil, 200, nil)
	settingsPageChecks(t, pool, base)
}
func postForm(t *testing.T, target string, form url.Values, status int) {
	t.Helper()
	req, err := http.NewRequest("POST", target, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		t.Fatalf("form %s: expected %d, got %d", target, status, response.StatusCode)
	}
}
