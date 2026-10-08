package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/settings"
)

func settingsChecks(t *testing.T, pool rdbms.Pool, base string) {
	t.Helper()
	settingsPreferencesChecks(t, pool, base)
	settingsCredentialsChecks(t, pool, base)
	apiJSON(t, "GET", base+"/api/settings/sync-status", nil, 200, nil)
	settingsPageChecks(t, pool, base)
}

func settingsPreferencesChecks(t *testing.T, pool rdbms.Pool, base string) {
	t.Helper()
	ctx := context.Background()
	service := settings.Service{Pool: pool}
	apiJSON(
		t,
		"POST",
		base+"/api/v1/settings/preferences",
		map[string]any{"semester_start": "2026-09-01", "download_base_path": "/Σπουδές"},
		200,
		nil,
	)
	apiJSON(t, "POST", base+"/api/v1/settings/preferences", map[string]any{"check_interval_minutes": "90"}, 200, nil)
	prefs, err := service.Preferences(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if prefs.CheckInterval != 90 || prefs.SemesterStart == nil || *prefs.SemesterStart != "2026-09-01" ||
		prefs.BasePath != "/Σπουδές" {
		t.Fatal("partial preferences overwrite previous fields")
	}
	apiJSON(t, "POST", base+"/api/v1/settings/preferences", map[string]any{"download_base_path": ""}, 200, nil)
	prefs, err = service.Preferences(ctx)
	if err != nil || prefs.BasePath != "" {
		t.Fatal("empty mirror path did not disable mirroring", prefs, err)
	}
	apiJSON(t, "POST", base+"/api/v1/settings/preferences", map[string]any{"download_base_path": "/"}, 422, nil)
	prefs, err = service.Preferences(ctx)
	if err != nil || prefs.BasePath != "" {
		t.Fatal("invalid root mirror path changed preferences", prefs, err)
	}
	apiJSON(
		t,
		"POST",
		base+"/api/v1/settings/preferences",
		map[string]any{"semester_start": "must-not-save", "retry_attempts": "11"},
		422,
		nil,
	)
	prefs, err = service.Preferences(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if *prefs.SemesterStart != "2026-09-01" {
		t.Fatal("failed preferences save partially committed")
	}
}

func settingsCredentialsChecks(t *testing.T, pool rdbms.Pool, base string) {
	t.Helper()
	ctx := context.Background()
	service := settings.Service{Pool: pool}
	apiJSON(
		t,
		"POST",
		base+"/api/v1/settings/credentials",
		map[string]any{"username": "synthetic-student", "password": "synthetic-password"},
		200,
		nil,
	)
	apiJSON(t, "POST", base+"/api/v1/settings/credentials", map[string]any{"username": "synthetic-other"}, 400, nil)
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
	apiJSON(
		t,
		"POST",
		base+"/api/v1/settings/credentials",
		map[string]any{"username": "synthetic-student", "clear_password": "on"},
		200,
		nil,
	)
	credentials, err = service.Credentials(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Password != "" {
		t.Fatal("explicit password clear did not persist")
	}
}
