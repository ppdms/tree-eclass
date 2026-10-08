package workflow

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/settings"
)

func settingsPageChecks(t *testing.T, pool rdbms.Pool, base string) {
	t.Helper()
	ctx := t.Context()
	service := settings.Service{Pool: pool}
	apiJSON(
		t,
		"POST",
		base+"/api/v1/settings/discord-exporter",
		map[string]any{
			"token":            "synthetic-discord-secret",
			"interval_minutes": "30",
			"include_threads":  "active",
			"enabled":          true,
			"media":            true,
		},
		200,
		nil,
	)
	apiJSON(t, "POST", base+"/api/v1/settings/discord-exporter", map[string]any{"interval_minutes": "0", "clear_token": "on"}, 400, nil)
	d, err := service.Discord(ctx)
	if err != nil || d.Token != "synthetic-discord-secret" || d.Interval != 1800 || d.Threads != "Active" || !d.Media {
		t.Fatal("Discord settings", d.Interval, d.Threads, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO app.discord_root_channels(root_channel_id,name) VALUES('1234567890123456789','Συνθετικό κανάλι')`); err != nil {
		t.Fatal(err)
	}
	var mapped struct {
		Status string `json:"status"`
		Mapped int    `json:"mapped"`
	}
	apiJSON(t, "POST", base+"/api/v1/settings/discord-course-map", map[string]any{"discord_course_1234567890123456789": "101"}, 200, &mapped)
	if mapped.Status != "saved" || mapped.Mapped != 1 {
		t.Fatal("discord course map response", mapped)
	}
	apiJSON(t, "POST", base+"/api/v1/settings/discord-course-map", map[string]any{"discord_course_1234567890123456789": "999999"}, 400, nil)
	channels, err := service.DiscordChannels(ctx)
	if err != nil || len(channels) != 1 || channels[0].CourseID == nil || *channels[0].CourseID != 101 {
		t.Fatal("mapping validation erased valid mapping", channels, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO knowledge.knowledge_state(key,value,updated_at) VALUES('synthetic_quota','{"status":"ready"}','now')`); err != nil {
		t.Fatal(err)
	}
	page, err := service.Page(ctx, map[string]string{"SYNTHETIC_API_KEY": "synthetic-ai-secret"})
	if err != nil || !page.AI.Credentials["synthetic"] || page.AI.Status["synthetic"].Status != "ready" {
		t.Fatal("AI settings presentation", err)
	}
	raw, err := json.Marshal(page)
	if err != nil || strings.Contains(string(raw), "synthetic-ai-secret") ||
		strings.Contains(string(raw), "synthetic-discord-secret") {
		t.Fatal("settings page exposed a provider/export token")
	}
	response := request(t, base+"/api/v1/settings", nil)
	raw, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != 200 || strings.Contains(string(raw), "synthetic-discord-secret") ||
		!strings.Contains(string(raw), `"storage"`) {
		t.Fatal("settings page API", response.StatusCode, err)
	}
	var probe struct {
		OK bool `json:"ok"`
	}
	apiJSON(t, "POST", base+"/api/settings/test-storage", nil, 200, &probe)
	if !probe.OK {
		t.Fatal("local S3 contract probe failed")
	}
	apiJSON(t, "POST", base+"/api/v1/settings/discord-exporter", map[string]any{"clear_token": "on"}, 200, nil)
	d, err = service.Discord(ctx)
	if err != nil || d.Token != "" {
		t.Fatal("explicit Discord token clear", err)
	}
}
