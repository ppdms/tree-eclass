package workflow

import (
	"encoding/json"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/settings"
)

func settingsPageChecks(t *testing.T, pool *pgxpool.Pool, base string) {
	t.Helper()
	ctx := t.Context()
	service := settings.Service{Pool: pool}
	postForm(
		t,
		base+"/settings/discord-exporter",
		url.Values{
			"token":            {"synthetic-discord-secret"},
			"interval_minutes": {"30"},
			"include_threads":  {"active"},
			"enabled":          {"on"},
			"media":            {"on"},
		},
		303,
	)
	postForm(t, base+"/settings/discord-exporter", url.Values{"interval_minutes": {"0"}, "clear_token": {"on"}}, 400)
	d, err := service.Discord(ctx)
	if err != nil || d.Token != "synthetic-discord-secret" || d.Interval != 1800 || d.Threads != "Active" || !d.Media {
		t.Fatal("Discord settings", d.Interval, d.Threads, err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO app.discord_root_channels(root_channel_id,name) VALUES('1234567890123456789','Συνθετικό κανάλι')`); err != nil {
		t.Fatal(err)
	}
	postForm(t, base+"/settings/discord-course-map", url.Values{"discord_course_1234567890123456789": {"101"}}, 303)
	postForm(t, base+"/settings/discord-course-map", url.Values{"discord_course_1234567890123456789": {"999999"}}, 400)
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
	postForm(t, base+"/settings/discord-exporter", url.Values{"clear_token": {"on"}}, 303)
	d, err = service.Discord(ctx)
	if err != nil || d.Token != "" {
		t.Fatal("explicit Discord token clear", err)
	}
}
