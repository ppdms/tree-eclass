package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/discord"
)

type syntheticDiscord struct {
	broken bool
	calls  int
}

func (f *syntheticDiscord) Run(ctx context.Context, dir, token string, args ...string) (string, error) {
	f.calls++
	switch args[0] {
	case "guilds":
		return "300 | Σχολή\n", nil
	case "channels":
		return "200 | Εξετάσεις\n", nil
	case "export":
		if err := os.MkdirAll(filepath.Join(dir, "media"), 0700); err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(dir, "media", "ύλη.html"), []byte("<html>untrusted attachment</html>"), 0600); err != nil {
			return "", err
		}
		var artifact map[string]any
		if err := json.Unmarshal(discordFixture(), &artifact); err != nil {
			return "", err
		}
		list := artifact["messages"].([]any)
		list[1].(map[string]any)["attachments"] = []any{
			map[string]any{"id": "8", "fileName": "ύλη.html", "fileSizeBytes": 33, "url": "media/ύλη.html"},
		}
		raw, _ := json.Marshal(artifact)
		if err := os.WriteFile(filepath.Join(dir, "export.json"), raw, 0600); err != nil {
			return "", err
		}
		if f.broken {
			if err := os.WriteFile(filepath.Join(dir, "export-2.json"), raw[:len(raw)-1], 0600); err != nil {
				return "", err
			}
		}
		return "Successfully exported 1 channel(s).", nil
	}
	return "", errors.New("unexpected fake exporter command")
}

type discordIntervalFixture struct {
	c       *Controller
	ctx     context.Context
	pool    *pgxpool.Pool
	objects *blob.Store
	runner  *syntheticDiscord
	service discord.Service
	now     time.Time
	reader  messages.Reader
	temp    string
}

func TestNativeDiscordIntervals(t *testing.T) {
	t.Parallel()
	fixture := newDiscordIntervalFixture(t)
	objectID := discordIntervalPublicationChecks(t, fixture)
	discordIntervalMediaChecks(t, fixture, objectID)
	discordIntervalMappingChecks(t, fixture, objectID)
}

func newDiscordIntervalFixture(t *testing.T) *discordIntervalFixture {
	t.Helper()
	c := nativeSharedController(t)
	conn, objects := startTestStorage(t, c)
	ctx := t.Context()
	t.Cleanup(func() { conn.Close(ctx) })
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err = pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(901,'Μάθημα','/901'),(902,'Άλλο','/902');INSERT INTO app.discord_course_channels(root_channel_id,course_id) VALUES('200',901);
 INSERT INTO app.discord_export_settings(id,enabled,token) VALUES(1,1,'synthetic') ON CONFLICT(id) DO UPDATE SET enabled=1,token='synthetic'`); err != nil {
		t.Fatal(err)
	}
	runner := &syntheticDiscord{broken: true}
	temp := t.TempDir()
	service := discord.Service{Pool: pool, Objects: objects, Runner: runner, Temp: temp}
	return &discordIntervalFixture{
		c: c, ctx: ctx, pool: pool, objects: objects, runner: runner, service: service,
		now: time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC), reader: messages.Reader{Pool: pool}, temp: temp,
	}
}

func discordIntervalPublicationChecks(t *testing.T, fixture *discordIntervalFixture) string {
	t.Helper()
	ctx, pool, service, now, runner := fixture.ctx, fixture.pool, fixture.service, fixture.now, fixture.runner
	if err := service.Tick(ctx, now, true); err == nil {
		t.Fatal("partial interval committed")
	}
	var count, cursor int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages.archive_sources`).Scan(&count); err != nil || count != 0 {
		t.Fatal("first partition escaped failed transaction", count, err)
	}
	if err := pool.QueryRow(ctx, `SELECT after_id FROM messages.export_cursors WHERE root_id=200 AND channel_id=200`).Scan(&cursor); err != nil || cursor != 0 {
		t.Fatal("failed interval advanced cursor", cursor, err)
	}
	runner.broken = false
	if err := service.Tick(ctx, now.Add(6*time.Minute), false); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT after_id FROM messages.export_cursors WHERE root_id=200 AND channel_id=200`).Scan(&cursor); err != nil || cursor <= 9007199254741005 {
		t.Fatal("complete interval did not advance", cursor, err)
	}
	var objectID string
	if err := pool.QueryRow(ctx, `SELECT object_id FROM messages.archive_media LIMIT 1`).Scan(&objectID); err != nil {
		t.Fatal(err)
	}
	return objectID
}

func discordIntervalMediaChecks(t *testing.T, fixture *discordIntervalFixture, objectID string) {
	t.Helper()
	ctx, now, reader := fixture.ctx, fixture.now, fixture.reader
	search, err := reader.Search(
		ctx,
		messages.SearchRequest{Query: "υλη εξετασης", Courses: []int64{901}, Mode: "hybrid", Limit: 20},
		now,
	)
	if err != nil || len(search.Results) == 0 {
		t.Fatal(search, err)
	}
	found := false
	for _, hit := range search.Results {
		reading, err := reader.Read(ctx, hit.ID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range reading.Messages {
			for _, attachment := range message.Attachments {
				if attachment["url"] == "/api/discord/media/"+objectID {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("local attachment was not converted to opaque S3 content URL")
	}
}

func discordIntervalMappingChecks(t *testing.T, fixture *discordIntervalFixture, objectID string) {
	t.Helper()
	ctx, c, pool, objects, temp := fixture.ctx, fixture.c, fixture.pool, fixture.objects, fixture.temp
	api, err := server.New(ctx, server.Config{
		Mode: "test", DatabaseURL: c.databaseURL(),
		ObjectsRoot: c.testObjectsRoot(), Temp: temp,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(api.Close)
	request := httptest.NewRequest("GET", "http://127.0.0.1/api/discord/media/"+objectID, nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != 200 || !strings.HasPrefix(response.Header().Get("Content-Disposition"), "attachment") ||
		response.Body.String() != "<html>untrusted attachment</html>" {
		t.Fatal("media response", response.Code, response.Header(), response.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE app.discord_course_channels SET course_id=902 WHERE root_channel_id='200'`); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.reader.Media(ctx, objectID); err == nil {
		t.Fatal("stale mapping exposes attachment")
	}
	importer := messages.Importer{Pool: pool, Blobs: objects, Temp: temp}
	if worked, err := importer.ReindexMapped(ctx); err != nil || !worked {
		t.Fatal("remap from durable S3 export", worked, err)
	}
	if _, err = fixture.reader.Media(ctx, objectID); err != nil {
		t.Fatal("remapped attachment unavailable", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE app.discord_export_settings SET enabled=0 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	calls := fixture.runner.calls
	if err = fixture.service.Tick(ctx, fixture.now.Add(time.Hour), true); err != nil || fixture.runner.calls != calls {
		t.Fatal("disabled exporter ran", err)
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatal("temporary exports retained", entries, err)
	}
}
