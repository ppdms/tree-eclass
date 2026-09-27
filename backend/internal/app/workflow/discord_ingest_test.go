package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/infrastructure/blob"
)

func discordFixture() []byte {
	list := []map[string]any{}
	for i := 0; i < 13; i++ {
		content := "Η ύλη εξέτασης περιλαμβάνει αλγορίθμους"
		if i == 0 {
			content = "https://tenor.com/a"
		}
		m := map[string]any{
			"id":        fmt.Sprint(int64(9007199254740993) + int64(i)),
			"timestamp": time.Date(2026, 9, 12, 11, i, 0, 0, time.UTC).Format(time.RFC3339),
			"content":   content,
			"author":    map[string]any{"id": "55", "name": "Νίκος\x00"},
			"type":      "Default",
		}
		if i == 12 {
			m["reference"] = map[string]any{"messageId": "9007199254740994"}
		}
		list = append(list, m)
	}
	raw, _ := json.Marshal(
		map[string]any{
			"guild":        map[string]any{"id": "300", "name": "Σχολή"},
			"channel":      map[string]any{"id": "200", "name": "Εξετάσεις", "type": "GuildTextChat"},
			"messages":     list,
			"exportedAt":   "2026-09-12T12:00:00Z",
			"messageCount": len(list),
		},
	)
	return raw
}

type discordIngestFixture struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	blobs    *blob.Store
	importer messages.Importer
	reader   messages.Reader
	source   messages.Archive
	raw      []byte
	result   messages.ImportResult
}

func TestNativeDiscordIngest(t *testing.T) {
	t.Parallel()
	fixture := newDiscordIngestFixture(t)
	discordInitialChecks(t, fixture)
	discordRetryChecks(t, fixture)
	discordMappingChecks(t, fixture)
}

func newDiscordIngestFixture(t *testing.T) *discordIngestFixture {
	t.Helper()
	c := nativeSharedController(t)
	conn, blobs := startTestStorage(t, c)
	ctx := t.Context()
	t.Cleanup(func() { conn.Close(ctx) })
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err = pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(901,'Μάθημα','/901'),(902,'Άλλο','/902');INSERT INTO app.discord_course_channels(root_channel_id,course_id) VALUES('100',901)`); err != nil {
		t.Fatal(err)
	}
	importer := messages.Importer{Pool: pool, Blobs: blobs, Temp: t.TempDir()}
	source := messages.Archive{Root: 100, Channel: 200, Course: 901}
	raw := discordFixture()
	result, err := importer.Import(ctx, source, strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return &discordIngestFixture{
		ctx: ctx, pool: pool, blobs: blobs, importer: importer, reader: messages.Reader{Pool: pool},
		source: source, raw: raw, result: result,
	}
}

func discordInitialChecks(t *testing.T, fixture *discordIngestFixture) {
	t.Helper()
	ctx, result := fixture.ctx, fixture.result
	if result.Messages != 13 || result.Conversations != 2 {
		t.Fatal("window publication", result)
	}
	object, err := fixture.blobs.Open(ctx, result.Object)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := io.ReadAll(object)
	object.Close()
	if err != nil || string(saved) != string(fixture.raw) {
		t.Fatal("raw export changed", err)
	}
	search, err := fixture.reader.Search(
		ctx,
		messages.SearchRequest{Query: "υλη εξετασης", Courses: []int64{901}, Mode: "hybrid", Limit: 20},
		time.Now(),
	)
	if err != nil || len(search.Results) != 2 {
		t.Fatal("search", search, err)
	}
	reading, err := fixture.reader.Read(ctx, search.Results[0].ID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(reading.Messages) == 0 || reading.Messages[0].Author != "Νίκος\x00" {
		t.Fatal("original author/text lost", reading)
	}
	var before int64
	if err = fixture.pool.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=901`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	repeated, err := fixture.importer.Import(ctx, fixture.source, strings.NewReader(string(fixture.raw)))
	if err != nil || repeated.Path != result.Path || repeated.Object.VersionID != result.Object.VersionID {
		t.Fatal("retry identity", repeated, err)
	}
	var after int64
	if err = fixture.pool.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=901`).Scan(&after); err != nil ||
		after != before {
		t.Fatal("idempotent import invalidated source", before, after, err)
	}
}

func discordRetryChecks(t *testing.T, fixture *discordIngestFixture) {
	t.Helper()
	ctx := fixture.ctx
	for index, broken := range []string{
		string(fixture.raw[:len(fixture.raw)-1]),
		strings.Replace(string(fixture.raw), `"messageCount":13`, `"messageCount":14`, 1),
		strings.Replace(string(fixture.raw), `9007199254740994`, `9007199254740993`, 1),
	} {
		if _, err := fixture.importer.Import(ctx, fixture.source, strings.NewReader(broken)); err == nil {
			t.Fatal("malformed archive accepted", index)
		}
	}
	fixture.source.Before = 9007199254740994
	if _, err := fixture.importer.Import(ctx, fixture.source, strings.NewReader(string(fixture.raw))); err == nil {
		t.Fatal("out-of-interval archive accepted")
	}
	fixture.source.Before = 0
}

func discordMappingChecks(t *testing.T, fixture *discordIngestFixture) {
	t.Helper()
	ctx := fixture.ctx
	var oldID string
	if err := fixture.pool.QueryRow(
		ctx,
		`SELECT conversation_id FROM messages.conversations WHERE course_id=901 ORDER BY conversation_id LIMIT 1`,
	).Scan(&oldID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE app.discord_course_channels SET course_id=902 WHERE root_channel_id='100'`); err != nil {
		t.Fatal(err)
	}
	search, err := fixture.reader.Search(
		ctx,
		messages.SearchRequest{Query: "υλη εξετασης", Courses: []int64{901}, Mode: "hybrid", Limit: 20},
		time.Now(),
	)
	if err != nil || len(search.Results) != 0 {
		t.Fatal("search", search, err)
	}
	if _, err = fixture.reader.Read(ctx, oldID, 0, 0); err == nil {
		t.Fatal("removed mapping remains readable")
	}
	if _, err = fixture.importer.Import(ctx, fixture.source, strings.NewReader(string(fixture.raw))); err == nil {
		t.Fatal("stale mapping published")
	}
	fixture.source.Course = 902
	remapped, err := fixture.importer.Import(ctx, fixture.source, strings.NewReader(string(fixture.raw)))
	if err != nil || remapped.Path != fixture.result.Path {
		t.Fatal("remap", remapped, err)
	}
	search, err = fixture.reader.Search(
		ctx,
		messages.SearchRequest{Query: "αλγόριθμοι", Courses: []int64{902}, Mode: "hybrid", Limit: 20},
		time.Now(),
	)
	if err != nil || len(search.Results) != 2 {
		t.Fatal("remapped evidence", search, err)
	}
}
