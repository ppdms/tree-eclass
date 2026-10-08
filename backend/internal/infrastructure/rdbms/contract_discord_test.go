package rdbms_test

import (
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/messages"
	"tree-eclass/internal/infrastructure/blob"
)

func TestDiscordIntervalCursorAndFractionalRetry(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			seedDiscordMapping(t, store)
			assertDiscordIntervalPublishesAtomically(t, store)
			assertDiscordFractionalRetry(t, store)
		})
	}
}

func assertDiscordIntervalPublishesAtomically(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	source, err := store.Discord().NextExportSource(ctx, now)
	if err != nil || source.Root != 511 || source.Channel != 512 || source.Course != 821 || source.After != 0 {
		t.Fatalf("initial export source = %+v, %v", source, err)
	}
	blobs, err := blob.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := blobs.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	importer := messages.Importer{Pool: store, Blobs: blobs, Temp: t.TempDir()}
	good := writeDiscordPartition(t, 9007199254740993, "2026-09-12T11:00:00Z")
	broken := writeDiscordPartition(t, 9007199254740995, "2026-09-12T11:01:00Z")
	truncateTestFile(t, broken)
	interval := messages.Archive{Root: 511, Channel: 512, Course: 821, After: 0, Before: 9007199254740997}
	if err := importer.ImportInterval(ctx, interval, []string{good, broken}, now, func(database.Tx) error {
		return nil
	}); err == nil {
		t.Fatal("truncated export partition committed")
	}
	if status, err := store.Community().CourseStatus(ctx, []int64{821}); err != nil || len(status) != 1 ||
		status[0].Messages != 0 || status[0].Conversations != 0 || status[0].Sources != 0 {
		t.Fatalf("truncated interval preserved = %+v, %v", status, err)
	}
	if source, err = store.Discord().NextExportSource(ctx, now); err != nil || source.After != 0 {
		t.Fatalf("truncated interval cursor = %+v, %v", source, err)
	}
	if err := importer.ImportInterval(ctx, interval, []string{good}, now.Add(time.Minute), func(database.Tx) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if status, err := store.Community().CourseStatus(ctx, []int64{821}); err != nil || len(status) != 1 ||
		status[0].Messages != 2 || status[0].Conversations == 0 || status[0].Sources != 1 {
		t.Fatalf("published interval status = %+v, %v", status, err)
	}
	if source, err = store.Discord().NextExportSource(ctx, now.Add(time.Minute)); err != nil ||
		source.After != 9007199254740996 {
		t.Fatalf("advanced interval cursor = %+v, %v", source, err)
	}
}

func assertDiscordFractionalRetry(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	retry := time.Date(2026, 9, 12, 12, 0, 0, 900000000, time.UTC)
	early := time.Date(2026, 9, 12, 12, 0, 0, 100000000, time.UTC)
	if err := store.Discord().RecordExportFailure(ctx, database.DiscordExportFailure{
		Root: 511, Channel: 512, After: 9007199254740996, NextAt: retry, Error: "retry",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Discord().NextExportSource(ctx, early); !errors.Is(err, database.ErrNoRows) {
		t.Fatalf("fractional retry due early: %v", err)
	}
	if source, err := store.Discord().NextExportSource(ctx, retry); err != nil || source.After != 9007199254740996 {
		t.Fatalf("fractional retry not due at instant = %+v, %v", source, err)
	}
}

func seedDiscordMapping(t *testing.T, store database.Store) {
	t.Helper()
	ctx := t.Context()
	if err := (courses.Service{Pool: store}).Add(ctx, 821, "discord"); err != nil {
		t.Fatal(err)
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := tx.Settings().ReplaceDiscordMapping(ctx, map[string]int64{"511": 821}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Discord().ReplaceDiscovery(ctx, []database.DiscoveredChannel{{
		ChannelID: 512, RootID: 511, GuildID: 513, Name: "general",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func writeDiscordPartition(t *testing.T, first int64, stamp string) string {
	t.Helper()
	path := t.TempDir() + "/export.json"
	firstLine := `{"id":"` + strconv.FormatInt(first, 10) + `","timestamp":"` + stamp +
		`","content":"interval material","author":{"id":"55","name":"author"},"type":"Default"}`
	secondLine := `{"id":"` + strconv.FormatInt(first+1, 10) + `","timestamp":"` + stamp +
		`","content":"interval material","author":{"id":"55","name":"author"},"type":"Default"}`
	payload := `{"guild":{"id":"513","name":"school"},"channel":{"id":"512","name":"general",` +
		`"type":"GuildTextChat"},"messages":[` + firstLine + `,` + secondLine +
		`],"exportedAt":"2026-09-12T12:00:00Z","messageCount":2}`
	if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func truncateTestFile(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw[:len(raw)-1], 0600); err != nil {
		t.Fatal(err)
	}
}
