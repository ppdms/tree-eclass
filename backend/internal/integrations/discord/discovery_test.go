package discord

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tree-eclass/internal/domain/messages"
)

func TestListingAndBoundaries(t *testing.T) {
	rows, err := listing("\x1b[32m100 | Ύλη\x1b[0m\n * 101 | Thread / Απορίες | Active\n200 | Νέα\n", true)
	if err != nil || len(rows) != 3 || rows[1].Root != 100 || !rows[1].Thread || rows[2].Root != 200 ||
		rows[0].Name != "Ύλη" {
		t.Fatal(rows, err)
	}
	for _, input := range []string{"* 1 | orphan", "1 | one\n1 | repeated", "-1 | invalid"} {
		if _, err = listing(input, true); err == nil {
			t.Fatal("invalid listing accepted", input)
		}
	}
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	id := ((now.AddDate(0, -3, 0).UnixMilli() - 1420070400000) << 22)
	before := boundary(messages.Archive{Channel: id}, now)
	stamp := time.UnixMilli((before >> 22) + 1420070400000)
	if !stamp.Equal(now.AddDate(0, -2, 0)) {
		t.Fatal("initial backfill boundary", stamp)
	}
	before = boundary(
		messages.Archive{Channel: id, After: ((now.Add(-time.Hour).UnixMilli() - 1420070400000) << 22)},
		now,
	)
	if !time.UnixMilli((before >> 22) + 1420070400000).Equal(now.Add(-time.Minute)) {
		t.Fatal("current safety boundary", before)
	}
}
func TestNativeRunnerSanitizesFailureAndCancels(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "helper")
	body := "#!/bin/sh\nif [ \"$1\" = fail ]; then echo \"$DISCORD_TOKEN\"; exit 1; fi\nif [ \"$1\" = env ]; then env; exit 0; fi\nexec sleep 30\n"
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	runner := NativeRunner{Binary: script}
	secret := "synthetic-private-token"
	if output, err := runner.Run(t.Context(), dir, secret, "fail"); err == nil || output != "" ||
		strings.Contains(err.Error(), secret) {
		t.Fatal("helper secret leaked", output, err)
	}
	t.Setenv("TREE_RUNTIME_CONFIG", "must-not-leak")
	t.Setenv("OPENAI_API_KEY", "must-not-leak")
	output, err := runner.Run(t.Context(), dir, secret, "env")
	if err != nil || strings.Contains(output, "must-not-leak") {
		t.Fatal("helper inherited unrelated credentials", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err = runner.Run(ctx, dir, secret, "wait"); err == nil || time.Since(start) > 3*time.Second {
		t.Fatal("helper cancellation", err)
	}
}

func TestInstalledExporterEnvironment(t *testing.T) {
	binary := os.Getenv("TREE_TEST_DISCORD")
	if binary == "" {
		t.Skip("set TREE_TEST_DISCORD for the pinned native exporter smoke check")
	}
	output, err := (NativeRunner{Binary: binary}).Run(t.Context(), t.TempDir(), "", "--version")
	if err != nil || strings.TrimSpace(output) != "v2.48.0" {
		t.Fatal("isolated native exporter", output, err)
	}
}
