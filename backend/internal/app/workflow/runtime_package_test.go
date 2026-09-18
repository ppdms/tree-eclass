package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/integrations/discord"
	"tree-eclass/internal/integrations/parser"
	"tree-eclass/internal/integrations/pdfdiff"
)

func packageFixtureHelpers(t *testing.T, stage string) {
	t.Helper()
	cfg := Config{Tessdata: os.Getenv("TREE_TEST_TESSDATA"), DiscordExporter: os.Getenv("TREE_TEST_DISCORD")}
	var err error
	cfg.PDFDiff, cfg.PDFDiffSHA, err = pdfTool()
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{Config: cfg}
	if err = c.packageNativeHelpers(t.Context(), stage); err != nil {
		t.Fatal(err)
	}
}
func bundledRenderingChecks(t *testing.T, root, fixtures string, runner *parser.Runner) {
	t.Helper()
	pages := 0
	err := runner.Run(
		t.Context(),
		parser.Request{Operation: "render", Path: filepath.Join(fixtures, "notes.pdf"), Pages: []int{1}},
		func(r parser.Record) error {
			if r.Type == "artifact" {
				pages++
				if info, err := os.Stat(r.Path); err != nil || info.Size() < 100 {
					t.Fatal("empty rendered page", err)
				}
			}
			return nil
		},
	)
	if err != nil || pages != 1 {
		t.Fatal("bundled PDF rendering", pages, err)
	}
	var cfg server.Config
	if err = applyRuntime(&cfg, root); err != nil {
		t.Fatal(err)
	}
	old, next := filepath.Join(fixtures, "notes.pdf"), filepath.Join(fixtures, "changed.pdf")
	if err = os.WriteFile(next, tinyPDF("modified synthetic fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	diff := pdfdiff.NativeRunner{
		Binary:    cfg.PDFDiff,
		SHA256:    cfg.PDFDiffSHA,
		Root:      root,
		ToolsRoot: cfg.NativeToolsRoot,
	}
	changed, err := diff.Run(t.Context(), t.TempDir(), old, next)
	if err != nil || !changed {
		t.Fatal("bundled visual difference", changed, err)
	}
	exporter := discord.NativeRunner{Binary: cfg.DiscordExporter}
	version, err := exporter.Run(t.Context(), t.TempDir(), "", "--version")
	if err != nil || !strings.Contains(version, "2.48") {
		t.Fatal("bundled Discord exporter", version, err)
	}
}
