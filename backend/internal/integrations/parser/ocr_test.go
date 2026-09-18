package parser

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/domain/identity"
)

func TestGreekAndEnglishOCR(t *testing.T) {
	models := os.Getenv("TREE_TEST_TESSDATA")
	if models == "" {
		t.Skip("set TREE_TEST_TESSDATA to verified Greek/English OCR models")
	}
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	runner := New(filepath.Join(repo, ".venv/bin/python"), repo, t.TempDir())
	runner.Tessdata = models
	path := filepath.Join(repo, "backend/internal/integrations/parser/testdata/greek-ocr.png")
	var text strings.Builder
	err = runner.Run(
		context.Background(),
		Request{
			Operation: "extract",
			Kind:      "image",
			Path:      path,
			Source: &Source{
				CourseID:    101,
				CourseName:  "Synthetic",
				DisplayName: "greek-ocr.png",
				SourcePath:  "/greek-ocr.png",
				SourceHash:  "synthetic",
				MIMEType:    "image/png",
			},
			Limits: map[string]any{"ocr_enabled": true, "ocr_languages": "ell+eng"},
		},
		func(record Record) error {
			if record.Type == "unit" {
				text.WriteString(record.Text)
			}
			if len(record.Warnings) > 0 {
				t.Errorf("OCR warnings: %v", record.Warnings)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	normalized := identity.Search(text.String())
	for _, word := range []string{"δενδρα", "μαθηματικα", "synthetic"} {
		if !strings.Contains(normalized, word) {
			t.Errorf("OCR did not recognize %q: %s", word, text.String())
		}
	}
}
