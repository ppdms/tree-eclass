package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupPDFToolStoresManagedCopy(t *testing.T) {
	source := filepath.Join(t.TempDir(), "diff-pdf")
	if err := os.WriteFile(source, []byte("synthetic diff-pdf"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Dir(source))
	root := t.TempDir()
	path, checksum, err := (&Controller{Root: root}).setupPDFTool()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(path, filepath.Join(root, "tools")+string(os.PathSeparator)) {
		t.Fatalf("PDF tool escaped managed tools: %s", path)
	}
	if strings.Contains(path, string(filepath.Separator)+"Cellar"+string(filepath.Separator)) {
		t.Fatalf("PDF tool retained package-manager path: %s", path)
	}
	if got, err := fileSHA(path); err != nil || got != checksum {
		t.Fatalf("managed PDF tool checksum = %q, %v; want %q", got, err, checksum)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "synthetic diff-pdf" {
		t.Fatalf("managed PDF tool contents = %q", data)
	}
}
