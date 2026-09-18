package nativebundle

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNativeHelpersRelocate(t *testing.T) {
	if os.Getenv("TREE_NATIVE_TOOLS_TEST") != "1" || runtime.GOOS != "darwin" {
		t.Skip("set TREE_NATIVE_TOOLS_TEST=1 to package installed document tools")
	}
	tools := map[string]string{}
	for _, name := range []string{"bun", "tesseract", "pdftoppm", "pdftotext", "7zz", "diff-pdf"} {
		path, err := exec.LookPath(name)
		if err != nil {
			t.Fatal(err)
		}
		tools[name] = path
	}
	root := t.TempDir()
	staged := filepath.Join(root, "temporary-build")
	if err := os.Mkdir(staged, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Bundle(t.Context(), filepath.Join(staged, "runtime"), tools); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "relocated-release")
	if err := os.Rename(staged, final); err != nil {
		t.Fatal(err)
	}
	if err := Verify(filepath.Join(final, "runtime")); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{"bun": {"--version"}, "tesseract": {"--version"}, "pdftoppm": {"-v"}, "pdftotext": {"-v"}, "7zz": {"i"}, "diff-pdf": {"--help"}} {
		cmd := exec.CommandContext(t.Context(), filepath.Join(final, "runtime/bin", name), args...)
		cmd.Dir = final
		cmd.Env = []string{
			"PATH=/usr/bin:/bin",
			"HOME=" + root,
			"TMPDIR=" + root,
			"FONTCONFIG_FILE=" + filepath.Join(final, "runtime/share/fontconfig/fonts.conf"),
			"XDG_CACHE_HOME=" + root,
		}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatal(name, err, string(out))
		}
	}
}
