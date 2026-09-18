package workflow

import (
	"archive/tar"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPythonArchiveRejectsTraversalAndSymlinkParents(t *testing.T) {
	for _, headers := range [][]*tar.Header{
		{{Name: "../escape", Mode: 0600, Typeflag: tar.TypeReg}},
		{{Name: "python/escape", Typeflag: tar.TypeSymlink, Linkname: "/tmp"}},
		{{Name: "python/escape", Typeflag: tar.TypeSymlink, Linkname: "../../tmp"}},
		{{Name: "python/alias", Typeflag: tar.TypeSymlink, Linkname: "lib"}, {Name: "python/alias/file", Mode: 0600, Typeflag: tar.TypeReg}},
	} {
		var data bytes.Buffer
		archive := tar.NewWriter(&data)
		for _, h := range headers {
			if err := archive.WriteHeader(h); err != nil {
				t.Fatal(err)
			}
		}
		if err := archive.Close(); err != nil {
			t.Fatal(err)
		}
		if err := extractPython(tar.NewReader(&data), t.TempDir()); err == nil {
			t.Fatal("unsafe archive accepted", headers)
		}
	}
}
func TestNativePythonDistributionRelocates(t *testing.T) {
	base := os.Getenv("TREE_TEST_PYTHON_BASE")
	if base == "" {
		t.Skip("set TREE_TEST_PYTHON_BASE to the verified standalone Python distribution")
	}
	t.Parallel()
	if err := verifyPythonBase(base); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	staged := filepath.Join(root, "temporary-build")
	if err := cloneArtifact(filepath.Join(base, "python"), staged); err != nil {
		t.Fatal(err)
	}
	if err := trimPython(staged); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "relocated-release")
	if err := os.Rename(staged, final); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(
		filepath.Join(final, "bin/python3.14"),
		"-I",
		"-B",
		"-c",
		`import ctypes,decimal,hashlib,json,os,pathlib,ssl,sqlite3,sys,sysconfig,xml.etree.ElementTree
root=pathlib.Path(sys.argv[1]).resolve()
assert pathlib.Path(sys.prefix).resolve()==root
assert pathlib.Path(sys.base_prefix).resolve()==root
assert pathlib.Path(json.__file__).resolve().is_relative_to(root)
assert sys.version_info[:3]==(3,14,7)
assert hashlib.sha256(b'fixture').hexdigest()
assert sqlite3.connect(':memory:').execute('select 1').fetchone()==(1,)
print('relocated interpreter and standard library work')`,
		final,
	)
	cmd.Dir = root
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root}
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(string(b), err)
	}
	if _, err := releaseFiles(final); err != nil {
		t.Fatal("relocated dependency escaped artifact", err)
	}
	if err := verifyPythonBase(base); err != nil {
		t.Fatal("packaging mutated original distribution", err)
	}
}
