package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"tree-eclass/internal/infrastructure/platform"
)

func pdfTool() (string, string, error) {
	binary, err := exec.LookPath("diff-pdf")
	if err != nil {
		return "", "", err
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return "", "", err
	}
	file, err := os.Open(binary)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", "", err
	}
	return binary, hex.EncodeToString(hash.Sum(nil)), nil
}

func (c *Controller) setupPDFTool() (string, string, error) {
	source, checksum, err := pdfTool()
	if err != nil {
		return "", "", err
	}
	// Resolve only for the APFS clone; the package-manager path is never persisted.
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return "", "", err
	}
	if current, err := fileSHA(source); err != nil {
		return "", "", err
	} else if current != checksum {
		return "", "", errors.New("visual PDF tool changed during setup")
	}
	root := filepath.Join(c.Root, "tools", "diff-pdf-"+checksum[:16])
	target := filepath.Join(root, "diff-pdf")
	if current, err := fileSHA(target); err == nil {
		if current != checksum {
			return "", "", errors.New("managed visual PDF tool changed")
		}
		return target, checksum, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	if err = os.MkdirAll(filepath.Dir(root), 0700); err != nil {
		return "", "", err
	}
	temp, err := os.MkdirTemp(filepath.Dir(root), ".diff-pdf-*")
	if err != nil {
		return "", "", err
	}
	defer os.RemoveAll(temp)
	target = filepath.Join(temp, "diff-pdf")
	if err = platform.CloneFile(source, target); err != nil {
		return "", "", err
	}
	if err = os.Chmod(target, 0700); err != nil {
		return "", "", err
	}
	if err = platform.SyncTree(temp); err != nil {
		return "", "", err
	}
	if err = os.Rename(temp, root); err != nil {
		return "", "", err
	}
	return filepath.Join(root, "diff-pdf"), checksum, platform.SyncDir(filepath.Dir(root))
}
