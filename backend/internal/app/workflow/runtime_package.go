package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/infrastructure/nativebundle"
	"tree-eclass/internal/infrastructure/platform"
)

type runtimeTools struct {
	Format     int    `json:"format"`
	PDFDiffSHA string `json:"pdf_diff_sha256"`
}

func (c *Controller) packageNativeHelpers(ctx context.Context, root string) error {
	if err := verifyTessdata(c.Config.Tessdata); err != nil {
		return err
	}
	if err := verifyDiscord(c.Config.DiscordExporter); err != nil {
		return err
	}
	if sum, err := fileSHA(c.Config.PDFDiff); err != nil || sum != c.Config.PDFDiffSHA {
		return errors.New("configured visual PDF tool changed before packaging")
	}
	tools := map[string]string{"diff-pdf": c.Config.PDFDiff}
	for _, name := range []string{"tesseract", "pdftoppm", "pdftotext", "7zz"} {
		binary, err := exec.LookPath(name)
		if err != nil {
			return err
		}
		tools[name] = binary
	}
	target := filepath.Join(root, "runtime")
	if err := nativebundle.Bundle(ctx, target, tools); err != nil {
		return err
	}
	if err := cloneArtifact(c.Config.Tessdata, filepath.Join(target, "tessdata")); err != nil {
		return err
	}
	if err := cloneArtifact(filepath.Dir(c.Config.DiscordExporter), filepath.Join(target, "discord")); err != nil {
		return err
	}
	sum, err := fileSHA(filepath.Join(target, "bin/diff-pdf"))
	if err != nil {
		return err
	}
	return platform.WriteJSON(filepath.Join(target, "runtime.json"), runtimeTools{1, sum})
}
func fileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
func applyRuntime(cfg *server.Config, root string) error {
	path := filepath.Join(root, "runtime")
	var tools runtimeTools
	if err := platform.ReadJSON(filepath.Join(path, "runtime.json"), &tools); err != nil {
		return err
	}
	if tools.Format != 1 || len(tools.PDFDiffSHA) != 64 {
		return errors.New("unsupported packaged runtime descriptor")
	}
	cfg.NativeToolsRoot = path
	cfg.Tessdata = filepath.Join(path, "tessdata")
	cfg.DiscordExporter = filepath.Join(path, "discord/DiscordChatExporter.Cli")
	cfg.PDFDiff = filepath.Join(path, "bin/diff-pdf")
	cfg.PDFDiffSHA = tools.PDFDiffSHA
	return nil
}
