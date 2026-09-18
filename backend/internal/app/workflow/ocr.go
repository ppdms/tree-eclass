package workflow

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"tree-eclass/internal/infrastructure/platform"
)

const tessdataRevision = "87416418657359cb625c412a48b6e1d6d41c29bd"

var tessdataFiles = map[string]string{
	"ell.traineddata": "4fba8a0b461038d51f1c20d043d4f2ac38c4e778f1b90830847f7bd8fa3ba726",
	"eng.traineddata": "7d4322bd2a7749724879683fc3912cb542f19906c83bcc1a52132556427170b2",
	"LICENSE":         "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
}

func (c *Controller) tessdataPath() string {
	return filepath.Join(c.Root, "tools", "tessdata-fast-"+tessdataRevision[:12])
}
func verifyTessdata(root string) error {
	if root == "" {
		return fmt.Errorf("Greek/English OCR models are not installed; run native setup")
	}
	for name, expected := range tessdataFiles {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != expected {
			return fmt.Errorf("OCR model checksum mismatch: %s", name)
		}
	}
	return nil
}
func installTessdata(ctx context.Context, root string) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	for name, hash := range tessdataFiles {
		path := filepath.Join(root, name)
		if _, err := os.Lstat(path); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := installOCRFile(ctx, path, name, hash); err != nil {
			return err
		}
	}
	return verifyTessdata(root)
}
func installOCRFile(ctx context.Context, path, name, expected string) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/"+tessdataRevision+"/"+name,
		nil,
	)
	if err != nil {
		return err
	}
	client := http.Client{Timeout: 45 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("OCR model download returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024+1))
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != expected {
		return fmt.Errorf("downloaded OCR model checksum mismatch: %s", name)
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".ocr-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return platform.SyncDir(filepath.Dir(path))
}
