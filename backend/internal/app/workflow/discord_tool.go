package workflow

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"tree-eclass/internal/domain/platform"
)

const discordVersion = "2.48"
const discordArchiveSHA = "623f9d2dce568e17a46b8fbd366a18dca49803d386216f4ba24507d2c000fee9"
const discordInventorySHA = "71fd8e502c85dcb95be4f0dda239b8de81fcddaf895f4fdf6712961c66bde89b"

func (c *Controller) discordPath() string {
	return filepath.Join(c.Root, "tools", "discord-exporter-"+discordVersion, "DiscordChatExporter.Cli")
}
func installDiscord(ctx context.Context, binary string) error {
	if _, err := os.Lstat(filepath.Dir(binary)); err == nil {
		return verifyDiscord(binary)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		return errors.New("automatic Discord exporter provisioning requires macOS ARM64")
	}
	parent := filepath.Dir(filepath.Dir(binary))
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(parent, ".discord-install-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	archive, err := downloadDiscord(ctx, parent)
	if err != nil {
		return err
	}
	defer os.Remove(archive.Name())
	defer archive.Close()
	if err = extractDiscordArchive(archive, dir); err != nil {
		return err
	}
	if err = verifyDiscord(filepath.Join(dir, "DiscordChatExporter.Cli")); err != nil {
		return err
	}
	if err = os.Rename(dir, filepath.Dir(binary)); err != nil {
		return err
	}
	return platform.SyncDir(parent)
}

func downloadDiscord(parentCtx context.Context, parent string) (*os.File, error) {
	archive, err := os.CreateTemp(parent, ".discord-download-*")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Minute)
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://github.com/Tyrrrz/DiscordChatExporter/releases/download/"+discordVersion+"/DiscordChatExporter.Cli.osx-arm64.zip",
		nil,
	)
	if err != nil {
		archive.Close()
		os.Remove(archive.Name())
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		archive.Close()
		os.Remove(archive.Name())
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		archive.Close()
		os.Remove(archive.Name())
		return nil, fmt.Errorf("Discord exporter download HTTP %d", response.StatusCode)
	}
	hash := sha256.New()
	if _, err = io.Copy(io.MultiWriter(archive, hash), io.LimitReader(response.Body, 20*1024*1024)); err != nil {
		archive.Close()
		os.Remove(archive.Name())
		return nil, err
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != discordArchiveSHA {
		archive.Close()
		os.Remove(archive.Name())
		return nil, errors.New("Discord exporter archive checksum mismatch")
	}
	return archive, nil
}

func extractDiscordArchive(archive *os.File, dir string) error {
	info, err := archive.Stat()
	if err != nil {
		return err
	}
	entries, err := zip.NewReader(archive, info.Size())
	if err != nil {
		return err
	}
	var total uint64
	for _, entry := range entries.File {
		total += entry.UncompressedSize64
		if filepath.Base(entry.Name) != entry.Name || entry.Name == "." || !entry.Mode().IsRegular() ||
			total > 64*1024*1024 {
			return errors.New("unsafe Discord exporter archive entry")
		}
		if err = extractDiscordEntry(entry, filepath.Join(dir, entry.Name)); err != nil {
			return err
		}
	}
	return nil
}

func extractDiscordEntry(entry *zip.File, target string) error {
	input, err := entry.Open()
	if err != nil {
		return err
	}
	defer input.Close()
	mode := os.FileMode(0600)
	if entry.Name == "DiscordChatExporter.Cli" || entry.Name == "createdump" {
		mode = 0700
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, err = io.Copy(output, input)
	if err == nil {
		err = output.Sync()
	}
	return errors.Join(err, output.Close())
}
func verifyDiscord(binary string) error {
	root := filepath.Dir(binary)
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("Discord exporter root must be a directory")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var inventory strings.Builder
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("Discord exporter contains a nonregular dependency")
		}
		file, err := os.Open(filepath.Join(root, entry.Name()))
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, err = io.Copy(hash, file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		fmt.Fprintf(&inventory, "%s\x00%x\n", entry.Name(), hash.Sum(nil))
	}
	if fmt.Sprintf("%x", sha256.Sum256([]byte(inventory.String()))) != discordInventorySHA {
		return errors.New("Discord exporter dependency inventory checksum mismatch; existing files preserved")
	}
	return nil
}
