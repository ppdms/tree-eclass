package platform

import (
	"golang.org/x/sys/unix"
	"io/fs"
)

// No fallback on macOS: a full copy could exhaust the laptop's small disk.
func cloneFile(source, target string, _ fs.FileMode) error {
	return unix.Clonefile(source, target, 0)
}
