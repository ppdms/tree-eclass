package platform

import (
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"io/fs"
	"os"
)

func cloneFile(source, target string, mode fs.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	if err = unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err == nil {
		return out.Sync()
	}
	info, err := in.Stat()
	if err != nil {
		return err
	}
	free, err := Available(target)
	if err != nil {
		return err
	}
	if free < uint64(info.Size())+5*1024*1024*1024 {
		return fmt.Errorf("cold copy needs %d bytes plus 5 GiB reserve", info.Size())
	}
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
