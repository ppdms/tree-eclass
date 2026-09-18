package process

import (
	"os"
	"sync"
)

const logLimit int64 = 8 * 1024 * 1024

type rotatingLog struct {
	mu   sync.Mutex
	path string
	file *os.File
	size int64
}

func openLog(path string) (*rotatingLog, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &rotatingLog{path: path, file: file, size: info.Size()}, nil
}
func (l *rotatingLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size+int64(len(p)) > logLimit {
		if err := l.file.Close(); err != nil {
			return 0, err
		}
		if err := os.Rename(l.path, l.path+".1"); err != nil {
			return 0, err
		}
		file, err := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return 0, err
		}
		l.file = file
		l.size = 0
	}
	n, err := l.file.Write(p)
	l.size += int64(n)
	return n, err
}
func (l *rotatingLog) Close() error { l.mu.Lock(); defer l.mu.Unlock(); return l.file.Close() }
