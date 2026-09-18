package server

import (
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"tree-eclass/internal/infrastructure/platform"
)

func (s *Server) exportLearner(w http.ResponseWriter, r *http.Request) {
	if !s.exportMu.TryLock() {
		writeFailure(w, http.StatusTooManyRequests, "A learner export is already in progress")
		return
	}
	defer s.exportMu.Unlock()
	file, err := os.CreateTemp(s.config.Temp, "learner-export-*")
	if err != nil {
		s.internal(w, err)
		return
	}
	defer os.Remove(file.Name())
	defer file.Close()
	spool := &exportSpool{target: file, available: func() (uint64, error) { return platform.Available(file.Name()) }}
	if err = s.settingsService().Export(r.Context(), spool); err != nil {
		if errors.Is(err, errExportSpace) {
			writeFailure(w, http.StatusInsufficientStorage, "Export paused to preserve 5 GiB of free disk space")
			return
		}
		s.internal(w, err)
		return
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		s.internal(w, err)
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="tree-eclass-learner-data.json"`)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	http.ServeContent(w, r, "tree-eclass-learner-data.json", time.Time{}, file)
}

var errExportSpace = errors.New("learner export free-space reserve reached")

type exportSpool struct {
	target    io.Writer
	available func() (uint64, error)
	remaining uint64
}

func (s *exportSpool) Write(data []byte) (int, error) {
	// Recheck at least every 4 MiB. Retain the same reserve as extraction and
	// cloning; the completed artifact remains private until generation succeeds.
	if uint64(len(data)) > s.remaining {
		free, err := s.available()
		if err != nil {
			return 0, err
		}
		const reserve = uint64(5 * 1024 * 1024 * 1024)
		if free <= reserve || uint64(len(data)) > free-reserve {
			return 0, errExportSpace
		}
		s.remaining = min(4*1024*1024, free-reserve)
	}
	n, err := s.target.Write(data)
	s.remaining -= min(uint64(n), s.remaining)
	return n, err
}
