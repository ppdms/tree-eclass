package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/study"
)

func (s *Server) coverageRoutes() {
	for _, pattern := range []string{"GET /api/v1/courses/coverage", "GET /api/v1/courses/coverage/{$}", "OPTIONS /api/v1/courses/coverage", "OPTIONS /api/v1/courses/coverage/{$}"} {
		s.mux.HandleFunc(pattern, s.courseCoverage)
	}
}

func (s *Server) courseCoverage(w http.ResponseWriter, r *http.Request) {
	result, err := s.courseService().Shelf(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) projectionWorker(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if _, err := s.courseService().RefreshCoverage(ctx); err != nil && ctx.Err() == nil {
			slog.Error("course coverage refresh", "error", err)
		}
		if _, err := (navigation.Service{Pool: s.db.Pool}).Refresh(ctx); err != nil && ctx.Err() == nil {
			slog.Error("course navigation refresh", "error", err)
		}
		if _, err := (study.Service{Pool: s.db.Pool}).Refresh(ctx, time.Now()); err != nil && ctx.Err() == nil {
			slog.Error("study projection refresh", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
