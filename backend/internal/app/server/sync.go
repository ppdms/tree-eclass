package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/integrations/eclass"
	"tree-eclass/internal/services/synchronization"
)

func (s *Server) syncService() synchronization.Service {
	return synchronization.Service{
		Pool: s.db.Pool, Objects: s.blobs, Temp: s.config.Temp,
		MirrorObjects: s.blobs,
	}
}
func (s *Server) syncRoutes() {
	s.mux.HandleFunc("POST /api/run-check", s.enqueueCheck)
	s.mux.HandleFunc("POST /courses/{course_id}/check", s.enqueueCheck)
}
func (s *Server) enqueueCheck(w http.ResponseWriter, r *http.Request) {
	if !s.config.ExternalWorkers {
		writeFailure(w, http.StatusServiceUnavailable, "External synchronization is disabled in this runtime")
		return
	}
	var id *int64
	if r.PathValue("course_id") != "" {
		value, ok := pathID(w, r, "course_id")
		if !ok {
			return
		}
		id = &value
	}
	_, err := s.syncService().EnqueueManual(r.Context(), id)
	if errors.Is(err, synchronization.ErrBusy) {
		writeJSON(
			w,
			http.StatusConflict,
			map[string]any{"detail": "A check is already in progress", "is_checking": true},
		)
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Course not found")
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	status := "success"
	if id != nil {
		status = "started"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status, "message": "Check started in background"})
}

func (s *Server) syncWorker(ctx context.Context) error {
	if !s.config.ExternalWorkers {
		<-ctx.Done()
		return nil
	}
	queue := jobs.Queue{Pool: s.db.Pool}
	service := s.syncService()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	nextSchedule := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if !now.Before(nextSchedule) {
				if err := service.Schedule(ctx, now); err != nil {
					slog.Error("sync scheduling failed", "error", err)
				}
				nextSchedule = now.Add(time.Minute)
			}
		}
		commands, err := queue.Claim(ctx, "sync")
		if err != nil {
			slog.Error("sync queue claim failed", "error", err)
			continue
		}
		for _, command := range commands {
			var request synchronization.Request
			var result synchronization.Result
			err = json.Unmarshal(command.Payload, &request)
			if err == nil {
				result, err = service.Run(ctx, request, eclass.BaseURL)
			}
			if ctx.Err() != nil {
				return nil
			}
			if finishErr := service.Finish(ctx, result, err); finishErr != nil {
				return finishErr
			}
			if err != nil {
				err = queue.Fail(ctx, command, err)
			} else {
				err = queue.Complete(ctx, command.ID)
			}
			if err != nil {
				return err
			}
		}
	}
}
