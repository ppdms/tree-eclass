package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"tree-eclass/internal/infrastructure/notifications"
)

func (s *Server) notificationStatus(w http.ResponseWriter, r *http.Request) {
	result, err := (notifications.Service{Pool: s.db.Pool}).Status(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func (s *Server) retryNotifications(w http.ResponseWriter, r *http.Request) {
	if !s.config.ExternalWorkers {
		writeFailure(w, http.StatusServiceUnavailable, "External notification delivery is disabled in this runtime")
		return
	}
	count, err := (notifications.Service{Pool: s.db.Pool}).Retry(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"queued": count})
}
func (s *Server) notificationWorker(ctx context.Context) error {
	if !s.config.ExternalWorkers {
		<-ctx.Done()
		return nil
	}
	sender := notifications.NewSender()
	defer sender.Close()
	service := notifications.Service{Pool: s.db.Pool, Sender: sender}
	if err := service.Recover(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if err := service.Tick(ctx); err != nil && ctx.Err() == nil {
			slog.Error("notification queue failed", "error", err)
		}
	}
}
