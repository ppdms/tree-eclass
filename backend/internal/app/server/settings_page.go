package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"tree-eclass/internal/domain/settings"
)

func (s *Server) settingsPageRoutes() {
	for _, path := range []string{"/api/v1/settings", "/api/v1/settings/{$}"} {
		s.mux.HandleFunc("GET "+path, s.settingsPage)
		s.mux.HandleFunc("OPTIONS "+path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", "GET, HEAD, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
		})
	}
	s.mux.HandleFunc("POST /api/settings/test-storage", s.testStorage)
	s.mux.HandleFunc("GET /api/settings/export", s.exportLearner)
	s.mux.HandleFunc("POST /settings/discord-exporter", s.saveDiscord)
	s.mux.HandleFunc("POST /settings/discord-course-map", s.saveDiscordMap)
}

func (s *Server) settingsPage(w http.ResponseWriter, r *http.Request) {
	page, err := s.settingsService().Page(r.Context(), s.config.ProviderKeys)
	if err != nil {
		s.internal(w, err)
		return
	}
	type storageInfo struct {
		Configured bool `json:"configured"`
	}
	writeJSON(w, http.StatusOK, struct {
		settings.Page
		Storage      storageInfo `json:"storage"`
		MappingSaved bool        `json:"discord_mapping_saved"`
		ExportSaved  bool        `json:"discord_export_saved"`
	}{page, storageInfo{s.config.ObjectsRoot != ""}, r.URL.Query().Get("discord_saved") == "1", r.URL.Query().Get("discord_export_saved") == "1"})
}

func (s *Server) testStorage(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	err := s.blobs.Check(ctx)
	message := "Local storage is ready and document versioning is enabled."
	if err != nil {
		message = "Local storage is unavailable or document versioning is disabled. Run tree doctor to check the services."
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": err == nil, "message": message})
}

func (s *Server) saveDiscord(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SaveDiscord(r.Context(), form); err != nil {
		s.settingsError(w, err, 400)
		return
	}
	http.Redirect(w, r, "/settings?discord_export_saved=1#discord-exporter", http.StatusSeeOther)
}

func (s *Server) saveDiscordMap(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	mapped, err := s.settingsService().SaveDiscordMap(r.Context(), form)
	if err != nil {
		s.settingsError(w, err, 400)
		return
	}
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		writeJSON(w, http.StatusOK, map[string]any{"status": "saved", "mapped": mapped})
		return
	}
	http.Redirect(w, r, "/settings?discord_saved=1#discord-course-mapping", http.StatusSeeOther)
}
