package server

import (
	"errors"
	"net/http"
	"net/url"

	"tree-eclass/internal/domain/settings"
)

func (s *Server) savePlanner(w http.ResponseWriter, r *http.Request) {
	var raw map[string]string
	if !bodyJSON(w, r, &raw) {
		return
	}
	form := url.Values{}
	for key, value := range raw {
		form.Set(key, value)
	}
	if err := s.settingsService().SavePlanner(r.Context(), form); err != nil {
		var issues settings.PlannerErrors
		if !errors.As(err, &issues) {
			s.internal(w, err)
			return
		}
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": issues})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}
