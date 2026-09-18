package server

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"tree-eclass/internal/domain/settings"
)

func (s *Server) savePlanner(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	wantsJSON := strings.Contains(r.Header.Get("Accept"), "application/json")
	if err := s.settingsService().SavePlanner(r.Context(), form); err != nil {
		var issues settings.PlannerErrors
		if !errors.As(err, &issues) {
			s.internal(w, err)
			return
		}
		if wantsJSON {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": issues})
			return
		}
		http.Redirect(w, r, "/study?planner_error="+url.QueryEscape(issues.Error()), http.StatusSeeOther)
		return
	}
	if wantsJSON {
		writeJSON(w, http.StatusOK, map[string]string{"status": "saved"})
		return
	}
	http.Redirect(w, r, "/study?planner_saved=1", http.StatusSeeOther)
}
