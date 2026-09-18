package server

import (
	"net/http"

	"tree-eclass/internal/domain/activity"
)

func (s *Server) activityRoutes() {
	for _, path := range []string{"/api/v1/inbox", "/api/v1/inbox/{$}", "/api/v1/timeline", "/api/v1/timeline/{$}"} {
		s.mux.HandleFunc("GET "+path, s.inbox)
		s.mux.HandleFunc("OPTIONS "+path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", "GET, HEAD, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
		})
	}
}

func (s *Server) inbox(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryInteger(w, r, "limit", 30)
	if !ok {
		return
	}
	offset, ok := queryInteger(w, r, "offset", 0)
	if !ok {
		return
	}
	if offset > 1<<31-1 {
		writeFailure(w, http.StatusUnprocessableEntity, "offset is too large")
		return
	}
	timeline, ok := queryBool(w, r, "include_timeline", true)
	if !ok {
		return
	}
	page, err := (activity.Reader{Pool: s.db.Pool}).Page(r.Context(), limit, offset, timeline)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
