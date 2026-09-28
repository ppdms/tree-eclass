package server

import (
	"net/http"
	"time"

	"tree-eclass/internal/domain/study"
)

func (s *Server) studyRoutes() {
	for _, pattern := range []string{"GET /api/v1/study", "GET /api/v1/study/{$}", "OPTIONS /api/v1/study", "OPTIONS /api/v1/study/{$}"} {
		s.mux.HandleFunc(pattern, s.studyFull)
	}
	s.mux.HandleFunc("GET /api/v1/study/intelligence", s.studyIntelligence)
	s.mux.HandleFunc("GET /api/v1/study/snapshot", s.studySnapshot)
	s.mux.HandleFunc("POST /api/v1/study/actions/event", s.studyEvent)
}

func (s *Server) studyFull(w http.ResponseWriter, r *http.Request) {
	s.studyDerived(w, r, true)
}

func (s *Server) studyIntelligence(w http.ResponseWriter, r *http.Request) {
	s.studyDerived(w, r, false)
}

func (s *Server) studyDerived(w http.ResponseWriter, r *http.Request, full bool) {
	selected, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	service := study.Service{Pool: s.db.Pool}
	read := service.Intelligence
	if full {
		read = service.Full
	}
	result, err := read(r.Context(), selected, time.Now())
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) studySnapshot(w http.ResponseWriter, r *http.Request) {
	selected, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	result, err := (study.Service{Pool: s.db.Pool}).Snapshot(r.Context(), selected, time.Now().UTC())
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
