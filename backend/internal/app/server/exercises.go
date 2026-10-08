package server

import (
	"errors"
	"net/http"
	"time"
	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/exercises"
)

func (s *Server) exerciseRoutes() {
	s.mux.HandleFunc("GET /api/v1/exercises", s.listExercises)
	s.mux.HandleFunc("GET /api/v1/exercises/{$}", s.listExercises)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/exercises/{exercise_id}", s.exerciseDetail)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/exercises/{exercise_id}/ignore", s.ignoreExercise)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/exercises/{exercise_id}/unignore", s.ignoreExercise)
	for _, path := range []string{"/api/v1/exercises", "/api/v1/exercises/{$}", "/api/v1/courses", "/api/v1/courses/{$}"} {
		s.mux.HandleFunc("OPTIONS "+path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", "GET, HEAD, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
		})
	}
}

func queryBool(w http.ResponseWriter, r *http.Request, key string, fallback bool) (bool, bool) {
	values, exists := r.URL.Query()[key]
	if !exists {
		return fallback, true
	}
	switch values[len(values)-1] {
	case "true", "True", "1", "on", "yes":
		return true, true
	case "false", "False", "0", "off", "no":
		return false, true
	default:
		writeFailure(w, http.StatusUnprocessableEntity, key+" must be a boolean")
		return false, false
	}
}

func (s *Server) listExercises(w http.ResponseWriter, r *http.Request) {
	ignored, ok := queryBool(w, r, "include_ignored", false)
	if !ok {
		return
	}
	details, ok := queryBool(w, r, "include_details", true)
	if !ok {
		return
	}
	items, err := (exercises.Service{Pool: s.db.Pool}).List(r.Context(), ignored, details, time.Now())
	if err != nil {
		s.internal(w, err)
		return
	}
	courses, err := s.courseService().List(r.Context(), false)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"exercises": items, "courses": courses})
}

func (s *Server) exerciseDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	item, err := (exercises.Service{Pool: s.db.Pool}).Get(r.Context(), id, r.PathValue("exercise_id"))
	if errors.Is(err, database.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Exercise not found")
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"exercise": item})
}

func (s *Server) ignoreExercise(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	var body struct{}
	if !bodyJSON(w, r, &body) {
		return
	}
	err := (exercises.Service{Pool: s.db.Pool}).Ignore(
		r.Context(),
		id,
		r.PathValue("exercise_id"),
		r.Pattern == "POST /api/v1/courses/{course_id}/exercises/{exercise_id}/ignore",
	)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
