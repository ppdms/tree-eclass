package server

import (
	"errors"
	"net/http"
	"strconv"
	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/annotations"
)

func (s *Server) annotationRoutes() {
	s.mux.HandleFunc("GET /api/study/annotations", s.listAnnotations)
	s.mux.HandleFunc("POST /api/study/annotations", s.createAnnotation)
	s.mux.HandleFunc("PATCH /api/study/annotations/{annotation_id}", s.updateAnnotation)
	s.mux.HandleFunc("DELETE /api/study/annotations/{annotation_id}", s.deleteAnnotation)
}
func (s *Server) annotationService() annotations.Service { return annotations.Service{Pool: s.db.Pool} }
func (s *Server) requireStudyCourse(w http.ResponseWriter, r *http.Request, id int64) bool {
	err := s.annotationService().RequireCourse(r.Context(), id)
	if errors.Is(err, database.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Course not found")
		return false
	}
	if err != nil {
		s.internal(w, err)
		return false
	}
	return true
}
func queryID(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	id, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64)
	if err != nil || id < 1 {
		writeFailure(w, http.StatusUnprocessableEntity, key+" must be a positive integer")
		return 0, false
	}
	return id, true
}
func (s *Server) annotationError(w http.ResponseWriter, err error) {
	if errors.Is(err, annotations.ErrIdempotencyConflict) {
		writeFailure(w, http.StatusConflict, err.Error())
		return
	}
	if errors.Is(err, database.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Annotation or indexed document not found")
		return
	}
	s.internal(w, err)
}
func (s *Server) listAnnotations(w http.ResponseWriter, r *http.Request) {
	id, ok := queryID(w, r, "course_id")
	if !ok || !s.requireStudyCourse(w, r, id) {
		return
	}
	deleted, ok := queryBool(w, r, "include_deleted", false)
	if !ok {
		return
	}
	result, err := s.annotationService().
		List(r.Context(), id, r.URL.Query().Get("document_id"), r.URL.Query().Get("action_id"), deleted)
	if err != nil {
		s.annotationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func (s *Server) createAnnotation(w http.ResponseWriter, r *http.Request) {
	var body annotations.Create
	if !bodyJSON(w, r, &body) {
		return
	}
	if err := body.Validate(); err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if !s.requireStudyCourse(w, r, body.CourseID) {
		return
	}
	item, err := s.annotationService().Create(r.Context(), body)
	if err != nil {
		s.annotationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"annotation": item})
}
func (s *Server) existingAnnotation(w http.ResponseWriter, r *http.Request) (annotations.Annotation, bool) {
	id, ok := pathID(w, r, "annotation_id")
	if !ok {
		return annotations.Annotation{}, false
	}
	item, err := s.annotationService().Get(r.Context(), id)
	if err != nil {
		s.annotationError(w, err)
		return item, false
	}
	return item, s.requireStudyCourse(w, r, item.CourseID)
}
func (s *Server) updateAnnotation(w http.ResponseWriter, r *http.Request) {
	var body annotations.Update
	if !bodyJSON(w, r, &body) {
		return
	}
	if err := body.Validate(); err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	existing, ok := s.existingAnnotation(w, r)
	if !ok {
		return
	}
	item, err := s.annotationService().Update(r.Context(), existing.ID, body)
	if err != nil {
		s.annotationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"annotation": item})
}
func (s *Server) deleteAnnotation(w http.ResponseWriter, r *http.Request) {
	existing, ok := s.existingAnnotation(w, r)
	if !ok {
		return
	}
	status := "deleted"
	if _, err := s.annotationService().Update(r.Context(), existing.ID, annotations.Update{Status: &status}); err != nil {
		s.annotationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": status, "id": existing.ID})
}
