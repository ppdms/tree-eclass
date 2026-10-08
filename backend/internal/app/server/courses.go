package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/infrastructure/rdbms"
)

func pathID(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue(key), 10, 64)
	if err != nil || id < 1 {
		writeFailure(w, http.StatusUnprocessableEntity, "Expected a positive integer ID")
		return 0, false
	}
	return id, true
}
func bodyJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	decoder := json.NewDecoder(r.Body)
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil || string(raw) == "null" {
		writeFailure(w, http.StatusUnprocessableEntity, "Expected valid JSON")
		return false
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeFailure(w, http.StatusUnprocessableEntity, "Expected one JSON value")
		return false
	}
	if err := json.Unmarshal(raw, target); err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, "Invalid JSON fields")
		return false
	}
	return true
}
func (s *Server) courseRoutes() {
	s.mux.HandleFunc("GET /api/v1/courses", s.listCourses)
	s.mux.HandleFunc("GET /api/v1/courses/available", s.availableCourses)
	s.mux.HandleFunc("GET /api/v1/navigation/courses", s.navigationCourses)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/tree", s.courseTree)
	s.mux.HandleFunc("POST /api/v1/courses", s.addCourse)
	s.mux.HandleFunc("PATCH /api/v1/courses/{course_id}", s.renameCourse)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/hide", s.hideCourse)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/show", s.hideCourse)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/delete", s.destructiveCourse)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/reset", s.destructiveCourse)
	s.mux.HandleFunc("POST /api/courses/reorder", s.reorderCourses)
	s.mux.HandleFunc("POST /api/courses/{course_id}/files/study-level", s.setStudyLevel)
	s.mux.HandleFunc("POST /api/courses/{course_id}/folders/collapsed", s.setFolderCollapsed)
}
func (s *Server) courseService() courses.Service { return courses.Service{Pool: s.db.Pool} }
func (s *Server) listCourses(w http.ResponseWriter, r *http.Request) {
	result, err := s.courseService().Shelf(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result.Courses)
}
func (s *Server) navigationCourses(w http.ResponseWriter, r *http.Request) {
	result, err := s.courseService().List(r.Context(), false)
	if err != nil {
		s.internal(w, err)
		return
	}
	type item struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	items := make([]item, 0, len(result))
	for _, course := range result {
		items = append(items, item{course.ID, course.Name})
	}
	writeJSON(w, http.StatusOK, map[string]any{"courses": items})
}
func (s *Server) addCourse(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CourseID  int64  `json:"course_id"`
		Name      string `json:"name"`
		ShortName string `json:"short_name"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	if err := s.courseService().Add(r.Context(), body.CourseID, body.Name, body.ShortName); err != nil {
		if rdbms.IsUniqueViolation(err) {
			writeFailure(w, http.StatusBadRequest, "Course already exists")
			return
		}
		writeFailure(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "created", "id": body.CourseID})
}
func (s *Server) renameCourse(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	if err := s.courseService().Rename(r.Context(), id, body.Name); err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "id": id})
}
func (s *Server) hideCourse(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	hidden := r.Pattern == "POST /api/v1/courses/{course_id}/hide"
	if err := s.courseService().Hide(r.Context(), id, hidden); err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "id": id, "hidden": hidden})
}
func (s *Server) reorderCourses(w http.ResponseWriter, r *http.Request) {
	var ids []int64
	if !bodyJSON(w, r, &ids) {
		return
	}
	if err := s.courseService().Reorder(r.Context(), ids); err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "course_ids": ids})
}
func (s *Server) courseError(w http.ResponseWriter, err error) {
	if errors.Is(err, rdbms.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Course not found")
		return
	}
	s.internal(w, err)
}
