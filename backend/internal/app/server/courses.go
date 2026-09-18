package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"tree-eclass/internal/domain/courses"
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
	s.mux.HandleFunc("GET /api/v1/courses/{$}", s.listCourses)
	s.mux.HandleFunc("GET /api/v1/navigation/courses", s.navigationCourses)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/tree", s.courseTree)
	s.mux.HandleFunc("POST /courses/add", s.addCourse)
	s.mux.HandleFunc("POST /courses/{course_id}/update", s.renameCourse)
	s.mux.HandleFunc("POST /courses/{course_id}/hide", s.hideCourse)
	s.mux.HandleFunc("POST /courses/{course_id}/show", s.hideCourse)
	s.mux.HandleFunc("POST /courses/{course_id}/delete", s.destructiveCourse)
	s.mux.HandleFunc("POST /courses/{course_id}/reset", s.destructiveCourse)
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
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(form.Get("course_id"), 10, 64)
	if err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, "Course ID must be an integer")
		return
	}
	if err = s.courseService().Add(r.Context(), id, form.Get("name")); err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) && pg.Code == "23505" {
			writeFailure(w, http.StatusBadRequest, "Course already exists")
			return
		}
		writeFailure(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	http.Redirect(w, r, "/courses", http.StatusSeeOther)
}
func (s *Server) renameCourse(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	if err := s.courseService().Rename(r.Context(), id, form.Get("name")); err != nil {
		s.courseError(w, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/courses/%d", id), http.StatusSeeOther)
}
func (s *Server) hideCourse(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	hidden := r.Pattern == "POST /courses/{course_id}/hide"
	if err := s.courseService().Hide(r.Context(), id, hidden); err != nil {
		s.courseError(w, err)
		return
	}
	target := "/settings"
	if hidden {
		target = "/courses"
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
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
	if errors.Is(err, pgx.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Course not found")
		return
	}
	s.internal(w, err)
}
