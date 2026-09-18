package server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/knowledge"
)

func (s *Server) knowledgeRoutes() {
	for _, action := range []string{"reconcile", "rebuild", "retry-failed"} {
		s.mux.HandleFunc("POST /api/knowledge/"+action, s.knowledgeMaintenance)
	}
	s.mux.HandleFunc("GET /api/knowledge/summary", s.knowledgeSummary)
	s.mux.HandleFunc("GET /api/knowledge/status", s.knowledgeStatus)
	s.mux.HandleFunc("GET /api/knowledge/overview", s.knowledgeOverview)
	s.mux.HandleFunc("GET /api/knowledge/search", s.knowledgeSearch)
	s.mux.HandleFunc("GET /api/knowledge/documents", s.knowledgeDocuments)
	s.mux.HandleFunc("GET /api/study/document/{document_id}/pages", s.documentPages)
}
func (s *Server) knowledgeReader() knowledge.Reader { return knowledge.Reader{Pool: s.db.Pool} }
func optionalCourse(w http.ResponseWriter, r *http.Request) (*int64, bool) {
	if !r.URL.Query().Has("course_id") {
		return nil, true
	}
	id, ok := queryID(w, r, "course_id")
	return &id, ok
}
func queryInteger(w http.ResponseWriter, r *http.Request, key string, fallback int64) (int64, bool) {
	if !r.URL.Query().Has(key) {
		return fallback, true
	}
	value, err := strconv.ParseInt(r.URL.Query().Get(key), 10, 64)
	if err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, key+" must be an integer")
		return 0, false
	}
	return value, true
}
func (s *Server) knowledgeSummary(w http.ResponseWriter, r *http.Request) {
	course, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	result, err := s.knowledgeReader().Summary(r.Context(), course)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"coverage": result})
}
func (s *Server) knowledgeSearch(w http.ResponseWriter, r *http.Request) {
	course, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	limit, ok := queryInteger(w, r, "limit", 8)
	if !ok {
		return
	}
	request := knowledge.SearchRequest{
		Query: r.URL.Query().Get("q"),
		Mode:  r.URL.Query().Get("mode"),
		Limit: int(limit),
	}
	if course != nil {
		request.CourseIDs = []int64{*course}
	}
	if err := request.Validate(); err != nil {
		writeFailure(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := s.knowledgeReader().Search(r.Context(), request)
	if errors.Is(err, knowledge.ErrUnavailable) {
		writeFailure(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
func (s *Server) knowledgeDocuments(w http.ResponseWriter, r *http.Request) {
	course, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	limit, ok := queryInteger(w, r, "limit", 200)
	if !ok {
		return
	}
	result, err := s.knowledgeReader().
		Documents(r.Context(), course, r.URL.Query().Get("status"), r.URL.Query().Get("q"), int(limit))
	if errors.Is(err, knowledge.ErrUnavailable) {
		writeFailure(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"documents": result})
}
func (s *Server) documentPages(w http.ResponseWriter, r *http.Request) {
	course, ok := queryID(w, r, "course_id")
	if !ok || !s.requireStudyCourse(w, r, course) {
		return
	}
	first, ok := queryInteger(w, r, "first", 1)
	if !ok {
		return
	}
	last, ok := queryInteger(w, r, "last", 1)
	if !ok {
		return
	}
	result, err := s.knowledgeReader().Pages(r.Context(), course, r.PathValue("document_id"), first, last)
	if errors.Is(err, pgx.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Document is not indexed or available in this course")
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
