package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/domain/workspace"
)

func (s *Server) workspaceRoutes() {
	s.mux.HandleFunc("GET /api/study/session/context", s.workspaceContext)
	s.mux.HandleFunc("POST /api/study/session/start", s.workspaceStart)
	s.mux.HandleFunc("POST /api/study/session/heartbeat", s.workspaceHeartbeat)
	s.mux.HandleFunc("POST /api/study/session/finish", s.workspaceFinish)
}

func (s *Server) workspaceContext(w http.ResponseWriter, r *http.Request) {
	course, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	if course == nil {
		writeFailure(w, http.StatusUnprocessableEntity, "course_id is required")
		return
	}
	include, ok := queryBool(w, r, "include_practice", true)
	if !ok {
		return
	}
	view, err := s.workspaceService().
		Context(
			r.Context(),
			workspace.ContextRequest{
				CourseID: *course,
				Action:   r.URL.Query().Get("action_id"),
				Document: r.URL.Query().Get("document_id"),
				Practice: include,
			},
		)
	if err != nil {
		s.workspaceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}
func (s *Server) workspaceService() workspace.Service { return workspace.Service{Pool: s.db.Pool} }

func (s *Server) workspaceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workspace.ErrInvalid):
		writeFailure(w, http.StatusUnprocessableEntity, "Invalid study session fields")
	case errors.Is(err, workspace.ErrDocumentPending):
		writeFailure(w, http.StatusConflict, "This document is still being prepared for study. It opens automatically once indexing finishes.")
	case errors.Is(err, workspace.ErrConflict):
		writeFailure(w, http.StatusConflict, "This study request conflicts with the saved session or current roadmap")
	case errors.Is(err, database.ErrNoRows):
		writeFailure(w, http.StatusNotFound, "Study session, document or course not found")
	default:
		s.internal(w, err)
	}
}

func number(w http.ResponseWriter, n json.Number, name string, low, high int64) (int64, bool) {
	v, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil || v < low || v > high {
		writeFailure(w, http.StatusUnprocessableEntity, name+" must be a valid whole integer in range")
		return 0, false
	}
	return v, true
}
func optionalNumber(w http.ResponseWriter, n *json.Number, name string, low, high int64) (*int64, bool) {
	if n == nil || *n == "" {
		return nil, true
	}
	v, ok := number(w, *n, name, low, high)
	return &v, ok
}

func (s *Server) workspaceStart(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Course   json.Number  `json:"course_id"`
		Key      string       `json:"session_key"`
		Action   string       `json:"action_id"`
		Unit     string       `json:"unit_key"`
		Revision string       `json:"plan_revision"`
		Planned  *json.Number `json:"planned_minutes"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	course, ok := number(w, body.Course, "course_id", 1, 1<<31)
	if !ok {
		return
	}
	planned, ok := optionalNumber(w, body.Planned, "planned_minutes", 0, 1440)
	if !ok {
		return
	}
	result, err := s.workspaceService().
		Start(
			r.Context(),
			workspace.Start{
				CourseID: course,
				Key:      body.Key,
				Action:   body.Action,
				Unit:     body.Unit,
				Revision: body.Revision,
				Planned:  planned,
			},
		)
	if err != nil {
		s.workspaceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": result})
}

func (s *Server) workspaceHeartbeat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Session  json.Number `json:"session_id"`
		Sequence json.Number `json:"sequence"`
		Page     json.Number `json:"page_number"`
		Interval json.Number `json:"interval_seconds"`
		Document string      `json:"document_id"`
		Active   bool        `json:"active"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	in := workspace.Beat{Document: body.Document, Active: body.Active}
	for _, field := range []struct {
		Value     json.Number
		Name      string
		Low, High int64
		Target    *int64
	}{
		{body.Session, "session_id", 1, 1<<63 - 1, &in.SessionID},
		{body.Sequence, "sequence", 0, 1 << 31, &in.Sequence},
		{body.Page, "page_number", 1, 100000, &in.Page},
		{body.Interval, "interval_seconds", 0, 3600, &in.Interval},
	} {
		var ok bool
		*field.Target, ok = number(w, field.Value, field.Name, field.Low, field.High)
		if !ok {
			return
		}
	}
	result, err := s.workspaceService().Heartbeat(r.Context(), in)
	if err != nil {
		s.workspaceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) workspaceFinish(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Session    json.Number  `json:"session_id"`
		Outcome    string       `json:"outcome"`
		Note       *string      `json:"note"`
		Confidence *json.Number `json:"confidence"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	id, ok := number(w, body.Session, "session_id", 1, 1<<63-1)
	if !ok {
		return
	}
	confidence, ok := optionalNumber(w, body.Confidence, "confidence", 0, 5)
	if !ok {
		return
	}
	result, err := s.workspaceService().
		Finish(r.Context(), workspace.Finish{SessionID: id, Outcome: body.Outcome, Note: body.Note, Confidence: confidence})
	if err != nil {
		s.workspaceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
