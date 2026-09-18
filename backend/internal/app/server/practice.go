package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"tree-eclass/internal/domain/practice"
)

func (s *Server) practiceRoutes() {
	s.mux.HandleFunc("GET /api/study/practice", s.practiceQuestions)
	s.mux.HandleFunc("POST /api/study/practice/attempt", s.practiceAttempt)
}

func (s *Server) practiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, practice.ErrInvalid):
		writeFailure(w, http.StatusUnprocessableEntity, "Invalid practice attempt fields")
	case errors.Is(err, practice.ErrConflict):
		writeFailure(
			w,
			http.StatusConflict,
			"The practice question changed or this request ID was already used differently",
		)
	default:
		s.courseError(w, err)
	}
}

func (s *Server) practiceQuestions(w http.ResponseWriter, r *http.Request) {
	course, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	if course == nil {
		writeFailure(w, http.StatusUnprocessableEntity, "course_id is required")
		return
	}
	view, err := (practice.Service{Pool: s.db.Pool}).Read(r.Context(), *course, r.URL.Query().Get("unit_key"))
	if err != nil {
		s.practiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) practiceAttempt(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Course     json.Number  `json:"course_id"`
		Question   string       `json:"question_id"`
		Outcome    string       `json:"outcome"`
		Key        string       `json:"idempotency_key"`
		Confidence *json.Number `json:"confidence"`
		Seconds    *json.Number `json:"seconds"`
		Answer     *string      `json:"answer"`
		Note       *string      `json:"note"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	course, ok := number(w, body.Course, "course_id", 1, 1<<63-1)
	if !ok {
		return
	}
	confidence, ok := optionalNumber(w, body.Confidence, "confidence", 0, 5)
	if !ok {
		return
	}
	seconds, ok := optionalNumber(w, body.Seconds, "seconds", 0, 86400)
	if !ok {
		return
	}
	service := practice.Service{Pool: s.db.Pool}
	attempt, err := service.Record(
		r.Context(),
		practice.Attempt{
			CourseID:   course,
			Question:   body.Question,
			Outcome:    body.Outcome,
			Key:        body.Key,
			Confidence: confidence,
			Seconds:    seconds,
			Answer:     body.Answer,
			Note:       body.Note,
		},
	)
	if err != nil {
		s.practiceError(w, err)
		return
	}
	view, err := service.Read(r.Context(), course, attempt.Unit)
	var result any = view
	var refreshError any
	if err != nil {
		result = nil
		refreshError = "The attempt was saved, but practice could not be refreshed."
	}
	writeJSON(
		w,
		http.StatusOK,
		map[string]any{"status": "recorded", "attempt": attempt, "practice": result, "practice_error": refreshError},
	)
}
