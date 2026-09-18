package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/study"
)

func (s *Server) studyEvent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Course     json.Number  `json:"course_id"`
		Action     string       `json:"action_id"`
		Revision   string       `json:"plan_revision"`
		Type       string       `json:"event_type"`
		Key        string       `json:"idempotency_key"`
		Confidence *json.Number `json:"confidence"`
		Minutes    *json.Number `json:"actual_minutes"`
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
	minutes, ok := optionalNumber(w, body.Minutes, "actual_minutes", 0, 1440)
	if !ok {
		return
	}
	event, err := (study.Service{Pool: s.db.Pool}).Record(
		r.Context(),
		study.Event{
			CourseID:   course,
			Action:     body.Action,
			Revision:   body.Revision,
			Type:       body.Type,
			Key:        body.Key,
			Confidence: confidence,
			Minutes:    minutes,
			Note:       body.Note,
		},
	)
	if err != nil {
		switch {
		case errors.Is(err, study.ErrInvalidEvent):
			writeFailure(w, http.StatusUnprocessableEntity, "Invalid study event fields")
		case errors.Is(err, study.ErrEventConflict), errors.Is(err, navigation.ErrActionConflict):
			writeFailure(w, http.StatusConflict, "The roadmap changed or this request ID was already used differently")
		default:
			s.courseError(w, err)
		}
		return
	}
	if r.URL.Path == "/api/study/actions/event" {
		s.studyEventPlan(w, r, event)
		return
	}
	view, err := (navigation.Service{Pool: s.db.Pool}).Read(
		r.Context(),
		navigation.Request{CourseID: course, IncludeHidden: true},
	)
	// The append already committed. A read failure must not report the write as
	// failed or encourage a different idempotency key on retry.
	var overview any = view
	var refreshError any
	if err != nil {
		overview = nil
		refreshError = "Progress was saved, but the overview could not be refreshed."
	}
	writeJSON(
		w,
		http.StatusOK,
		map[string]any{
			"event_id":       event.ID,
			"event":          event,
			"overview":       overview,
			"status":         "recorded",
			"overview_error": refreshError,
		},
	)
}

func (s *Server) studyEventPlan(w http.ResponseWriter, r *http.Request, event study.Event) {
	view, err := (study.Service{Pool: s.db.Pool}).Intelligence(r.Context(), nil, time.Now())
	var plan, planError any
	if err != nil {
		planError = "Progress was saved, but the study plan could not be refreshed."
	} else if view["adaptive_plan_available"] != true {
		planError = "Progress was saved. The study plan is being updated."
	} else {
		plan = view["adaptive_plan"]
	}
	writeJSON(
		w,
		http.StatusOK,
		map[string]any{"status": "recorded", "event": event, "plan": plan, "plan_error": planError},
	)
}
