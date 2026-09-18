package server

import (
	"net/http"

	"tree-eclass/internal/domain/navigation"
)

func (s *Server) navigationRoutes() {
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}", s.courseDetail)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/overview", s.courseOverview)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/roadmap", s.courseRoadmap)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/roadmap/units/{unit_key}", s.roadmapUnit)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/roadmap/sections/{section}", s.roadmapSection)
}

func (s *Server) courseDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	view, err := (navigation.Service{Pool: s.db.Pool}).Detail(r.Context(), id)
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) navigationRead(
	w http.ResponseWriter,
	r *http.Request,
	roadmap, actions bool,
	unit *string,
) (navigation.View, bool) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return navigation.View{}, false
	}
	view, err := (navigation.Service{Pool: s.db.Pool}).Read(
		r.Context(),
		navigation.Request{CourseID: id, Roadmap: roadmap, IncludeActions: actions, Unit: unit},
	)
	if err != nil {
		s.courseError(w, err)
		return view, false
	}
	return view, true
}

func (s *Server) courseOverview(w http.ResponseWriter, r *http.Request) {
	view, ok := s.navigationRead(w, r, false, false, nil)
	if ok {
		writeJSON(w, http.StatusOK, view)
	}
}

func (s *Server) courseRoadmap(w http.ResponseWriter, r *http.Request) {
	actions, ok := queryBool(w, r, "include_actions", true)
	if !ok {
		return
	}
	view, ok := s.navigationRead(w, r, true, actions, nil)
	if ok {
		writeJSON(w, http.StatusOK, view)
	}
}

func (s *Server) roadmapRevision(w http.ResponseWriter, r *http.Request, unit string) (map[string]any, bool) {
	revision := r.URL.Query().Get("revision")
	if revision == "" {
		writeFailure(w, http.StatusUnprocessableEntity, "revision is required")
		return nil, false
	}
	view, ok := s.navigationRead(w, r, true, true, &unit)
	if !ok {
		return nil, false
	}
	if view.Blueprint["usable"] != true || view.Blueprint["revision_id"] != revision {
		writeFailure(w, http.StatusConflict, "The roadmap changed. Reopen the Roadmap tab.")
		return nil, false
	}
	return view.Blueprint, true
}

func (s *Server) roadmapUnit(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("unit_key")
	view, ok := s.roadmapRevision(w, r, key)
	if !ok {
		return
	}
	blueprint, _ := view["blueprint"].(map[string]any)
	units, _ := blueprint["units"].([]any)
	for _, raw := range units {
		unit, ok := raw.(map[string]any)
		if ok && unit["key"] == key {
			unit["actions"] = view["actions"]
			writeJSON(w, http.StatusOK, map[string]any{"unit": unit, "revision_id": view["revision_id"]})
			return
		}
	}
	writeFailure(w, http.StatusNotFound, "Roadmap unit not found")
}

func (s *Server) roadmapSection(w http.ResponseWriter, r *http.Request) {
	section := r.PathValue("section")
	if section != "strategy" && section != "strategy-evidence" && section != "support" {
		writeFailure(w, http.StatusUnprocessableEntity, "Unknown roadmap section")
		return
	}
	view, ok := s.roadmapRevision(w, r, "")
	if !ok {
		return
	}
	blueprint, _ := view["blueprint"].(map[string]any)
	strategy, _ := blueprint["exam_strategy"].(map[string]any)
	payload := map[string]any{}
	switch section {
	case "strategy":
		delete(strategy, "evidence_links")
		payload["exam_strategy"] = strategy
	case "strategy-evidence":
		links := strategy["evidence_links"]
		if links == nil {
			links = []any{}
		}
		payload["exam_strategy"] = map[string]any{"evidence_links": links}
	default:
		for _, key := range []string{"question_families", "conflicts", "coverage_gaps"} {
			value := blueprint[key]
			if value == nil {
				value = []any{}
			}
			payload[key] = value
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"roadmap": payload, "revision_id": view["revision_id"]})
}
