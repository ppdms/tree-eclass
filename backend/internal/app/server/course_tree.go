package server

import "net/http"

func (s *Server) courseTree(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	tree, err := s.courseService().Tree(r.Context(), id)
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tree": tree})
}
