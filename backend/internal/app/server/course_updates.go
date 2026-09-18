package server

import (
	"net/http"

	"tree-eclass/internal/domain/activity"
)

func (s *Server) courseUpdates(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	limit, ok := queryInteger(w, r, "limit", 10)
	if !ok {
		return
	}
	offset, ok := queryInteger(w, r, "offset", 0)
	if !ok {
		return
	}
	if limit < 1 || limit > 50 || offset < 0 || offset > 1<<31-1 {
		writeFailure(
			w,
			http.StatusUnprocessableEntity,
			"limit must be 1 to 50 and offset must be a nonnegative page offset",
		)
		return
	}
	result, err := (activity.Reader{Pool: s.db.Pool}).CoursePage(r.Context(), id, limit, offset)
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
