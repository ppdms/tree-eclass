package server

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"tree-eclass/internal/domain/courses"
)

func (s *Server) destructiveCourse(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	action := "reset"
	if r.Pattern == "POST /courses/{course_id}/delete" {
		action = "delete"
	}
	if r.Header.Get("X-Tree-Eclass-Confirmation") != fmt.Sprintf("%s:%d", action, id) {
		writeFailure(w, http.StatusPreconditionRequired, "This destructive action needs an explicit confirmation.")
		return
	}
	key := strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	if !courses.ValidMutationKey(key) {
		writeFailure(w, http.StatusBadRequest, "A valid idempotency key is required.")
		return
	}
	if err := s.courseService().Destructive(r.Context(), id, action, key); err != nil {
		if errors.Is(err, courses.ErrReplay) || errors.Is(err, courses.ErrBusy) {
			writeFailure(w, http.StatusConflict, err.Error())
			return
		}
		s.courseError(w, err)
		return
	}
	target := "/courses"
	if action == "reset" {
		target = fmt.Sprintf("/courses/%d", id)
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
