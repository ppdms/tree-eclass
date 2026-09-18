package server

import (
	"net/http"
	"unicode/utf8"
)

func (s *Server) setStudyLevel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	var body struct {
		Path  string `json:"file_path"`
		Level *int64 `json:"level"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	if body.Path == "" || utf8.RuneCountInString(body.Path) > 4096 || body.Level == nil || *body.Level < 0 ||
		*body.Level > 5 {
		writeFailure(w, http.StatusUnprocessableEntity, "file_path and a level between 0 and 5 are required")
		return
	}
	if err := s.courseService().StudyLevel(r.Context(), id, body.Path, *body.Level); err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "level": *body.Level})
}

func (s *Server) setFolderCollapsed(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	var body struct {
		Key       string `json:"folder_key"`
		Collapsed *bool  `json:"collapsed"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	if body.Key == "" || utf8.RuneCountInString(body.Key) > 4096 || body.Collapsed == nil {
		writeFailure(w, http.StatusUnprocessableEntity, "folder_key and a boolean collapsed value are required")
		return
	}
	if err := s.courseService().FolderCollapsed(r.Context(), id, body.Key, *body.Collapsed); err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "collapsed": *body.Collapsed})
}
