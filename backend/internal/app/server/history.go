package server

import (
	"net/http"
	"path"
	"strings"
)

func (s *Server) historyRoutes() {
	s.mux.HandleFunc("GET /api/courses/{course_id}/changes/{change_no}", s.changeRecord)
	s.mux.HandleFunc("GET /api/courses/{course_id}/file-versions", s.fileVersions)
	s.mux.HandleFunc("GET /api/courses/{course_id}/deleted-files", s.deletedFiles)
}
func (s *Server) changeRecord(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	record, items, err := s.syncService().History(r.Context(), id, r.PathValue("change_no"))
	if err != nil {
		s.fileError(w, err)
		return
	}
	course, err := s.courseService().Get(r.Context(), id)
	if err != nil {
		s.fileError(w, err)
		return
	}
	writeJSON(
		w,
		http.StatusOK,
		map[string]any{
			"change_record": record,
			"changes":       items,
			"course":        course,
			"webdav_folder": path.Join(course.StoragePrefix, "eclass"),
		},
	)
}
func (s *Server) fileVersions(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	if !r.URL.Query().Has("file_path") {
		writeFailure(w, http.StatusUnprocessableEntity, "File path is required")
		return
	}
	file := r.URL.Query().Get("file_path")
	versions, err := s.syncService().Versions(r.Context(), id, "modified", &file, nil)
	if err != nil {
		s.fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": versions})
}
func (s *Server) deletedFiles(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	var folder *string
	if r.URL.Query().Has("folder") {
		value := strings.TrimRight(r.URL.Query().Get("folder"), "/")
		folder = &value
	}
	versions, err := s.syncService().Versions(r.Context(), id, "deleted", nil, folder)
	if err != nil {
		s.fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": versions})
}
