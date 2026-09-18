package server

import (
	"net/http"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/materials"
)

func (s *Server) courseFileRoutes() {
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/files", s.courseFiles)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/file-metadata", s.courseFileMetadata)
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/files/{document_id}/guide", s.courseFileGuide)
}

func (s *Server) courseFiles(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	include, ok := queryBool(w, r, "include_knowledge", true)
	if !ok {
		return
	}
	files, err := s.courseService().Files(r.Context(), id)
	if err != nil {
		s.courseError(w, err)
		return
	}
	if !include {
		writeJSON(w, http.StatusOK, files)
		return
	}
	// Metadata and individual guides remain separate, even for this compatibility
	// response. The UI loads full analyses only for the guide the user opens.
	metadata, err := s.knowledgeReader().FileMetadata(r.Context(), id, s.config.ProviderKeys)
	if err != nil {
		s.courseError(w, err)
		return
	}
	external, ok := s.materialList(w, r, id, "")
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, struct {
		courses.Files
		Insights map[string]knowledge.FileMetadata `json:"file_insights"`
		External []materials.Material              `json:"external_materials"`
	}{files, metadata, external})
}

func (s *Server) courseFileMetadata(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	result, err := s.knowledgeReader().FileMetadata(r.Context(), id, s.config.ProviderKeys)
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"file_insights": result})
}

func (s *Server) courseFileGuide(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	result, err := s.knowledgeReader().FileGuide(r.Context(), id, r.PathValue("document_id"))
	if err != nil {
		s.courseError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
