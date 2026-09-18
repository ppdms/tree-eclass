package server

import (
	"errors"
	"net/http"
	"strings"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/infrastructure/jobs"
)

func (s *Server) knowledgeStatus(w http.ResponseWriter, r *http.Request) {
	s.knowledgeAdmin(w, r, false)
}

func (s *Server) knowledgeOverview(w http.ResponseWriter, r *http.Request) {
	s.knowledgeAdmin(w, r, true)
}

func (s *Server) knowledgeAdmin(w http.ResponseWriter, r *http.Request, overview bool) {
	course, ok := optionalCourse(w, r)
	if !ok {
		return
	}
	result, err := s.knowledgeReader().Status(r.Context(), course)
	if errors.Is(err, knowledge.ErrUnavailable) {
		writeFailure(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		s.internal(w, err)
		return
	}
	if overview {
		documents, err := s.knowledgeReader().Documents(r.Context(), course, "", "", 200)
		if err != nil {
			s.internal(w, err)
			return
		}
		result = map[string]any{
			"courses":   result["coverage"],
			"documents": documents,
			"jobs":      result["jobs"],
			"status":    result,
			"embedding": result["embedding"],
			"ocr": map[string]any{
				"enabled":   s.config.Tessdata != "",
				"languages": "ell+eng",
			},
			"retrieval": map[string]any{"default_mode": "hybrid", "embedding_model": knowledge.LocalEmbeddingModel},
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) knowledgeMaintenance(w http.ResponseWriter, r *http.Request) {
	action := strings.TrimPrefix(r.URL.Path, "/api/knowledge/")
	if action == "retry-failed" {
		action = "retry_failed"
	}
	id, err := (jobs.Queue{Pool: s.db.Pool}).Enqueue(r.Context(), "index", action, map[string]any{}, true)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "queued", "action": action, "command_id": id})
}
