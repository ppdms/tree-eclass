package server

import (
	"errors"
	"net/http"
	"strings"
	"tree-eclass/internal/domain/database"

	"tree-eclass/internal/infrastructure/markdown"
	"tree-eclass/internal/services/chat"
)

func (s *Server) conversationRoutes() {
	s.mux.HandleFunc("POST /api/ask/stream", s.ask)
	s.mux.HandleFunc("GET /api/ask/conversations", s.conversations)
	s.mux.HandleFunc("GET /api/ask/conversations/{conversation_id}", s.conversation)
	s.mux.HandleFunc("DELETE /api/ask/conversations/{conversation_id}", s.deleteConversation)
	s.mux.HandleFunc("PATCH /api/ask/conversations/{conversation_id}", s.renameConversation)
	s.mux.HandleFunc("GET /api/ask/bootstrap", s.askBootstrap)
}

func (s *Server) askBootstrap(w http.ResponseWriter, r *http.Request) {
	var conversation chat.Conversation
	var id *int64
	if r.URL.Query().Has("conversation_id") {
		value, ok := queryID(w, r, "conversation_id")
		if !ok {
			return
		}
		id = &value
		var err error
		conversation, err = s.chatStore().Get(r.Context(), value)
		if err != nil {
			s.conversationError(w, err)
			return
		}
	}
	config, err := s.settingsService().AI(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	available := config.ProviderAvailable(s.config.ProviderKeys)
	models := []string{}
	if available {
		models = config.ChatModels
	}
	writeJSON(
		w,
		http.StatusOK,
		map[string]any{
			"examples": []string{
				"What should I study today?",
				"What's examinable in Τεχνολογία Λογισμικού?",
				"Find the UML use case slides",
				"Any deadlines I've missed?",
			},
			"models":             models,
			"default_model":      config.DefaultModel,
			"provider_available": available,
			"conversation_id":    id,
			"initial_messages":   chat.InitialMessages(conversation),
		},
	)
}
func (s *Server) chatStore() chat.Store { return chat.Store{Pool: s.db.Pool} }
func (s *Server) conversationError(w http.ResponseWriter, err error) {
	if errors.Is(err, database.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Conversation not found")
		return
	}
	s.internal(w, err)
}
func (s *Server) conversations(w http.ResponseWriter, r *http.Request) {
	rows, err := s.chatStore().List(r.Context(), 50)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"conversations": rows})
}
func (s *Server) conversation(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "conversation_id")
	if !ok {
		return
	}
	result, err := s.chatStore().Get(r.Context(), id)
	if err != nil {
		s.conversationError(w, err)
		return
	}
	for i, message := range result.Messages {
		if message.Role == "assistant" {
			result.Messages[i].ContentHTML = markdown.Render(message.Content)
		}
	}
	writeJSON(w, http.StatusOK, result)
}
func (s *Server) deleteConversation(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "conversation_id")
	if !ok {
		return
	}
	if err := s.chatStore().Delete(r.Context(), id); err != nil {
		s.conversationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}
func (s *Server) renameConversation(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "conversation_id")
	if !ok {
		return
	}
	var body struct {
		Title string `json:"title"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Title) == "" {
		writeFailure(w, http.StatusBadRequest, "A title is required")
		return
	}
	if err := s.chatStore().Rename(r.Context(), id, body.Title); err != nil {
		s.conversationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id})
}
