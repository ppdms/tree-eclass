package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/services/chat"
)

// WithInferenceClient supplies the shared transport for offline integration
// tests and embedded callers. Runtime configuration cannot override endpoints.
type Option func(*Server)

type askInput struct {
	question string
	model    string
	id       *int64
}

func WithInferenceClient(client chat.Streamer) Option {
	return func(s *Server) { s.inference = client }
}

func (s *Server) ask(w http.ResponseWriter, r *http.Request) {
	input, ok := readAskInput(w, r)
	if !ok {
		return
	}
	if !s.askMu.TryLock() {
		writeFailure(w, http.StatusConflict, "An answer is already running. Stop it or wait for it to finish.")
		return
	}
	defer s.askMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Minute)
	defer cancel()
	if err := s.answerAsk(ctx, w, input); err != nil {
		s.internal(w, err)
	}
}

func readAskInput(w http.ResponseWriter, r *http.Request) (askInput, bool) {
	var body struct {
		Question     string          `json:"question"`
		Model        string          `json:"model"`
		Conversation json.RawMessage `json:"conversation_id"`
	}
	if !bodyJSON(w, r, &body) {
		return askInput{}, false
	}
	question := strings.TrimSpace(body.Question)
	if question == "" || !utf8.ValidString(question) || utf8.RuneCountInString(question) > 16000 {
		writeFailure(w, http.StatusBadRequest, "A question must contain 1 to 16000 characters")
		return askInput{}, false
	}
	id, err := conversationID(body.Conversation)
	if err != nil {
		writeFailure(w, http.StatusBadRequest, "Invalid conversation id")
		return askInput{}, false
	}
	return askInput{question: question, model: strings.TrimSpace(body.Model), id: id}, true
}

func (s *Server) answerAsk(ctx context.Context, w http.ResponseWriter, input askInput) error {
	history := []chat.Message{}
	if input.id != nil {
		var err error
		history, err = s.chatStore().History(ctx, *input.id)
		if err != nil {
			s.conversationError(w, err)
			return nil
		}
	}
	a, err := s.settingsService().AI(ctx)
	if err != nil {
		return err
	}
	candidates := inference.ChatCandidates(a, s.config.ProviderKeys, input.model)
	if len(candidates) == 0 {
		writeFailure(
			w,
			http.StatusServiceUnavailable,
			"No chat provider is configured. Add a provider key and enable it in Settings.",
		)
		return nil
	}
	return s.streamAsk(ctx, w, input, history, candidates)
}

func (s *Server) streamAsk(
	ctx context.Context,
	w http.ResponseWriter,
	input askInput,
	history []chat.Message,
	candidates []inference.Candidate,
) error {
	stream := newUIStream(ctx, w)
	if err := stream.start(); err != nil {
		return err
	}
	result, err := (chat.Engine{Client: s.inference, Tools: s.library}).Stream(
		ctx,
		candidates,
		input.question,
		history,
		stream.event,
	)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = stream.closeText()
	}
	if err != nil {
		stream.finishError(err)
		return nil
	}
	return s.persistAsk(ctx, stream, input, result)
}

func (s *Server) persistAsk(ctx context.Context, stream *uiStream, input askInput, result chat.Answer) error {
	saved, err := s.chatStore().
		SaveTurn(
			ctx,
			chat.Turn{
				ConversationID: input.id,
				Question:       input.question,
				Answer:         result.Text,
				Model:          result.Model,
				Consulted:      result.Consulted,
			},
		)
	if err != nil {
		stream.finishError(err)
		return nil
	}
	if err = stream.frame(map[string]any{"type": "data-conversation", "data": saved}); err != nil {
		return err
	}
	_ = stream.finish()
	return nil
}

func conversationID(raw json.RawMessage) (*int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	value := string(raw)
	if strings.HasPrefix(value, `"`) {
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id < 1 {
		return nil, errors.New("invalid conversation id")
	}
	return &id, nil
}
