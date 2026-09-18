package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"tree-eclass/internal/services/chat"
)

type uiStream struct {
	ctx      context.Context
	writer   http.ResponseWriter
	control  *http.ResponseController
	textID   string
	textOpen bool
	toolID   string
	sequence int
}

func newUIStream(ctx context.Context, w http.ResponseWriter) *uiStream {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Vercel-AI-UI-Message-Stream", "v1")
	return &uiStream{ctx: ctx, writer: w, control: http.NewResponseController(w), textID: "text-0"}
}
func (s *uiStream) start() error {
	if err := s.frame(map[string]any{"type": "start", "messageId": rand.Text()}); err != nil {
		return err
	}
	return s.frame(map[string]string{"type": "start-step"})
}
func (s *uiStream) frame(value any) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.write("data: " + string(raw) + "\n\n")
}
func (s *uiStream) write(value string) error {
	_ = s.control.SetWriteDeadline(time.Now().Add(15 * time.Second))
	if _, err := fmt.Fprint(s.writer, value); err != nil {
		return err
	}
	return s.control.Flush()
}
func (s *uiStream) closeText() error {
	if !s.textOpen {
		return nil
	}
	s.textOpen = false
	return s.frame(map[string]string{"type": "text-end", "id": s.textID})
}
func (s *uiStream) event(e chat.Event) error {
	switch e.Type {
	case "model":
		return s.frame(
			map[string]any{"type": "data-model", "data": map[string]string{"provider": e.Provider, "model": e.Model}},
		)
	case "text":
		if !s.textOpen {
			if err := s.frame(map[string]string{"type": "text-start", "id": s.textID}); err != nil {
				return err
			}
			s.textOpen = true
		}
		return s.frame(map[string]string{"type": "text-delta", "id": s.textID, "delta": e.Text})
	case "tool_call":
		if err := s.closeText(); err != nil {
			return err
		}
		s.sequence++
		s.toolID = "tool-" + strconv.Itoa(s.sequence)
		s.textID = "text-" + strconv.Itoa(s.sequence)
		return s.frame(
			map[string]any{
				"type":       "tool-input-available",
				"toolCallId": s.toolID,
				"toolName":   e.Tool,
				"input":      e.Arguments,
			},
		)
	case "tool_result":
		return s.frame(
			map[string]any{
				"type":       "tool-output-available",
				"toolCallId": s.toolID,
				"output":     map[string]bool{"failed": e.Failed},
			},
		)
	}
	return nil
}
func (s *uiStream) finish() error {
	for _, kind := range []string{"finish-step", "finish"} {
		if err := s.frame(map[string]string{"type": kind}); err != nil {
			return err
		}
	}
	return s.write("data: [DONE]\n\n")
}
func (s *uiStream) finishError(err error) {
	if s.ctx.Err() != nil {
		return
	}
	detail := "The answer model could not complete this question. Try again."
	if errors.Is(err, chat.ErrNoProvider) {
		detail = "No answer model is currently available. Try again later."
	}
	if errors.Is(err, chat.ErrLookupLimit) {
		detail = "This question needs too many lookups. Try a narrower question."
	}
	if s.closeText() != nil {
		return
	}
	if s.frame(map[string]string{"type": "error", "errorText": detail}) != nil {
		return
	}
	_ = s.finish()
}
