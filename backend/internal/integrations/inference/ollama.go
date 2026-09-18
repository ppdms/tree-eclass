package inference

import (
	"encoding/json"
	"errors"
	"io"
)

func streamOllama(r io.Reader, emit func(Delta) error) error {
	scan := scanner(r)
	state := streamState{calls: map[int]*ToolCall{}}
	for scan.Scan() {
		raw := scan.Bytes()
		state.bytes += len(raw) + 1
		if state.bytes > maxStreamBytes {
			return errors.New("provider stream exceeds 32 MiB")
		}
		var event struct {
			Done    bool   `json:"done"`
			Reason  string `json:"done_reason"`
			Error   string `json:"error"`
			Message struct {
				Content  string `json:"content"`
				Thinking string `json:"thinking"`
				Calls    []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string          `json:"name"`
						Arguments json.RawMessage `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return errors.New("malformed Ollama stream event")
		}
		if event.Error != "" {
			return errors.New("Ollama reported a stream error")
		}
		for _, call := range event.Message.Calls {
			if err := state.merge(callDelta{
				Index: len(state.calls),
				ID:    call.ID,
				Function: Function{
					Name:      call.Function.Name,
					Arguments: string(call.Function.Arguments),
				},
			}); err != nil {
				return err
			}
		}
		if event.Message.Content != "" || event.Message.Thinking != "" {
			if err := emit(Delta{Text: event.Message.Content, Thinking: event.Message.Thinking}); err != nil {
				return err
			}
		}
		if event.Done {
			state.finished = event.Reason
			if state.finished == "" {
				state.finished = "stop"
			}
			return state.complete(emit)
		}
	}
	return errors.Join(errors.New("Ollama stream ended before completion"), scan.Err())
}
