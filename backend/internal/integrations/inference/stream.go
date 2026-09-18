package inference

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
)

const maxStreamBytes = 32 * 1024 * 1024

type callDelta struct {
	Index    int      `json:"index"`
	ID       string   `json:"id"`
	Function Function `json:"function"`
}
type streamState struct {
	calls        map[int]*ToolCall
	finished     string
	bytes, tools int
}

func scanner(r io.Reader) *bufio.Scanner {
	s := bufio.NewScanner(io.LimitReader(r, maxStreamBytes+1))
	s.Buffer(make([]byte, 8192), 1024*1024)
	return s
}

func streamSSE(r io.Reader, emit func(Delta) error) error {
	scan := scanner(r)
	state := streamState{calls: map[int]*ToolCall{}}
	data := ""
	for scan.Scan() {
		line := scan.Text()
		state.bytes += len(line) + 1
		if state.bytes > maxStreamBytes {
			return errors.New("provider stream exceeds 32 MiB")
		}
		if strings.HasPrefix(line, "data:") {
			if data != "" {
				data += "\n"
			}
			data += strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			if len(data) > 1024*1024 {
				return errors.New("provider event exceeds 1 MiB")
			}
		}
		if line != "" || data == "" {
			continue
		}
		if data == "[DONE]" {
			return state.complete(emit)
		}
		if err := state.event([]byte(data), emit); err != nil {
			return err
		}
		data = ""
	}
	return errors.Join(errors.New("provider stream ended before its terminator"), scan.Err())
}

func (s *streamState) event(raw []byte, emit func(Delta) error) error {
	var event struct {
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			Index  int     `json:"index"`
			Finish *string `json:"finish_reason"`
			Delta  struct {
				Content  string      `json:"content"`
				Thinking string      `json:"reasoning_content"`
				Calls    []callDelta `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return errors.New("malformed provider stream event")
	}
	if len(event.Error) > 0 && string(event.Error) != "null" {
		return errors.New("provider reported a stream error")
	}
	for _, choice := range event.Choices {
		if choice.Index != 0 {
			return errors.New("unexpected multiple provider completions")
		}
		if s.finished != "" {
			return errors.New("provider continued a finished completion")
		}
		for _, delta := range choice.Delta.Calls {
			if err := s.merge(delta); err != nil {
				return err
			}
		}
		if choice.Delta.Content != "" || choice.Delta.Thinking != "" {
			if err := emit(Delta{Text: choice.Delta.Content, Thinking: choice.Delta.Thinking}); err != nil {
				return err
			}
		}
		if choice.Finish != nil {
			s.finished = *choice.Finish
		}
	}
	return nil
}

func (s *streamState) merge(d callDelta) error {
	if d.Index < 0 || d.Index >= 32 {
		return errors.New("provider exceeded 32 tool calls per round")
	}
	s.tools += len(d.ID) + len(d.Function.Name) + len(d.Function.Arguments)
	if s.tools > 256*1024 {
		return errors.New("provider tool arguments exceed 256 KiB")
	}
	c := s.calls[d.Index]
	if c == nil {
		c = &ToolCall{Type: "function"}
		s.calls[d.Index] = c
	}
	if d.ID != "" {
		if c.ID != "" && c.ID != d.ID {
			return errors.New("provider changed tool call identity")
		}
		c.ID = d.ID
	}
	// Function names can themselves arrive in fragments.
	c.Function.Name += d.Function.Name
	c.Function.Arguments += d.Function.Arguments
	return nil
}

func (s *streamState) complete(emit func(Delta) error) error {
	if s.finished != "stop" && s.finished != "tool_calls" {
		return errors.New("provider did not finish its answer or tool request")
	}
	keys := []int{}
	for key := range s.calls {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	calls, seen := []ToolCall{}, map[string]bool{}
	for _, key := range keys {
		call := *s.calls[key]
		if call.ID == "" {
			call.ID = "call_" + rand.Text()
		}
		var args map[string]json.RawMessage
		if seen[call.ID] || len(call.ID) > 256 || call.Function.Name == "" || len(call.Function.Name) > 128 ||
			json.Unmarshal([]byte(call.Function.Arguments), &args) != nil ||
			args == nil {
			return errors.New("provider returned invalid tool arguments or identity")
		}
		seen[call.ID] = true
		calls = append(calls, call)
	}
	if s.finished == "tool_calls" && len(calls) == 0 {
		return errors.New("provider finished without its requested tools")
	}
	return emit(Delta{Finish: s.finished, Calls: calls})
}
