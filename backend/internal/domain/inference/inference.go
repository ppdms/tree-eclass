// Package inference defines the bounded provider-streaming contract shared by
// Ask and source analysis. It owns no HTTP transport or database.
package inference

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Candidate struct {
	ThinkingLevel             string
	Provider, Model, Endpoint string
	APIKey                    string `json:"-"`
	Think                     *bool
}
type Function struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}
type ToolCall struct {
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Function Function `json:"function"`
}
type Message struct {
	Images   []Image    `json:"-"`
	Role     string     `json:"role"`
	Content  any        `json:"content"`
	Thinking string     `json:"reasoning_content,omitempty"`
	Calls    []ToolCall `json:"tool_calls,omitempty"`
	ToolID   string     `json:"tool_call_id,omitempty"`
	ToolName string     `json:"-"`
}
type Tool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string         `json:"name"`
		Description string         `json:"description"`
		Parameters  map[string]any `json:"parameters"`
	} `json:"function"`
}
type Request struct {
	Messages []Message
	Tools    []Tool
	Schema   map[string]any
	JSON     bool
}
type Delta struct {
	Text, Thinking, Finish string
	Calls                  []ToolCall
}
type Image struct{ MIMEType, Data string }

type Error struct {
	Provider   string
	Status     int
	Retryable  bool
	RetryAfter time.Duration
	Reason     string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (HTTP %d)", e.Provider, e.Reason, e.Status)
}

// Streamer emits provider deltas synchronously through consume.
type Streamer interface {
	Stream(ctx context.Context, c Candidate, r Request, emit func(Delta) error) error
}

// RetryAfter bounds an upstream retry delay to one day.
func RetryAfter(value string) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		return time.Duration(min(86400, max(0, seconds))) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil {
		return min(24*time.Hour, max(0, time.Until(date)))
	}
	return 0
}
