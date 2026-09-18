package chat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"tree-eclass/internal/integrations/inference"
)

type streamFunc func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error

func (f streamFunc) Stream(
	ctx context.Context,
	c inference.Candidate,
	r inference.Request,
	emit func(inference.Delta) error,
) error {
	return f(ctx, c, r, emit)
}

type toolsFunc func(context.Context, string, json.RawMessage) (any, error)

func (toolsFunc) Definitions() []inference.Tool { return []inference.Tool{} }
func (f toolsFunc) Call(ctx context.Context, name string, args json.RawMessage) (any, error) {
	return f(ctx, name, args)
}

func TestToolLoopAndCompleteAnswer(t *testing.T) {
	rounds, lookups := 0, 0
	engine := Engine{Tools: toolsFunc(func(_ context.Context, name string, args json.RawMessage) (any, error) {
		lookups++
		if name != "read_material" || string(args) != `{"document_id":"δ"}` {
			t.Fatal(name, string(args))
		}
		return map[string]any{"text": "Untrusted source", "untrusted_content": true}, nil
	})}
	engine.Client = streamFunc(
		func(_ context.Context, _ inference.Candidate, r inference.Request, emit func(inference.Delta) error) error {
			rounds++
			if rounds == 1 {
				if len(r.Messages) != 2 || r.Messages[0].Role != "system" {
					t.Fatal(r.Messages)
				}
				if err := emit(inference.Delta{Text: "Let me check.", Thinking: "internal"}); err != nil {
					return err
				}
				return emit(
					inference.Delta{
						Finish: "tool_calls",
						Calls: []inference.ToolCall{
							{
								ID:       "lookup",
								Type:     "function",
								Function: inference.Function{Name: "read_material", Arguments: `{"document_id":"δ"}`},
							},
						},
					},
				)
			}
			if len(r.Messages) != 4 || r.Messages[2].Thinking != "internal" || r.Messages[3].ToolID != "lookup" ||
				!strings.Contains(r.Messages[3].Content.(string), "Untrusted source") {
				t.Fatal(r.Messages)
			}
			if err := emit(inference.Delta{Text: "Τελική απάντηση"}); err != nil {
				return err
			}
			return emit(inference.Delta{Finish: "stop"})
		},
	)
	var events []Event
	answer, err := engine.Stream(
		t.Context(),
		[]inference.Candidate{{Provider: "fixture", Model: "model"}},
		"question",
		nil,
		func(e Event) error { events = append(events, e); return nil },
	)
	if err != nil || rounds != 2 || lookups != 1 || answer.Text != "Τελική απάντηση" || len(answer.Consulted) != 1 ||
		answer.Model != "model" {
		t.Fatal(answer, err)
	}
	if len(events) != 5 || events[2].Type != "tool_call" || events[3].Type != "tool_result" {
		t.Fatal(events)
	}
}

func TestProviderFallbackNeverSplicesAnswers(t *testing.T) {
	for _, emitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "before text", true: "after text"}[emitted], func(t *testing.T) {
			requests := 0
			engine := Engine{
				Tools: toolsFunc(func(context.Context, string, json.RawMessage) (any, error) { return nil, nil }),
			}
			engine.Client = streamFunc(
				func(_ context.Context, c inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
					requests++
					if c.Provider == "first" {
						if emitted {
							if err := emit(inference.Delta{Text: "partial"}); err != nil {
								return err
							}
						}
						return &inference.Error{Provider: c.Provider, Status: 429, Retryable: true}
					}
					if err := emit(inference.Delta{Text: "complete"}); err != nil {
						return err
					}
					return emit(inference.Delta{Finish: "stop"})
				},
			)
			answer, err := engine.Stream(
				t.Context(),
				[]inference.Candidate{{Provider: "first", Model: "first"}, {Provider: "second", Model: "second"}},
				"question",
				nil,
				func(Event) error { return nil },
			)
			if emitted && (err == nil || requests != 1 || answer.Text != "") {
				t.Fatal("failed answer spliced or saved", answer, requests, err)
			}
			if !emitted && (err != nil || requests != 2 || answer.Text != "complete" || answer.Model != "second") {
				t.Fatal("safe fallback failed", answer, requests, err)
			}
		})
	}
}

func TestLookupLimitAndSafeToolFailure(t *testing.T) {
	rounds := 0
	engine := Engine{Tools: toolsFunc(func(context.Context, string, json.RawMessage) (any, error) {
		return nil, errors.New("private database details")
	})}
	engine.Client = streamFunc(
		func(_ context.Context, _ inference.Candidate, r inference.Request, emit func(inference.Delta) error) error {
			rounds++
			for _, message := range r.Messages {
				if text, ok := message.Content.(string); ok && strings.Contains(text, "private database") {
					t.Fatal("tool error leaked")
				}
			}
			return emit(
				inference.Delta{
					Finish: "tool_calls",
					Calls: []inference.ToolCall{
						{
							ID:       "lookup",
							Type:     "function",
							Function: inference.Function{Name: "unknown", Arguments: "{}"},
						},
					},
				},
			)
		},
	)
	_, err := engine.Stream(
		t.Context(),
		[]inference.Candidate{{Provider: "fixture"}},
		"question",
		nil,
		func(e Event) error {
			if e.Type == "tool_result" && !e.Failed {
				t.Fatal("failed lookup reported successful")
			}
			return nil
		},
	)
	if !errors.Is(err, ErrLookupLimit) || rounds != 6 {
		t.Fatal(rounds, err)
	}
}

func TestAbandonedStreamStopsToolExecution(t *testing.T) {
	engine := Engine{Tools: toolsFunc(func(context.Context, string, json.RawMessage) (any, error) {
		t.Fatal("abandoned stream ran a lookup")
		return nil, nil
	})}
	engine.Client = streamFunc(
		func(_ context.Context, _ inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
			return emit(
				inference.Delta{
					Finish: "tool_calls",
					Calls: []inference.ToolCall{
						{
							ID:       "lookup",
							Type:     "function",
							Function: inference.Function{Name: "unknown", Arguments: "{}"},
						},
					},
				},
			)
		},
	)
	_, err := engine.Stream(
		t.Context(),
		[]inference.Candidate{{Provider: "fixture"}},
		"question",
		nil,
		func(e Event) error {
			if e.Type == "tool_call" {
				return context.Canceled
			}
			return nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
