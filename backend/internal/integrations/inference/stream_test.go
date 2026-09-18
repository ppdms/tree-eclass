package inference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func frame(value any) string {
	raw, _ := json.Marshal(value)
	return "data: " + string(raw) + "\n\n"
}
func event(delta any, finish any) string {
	return frame(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}})
}

func TestStreamedToolAssembly(t *testing.T) {
	first := event(
		map[string]any{
			"content":           "Αναζήτηση",
			"reasoning_content": "internal reasoning",
			"tool_calls": []any{
				map[string]any{
					"index":    1,
					"id":       "second",
					"function": map[string]any{"name": "read", "arguments": "{"},
				},
				map[string]any{
					"index":    0,
					"id":       "first",
					"function": map[string]any{"name": "list_", "arguments": "{"},
				},
			},
		},
		nil,
	)
	second := event(
		map[string]any{
			"tool_calls": []any{
				map[string]any{"index": 0, "function": map[string]any{"name": "courses", "arguments": "}"}},
				map[string]any{"index": 1, "function": map[string]any{"arguments": `"document_id":"ένα"}`}},
			},
		},
		"tool_calls",
	)
	stream := ": keepalive\n\n" + first + second + frame(
		map[string]any{"choices": []any{}, "usage": map[string]int{"total_tokens": 10}},
	) + "data: [DONE]\n\n"
	got := []Delta{}
	if err := streamSSE(strings.NewReader(stream), func(d Delta) error { got = append(got, d); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Text != "Αναζήτηση" || got[0].Thinking != "internal reasoning" ||
		got[1].Calls[0].Function.Name != "list_courses" ||
		got[1].Calls[1].Function.Arguments != `{"document_id":"ένα"}` {
		t.Fatal("wire fragments lost identity/order/text", got)
	}
}

func TestIncompleteAndMalformedStreams(t *testing.T) {
	for name, raw := range map[string]string{
		"no terminator":     event(map[string]any{"content": "partial"}, "stop"),
		"no finish":         event(map[string]any{"content": "partial"}, nil) + "data: [DONE]\n\n",
		"length limit":      event(map[string]any{"content": "partial"}, "length") + "data: [DONE]\n\n",
		"malformed JSON":    "data: {broken}\n\n",
		"provider error":    frame(map[string]any{"error": map[string]string{"message": "sensitive response"}}),
		"missing calls":     event(map[string]any{}, "tool_calls") + "data: [DONE]\n\n",
		"invalid arguments": event(map[string]any{"tool_calls": []any{map[string]any{"index": 0, "function": map[string]any{"name": "tool", "arguments": "[]"}}}}, "tool_calls") + "data: [DONE]\n\n",
		"too many tools":    event(map[string]any{"tool_calls": []any{map[string]any{"index": 32}}}, "tool_calls") + "data: [DONE]\n\n",
		"overlong frame":    "data: " + strings.Repeat(" ", 1024*1024+1),
	} {
		t.Run(name, func(t *testing.T) {
			err := streamSSE(strings.NewReader(raw), func(d Delta) error {
				if d.Finish != "" {
					t.Fatal("invalid stream claimed completion")
				}
				return nil
			})
			if err == nil || strings.Contains(err.Error(), "sensitive response") {
				t.Fatal("invalid provider frame", err)
			}
		})
	}
}

func TestOllamaNativeTools(t *testing.T) {
	raw := `{"message":{"content":"Greek: δένδρα","thinking":"reason","tool_calls":[{"function":{"name":"list_courses","arguments":{}}}]},"done":false}` + "\n" + `{"message":{"content":""},"done":true,"done_reason":"stop"}` + "\n"
	var got []Delta
	if err := streamOllama(strings.NewReader(raw), func(d Delta) error { got = append(got, d); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Calls[0].ID == "" || got[1].Calls[0].Function.Arguments != "{}" {
		t.Fatal(got)
	}
	for _, raw := range []string{`{"done":false}`, `{"done":true,"done_reason":"length"}`, `{"error":"private error"}`} {
		if err := streamOllama(strings.NewReader(raw), func(Delta) error { return nil }); err == nil {
			t.Fatal("incomplete Ollama stream accepted", raw)
		}
	}
}

func TestHTTPTransportIsolationAndCancellation(t *testing.T) {
	client := New()
	defer client.Close()
	in := Request{Messages: []Message{{Role: "user", Content: "test"}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-secret" {
			t.Error("credential missing")
		}
		switch r.URL.Path {
		case "/quota":
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
			fmt.Fprint(w, "synthetic-secret private source")
		case "/redirect":
			http.Redirect(w, r, "/must-not-follow", 302)
		case "/wait":
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			t.Error("redirect leaked request")
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	candidate := Candidate{Provider: "synthetic", Model: "syn:test", APIKey: "synthetic-secret"}
	for _, path := range []string{"/quota", "/redirect"} {
		candidate.Endpoint = server.URL + path
		err := client.Stream(t.Context(), candidate, in, func(Delta) error { return nil })
		var failure *Error
		if !errors.As(err, &failure) || strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatal("transport leaked provider response", err)
		}
		if path == "/quota" && (!failure.Retryable || failure.RetryAfter != 2*time.Minute) {
			t.Fatal(failure)
		}
	}
	candidate.Endpoint = server.URL + "/wait"
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := client.Stream(ctx, candidate, in, func(Delta) error { return nil }); err == nil ||
		time.Since(start) > time.Second {
		t.Fatal("abandoned stream did not cancel", err)
	}
}

func TestNativePayloadAdaptsToolIdentity(t *testing.T) {
	think := true
	in := Request{
		Messages: []Message{
			{
				Role:     "assistant",
				Content:  "",
				Thinking: "reason",
				Calls: []ToolCall{
					{ID: "one", Type: "function", Function: Function{Name: "read", Arguments: `{"id":"δ"}`}},
				},
			},
			{Role: "tool", Content: "result", ToolID: "one", ToolName: "read"},
		},
	}
	p, err := wirePayload(Candidate{Provider: "ollama", Model: "model", Think: &think}, in)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(p)
	if strings.Contains(string(raw), "tool_call_id") || strings.Contains(string(raw), "reasoning_content") ||
		!strings.Contains(string(raw), `"tool_name":"read"`) ||
		!strings.Contains(string(raw), `"arguments":{"id":"δ"}`) {
		t.Fatal(string(raw))
	}
	p, err = wirePayload(Candidate{Provider: "opencode-go", Think: &think}, in)
	if err != nil || p["think"] != true || p["temperature"] != nil {
		t.Fatal(p, err)
	}
}
