package inference

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestImageWireContractsAndExactOllamaIDs(t *testing.T) {
	request := Request{
		JSON:     true,
		Messages: []Message{{Role: "user", Content: "source", Images: []Image{{MIMEType: "image/jpeg", Data: "YQ=="}}}},
	}
	for _, provider := range []string{"synthetic", "ollama"} {
		result, err := wirePayload(Candidate{Provider: provider, Model: "fixture"}, request)
		if err != nil {
			t.Fatal(err)
		}
		if provider == "ollama" {
			m := result["messages"].([]map[string]any)[0]
			if m["content"] != "source" || m["images"].([]string)[0] != "YQ==" || result["format"] != "json" {
				t.Fatal(result)
			}
		} else {
			m := result["messages"].([]Message)[0]
			if len(m.Content.([]map[string]any)) != 2 {
				t.Fatal(result)
			}
		}
	}
	request.Messages = []Message{
		{
			Role:    "assistant",
			Content: "",
			Calls:   []ToolCall{{Function: Function{Name: "course", Arguments: `{"course_id":9007199254740993}`}}},
		},
	}
	result, err := wirePayload(Candidate{Provider: "ollama"}, request)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	if !strings.Contains(string(raw), "9007199254740993") {
		t.Fatal("tool identity rounded", string(raw))
	}
}
