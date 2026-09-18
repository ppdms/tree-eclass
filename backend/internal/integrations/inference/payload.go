package inference

import (
	"encoding/json"
	"strings"
)

func wirePayload(c Candidate, in Request) (map[string]any, error) {
	messages := make([]Message, 0, len(in.Messages))
	for _, m := range in.Messages {
		content, err := imageContent(m)
		if err != nil {
			return nil, err
		}
		m.Content = content
		messages = append(messages, m)
	}
	p := map[string]any{"model": c.Model, "messages": messages, "stream": true}
	if in.JSON {
		p["response_format"] = map[string]string{"type": "json_object"}
	}
	if len(in.Tools) > 0 {
		p["tools"] = in.Tools
	}
	if in.Schema != nil {
		p["response_format"] = map[string]any{
			"type":        "json_schema",
			"json_schema": map[string]any{"name": "source_analysis", "strict": true, "schema": in.Schema},
		}
	}
	if c.Provider == "ollama" {
		if err := ollamaPayload(in, p); err != nil {
			return nil, err
		}
	}
	if c.Think != nil {
		thinkPayload(c, p)
	}
	if err := analysisThinking(c, p); err != nil {
		return nil, err
	}
	return p, nil
}

func ollamaPayload(in Request, p map[string]any) error {
	messages := []map[string]any{}
	for _, m := range in.Messages {
		row, err := ollamaMessage(m)
		if err != nil {
			return err
		}
		messages = append(messages, row)
	}
	p["messages"] = messages
	delete(p, "response_format")
	if in.JSON {
		p["format"] = "json"
	}
	if in.Schema != nil {
		p["format"] = in.Schema
	}
	return nil
}

func ollamaMessage(m Message) (map[string]any, error) {
	row := map[string]any{"role": m.Role, "content": m.Content}
	if len(m.Images) > 0 {
		images := []string{}
		for _, img := range m.Images {
			images = append(images, img.Data)
		}
		row["images"] = images
	}
	if m.Thinking != "" {
		row["thinking"] = m.Thinking
	}
	if m.ToolName != "" {
		row["tool_name"] = m.ToolName
	}
	calls := []map[string]any{}
	for _, call := range m.Calls {
		var arguments map[string]any
		decoder := json.NewDecoder(strings.NewReader(call.Function.Arguments))
		decoder.UseNumber()
		if err := decoder.Decode(&arguments); err != nil {
			return nil, err
		}
		calls = append(
			calls,
			map[string]any{"function": map[string]any{"name": call.Function.Name, "arguments": arguments}},
		)
	}
	if len(calls) > 0 {
		row["tool_calls"] = calls
	}
	return row, nil
}

func thinkPayload(c Candidate, p map[string]any) {
	switch c.Provider {
	case "ollama", "opencode-go":
		p["think"] = *c.Think
	case "alibaba":
		p["enable_thinking"] = *c.Think
		if *c.Think {
			p["reasoning_effort"] = "high"
			if c.Model == "qwen3.8-flash" {
				p["reasoning_effort"], p["tool_stream"] = "xhigh", true
			}
		}
	case "zai":
		kind := "disabled"
		if *c.Think {
			kind = "enabled"
		}
		p["thinking"] = map[string]string{"type": kind}
	}
}
