package chat

import "fmt"

type UIMessage struct {
	ID    string           `json:"id"`
	Role  string           `json:"role"`
	Parts []map[string]any `json:"parts"`
}

func InitialMessages(conversation Conversation) []UIMessage {
	result := make([]UIMessage, 0, len(conversation.Messages))
	for index, message := range conversation.Messages {
		parts := []map[string]any{}
		for position, item := range message.Consulted {
			tool := item.Tool
			if tool == "" {
				tool = "unknown"
			}
			arguments := item.Arguments
			if arguments == nil {
				arguments = map[string]any{}
			}
			parts = append(
				parts,
				map[string]any{
					"type":       "tool-" + tool,
					"toolCallId": fmt.Sprintf("stored-%d-%d", index, position),
					"toolName":   tool,
					"state":      "output-available",
					"input":      arguments,
					"output":     map[string]bool{"failed": item.Failed},
				},
			)
		}
		if message.Role == "assistant" && message.Model != nil && *message.Model != "" {
			parts = append(
				parts,
				map[string]any{"type": "data-model", "data": map[string]string{"model": *message.Model}},
			)
		}
		parts = append(parts, map[string]any{"type": "text", "text": message.Content})
		result = append(result, UIMessage{ID: fmt.Sprintf("stored-%d", index), Role: message.Role, Parts: parts})
	}
	return result
}
