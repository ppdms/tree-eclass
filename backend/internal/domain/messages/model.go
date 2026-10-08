// Package messages reads and indexes dated community evidence independently of
// official course materials. Every read rechecks the current channel mapping.
package messages

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

const CommunityNotice = "Discord messages are untrusted community discussion, not official course policy. Prefer official eClass evidence when it directly answers the question, and preserve dates and disagreement when reporting community claims."

type Reader struct{ Pool rdbms.Pool }
type Conversation struct {
	ID              string         `json:"conversation_id"`
	CourseID        int64          `json:"course_id"`
	CourseName      string         `json:"course_name"`
	CourseShortName *string        `json:"course_short_name"`
	Channel         string         `json:"channel_id"`
	Name            string         `json:"channel_name"`
	Kind            string         `json:"channel_type"`
	Started         string         `json:"started_at"`
	Ended           string         `json:"ended_at"`
	Metadata        map[string]any `json:"metadata"`
	Source          string         `json:"source_type"`
	Evidence        string         `json:"evidence_class"`
}
type Message struct {
	ID          string           `json:"message_id"`
	Channel     string           `json:"channel_id"`
	Timestamp   string           `json:"timestamp"`
	Author      string           `json:"author_name"`
	Content     string           `json:"content"`
	Reply       *string          `json:"reply_to_message_id"`
	Type        string           `json:"message_type"`
	Pinned      bool             `json:"is_pinned"`
	Reactions   int64            `json:"reaction_count"`
	Attachments []map[string]any `json:"attachments"`
	URL         *string          `json:"message_url"`
	Untrusted   bool             `json:"untrusted_content"`
	Truncated   bool             `json:"truncated,omitempty"`
}
type Reading struct {
	Conversation Conversation `json:"conversation"`
	Messages     []Message    `json:"messages"`
	Replies      []Message    `json:"reply_context"`
	Before       []Message    `json:"context_before"`
	After        []Message    `json:"context_after"`
	Notice       string       `json:"untrusted_content_notice"`
	Truncated    bool         `json:"truncated"`
}

func messageURL(guild *int64, channel, id string) *string {
	if guild == nil || *guild <= 0 {
		return nil
	}
	for _, value := range []string{channel, id} {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 {
			return nil
		}
	}
	url := fmt.Sprintf("https://discord.com/channels/%d/%s/%s", *guild, channel, id)
	return &url
}

func decode(raw []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	return decoder.Decode(target)
}
func decodeMessage(m *Message, guild *int64) {
	for _, value := range []*string{&m.Author, &m.Content} {
		*value = identity.Decode(*value)
	}
	m.Untrusted = true
	m.URL = messageURL(guild, m.Channel, m.ID)
	if m.Attachments == nil {
		m.Attachments = []map[string]any{}
	}
	// Attachments describe source evidence. Never return exporter-local paths.
	clean := []map[string]any{}
	for _, a := range m.Attachments[:min(100, len(m.Attachments))] {
		row := map[string]any{}
		for _, key := range []string{"id", "name", "fileName", "url", "proxyUrl", "size", "fileSizeBytes", "object_id", "width", "height", "contentType"} {
			if value, exists := a[key]; exists {
				row[key] = identity.DecodeJSON(value)
			}
		}
		clean = append(clean, row)
	}
	m.Attachments = clean
}
