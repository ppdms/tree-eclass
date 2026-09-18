package messages

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"tree-eclass/internal/domain/identity"
)

type snowflake int64

func (s *snowflake) UnmarshalJSON(raw []byte) error {
	text := strings.Trim(string(raw), "\"")
	if text == "null" {
		*s = 0
		return nil
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return errors.New("invalid positive Discord snowflake")
	}
	*s = snowflake(value)
	return nil
}

type exportHeader struct {
	Guild struct {
		ID   snowflake
		Name string
	}
	Channel struct {
		ID                snowflake
		Name, Type, Topic string
		CategoryID        snowflake
	}
	Exported string
}
type rawMessage struct {
	ID                       snowflake
	Timestamp, Content, Type string
	IsPinned                 bool
	Author                   struct {
		ID             snowflake
		Nickname, Name string
	}
	Reference   struct{ MessageID snowflake }
	Attachments []struct {
		ID            snowflake
		FileName      string
		FileSizeBytes int64
		URL           string
	}
	Embeds []struct {
		Title, Description, URL string
		Fields                  []struct{ Name, Value string }
	}
	Reactions        []struct{ Count int64 }
	ForwardedMessage *rawMessage
}
type stagedMessage struct {
	ID                                 int64
	Timestamp                          string
	Epoch                              float64
	AuthorKey, Author, Content, Search string
	Reply                              *int64
	Type                               string
	Pinned, Reactions                  int64
	Attachments                        string
}

func (s *exportStream) header() (exportHeader, error) {
	var h exportHeader
	for key, target := range map[string]any{"guild": &h.Guild, "channel": &h.Channel, "exportedAt": &h.Exported} {
		if err := json.Unmarshal(s.fields[key], target); err != nil {
			return h, fmt.Errorf("invalid Discord %s metadata", key)
		}
	}
	if h.Guild.ID <= 0 || h.Channel.ID <= 0 {
		return h, errors.New("Discord export requires guild and channel IDs")
	}
	stamp, err := time.Parse(time.RFC3339Nano, h.Exported)
	if err != nil {
		return h, errors.New("invalid Discord export timestamp")
	}
	h.Exported = stamp.UTC().Format(time.RFC3339Nano)
	if len(h.Channel.Name) > 4096 || len(h.Channel.Topic) > 32768 || len(h.Guild.Name) > 4096 {
		return h, errors.New("Discord channel metadata exceeds limits")
	}
	return h, nil
}
func normalizeMessage(raw []byte) (stagedMessage, error) {
	var m rawMessage
	var out stagedMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return out, err
	}
	if m.ID <= 0 {
		return out, errors.New("Discord message has no ID")
	}
	stamp, err := time.Parse(time.RFC3339Nano, m.Timestamp)
	if err != nil {
		return out, errors.New("invalid Discord message timestamp")
	}
	content, err := messageText(m, 0)
	if err != nil {
		return out, err
	}
	attachments, err := attachmentMetadata(m)
	if err != nil {
		return out, err
	}
	author, err := authorName(m)
	if err != nil {
		return out, err
	}
	out = stagedMessage{
		ID:          int64(m.ID),
		Timestamp:   stamp.UTC().Format(time.RFC3339Nano),
		Epoch:       float64(stamp.Unix()) + float64(stamp.Nanosecond())/1e9,
		Author:      identity.Encode(author),
		Content:     identity.Encode(content),
		Search:      identity.Encode(identity.Search(content)),
		Type:        m.Type,
		Attachments: attachments,
	}
	return finalizeMessage(m, out)
}

func attachmentMetadata(m rawMessage) (string, error) {
	attachments := make([]map[string]any, 0, len(m.Attachments))
	for _, a := range m.Attachments {
		if a.FileSizeBytes < 0 {
			return "", errors.New("negative Discord attachment size")
		}
		attachments = append(
			attachments,
			map[string]any{
				"id":            fmt.Sprint(a.ID),
				"fileName":      a.FileName,
				"fileSizeBytes": a.FileSizeBytes,
				"url":           a.URL,
			},
		)
	}
	encoded, err := json.Marshal(identity.EncodeJSON(attachments))
	if err != nil {
		return "", err
	}
	if len(encoded) > 65536 {
		return "", errors.New("Discord attachments exceed 64 KiB")
	}
	return string(encoded), nil
}

func authorName(m rawMessage) (string, error) {
	author := m.Author.Nickname
	if author == "" {
		author = m.Author.Name
	}
	if author == "" {
		author = "Unknown user"
	}
	if len(author) > 4096 {
		return "", errors.New("Discord author name exceeds limit")
	}
	return author, nil
}

func finalizeMessage(m rawMessage, out stagedMessage) (stagedMessage, error) {
	if m.Author.ID > 0 {
		out.AuthorKey = fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprint(m.Author.ID))))[:20]
	}
	if m.Reference.MessageID > 0 {
		v := int64(m.Reference.MessageID)
		out.Reply = &v
	}
	if m.IsPinned {
		out.Pinned = 1
	}
	if out.Type == "" {
		out.Type = "Default"
	}
	for _, r := range m.Reactions {
		if r.Count < 0 {
			continue
		}
		if r.Count > 1e9 || out.Reactions > 1e9-r.Count {
			return out, errors.New("Discord reaction count exceeds limit")
		}
		out.Reactions += r.Count
	}
	return out, nil
}
func messageText(m rawMessage, depth int) (string, error) {
	if depth > 4 || len(m.Attachments) > 100 || len(m.Embeds) > 100 {
		return "", errors.New("Discord message expansion exceeds limits")
	}
	parts := []string{strings.TrimSpace(m.Content)}
	for _, a := range m.Attachments {
		if a.FileName != "" {
			parts = append(parts, "[Attachment: "+a.FileName+"]")
		}
	}
	for _, e := range m.Embeds {
		parts = append(parts, e.Title, e.Description, e.URL)
		for _, f := range e.Fields {
			parts = append(parts, f.Name, f.Value)
		}
	}
	if m.ForwardedMessage != nil {
		content, err := messageText(*m.ForwardedMessage, depth+1)
		if err != nil {
			return "", err
		}
		if content != "" {
			parts = append(parts, "[Forwarded message] "+content)
		}
	}
	nonempty := parts[:0]
	for _, p := range parts {
		if p != "" {
			nonempty = append(nonempty, p)
		}
	}
	text := strings.TrimSpace(strings.Join(nonempty, "\n"))
	if utf8.RuneCountInString(text) > 40000 {
		return "", errors.New("Discord message text exceeds 40000 characters")
	}
	return text, nil
}
