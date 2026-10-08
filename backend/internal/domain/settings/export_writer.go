package settings

import (
	"context"
	"encoding/json"
	"io"
	"strconv"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type exportWriter struct {
	out io.Writer
	err error
	ctx context.Context
	ops database.Operations
}

func (w *exportWriter) text(value string) {
	if w.err == nil {
		_, w.err = io.WriteString(w.out, value)
	}
}
func (w *exportWriter) value(value any) {
	if w.err == nil {
		w.err = json.NewEncoder(w.out).Encode(value)
	}
}

// decodeExportRow converts one normalized export row to its JSON shape.
// Strings arrive stored-encoded and are decoded here; the *_json columns
// become parsed arrays under their public names.
func decodeExportRow(row database.SettingsExportRow) (map[string]any, error) {
	out := make(map[string]any, len(row))
	for key, value := range row {
		if text, ok := value.(string); ok {
			out[key] = identity.Decode(text)
		} else {
			out[key] = value
		}
	}
	fields := map[string]string{"rects_json": "rects", "tags_json": "tags", "consulted_json": "consulted"}
	for field, name := range fields {
		if value, exists := out[field]; exists {
			var decoded any = []any{}
			if text, ok := value.(string); ok && text != "" {
				if json.Unmarshal([]byte(text), &decoded) != nil {
					decoded = []any{}
				}
			}
			delete(out, field)
			out[name] = decoded
		}
	}
	return out, nil
}

func (w *exportWriter) table(name string) {
	if w.err != nil {
		return
	}
	iterator, err := w.ops.Settings().ExportRows(w.ctx, name)
	if err != nil {
		w.err = err
		return
	}
	defer iterator.Close()
	w.text("[")
	separator := ""
	for w.err == nil && iterator.Next() {
		row, err := decodeExportRow(iterator.Value())
		if err != nil {
			w.err = err
			return
		}
		w.text(separator)
		w.value(row)
		separator = ","
		if w.err != nil {
			return
		}
	}
	if w.err == nil {
		w.err = iterator.Err()
	}
	w.text("]")
}

func (w *exportWriter) conversations() {
	w.text("[")
	separator := ""
	last := int64(0)
	for w.err == nil {
		items, next, done := w.conversationPage(last)
		if done || w.err != nil {
			break
		}
		last = next
		for _, item := range items {
			w.conversationEntry(item, separator)
			separator = ","
			if w.err != nil {
				return
			}
		}
	}
	w.text("]")
}

type exportConversation struct {
	ID      int64  `json:"id"`
	Title   string `json:"title"`
	Created string `json:"created_at"`
	Updated string `json:"updated_at"`
}

func (w *exportWriter) conversationPage(last int64) ([]exportConversation, int64, bool) {
	// Fetch a bounded page, then read messages on the same snapshot
	// connection. Message bodies stream one conversation at a time.
	headers, err := w.ops.Chat().ConversationPage(w.ctx, last, 64)
	if err != nil {
		w.err = err
		return nil, last, false
	}
	items := make([]exportConversation, 0, len(headers))
	for _, header := range headers {
		items = append(items, exportConversation{
			ID:      header.ID,
			Title:   header.Title,
			Created: header.CreatedAt,
			Updated: header.UpdatedAt,
		})
	}
	if len(items) == 0 {
		return nil, last, true
	}
	return items, items[len(items)-1].ID, false
}

func (w *exportWriter) conversationEntry(item exportConversation, separator string) {
	item.Title = identity.Decode(item.Title)
	data, err := json.Marshal(item)
	if err != nil {
		w.err = err
		return
	}
	w.text(separator)
	w.text(string(data[:len(data)-1]))
	w.text(`,"messages":`)
	stream, err := w.ops.Chat().StreamMessages(w.ctx, item.ID)
	if err != nil {
		w.err = err
		return
	}
	defer stream.Close()
	w.text("[")
	index := 0
	for w.err == nil && stream.Next() {
		message := stream.Value()
		row, err := decodeExportRow(database.SettingsExportRow{
			"id":              json.Number(jsonInt(message.ID)),
			"conversation_id": json.Number(jsonInt(message.ConversationID)),
			"role":            message.Role,
			"content":         message.Content,
			"consulted_json":  nullableString(message.ConsultedJSON),
			"model":           nullableString(message.Model),
			"created_at":      message.CreatedAt,
		})
		if err != nil {
			w.err = err
			return
		}
		if index > 0 {
			w.text(",")
		}
		w.value(row)
		index++
	}
	if w.err == nil {
		w.err = stream.Err()
	}
	w.text("]")
	w.text("}")
}

func jsonInt(value int64) string {
	return strconv.FormatInt(value, 10)
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
