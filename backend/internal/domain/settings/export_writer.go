package settings

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type exportWriter struct {
	out io.Writer
	err error
	ctx context.Context
	tx  rdbms.Tx
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

// decodeExportRow converts one scanned export row to its JSON shape. Columns
// arrive via columnNames so the query stays portable: the legacy
// SELECT to_jsonb(export_row) wrapper has no sqlite form and fails at prepare
// time. Numbers surface as json.Number and booleans as bool so the export
// text matches the old to_jsonb output on both backends.
func decodeExportRow(names []string, raw []any) (map[string]any, error) {
	row := make(map[string]any, len(names))
	for i, name := range names {
		value, err := exportValue(name, raw[i])
		if err != nil {
			return nil, err
		}
		row[name] = value
	}
	for key, value := range row {
		if text, ok := value.(string); ok {
			row[key] = identity.Decode(text)
		}
	}
	for field, name := range map[string]string{"rects_json": "rects", "tags_json": "tags", "consulted_json": "consulted"} {
		if value, exists := row[field]; exists {
			var decoded any = []any{}
			if text, ok := value.(string); ok && text != "" {
				if json.Unmarshal([]byte(text), &decoded) != nil {
					decoded = []any{}
				}
			}
			delete(row, field)
			row[name] = decoded
		}
	}
	return row, nil
}

// exportValue renders one scanned column the way to_jsonb did: NULL stays
// nil, integers/floats become json.Number, booleans stay bool, and everything
// else stays text for identity decoding by the caller.
func exportValue(name string, value any) (any, error) {
	// The exam-plan enabled flag is a boolean expression: pgx scans it as
	// bool, modernc as int64 0/1. Normalize both to bool so the export
	// text matches on both backends.
	if name == "enabled" {
		switch v := value.(type) {
		case nil:
			return nil, nil
		case bool:
			return v, nil
		case int64:
			return v != 0, nil
		case int32:
			return v != 0, nil
		case int:
			return v != 0, nil
		case []byte:
			text := string(v)
			return text == "t" || text == "true" || text == "1", nil
		case string:
			return v == "t" || v == "true" || v == "1", nil
		default:
			return nil, fmt.Errorf("settings: unexpected %s type %T", name, value)
		}
	}
	switch v := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case int64:
		return json.Number(strconv.FormatInt(v, 10)), nil
	case int32:
		return json.Number(strconv.FormatInt(int64(v), 10)), nil
	case int:
		return json.Number(strconv.Itoa(v)), nil
	case float64:
		return json.Number(strconv.FormatFloat(v, 'g', -1, 64)), nil
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	default:
		return nil, fmt.Errorf("settings: unexpected %s type %T", name, value)
	}
}

func (w *exportWriter) table(query string, columns []string, args ...any) {
	w.rows(query, columns, args...)
}

func (w *exportWriter) rows(query string, columns []string, args ...any) {
	if w.err != nil {
		return
	}
	rows, err := w.tx.Query(w.ctx, query, args...)
	if err != nil {
		w.err = err
		return
	}
	defer rows.Close()
	w.text("[")
	separator := ""
	for w.err == nil && rows.Next() {
		raw := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range raw {
			pointers[i] = &raw[i]
		}
		if w.err = rows.Scan(pointers...); w.err != nil {
			return
		}
		row, err := decodeExportRow(columns, raw)
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
		w.err = rows.Err()
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
	// Fetch a bounded page, then close its cursor before reading messages on
	// the same snapshot connection. Message bodies stream one row at a time.
	rows, err := w.tx.Query(
		w.ctx,
		`SELECT id,title,created_at,updated_at FROM app.chat_conversations WHERE id>$1 ORDER BY id LIMIT 64`,
		last,
	)
	if err != nil {
		w.err = err
		return nil, last, false
	}
	var items []exportConversation
	for rows.Next() {
		var c exportConversation
		if err := rows.Scan(&c.ID, &c.Title, &c.Created, &c.Updated); err != nil {
			rows.Close()
			w.err = err
			return nil, last, false
		}
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		w.err = err
		rows.Close()
		return nil, last, false
	}
	rows.Close()
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
	w.rows(
		`SELECT id,conversation_id,role,content,consulted_json,model,created_at FROM app.chat_messages WHERE conversation_id=$1 ORDER BY id`,
		[]string{"id", "conversation_id", "role", "content", "consulted_json", "model", "created_at"},
		item.ID,
	)
	w.text("}")
}
