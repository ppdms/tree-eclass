package settings

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

type exportWriter struct {
	out io.Writer
	err error
	ctx context.Context
	tx  pgx.Tx
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

func decodeExportRow(raw []byte) (map[string]any, error) {
	var row map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&row); err != nil {
		return nil, err
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

func (w *exportWriter) rows(query string, args ...any) {
	if w.err != nil {
		return
	}
	rows, err := w.tx.Query(w.ctx, "SELECT to_jsonb(export_row) FROM ("+query+") export_row", args...)
	if err != nil {
		w.err = err
		return
	}
	defer rows.Close()
	w.text("[")
	separator := ""
	for w.err == nil && rows.Next() {
		var raw []byte
		if w.err = rows.Scan(&raw); w.err != nil {
			return
		}
		row, err := decodeExportRow(raw)
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
		// Fetch a bounded page, then close its cursor before reading messages on
		// the same snapshot connection. Message bodies stream one row at a time.
		rows, err := w.tx.Query(
			w.ctx,
			`SELECT id,title,created_at,updated_at FROM app.chat_conversations WHERE id>$1 ORDER BY id LIMIT 64`,
			last,
		)
		if err != nil {
			w.err = err
			return
		}
		type conversation struct {
			ID      int64  `json:"id"`
			Title   string `json:"title"`
			Created string `json:"created_at"`
			Updated string `json:"updated_at"`
		}
		items, err := pgx.CollectRows(rows, pgx.RowToStructByPos[conversation])
		if err != nil {
			w.err = err
			return
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
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
				item.ID,
			)
			w.text("}")
			last = item.ID
			separator = ","
			if w.err != nil {
				return
			}
		}
	}
	w.text("]")
}
