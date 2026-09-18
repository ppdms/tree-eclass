// Package chat owns complete conversation turns and provider/tool streaming.
package chat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
)

type Store struct{ Pool *pgxpool.Pool }
type Consultation struct {
	Tool      string         `json:"tool"`
	Arguments map[string]any `json:"arguments"`
	Failed    bool           `json:"failed"`
}
type Message struct {
	ID          int64          `json:"id"`
	Role        string         `json:"role"`
	Content     string         `json:"content"`
	Consulted   []Consultation `json:"consulted"`
	Model       *string        `json:"model"`
	CreatedAt   string         `json:"created_at"`
	ContentHTML string         `json:"content_html,omitempty"`
}
type Conversation struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	CreatedAt string    `json:"created_at"`
	UpdatedAt string    `json:"updated_at"`
	Messages  []Message `json:"messages"`
}
type Summary struct {
	ID           int64  `json:"id"`
	Title        string `json:"title"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	MessageCount int64  `json:"message_count"`
	Excerpt      string `json:"last_message_excerpt"`
	DisplayTitle string `json:"display_title"`
}

func title(question string) string {
	text := strings.Join(strings.Fields(question), " ")
	if text == "" {
		return "New conversation"
	}
	if chars := []rune(text); len(chars) > 78 {
		return string(chars[:77]) + "…"
	}
	return text
}
func (s Store) List(ctx context.Context, limit int) ([]Summary, error) {
	rows, err := s.Pool.Query(
		ctx,
		`SELECT c.id,c.title,c.created_at,c.updated_at,(SELECT count(*) FROM app.chat_messages m WHERE m.conversation_id=c.id),coalesce((SELECT substr(m.content,1,140) FROM app.chat_messages m WHERE m.conversation_id=c.id AND m.role='user' ORDER BY m.id DESC LIMIT 1),'') FROM app.chat_conversations c ORDER BY c.updated_at DESC,coalesce((SELECT max(m.id) FROM app.chat_messages m WHERE m.conversation_id=c.id),0) DESC,c.id DESC LIMIT $1`,
		min(max(limit, 1), 200),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Summary{}
	counts := map[string]int{}
	for rows.Next() {
		var item Summary
		if err = rows.Scan(
			&item.ID,
			&item.Title,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.MessageCount,
			&item.Excerpt,
		); err != nil {
			return nil, err
		}
		item.Title, item.Excerpt = identity.Decode(item.Title), identity.Decode(item.Excerpt)
		item.DisplayTitle = strings.Join(strings.Fields(item.Title), " ")
		if item.DisplayTitle == "" {
			item.DisplayTitle = "New conversation"
		}
		counts[item.DisplayTitle]++
		result = append(result, item)
	}
	for i, item := range result {
		if counts[item.DisplayTitle] > 1 && item.Excerpt != "" {
			chars := []rune(item.Excerpt)
			result[i].DisplayTitle += " · " + string(chars[:min(64, len(chars))])
		}
	}
	return result, rows.Err()
}
func (s Store) Get(ctx context.Context, id int64) (Conversation, error) {
	result := Conversation{Messages: []Message{}}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT id,title,created_at,updated_at FROM app.chat_conversations WHERE id=$1`, id).
		Scan(&result.ID, &result.Title, &result.CreatedAt, &result.UpdatedAt)
	if err != nil {
		return result, err
	}
	result.Title = identity.Decode(result.Title)
	rows, err := tx.Query(
		ctx,
		`SELECT id,role,content,consulted_json,model,created_at FROM app.chat_messages WHERE conversation_id=$1 ORDER BY id`,
		id,
	)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var message Message
		var consulted *string
		if err = rows.Scan(
			&message.ID,
			&message.Role,
			&message.Content,
			&consulted,
			&message.Model,
			&message.CreatedAt,
		); err != nil {
			rows.Close()
			return result, err
		}
		message.Content = identity.Decode(message.Content)
		message.Consulted = []Consultation{}
		if consulted != nil {
			if err := json.Unmarshal([]byte(identity.Decode(*consulted)), &message.Consulted); err != nil {
				message.Consulted = []Consultation{}
			}
		}
		if message.Consulted == nil {
			message.Consulted = []Consultation{}
		}
		result.Messages = append(result.Messages, message)
	}
	if err = rows.Err(); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}
func (s Store) Delete(ctx context.Context, id int64) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM app.chat_conversations WHERE id=$1`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}
func (s Store) Rename(ctx context.Context, id int64, value string) error {
	clean := []rune(strings.Join(strings.Fields(value), " "))
	if len(clean) == 0 {
		return errors.New("a title is required")
	}
	tag, err := s.Pool.Exec(
		ctx,
		`UPDATE app.chat_conversations SET title=$2 WHERE id=$1`,
		id,
		identity.Encode(string(clean[:min(len(clean), 120)])),
	)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

type Turn struct {
	ConversationID          *int64
	Question, Answer, Model string
	Consulted               []Consultation
}
type Saved struct {
	ID      int64 `json:"id"`
	Started bool  `json:"started"`
}

func (s Store) SaveTurn(ctx context.Context, turn Turn) (Saved, error) {
	result := Saved{Started: turn.ConversationID == nil}
	if strings.TrimSpace(turn.Question) == "" || strings.TrimSpace(turn.Answer) == "" {
		return result, errors.New("a complete question and answer are required")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	if turn.ConversationID == nil {
		err = tx.QueryRow(ctx, `INSERT INTO app.chat_conversations(title) VALUES($1) RETURNING id`, identity.Encode(title(turn.Question))).
			Scan(&result.ID)
	} else {
		err = tx.QueryRow(ctx, `SELECT id FROM app.chat_conversations WHERE id=$1 FOR UPDATE`, *turn.ConversationID).Scan(&result.ID)
	}
	if err != nil {
		return result, err
	}
	consulted, err := json.Marshal(turn.Consulted)
	if err != nil {
		return result, err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO app.chat_messages(conversation_id,role,content,consulted_json,model) VALUES($1,'user',$2,NULL,NULL),($1,'assistant',$3,$4,$5)`,
		result.ID,
		identity.Encode(turn.Question),
		identity.Encode(turn.Answer),
		identity.Encode(string(consulted)),
		turn.Model,
	)
	if err != nil {
		return result, err
	}
	_, err = tx.Exec(
		ctx,
		`UPDATE app.chat_conversations SET updated_at=to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.MS') WHERE id=$1`,
		result.ID,
	)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}
