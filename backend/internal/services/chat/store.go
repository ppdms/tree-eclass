// Package chat owns complete conversation turns and provider/tool streaming.
package chat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Store struct{ Pool database.Store }
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
	rows, err := s.Pool.Chat().ListConversations(ctx, min(max(limit, 1), 200))
	if err != nil {
		return nil, err
	}
	result := []Summary{}
	counts := map[string]int{}
	for _, row := range rows {
		item := Summary{
			ID:           row.ID,
			Title:        identity.Decode(row.Title),
			CreatedAt:    row.CreatedAt,
			UpdatedAt:    row.UpdatedAt,
			MessageCount: row.MessageCount,
			Excerpt:      identity.Decode(row.Excerpt),
		}
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
	return result, nil
}
func (s Store) Get(ctx context.Context, id int64) (Conversation, error) {
	result := Conversation{Messages: []Message{}}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	header, err := tx.Chat().GetConversation(ctx, id)
	if err != nil {
		return result, err
	}
	result.ID, result.CreatedAt, result.UpdatedAt = header.ID, header.CreatedAt, header.UpdatedAt
	result.Title = identity.Decode(header.Title)
	rows, err := tx.Chat().ListMessages(ctx, id)
	if err != nil {
		return result, err
	}
	for _, row := range rows {
		message := Message{ID: row.ID, Role: row.Role, Content: identity.Decode(row.Content)}
		message.Model, message.CreatedAt = row.Model, row.CreatedAt
		message.Consulted = []Consultation{}
		if row.ConsultedJSON != nil {
			if err := json.Unmarshal([]byte(identity.Decode(*row.ConsultedJSON)), &message.Consulted); err != nil {
				message.Consulted = []Consultation{}
			}
		}
		if message.Consulted == nil {
			message.Consulted = []Consultation{}
		}
		result.Messages = append(result.Messages, message)
	}
	return result, tx.Commit(ctx)
}
func (s Store) Delete(ctx context.Context, id int64) error {
	return s.Pool.Chat().DeleteConversation(ctx, id)
}
func (s Store) Rename(ctx context.Context, id int64, value string) error {
	clean := []rune(strings.Join(strings.Fields(value), " "))
	if len(clean) == 0 {
		return errors.New("a title is required")
	}
	return s.Pool.Chat().RenameConversation(ctx, id, identity.Encode(string(clean[:min(len(clean), 120)])))
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
	consulted, err := json.Marshal(turn.Consulted)
	if err != nil {
		return result, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	if turn.ConversationID == nil {
		result.ID, err = tx.Chat().CreateConversation(ctx, identity.Encode(title(turn.Question)))
	} else {
		result.ID, err = tx.Chat().LockConversation(ctx, *turn.ConversationID)
	}
	if err != nil {
		return result, err
	}
	err = tx.Chat().InsertTurnMessages(
		ctx,
		database.ChatTurnMessages{
			ConversationID:   result.ID,
			UserContent:      identity.Encode(turn.Question),
			AssistantContent: identity.Encode(turn.Answer),
			Consulted:        identity.Encode(string(consulted)),
			Model:            turn.Model,
		},
	)
	if err != nil {
		return result, err
	}
	if err = tx.Chat().TouchConversation(ctx, result.ID); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}
