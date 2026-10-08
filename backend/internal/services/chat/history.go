package chat

import (
	"context"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

// History bounds database transfer before allocating a provider context.
func (s Store) History(ctx context.Context, id int64) ([]Message, error) {
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Chat().GetConversation(ctx, id); err != nil {
		return nil, err
	}
	rows, err := tx.Chat().HistoryMessages(ctx, id)
	if err != nil {
		return nil, err
	}
	result := []Message{}
	for _, row := range rows {
		result = append(result, Message{Role: row.Role, Content: identity.Decode(row.Content)})
	}
	return result, tx.Commit(ctx)
}
