package chat

import (
	"context"
	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
)

// History bounds database transfer before allocating a provider context.
func (s Store) History(ctx context.Context, id int64) ([]Message, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.chat_conversations WHERE id=$1`, id).Scan(&found); err != nil {
		return nil, err
	}
	rows, err := tx.Query(
		ctx,
		`WITH recent AS (SELECT id,role,content FROM app.chat_messages WHERE conversation_id=$1 ORDER BY id DESC LIMIT 20), bounded AS (SELECT *,sum(length(content)) OVER(ORDER BY id DESC) characters FROM recent) SELECT role,content FROM bounded WHERE characters<=400000 ORDER BY id`,
		id,
	)
	if err != nil {
		return nil, err
	}
	result := []Message{}
	for rows.Next() {
		var m Message
		if err = rows.Scan(&m.Role, &m.Content); err != nil {
			rows.Close()
			return nil, err
		}
		m.Content = identity.Decode(m.Content)
		result = append(result, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}
