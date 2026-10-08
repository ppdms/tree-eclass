package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type postgresChat struct{ db nativeDBTX }

func (r postgresChat) ListConversations(ctx context.Context, limit int) ([]database.ChatSummary, error) {
	rows, err := r.db.Query(ctx, `SELECT c.id,c.title,c.created_at,c.updated_at,`+
		`(SELECT count(*) FROM app.chat_messages m WHERE m.conversation_id=c.id),`+
		`coalesce((SELECT substr(m.content,1,140) FROM app.chat_messages m `+
		`WHERE m.conversation_id=c.id AND m.role='user' ORDER BY m.id DESC LIMIT 1),'') `+
		`FROM app.chat_conversations c ORDER BY c.updated_at DESC,`+
		`coalesce((SELECT max(m.id) FROM app.chat_messages m WHERE m.conversation_id=c.id),0) DESC,`+
		`c.id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []database.ChatSummary{}
	for rows.Next() {
		var item database.ChatSummary
		if err := rows.Scan(&item.ID, &item.Title, &item.CreatedAt, &item.UpdatedAt,
			&item.MessageCount, &item.Excerpt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r postgresChat) GetConversation(ctx context.Context, id int64) (database.ChatConversation, error) {
	var result database.ChatConversation
	err := r.db.QueryRow(ctx,
		`SELECT id,title,created_at,updated_at FROM app.chat_conversations WHERE id=$1`, id).
		Scan(&result.ID, &result.Title, &result.CreatedAt, &result.UpdatedAt)
	return result, err
}

func (r postgresChat) ConversationPage(ctx context.Context, afterID int64, limit int) ([]database.ChatConversation,
	error) {
	rows, err := r.db.Query(ctx,
		`SELECT id,title,created_at,updated_at FROM app.chat_conversations WHERE id>$1 ORDER BY id LIMIT $2`,
		afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []database.ChatConversation
	for rows.Next() {
		var item database.ChatConversation
		if err := rows.Scan(&item.ID, &item.Title, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func scanChatMessage(row nativeRows) (database.ChatMessage, error) {
	var item database.ChatMessage
	err := row.Scan(&item.ID, &item.ConversationID, &item.Role, &item.Content,
		&item.ConsultedJSON, &item.Model, &item.CreatedAt)
	return item, err
}

func (r postgresChat) ListMessages(ctx context.Context, conversationID int64) ([]database.ChatMessage, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id,conversation_id,role,content,consulted_json,model,created_at `+
			`FROM app.chat_messages WHERE conversation_id=$1 ORDER BY id`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []database.ChatMessage{}
	for rows.Next() {
		item, err := scanChatMessage(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r postgresChat) StreamMessages(ctx context.Context, id int64) (database.Iterator[database.ChatMessage],
	error) {
	rows, err := r.db.Query(ctx,
		`SELECT id,conversation_id,role,content,consulted_json,model,created_at `+
			`FROM app.chat_messages WHERE conversation_id=$1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, scanChatMessage), nil
}

func (r postgresChat) HistoryMessages(ctx context.Context, conversationID int64) ([]database.ChatHistoryMessage,
	error) {
	rows, err := r.db.Query(ctx,
		`WITH recent AS (SELECT id,role,content FROM app.chat_messages WHERE conversation_id=$1 `+
			`ORDER BY id DESC LIMIT 20), bounded AS (SELECT *,`+
			`sum(length(content)) OVER(ORDER BY id DESC) characters FROM recent) `+
			`SELECT role,content FROM bounded WHERE characters<=400000 ORDER BY id`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []database.ChatHistoryMessage{}
	for rows.Next() {
		var item database.ChatHistoryMessage
		if err := rows.Scan(&item.Role, &item.Content); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r postgresChat) DeleteConversation(ctx context.Context, id int64) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM app.chat_conversations WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return database.ErrNoRows
	}
	return nil
}

func (r postgresChat) RenameConversation(ctx context.Context, id int64, titleEncoded string) error {
	tag, err := r.db.Exec(ctx, `UPDATE app.chat_conversations SET title=$2 WHERE id=$1`, id, titleEncoded)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return database.ErrNoRows
	}
	return nil
}

func (r postgresChat) CreateConversation(ctx context.Context, titleEncoded string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO app.chat_conversations(title) VALUES($1) RETURNING id`, titleEncoded).Scan(&id)
	return id, err
}

func (r postgresChat) LockConversation(ctx context.Context, id int64) (int64, error) {
	var found int64
	err := r.db.QueryRow(ctx,
		`SELECT id FROM app.chat_conversations WHERE id=$1 FOR UPDATE`, id).Scan(&found)
	return found, err
}

func (r postgresChat) InsertTurnMessages(ctx context.Context, params database.ChatTurnMessages) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO app.chat_messages(conversation_id,role,content,consulted_json,model) `+
			`VALUES($1,'user',$2,NULL,NULL),($1,'assistant',$3,$4,$5)`,
		params.ConversationID, params.UserContent, params.AssistantContent, params.Consulted, params.Model)
	return err
}

func (r postgresChat) TouchConversation(ctx context.Context, id int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE app.chat_conversations SET updated_at=`+
			`to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.MS') WHERE id=$1`, id)
	return err
}
