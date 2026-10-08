package rdbms

import (
	"context"

	"tree-eclass/internal/domain/database"
)

type sqliteChat struct{ db nativeDBTX }

func (r sqliteChat) ListConversations(ctx context.Context, limit int) ([]database.ChatSummary, error) {
	rows, err := r.db.Query(ctx, `SELECT c.id,c.title,c.created_at,c.updated_at,`+
		`(SELECT count(*) FROM chat_messages m WHERE m.conversation_id=c.id),`+
		`coalesce((SELECT substr(m.content,1,140) FROM chat_messages m `+
		`WHERE m.conversation_id=c.id AND m.role='user' ORDER BY m.id DESC LIMIT 1),'') `+
		`FROM chat_conversations c ORDER BY c.updated_at DESC,`+
		`coalesce((SELECT max(m.id) FROM chat_messages m WHERE m.conversation_id=c.id),0) DESC,`+
		`c.id DESC LIMIT ?`, limit)
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

func (r sqliteChat) GetConversation(ctx context.Context, id int64) (database.ChatConversation, error) {
	var result database.ChatConversation
	err := r.db.QueryRow(ctx,
		`SELECT id,title,created_at,updated_at FROM chat_conversations WHERE id=?`, id).
		Scan(&result.ID, &result.Title, &result.CreatedAt, &result.UpdatedAt)
	return result, err
}

func (r sqliteChat) ConversationPage(ctx context.Context, afterID int64, limit int) ([]database.ChatConversation,
	error) {
	rows, err := r.db.Query(ctx,
		`SELECT id,title,created_at,updated_at FROM chat_conversations WHERE id>? ORDER BY id LIMIT ?`,
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

func (r sqliteChat) ListMessages(ctx context.Context, conversationID int64) ([]database.ChatMessage, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id,conversation_id,role,content,consulted_json,model,created_at `+
			`FROM chat_messages WHERE conversation_id=? ORDER BY id`, conversationID)
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

func (r sqliteChat) StreamMessages(ctx context.Context, id int64) (database.Iterator[database.ChatMessage],
	error) {
	rows, err := r.db.Query(ctx,
		`SELECT id,conversation_id,role,content,consulted_json,model,created_at `+
			`FROM chat_messages WHERE conversation_id=? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, scanChatMessage), nil
}

func (r sqliteChat) HistoryMessages(ctx context.Context, conversationID int64) ([]database.ChatHistoryMessage, error) {
	rows, err := r.db.Query(ctx,
		`WITH recent AS (SELECT id,role,content FROM chat_messages WHERE conversation_id=? `+
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

func (r sqliteChat) DeleteConversation(ctx context.Context, id int64) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM chat_conversations WHERE id=?`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return database.ErrNoRows
	}
	return nil
}

func (r sqliteChat) RenameConversation(ctx context.Context, id int64, titleEncoded string) error {
	tag, err := r.db.Exec(ctx, `UPDATE chat_conversations SET title=? WHERE id=?`, titleEncoded, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return database.ErrNoRows
	}
	return nil
}

func (r sqliteChat) CreateConversation(ctx context.Context, titleEncoded string) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx,
		`INSERT INTO chat_conversations(title) VALUES(?) RETURNING id`, titleEncoded).Scan(&id)
	return id, err
}

func (r sqliteChat) LockConversation(ctx context.Context, id int64) (int64, error) {
	// SQLite has no row locks; the caller holds a writer transaction, so the
	// existence check plus writer admission is the exclusion guarantee.
	if _, err := advisoryLock(ctx, r.db, "chat", true, false); err != nil {
		return 0, err
	}
	var found int64
	err := r.db.QueryRow(ctx, `SELECT id FROM chat_conversations WHERE id=?`, id).Scan(&found)
	return found, err
}

func (r sqliteChat) InsertTurnMessages(ctx context.Context, params database.ChatTurnMessages) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO chat_messages(conversation_id,role,content,consulted_json,model) `+
			`VALUES(?,'user',?,NULL,NULL),(?,'assistant',?,?,?)`,
		params.ConversationID, params.UserContent,
		params.ConversationID, params.AssistantContent, params.Consulted, params.Model)
	return err
}

func (r sqliteChat) TouchConversation(ctx context.Context, id int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE chat_conversations SET updated_at=strftime('%Y-%m-%d %H:%M:%f','now') WHERE id=?`, id)
	return err
}
