package database

import "context"

// ChatConversation is one stored conversation header. Title stays
// identity-encoded exactly as stored; callers decode via identity.Decode.
// CreatedAt and UpdatedAt are schema UTC text timestamps.
type ChatConversation struct {
	ID        int64
	Title     string
	CreatedAt string
	UpdatedAt string
}

// ChatMessage is one stored message. Content and ConsultedJSON stay encoded
// exactly as stored; callers decode via identity.Decode. ConsultedJSON is nil
// for user rows (SQL NULL); Model is nil for user rows (SQL NULL).
type ChatMessage struct {
	ID             int64
	ConversationID int64
	Role           string
	Content        string
	ConsultedJSON  *string
	Model          *string
	CreatedAt      string
}

// ChatSummary is one conversation list row. Title and Excerpt stay encoded;
// Excerpt is the latest user-message prefix (up to 140 characters) or "".
type ChatSummary struct {
	ID           int64
	Title        string
	CreatedAt    string
	UpdatedAt    string
	MessageCount int64
	Excerpt      string
}

// ChatHistoryMessage is one provider-context row. Content stays encoded.
type ChatHistoryMessage struct {
	Role    string
	Content string
}

// ChatTurnMessages inserts one complete user+assistant pair. All content
// fields are already identity-encoded; Consulted holds encoded JSON for the
// assistant row; Model is stored verbatim ("" stays "").
type ChatTurnMessages struct {
	ConversationID   int64
	UserContent      string
	AssistantContent string
	Consulted        string
	Model            string
}

// Chat is the typed conversation port. Readers use the caller's snapshot;
// writers bind to the caller's transaction with no implicit commits.
type Chat interface {
	// ListConversations returns up to limit summaries ordered by updated_at
	// DESC, max message id DESC, id DESC. The caller clamps limit to 1..200.
	ListConversations(ctx context.Context, limit int) ([]ChatSummary, error)
	// GetConversation returns one header or ErrNoRows.
	GetConversation(ctx context.Context, id int64) (ChatConversation, error)
	// ConversationPage returns headers with id>afterID ordered by id ascending,
	// bounded by limit. The settings export pages through this method.
	ConversationPage(ctx context.Context, afterID int64, limit int) ([]ChatConversation, error)
	// ListMessages returns every message for a conversation ordered by id
	// ascending. It returns an empty slice when the conversation has no rows.
	// Chat history views use this bounded read; the learner export streams.
	ListMessages(ctx context.Context, conversationID int64) ([]ChatMessage, error)
	// StreamMessages streams every message for a conversation ordered by id
	// ascending on the caller's transaction snapshot. The caller closes the
	// iterator and checks Err after Next returns false.
	StreamMessages(ctx context.Context, conversationID int64) (Iterator[ChatMessage], error)
	// HistoryMessages returns the bounded provider context: at most the latest
	// 20 messages whose cumulative content length stays within 400000
	// characters, ordered by id ascending. It does not check existence.
	HistoryMessages(ctx context.Context, conversationID int64) ([]ChatHistoryMessage, error)
	// DeleteConversation removes one conversation (messages cascade) or
	// returns ErrNoRows when the id is unknown.
	DeleteConversation(ctx context.Context, id int64) error
	// RenameConversation replaces one encoded title or returns ErrNoRows.
	RenameConversation(ctx context.Context, id int64, titleEncoded string) error
	// CreateConversation inserts one conversation with an encoded title and
	// returns its id.
	CreateConversation(ctx context.Context, titleEncoded string) (int64, error)
	// LockConversation returns the id after taking the backend write guard:
	// a row lock on PostgreSQL, writer-transaction exclusion on SQLite (the
	// caller must already hold a writer transaction). ErrNoRows when missing.
	LockConversation(ctx context.Context, id int64) (int64, error)
	// InsertTurnMessages appends the user row (NULL consulted/model) and the
	// assistant row in one statement. The caller holds the conversation guard
	// in the same transaction so failed turns leave no half rows.
	InsertTurnMessages(ctx context.Context, params ChatTurnMessages) error
	// TouchConversation refreshes updated_at from the backend UTC clock in
	// "YYYY-MM-DD HH24:MI:SS.MS" text. The caller holds the guard, so a
	// vanished row only reflects a rolled-back transaction.
	TouchConversation(ctx context.Context, id int64) error
}
