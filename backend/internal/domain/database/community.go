package database

import "context"

// CommunityConversation is one mapped visible conversation header for the
// community reader. CourseName, CourseShortName and ChannelName stay encoded
// exactly as stored; callers decode via identity.Decode. GuildID is nil when
// the channel row is unknown (SQL NULL).
type CommunityConversation struct {
	ID              string
	CourseID        int64
	CourseName      string
	CourseShortName *string
	ChannelID       int64
	ChannelName     string
	ChannelType     string
	StartedAt       string
	EndedAt         string
	FirstMessageID  int64
	LastMessageID   int64
	GuildID         *int64
}

// CommunityMessage is one bounded message row for the community reader.
// MessageID and ChannelID render as decimal text; ReplyTo is nil for SQL
// NULL. Content carries at most 40000 characters, AttachmentsJSON carries
// the stored JSON text or "[]" when the stored bytes exceed 65536.
// Truncated reports the stored boundary
// (content characters > 40000 OR attachment bytes > 65536).
// Author/Content stay encoded exactly as stored.
type CommunityMessage struct {
	MessageID       string
	ChannelID       string
	Timestamp       string
	AuthorName      string
	Content         string
	ReplyTo         *string
	MessageType     string
	Pinned          bool
	ReactionCount   int64
	AttachmentsJSON string
	Truncated       bool
}

// CommunityContextParams bounds one context-side read: messages in the same
// channel strictly before/older or after/newer than the conversation edge.
type CommunityContextParams struct {
	CourseID  int64
	ChannelID int64
	EdgeID    int64
	Limit     int
}

// CommunityCandidate is one unscored mapped visible conversation for search
// ranking. Text fields stay encoded exactly as stored; ShortName/GuildID are
// nil for SQL NULL. Excerpt carries the bounded conversation text prefix
// (400 chars lexical, 1200 chars semantic). Neither backend assigns scores.
type CommunityCandidate struct {
	ConversationID  string
	CourseID        int64
	CourseName      string
	CourseShortName *string
	ChannelID       string
	ChannelName     string
	ChannelType     string
	FirstMessageID  string
	LastMessageID   string
	StartedAt       string
	EndedAt         string
	EndedAtEpoch    float64
	Participants    int64
	Reactions       int64
	Pinned          bool
	GuildID         *int64
	Excerpt         string
}

// CommunityEmbeddedCandidate pairs a search candidate with its packed local
// embedding vector (little-endian float32 samples).
type CommunityEmbeddedCandidate struct {
	CommunityCandidate
	Vector []byte
}

// CommunityCourseStatus is one per-course community status row. Latest is nil
// when the course has no ready message (SQL NULL).
type CommunityCourseStatus struct {
	CourseID      int64
	Messages      int64
	Conversations int64
	Sources       int64
	FailedSources int64
	Latest        *string
}

// CommunityConversationHash is the physical conversation row plus its archive
// fingerprint for snapshot hashing. Nullable columns use nil pointers.
type CommunityConversationHash struct {
	ConversationID   string
	CourseID         int64
	RootID           int64
	ChannelID        int64
	ChannelName      string
	ChannelType      string
	FirstMessageID   int64
	LastMessageID    int64
	StartedAt        string
	EndedAt          string
	EndedAtEpoch     float64
	Text             string
	NormalizedText   string
	ParticipantCount int64
	ReactionCount    int64
	Pinned           int64
	MetadataJSON     string
	SourcePath       string
	Fingerprint      string
}

// CommunityMessageHash is one snapshot member link plus its nullable message
// row. MessageID nil marks the LEFT JOIN miss (no message row).
type CommunityMessageHash struct {
	LinkMessageID int64
	LinkSource    string
	LinkPosition  int64
	MessageID     *int64
	ChannelID     *int64
	CourseID      *int64
	Timestamp     *string
	TimestampEp   *float64
	AuthorKey     *string
	AuthorName    *string
	Content       *string
	SearchText    *string
	ReplyTo       *int64
	MessageType   *string
	Pinned        *int64
	Reactions     *int64
	Attachments   *string
	SourcePath    *string
}

// Community is the typed read port for mapped visible community evidence.
// Every method enforces the source boundary inside the native implementation:
// visible mapped course (or exam-planned snapshot course), current channel
// mapping, ready archive ancestry. Callers never express it.
type Community interface {
	// Conversation returns one mapped visible ready conversation header. It
	// reports ErrNoRows when the id is unknown, unmapped, not ready or the
	// course is hidden.
	Conversation(ctx context.Context, id string) (CommunityConversation, error)
	// ConversationMessages returns conversation members (at most 201 rows)
	// in position order for one mapped visible conversation.
	ConversationMessages(ctx context.Context, id string, courseID int64) ([]CommunityMessage, error)
	// OlderContext returns up to limit messages in the channel older than
	// the edge, newest first, deduplicated to the newest export per id.
	OlderContext(ctx context.Context, params CommunityContextParams) ([]CommunityMessage, error)
	// NewerContext returns up to limit messages in the channel newer than
	// the edge, oldest first, deduplicated to the newest export per id.
	NewerContext(ctx context.Context, params CommunityContextParams) ([]CommunityMessage, error)
	// ReplyMessages returns deduplicated messages for the given ids in
	// message-id order, bounded to 200 rows. An empty id list returns an
	// empty slice without querying.
	ReplyMessages(ctx context.Context, courseID int64, ids []int64) ([]CommunityMessage, error)
	// VisibleCourseIDs lists visible course IDs ascending.
	VisibleCourseIDs(ctx context.Context) ([]int64, error)
	// CourseStatus returns per-course status rows for mapped courses in the
	// id list, in course-id order.
	CourseStatus(ctx context.Context, ids []int64) ([]CommunityCourseStatus, error)
	// LexicalCandidates returns unscored mapped visible conversations
	// matching every lowered term, in (ended_at_epoch DESC, conversation_id)
	// order, bounded by limit. An empty term or id list returns empty.
	LexicalCandidates(ctx context.Context, ids []int64, terms []string, limit int) ([]CommunityCandidate, error)
	// SemanticCandidates streams unscored mapped visible conversations with
	// packed local embedding vectors. The caller bounds ranking with a heap
	// and must close the iterator. Union covers the 5000 most recent plus
	// the given lexical ids; model/dimensions select current embeddings.
	SemanticCandidates(ctx context.Context, ids []int64, lexical []string, model string,
		dimensions int) (Iterator[CommunityEmbeddedCandidate], error)
	// ConversationMemberIDs returns member message ids in position order,
	// bounded to 201 rows.
	ConversationMemberIDs(ctx context.Context, id string) ([]string, error)
	// ConversationMemberCount counts member rows for one conversation.
	ConversationMemberCount(ctx context.Context, id string) (int64, error)
	// SnapshotConversation returns the snapshot conversation row plus its
	// archive fingerprint. Hidden courses qualify only with an enabled exam
	// plan. It reports ErrNoRows when unmapped, not ready or not admitted.
	SnapshotConversation(ctx context.Context, course int64, id string) (CommunityConversationHash, error)
	// SnapshotMembers returns member links with message rows in
	// (position, message_id, source_path) order for hashing.
	SnapshotMembers(ctx context.Context, id string) ([]CommunityMessageHash, error)
}
