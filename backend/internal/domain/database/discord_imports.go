package database

import (
	"context"
	"time"
)

// DiscordStagedMessage is one normalized export message held in the
// transaction-scoped staging area. All text stays encoded exactly as the
// domain produced it; adapters persist it verbatim. ReplyTo is nil when the
// message is not a reply. Pinned is 0 or 1.
type DiscordStagedMessage struct {
	ID          int64
	Timestamp   string
	Epoch       float64
	AuthorKey   string
	AuthorName  string
	Content     string
	SearchText  string
	ReplyTo     *int64
	MessageType string
	Pinned      int64
	Reactions   int64
	Attachments string
}

// DiscordArchiveIdentity identifies one immutable published archive: its
// derived source path, owning course and content digest.
type DiscordArchiveIdentity struct {
	Path     string
	CourseID int64
	SHA256   string
}

// DiscordArchivePublication persists one staged archive's derived rows.
// RootID/ChannelID/CourseID scope the archive; GuildID names the exporter
// guild; the remaining fields carry the already-encoded header metadata.
// ParentChannelID is nil when the export has no category.
type DiscordArchivePublication struct {
	Path            string
	RootID          int64
	ChannelID       int64
	CourseID        int64
	GuildID         int64
	ChannelName     string
	ChannelType     string
	ChannelTopic    string
	ParentChannelID *int64
	ExportedAt      string
	ObjectID        string
}

// DiscordConversation publishes one grouped conversation with its derived
// index rows. Text fields stay encoded exactly as the domain produced them.
type DiscordConversation struct {
	ID               string
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
	// MessageIDs lists staged message ids in position order.
	MessageIDs []int64
	// EmbeddingModel/Vector/Dimensions persist the local conversation
	// embedding; Vector holds packed little-endian float32 samples.
	EmbeddingModel      string
	EmbeddingVector     []byte
	EmbeddingDimensions int64
}

// DiscordMediaEntry pairs one archive-relative media path with its immutable
// stored object. RelativePath stays encoded exactly as stored.
type DiscordMediaEntry struct {
	RelativePath string
	Object       ObjectReference
}

// DiscordMediaItem pairs one archive-relative media path (encoded exactly as
// stored) with its content digest for inventory comparison.
type DiscordMediaItem struct {
	RelativePath string
	ObjectID     string
}

// DiscordMappedSource is the oldest archive whose owning course no longer
// matches the current channel mapping, with its durable raw object.
type DiscordMappedSource struct {
	Path      string
	RootID    int64
	ChannelID int64
	CourseID  int64
	Object    ObjectReference
}

// DiscordExportCursor advances one channel cursor after a complete interval.
type DiscordExportCursor struct {
	RootID    int64
	ChannelID int64
	AfterID   int64
	NextAt    time.Time
}

// DiscordImports is the typed port for Discord archive staging, publication,
// interval cursors, reindex discovery and attachment reads. Staging methods
// require a transaction-bound handle: the staging area is scoped to the
// caller's transaction and vanishes on commit/rollback. Publication methods
// on the same handle commit atomically with the import transaction.
type DiscordImports interface {
	// BeginStage resets the transaction-scoped staging area.
	BeginStage(ctx context.Context) error
	// AppendStagedMessages persists normalized messages into the staging
	// area in id order. Batches stream; the caller never loads the whole
	// export into memory at once.
	AppendStagedMessages(ctx context.Context, messages []DiscordStagedMessage) error
	// StagedMessages streams staged messages with ids above afterID in id
	// order, bounded by limit.
	StagedMessages(ctx context.Context, afterID int64, limit int) (Iterator[DiscordStagedMessage], error)
	// ReplyContext resolves the encoded content of one parent message,
	// preferring the staged copy, else the newest ready mapped row for the
	// course and guild. It reports ErrNoRows when no candidate exists.
	ReplyContext(ctx context.Context, messageID, courseID, guildID int64) (string, error)
	// StagedOverlapsPublished reports whether any staged message id is
	// already published under one of the given source paths.
	StagedOverlapsPublished(ctx context.Context, paths []string) (bool, error)
	// LockDiscordMapping serializes the import against channel-mapping
	// changes using the shared extended settings namespace. It requires a
	// transaction-bound handle.
	LockDiscordMapping(ctx context.Context) error
	// MappedCourseForRoot returns the currently mapped course for one root
	// channel id. It reports ErrNoRows when the root is unmapped.
	MappedCourseForRoot(ctx context.Context, rootID string) (int64, error)
	// ArchiveSourceCourse returns the owning course of one archive source
	// path. It reports ErrNoRows when the path is unknown.
	ArchiveSourceCourse(ctx context.Context, path string) (int64, error)
	// ArchiveReady reports whether the archive is already published ready
	// with the same course, fingerprint and object identity.
	ArchiveReady(ctx context.Context, identity DiscordArchiveIdentity) (bool, error)
	// ArchiveConversationCount counts published conversations for one
	// source path.
	ArchiveConversationCount(ctx context.Context, path string) (int64, error)
	// PublishArchive persists one staged archive: it discards prior derived
	// rows for the path, records the source, upserts the channel and copies
	// staged messages ordered by id into the published table.
	PublishArchive(ctx context.Context, publication DiscordArchivePublication) error
	// PublishConversation persists one conversation with its member rows,
	// full-text row and local embedding row.
	PublishConversation(ctx context.Context, conversation DiscordConversation) error
	// RegisterArchiveMedia persists one media inventory row for a source
	// path. Callers register the object via the Objects port on the same
	// transaction first.
	RegisterArchiveMedia(ctx context.Context, sourcePath, relativePath, objectID string) error
	// ArchiveMedia lists the stored objects for one source path, ordered by
	// relative path, bounded by limit.
	ArchiveMedia(ctx context.Context, sourcePath string, limit int) ([]DiscordMediaEntry, error)
	// MediaInventory lists (relative path, object id) pairs for one source
	// path, bounded by limit, for immutable-inventory comparison.
	MediaInventory(ctx context.Context, sourcePath string, limit int) ([]DiscordMediaItem, error)
	// ReadyMediaObject resolves the servable object for one content digest
	// when it backs a ready mapped archive of a visible course. It reports
	// ErrNoRows when the digest is unknown or not currently servable.
	ReadyMediaObject(ctx context.Context, id string) (ObjectReference, error)
	// ExportCursorForUpdate locks one export cursor row and returns its
	// after id. It reports ErrNoRows when no cursor exists.
	ExportCursorForUpdate(ctx context.Context, rootID, channelID int64) (int64, error)
	// AdvanceExportCursor upserts one cursor after a complete interval and
	// clears any recorded error.
	AdvanceExportCursor(ctx context.Context, cursor DiscordExportCursor) error
	// MismappedSource returns the oldest archive whose course differs from
	// the current channel mapping. It reports ErrNoRows when none exists.
	MismappedSource(ctx context.Context) (DiscordMappedSource, error)
}
