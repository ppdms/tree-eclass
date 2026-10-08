package messages

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"tree-eclass/internal/domain/database"
)

// Snapshot binds saved synthesis to the exact mapped conversation and message
// records. Conversation and message rows load as typed values and the
// fingerprint hash assembles in Go from one canonical rendering used by both
// backends: jsonbArray(convJSON, fingerprint, membersHex) where convJSON is
// the ordered-struct conversation JSON below and each member hashes
// jsonbArray(message_id, source_path, position, msgJSON).
func Snapshot(ctx context.Context, tx database.Tx, course int64, id string) (string, error) {
	stored, err := tx.Community().SnapshotConversation(ctx, course, id)
	if err != nil {
		return "", err
	}
	conv := conversationHashRow{
		conversationID:   stored.ConversationID,
		courseID:         stored.CourseID,
		rootID:           stored.RootID,
		channelID:        stored.ChannelID,
		channelName:      stored.ChannelName,
		channelType:      stored.ChannelType,
		firstMessageID:   stored.FirstMessageID,
		lastMessageID:    stored.LastMessageID,
		startedAt:        stored.StartedAt,
		endedAt:          stored.EndedAt,
		endedAtEpoch:     stored.EndedAtEpoch,
		text:             stored.Text,
		normalizedText:   stored.NormalizedText,
		participantCount: stored.ParticipantCount,
		reactionCount:    stored.ReactionCount,
		isPinned:         stored.Pinned,
		metadataJSON:     stored.MetadataJSON,
		sourcePath:       stored.SourcePath,
	}
	members, err := tx.Community().SnapshotMembers(ctx, conv.conversationID)
	if err != nil {
		return "", err
	}
	hashed := []string{}
	for _, row := range members {
		link := messageLink{messageID: row.LinkMessageID, sourcePath: row.LinkSource, position: row.LinkPosition}
		msg := messageHashRow{
			messageID:      row.MessageID,
			channelID:      row.ChannelID,
			courseID:       row.CourseID,
			timestamp:      row.Timestamp,
			timestampEpoch: row.TimestampEp,
			authorKey:      row.AuthorKey,
			authorName:     row.AuthorName,
			content:        row.Content,
			searchableText: row.SearchText,
			replyTo:        row.ReplyTo,
			messageType:    row.MessageType,
			isPinned:       row.Pinned,
			reactionCount:  row.Reactions,
			attachments:    row.Attachments,
			sourcePath:     row.SourcePath,
		}
		member, err := hashMember(link, msg)
		if err != nil {
			return "", err
		}
		hashed = append(hashed, member)
	}
	convJSON, err := json.Marshal(conv.ordered())
	if err != nil {
		return "", err
	}
	joined := ""
	for i, member := range hashed {
		if i > 0 {
			joined += ","
		}
		joined += member
	}
	return hashTopLevel(string(convJSON), stored.Fingerprint, joined), nil
}

// conversationHashRow holds one conversation row for hashing. Numbers stay
// int64/float64 so the JSON rendering matches the old jsonb numbers;
// nullable text uses *string so NULL renders null as before.
type conversationHashRow struct {
	conversationID                  string
	courseID, rootID                int64
	channelID                       int64
	channelName, channelType        string
	firstMessageID, lastMessageID   int64
	startedAt, endedAt              string
	endedAtEpoch                    float64
	text, normalizedText            string
	participantCount, reactionCount int64
	isPinned                        int64
	metadataJSON, sourcePath        string
}

// ordered renders the conversation as JSON with keys in physical column
// order, the layout the old to_jsonb(c) produced. json.RawMessage embeds the
// stored metadata_json verbatim (it holds JSON text, never a plain string).
func (row conversationHashRow) ordered() *snapshotConversationJSON {
	return &snapshotConversationJSON{
		ConversationID:   row.conversationID,
		CourseID:         row.courseID,
		RootID:           row.rootID,
		ChannelID:        row.channelID,
		ChannelName:      row.channelName,
		ChannelType:      row.channelType,
		FirstMessageID:   row.firstMessageID,
		LastMessageID:    row.lastMessageID,
		StartedAt:        row.startedAt,
		EndedAt:          row.endedAt,
		EndedAtEpoch:     row.endedAtEpoch,
		Text:             row.text,
		NormalizedText:   row.normalizedText,
		ParticipantCount: row.participantCount,
		ReactionCount:    row.reactionCount,
		IsPinned:         row.isPinned,
		MetadataJSON:     json.RawMessage(row.metadataJSON),
		SourcePath:       row.sourcePath,
	}
}

// snapshotConversationJSON is the ordered-struct rendering of one
// conversation row for hashing. Field order is the physical column order so
// the bytes match what to_jsonb(c) emitted; encoding/json preserves struct
// order (unlike maps, which sort keys).
type snapshotConversationJSON struct {
	ConversationID   string          `json:"conversation_id"`
	CourseID         int64           `json:"course_id"`
	RootID           int64           `json:"root_id"`
	ChannelID        int64           `json:"channel_id"`
	ChannelName      string          `json:"channel_name"`
	ChannelType      string          `json:"channel_type"`
	FirstMessageID   int64           `json:"first_message_id"`
	LastMessageID    int64           `json:"last_message_id"`
	StartedAt        string          `json:"started_at"`
	EndedAt          string          `json:"ended_at"`
	EndedAtEpoch     float64         `json:"ended_at_epoch"`
	Text             string          `json:"text"`
	NormalizedText   string          `json:"normalized_text"`
	ParticipantCount int64           `json:"participant_count"`
	ReactionCount    int64           `json:"reaction_count"`
	IsPinned         int64           `json:"is_pinned"`
	MetadataJSON     json.RawMessage `json:"metadata_json"`
	SourcePath       string          `json:"source_path"`
}

// messageLink holds one conversation_messages row: the join keys plus the
// member position inside the conversation.
type messageLink struct {
	messageID  int64
	sourcePath string
	position   int64
}

// messageHashRow holds one messages row for hashing; messageID nil marks the
// LEFT JOIN miss (no message row for this conversation member).
type messageHashRow struct {
	messageID      *int64
	channelID      *int64
	courseID       *int64
	timestamp      *string
	timestampEpoch *float64
	authorKey      *string
	authorName     *string
	content        *string
	searchableText *string
	replyTo        *int64
	messageType    *string
	isPinned       *int64
	reactionCount  *int64
	attachments    *string
	sourcePath     *string
}

// ordered renders one message row as JSON with keys in physical column order,
// the layout the old to_jsonb(m) produced. Text JSON columns (attachments)
// embed verbatim; a missing LEFT JOIN row renders JSON null (never {}).
func (row messageHashRow) ordered() any {
	if row.messageID == nil {
		return nil
	}
	return &snapshotMessageJSON{
		MessageID:      row.messageID,
		ChannelID:      row.channelID,
		CourseID:       row.courseID,
		Timestamp:      row.timestamp,
		TimestampEpoch: row.timestampEpoch,
		AuthorKey:      row.authorKey,
		AuthorName:     row.authorName,
		Content:        row.content,
		SearchableText: row.searchableText,
		ReplyTo:        row.replyTo,
		MessageType:    row.messageType,
		IsPinned:       row.isPinned,
		ReactionCount:  row.reactionCount,
		Attachments:    json.RawMessage(derefOrEmpty(row.attachments)),
		SourcePath:     row.sourcePath,
	}
}

// snapshotMessageJSON is the ordered-struct rendering of one message row for
// hashing. Pointer fields render NULL as null, matching to_jsonb(m); the
// attachments text column holds JSON and embeds verbatim.
type snapshotMessageJSON struct {
	MessageID      *int64          `json:"message_id"`
	ChannelID      *int64          `json:"channel_id"`
	CourseID       *int64          `json:"course_id"`
	Timestamp      *string         `json:"timestamp"`
	TimestampEpoch *float64        `json:"timestamp_epoch"`
	AuthorKey      *string         `json:"author_key"`
	AuthorName     *string         `json:"author_name"`
	Content        *string         `json:"content"`
	SearchableText *string         `json:"searchable_text"`
	ReplyTo        *int64          `json:"reply_to_message_id"`
	MessageType    *string         `json:"message_type"`
	IsPinned       *int64          `json:"is_pinned"`
	ReactionCount  *int64          `json:"reaction_count"`
	Attachments    json.RawMessage `json:"attachment_metadata_json"`
	SourcePath     *string         `json:"source_path"`
}

func derefOrEmpty(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

// hashMember renders one conversation member the way the old inner
// jsonb_build_array(cm.message_id, cm.source_path, cm.position, to_jsonb(m))
// did: numbers unquoted, text as JSON strings, the row object in column
// order, elements joined with ", ". The member hash is sha256 hex of that
// array text, matching the old string_agg input per row.
func hashMember(link messageLink, msg messageHashRow) (string, error) {
	msgJSON, err := json.Marshal(msg.ordered())
	if err != nil {
		return "", err
	}
	array := "[" + strconv.FormatInt(link.messageID, 10) + ", " + quoteJSON(link.sourcePath) +
		", " + strconv.FormatInt(link.position, 10) + ", " + string(msgJSON) + "]"
	sum := sha256.Sum256([]byte(array))
	return hex.EncodeToString(sum[:]), nil
}

// hashTopLevel renders the outer jsonb_build_array(conversation, fingerprint,
// membersHex) and returns its sha256 hex. membersHex is the comma-joined
// member hashes (no spaces, like string_agg(...,',')); a conversation with
// no members hashes NULL the way the empty string_agg did.
func hashTopLevel(convJSON, fingerprint, members string) string {
	var membersJSON string
	if members == "" {
		membersJSON = "null"
	} else {
		membersJSON = quoteJSON(members)
	}
	array := "[" + convJSON + ", " + quoteJSON(fingerprint) + ", " + membersJSON + "]"
	sum := sha256.Sum256([]byte(array))
	return hex.EncodeToString(sum[:])
}

// quoteJSON renders text with postgres jsonb string escaping (short \b \f
// \n \r \t forms, \u00xx otherwise, raw UTF-8 passthrough, no HTML
// escaping), matching the sqlite jsonbEncode shim byte-for-byte.
func quoteJSON(text string) string {
	raw, err := json.Marshal(text)
	if err != nil {
		panic(fmt.Sprintf("snapshot: cannot quote %q: %v", text, err))
	}
	return string(raw)
}
