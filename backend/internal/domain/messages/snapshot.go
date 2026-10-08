package messages

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"tree-eclass/internal/infrastructure/rdbms"
)

// Snapshot binds saved synthesis to the exact mapped conversation and message
// records. Conversation and message rows load as explicit columns and the
// fingerprint hash assembles in Go so the query stays portable: to_jsonb(row)
// has no sqlite form and fails at prepare time.
//
// Hash-stability decision: the hash input CANNOT stay byte-identical to the
// old postgres expression. The old input embedded to_jsonb(c)/to_jsonb(m) row
// objects (keys in physical column order, postgres jsonb spacing) inside
// jsonb_build_array text; a Go map marshal would sort keys differently and a
// faithful replica would need ordered-struct rendering of all 18 conversation
// plus 15 message columns, including driver-dependent number formatting.
// Instead Snapshot hashing restarts on one canonical Go rendering used by
// both backends going forward: jsonbArray(convJSON, fingerprint, membersHex)
// where convJSON is the ordered-struct conversation JSON below and each
// member hashes jsonbArray(message_id, source_path, position, msgJSON).
// Fingerprints computed before this change differ one final time; afterwards
// both drivers agree byte-for-byte because the same Go code renders both.
// Stored content_hash values recompute once (next synthesis validation
// reports cached_community_evidence_stale until then); no live hash in the
// test fixtures is asserted byte-equal across the change.
// snapshotConversationQuery loads one conversation row plus the archive
// fingerprint guarding it, with an explicit column list hashed in Go.
const snapshotConversationQuery = `SELECT ` + conversationHashColumns + ` FROM messages.conversations c
 JOIN app.courses co ON co.id=c.course_id
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=CAST(c.root_id AS TEXT) AND mapping.course_id=c.course_id
 JOIN messages.archive_sources a ON a.path=c.source_path AND a.course_id=c.course_id AND a.root_id=CAST(c.root_id AS TEXT) AND a.status='ready'
 WHERE c.course_id=$1 AND c.conversation_id=$2 AND (co.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p WHERE p.course_id=co.id AND p.enabled=1))`

// snapshotMembersQuery loads the conversation members in hash order with
// their message rows for Go-side hashing.
const snapshotMembersQuery = `SELECT cm.message_id,cm.source_path,cm.position,` + messageHashColumns + ` FROM messages.conversation_messages cm LEFT JOIN messages.messages m ON m.message_id=cm.message_id AND m.source_path=cm.source_path WHERE cm.conversation_id=$1 ORDER BY cm.position,cm.message_id,cm.source_path`

func Snapshot(ctx context.Context, tx rdbms.Tx, course int64, id string) (string, error) {
	var conv conversationHashRow
	var fingerprint string
	err := tx.QueryRow(ctx, snapshotConversationQuery, course, id).
		Scan(
			&conv.conversationID, &conv.courseID, &conv.rootID, &conv.channelID,
			&conv.channelName, &conv.channelType, &conv.firstMessageID, &conv.lastMessageID,
			&conv.startedAt, &conv.endedAt, &conv.endedAtEpoch, &conv.text,
			&conv.normalizedText, &conv.participantCount, &conv.reactionCount,
			&conv.isPinned, &conv.metadataJSON, &conv.sourcePath, &fingerprint,
		)
	if err != nil {
		return "", err
	}
	rows, err := tx.Query(ctx, snapshotMembersQuery, conv.conversationID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	members := []string{}
	for rows.Next() {
		var link messageLink
		var msg messageHashRow
		hasMsg, err := scanSnapshotMember(rows, &link, &msg)
		if err != nil {
			return "", err
		}
		if hasMsg == nil {
			msg.messageID = nil
		}
		member, err := hashMember(link, msg)
		if err != nil {
			return "", err
		}
		members = append(members, member)
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	convJSON, err := json.Marshal(conv.ordered())
	if err != nil {
		return "", err
	}
	joined := ""
	for i, member := range members {
		if i > 0 {
			joined += ","
		}
		joined += member
	}
	return hashTopLevel(string(convJSON), fingerprint, joined), nil
}

// scanSnapshotMember scans one members row into its link and message parts.
// hasMsg carries the leading probe column: nil means the LEFT JOIN missed
// and the message renders JSON null. Callers clear msg.messageID on a miss.
func scanSnapshotMember(rows rdbms.Rows, link *messageLink, msg *messageHashRow) (hasMsg *string, err error) {
	err = rows.Scan(
		&link.messageID, &link.sourcePath, &link.position,
		&hasMsg, &msg.messageID, &msg.channelID, &msg.courseID,
		&msg.timestamp, &msg.timestampEpoch, &msg.authorKey, &msg.authorName,
		&msg.content, &msg.searchableText, &msg.replyTo, &msg.messageType,
		&msg.isPinned, &msg.reactionCount, &msg.attachments, &msg.sourcePath,
	)
	return hasMsg, err
}

// conversationHashColumns lists messages.conversations in physical column
// order plus the archive fingerprint, matching the old to_jsonb(c) key order.
const conversationHashColumns = `c.conversation_id,c.course_id,c.root_id,c.channel_id,c.channel_name,c.channel_type,c.first_message_id,c.last_message_id,c.started_at,c.ended_at,c.ended_at_epoch,c.text,c.normalized_text,c.participant_count,c.reaction_count,c.is_pinned,c.metadata_json,c.source_path,a.fingerprint`

// messageHashColumns lists messages.messages in physical column order. The
// leading m.message_id probe detects the LEFT JOIN miss (all message columns
// NULL) without collapsing a real row.
const messageHashColumns = `m.message_id,m.message_id,m.channel_id,m.course_id,m.timestamp,m.timestamp_epoch,m.author_key,m.author_name,m.content,m.searchable_text,m.reply_to_message_id,m.message_type,m.is_pinned,m.reaction_count,m.attachment_metadata_json,m.source_path`

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
