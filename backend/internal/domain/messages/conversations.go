package messages

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
)

var gifOnly = regexp.MustCompile(`(?i)^https?://(?:www\.)?(?:tenor\.com|giphy\.com)/\S+$`)

func informative(content string) bool {
	content = strings.TrimSpace(identity.Decode(content))
	if content == "" || strings.EqualFold(content, "pinned a message.") || gifOnly.MatchString(content) {
		return false
	}
	return strings.ContainsFunc(content, func(r rune) bool { return unicode.IsLetter(r) || unicode.IsNumber(r) })
}
func buildConversations(ctx context.Context, tx pgx.Tx, source Archive, h exportHeader, path string) (int64, error) {
	var count, last int64
	group := []stagedMessage{}
	chars := 0
	publish := func() error {
		if len(group) == 0 {
			return nil
		}
		if err := publishConversation(ctx, tx, source, h, path, group); err != nil {
			return err
		}
		count++
		group = nil
		chars = 0
		return nil
	}
	for {
		batch, err := collectStageBatch(ctx, tx, last)
		if err != nil {
			return count, err
		}
		if len(batch) == 0 {
			break
		}
		for _, m := range batch {
			last = m.ID
			if !informative(m.Content) {
				continue
			}
			length := utf8.RuneCountInString(m.Content)
			if len(group) > 0 && (m.Epoch-group[len(group)-1].Epoch > 900 || len(group) >= 10 || chars+length > 3500) {
				if err = publish(); err != nil {
					return count, err
				}
			}
			group = append(group, m)
			chars += length
		}
	}
	err := publish()
	return count, err
}

func collectStageBatch(ctx context.Context, tx pgx.Tx, last int64) ([]stagedMessage, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT message_id,timestamp,timestamp_epoch,author_key,author_name,content,reply_to_message_id,is_pinned,reaction_count FROM tree_discord_stage WHERE message_id>$1 ORDER BY message_id LIMIT 32`,
		last,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (stagedMessage, error) {
		var m stagedMessage
		err := row.Scan(
			&m.ID,
			&m.Timestamp,
			&m.Epoch,
			&m.AuthorKey,
			&m.Author,
			&m.Content,
			&m.Reply,
			&m.Pinned,
			&m.Reactions,
		)
		return m, err
	})
}

func conversationText(
	ctx context.Context,
	tx pgx.Tx,
	source Archive,
	h exportHeader,
	group []stagedMessage,
) (string, error) {
	ids := map[int64]bool{}
	for _, m := range group {
		ids[m.ID] = true
	}
	parents := map[int64]bool{}
	lines := []string{}
	for _, m := range group {
		marker := ""
		if m.Reply != nil {
			marker = fmt.Sprintf(" reply-to=%d", *m.Reply)
			if !ids[*m.Reply] && !parents[*m.Reply] {
				var content string
				err := tx.QueryRow(ctx, `SELECT content FROM (
 SELECT content,0 priority,'' indexed_at FROM tree_discord_stage WHERE message_id=$1
 UNION ALL SELECT m.content,1,a.indexed_at FROM messages.messages m JOIN messages.archive_sources a ON a.path=m.source_path AND a.course_id=m.course_id AND a.status='ready'
 JOIN app.discord_course_channels mapping ON mapping.root_channel_id=a.root_id AND mapping.course_id=m.course_id
 JOIN messages.channels ch ON ch.channel_id=m.channel_id AND ch.course_id=m.course_id AND ch.guild_id=$3
 WHERE m.message_id=$1 AND m.course_id=$2) candidates ORDER BY priority,indexed_at DESC LIMIT 1`, *m.Reply, source.Course, int64(h.Guild.ID)).Scan(&content)
				if err != nil && err != pgx.ErrNoRows {
					return "", err
				}
				if err == nil {
					runes := []rune(identity.Decode(content))
					lines = append(
						lines,
						fmt.Sprintf(
							"[Reply context from message %d] %s",
							*m.Reply,
							string(runes[:min(2000, len(runes))]),
						),
					)
					parents[*m.Reply] = true
				}
			}
		}
		lines = append(
			lines,
			fmt.Sprintf(
				"[%s message=%d%s] %s: %s",
				m.Timestamp,
				m.ID,
				marker,
				identity.Decode(m.Author),
				identity.Decode(m.Content),
			),
		)
	}
	return strings.Join(lines, "\n"), nil
}

func publishConversation(
	ctx context.Context,
	tx pgx.Tx,
	s Archive,
	h exportHeader,
	path string,
	group []stagedMessage,
) error {
	text, err := conversationText(ctx, tx, s, h, group)
	if err != nil {
		return err
	}
	first, last := group[0], group[len(group)-1]
	id := identity.Stable("dconv", fmt.Sprint(s.Channel), path, fmt.Sprint(first.ID), fmt.Sprint(last.ID))
	participants, reactions, pinned := conversationStats(group)
	err = insertConversation(ctx, tx, s, h, path, id, text, first, last, participants, reactions, pinned)
	if err != nil {
		return err
	}
	return queueConversationRows(ctx, tx, id, path, text, h, group)
}

func conversationStats(group []stagedMessage) (map[string]bool, int64, int64) {
	participants := map[string]bool{}
	var reactions, pinned int64
	for _, m := range group {
		if m.AuthorKey != "" {
			participants[m.AuthorKey] = true
		}
		reactions += m.Reactions
		pinned = max(pinned, m.Pinned)
	}
	return participants, reactions, pinned
}

func conversationMetadata(h exportHeader) ([]byte, error) {
	return json.Marshal(
		identity.EncodeJSON(
			map[string]any{
				"guild_id":          fmt.Sprint(h.Guild.ID),
				"guild_name":        h.Guild.Name,
				"parent_channel_id": fmt.Sprint(h.Channel.CategoryID),
				"channel_topic":     h.Channel.Topic,
			},
		),
	)
}

func insertConversation(
	ctx context.Context,
	tx pgx.Tx,
	s Archive,
	h exportHeader,
	path, id, text string,
	first, last stagedMessage,
	participants map[string]bool,
	reactions, pinned int64,
) error {
	metadata, err := conversationMetadata(h)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO messages.conversations(conversation_id,course_id,root_id,channel_id,channel_name,channel_type,first_message_id,last_message_id,started_at,ended_at,ended_at_epoch,text,normalized_text,participant_count,reaction_count,is_pinned,metadata_json,source_path)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		id,
		s.Course,
		s.Root,
		s.Channel,
		identity.Encode(h.Channel.Name),
		h.Channel.Type,
		first.ID,
		last.ID,
		first.Timestamp,
		last.Timestamp,
		last.Epoch,
		identity.Encode(text),
		identity.Encode(identity.Search(text)),
		len(participants),
		reactions,
		pinned,
		string(metadata),
		path,
	)
	return err
}

func queueConversationRows(
	ctx context.Context,
	tx pgx.Tx,
	id, path, text string,
	h exportHeader,
	group []stagedMessage,
) error {
	batch := &pgx.Batch{}
	for position, m := range group {
		batch.Queue(
			`INSERT INTO messages.conversation_messages(conversation_id,message_id,source_path,position) VALUES($1,$2,$3,$4)`,
			id,
			m.ID,
			path,
			position,
		)
	}
	batch.Queue(
		`INSERT INTO messages.conversations_fts(conversation_id,text,normalized_text,channel_name) VALUES($1,$2,$3,$4)`,
		id,
		identity.Encode(text),
		identity.Encode(identity.Search(text)),
		identity.Encode(h.Channel.Name),
	)
	batch.Queue(
		`INSERT INTO messages.conversation_embeddings(conversation_id,model,vector,dimensions) VALUES($1,$2,$3,$4)`,
		id,
		knowledge.LocalEmbeddingModel,
		knowledge.Pack(knowledge.Embed(text)),
		knowledge.EmbeddingDimensions,
	)
	return tx.SendBatch(ctx, batch).Close()
}
