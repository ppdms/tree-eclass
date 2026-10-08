package messages

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"tree-eclass/internal/domain/database"
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

func buildConversations(
	ctx context.Context, tx database.Tx, source Archive, h exportHeader, path string,
) (int64, error) {
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

func collectStageBatch(ctx context.Context, tx database.Tx, last int64) ([]stagedMessage, error) {
	stream, err := tx.DiscordImports().StagedMessages(ctx, last, 32)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	var out []stagedMessage
	for stream.Next() {
		out = append(out, fromStagedMessage(stream.Value()))
	}
	return out, stream.Err()
}

func conversationText(
	ctx context.Context,
	tx database.Tx,
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
			line, err := replyContextLine(ctx, tx, source, h, ids, parents, *m.Reply)
			if err != nil {
				return "", err
			}
			lines = append(lines, line...)
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

func replyContextLine(
	ctx context.Context,
	tx database.Tx,
	source Archive,
	h exportHeader,
	ids, parents map[int64]bool,
	reply int64,
) ([]string, error) {
	if ids[reply] || parents[reply] {
		return nil, nil
	}
	content, err := tx.DiscordImports().ReplyContext(ctx, reply, source.Course, int64(h.Guild.ID))
	if database.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	runes := []rune(identity.Decode(content))
	parents[reply] = true
	return []string{fmt.Sprintf(
		"[Reply context from message %d] %s",
		reply,
		string(runes[:min(2000, len(runes))]),
	)}, nil
}

func publishConversation(
	ctx context.Context,
	tx database.Tx,
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
	metadata, err := conversationMetadata(h)
	if err != nil {
		return err
	}
	messageIDs := make([]int64, len(group))
	for i, m := range group {
		messageIDs[i] = m.ID
	}
	return tx.DiscordImports().PublishConversation(ctx, database.DiscordConversation{
		ID:                  id,
		CourseID:            s.Course,
		RootID:              s.Root,
		ChannelID:           s.Channel,
		ChannelName:         identity.Encode(h.Channel.Name),
		ChannelType:         h.Channel.Type,
		FirstMessageID:      first.ID,
		LastMessageID:       last.ID,
		StartedAt:           first.Timestamp,
		EndedAt:             last.Timestamp,
		EndedAtEpoch:        last.Epoch,
		Text:                identity.Encode(text),
		NormalizedText:      identity.Encode(identity.Search(text)),
		ParticipantCount:    int64(len(participants)),
		ReactionCount:       reactions,
		Pinned:              pinned,
		MetadataJSON:        string(metadata),
		SourcePath:          path,
		MessageIDs:          messageIDs,
		EmbeddingModel:      knowledge.LocalEmbeddingModel,
		EmbeddingVector:     knowledge.Pack(knowledge.Embed(text)),
		EmbeddingDimensions: knowledge.EmbeddingDimensions,
	})
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
