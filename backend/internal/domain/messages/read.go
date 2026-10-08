package messages

import (
	"context"
	"errors"
	"slices"
	"strconv"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

func (s Reader) Read(ctx context.Context, id string, before, after int) (Reading, error) {
	result := Reading{
		Messages: []Message{},
		Replies:  []Message{},
		Before:   []Message{},
		After:    []Message{},
		Notice:   CommunityNotice,
	}
	if id == "" || len(id) > 256 || before < 0 || before > 20 || after < 0 || after > 20 {
		return result, errors.New("invalid conversation or context bounds")
	}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	c := &result.Conversation
	channel, first, last, guild, err := s.readConversation(ctx, tx, id, c)
	if err != nil {
		return result, err
	}
	c.Channel, c.Source, c.Evidence = strconv.FormatInt(channel, 10), "discord", "community_discussion"
	c.Metadata = map[string]any{"guild_id": guild}
	stored, err := tx.Community().ConversationMessages(ctx, id, c.CourseID)
	if err != nil {
		return result, err
	}
	result.Messages, err = assembleCommunityMessages(stored, guild)
	if err != nil {
		return result, err
	}
	result.Truncated = len(result.Messages) > 200
	result.Messages = result.Messages[:min(200, len(result.Messages))]
	result.Before, result.After, err = s.readContextSides(
		ctx,
		tx,
		guild,
		c.CourseID,
		channel,
		first,
		last,
		before,
		after,
	)
	if err != nil {
		return result, err
	}
	result.Replies, err = s.readReplies(ctx, tx, guild, c.CourseID, result.Messages)
	if err != nil {
		return result, err
	}
	result.foldTruncated()
	return result, tx.Commit(ctx)
}

func (s Reader) readConversation(
	ctx context.Context,
	tx database.Tx,
	id string,
	c *Conversation,
) (channel, first, last int64, guild *int64, err error) {
	row, err := tx.Community().Conversation(ctx, id)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	c.ID, c.CourseID = row.ID, row.CourseID
	c.CourseName, c.CourseShortName = row.CourseName, row.CourseShortName
	c.Name, c.Kind = row.ChannelName, row.ChannelType
	c.Started, c.Ended = row.StartedAt, row.EndedAt
	for _, value := range []*string{&c.CourseName, c.CourseShortName, &c.Name} {
		if value != nil {
			*value = identity.Decode(*value)
		}
	}
	return row.ChannelID, row.FirstMessageID, row.LastMessageID, row.GuildID, nil
}

func (s Reader) readContextSides(
	ctx context.Context,
	tx database.Tx,
	guild *int64,
	courseID, channel, first, last int64,
	before, after int,
) ([]Message, []Message, error) {
	ops := tx.Community()
	olderStored, err := ops.OlderContext(ctx, database.CommunityContextParams{
		CourseID:  courseID,
		ChannelID: channel,
		EdgeID:    first,
		Limit:     before,
	})
	if err != nil {
		return nil, nil, err
	}
	older, err := assembleCommunityMessages(olderStored, guild)
	if err != nil {
		return nil, nil, err
	}
	slices.Reverse(older)
	newerStored, err := ops.NewerContext(ctx, database.CommunityContextParams{
		CourseID:  courseID,
		ChannelID: channel,
		EdgeID:    last,
		Limit:     after,
	})
	if err != nil {
		return nil, nil, err
	}
	newer, err := assembleCommunityMessages(newerStored, guild)
	return older, newer, err
}

func (s Reader) readReplies(
	ctx context.Context,
	tx database.Tx,
	guild *int64,
	courseID int64,
	messages []Message,
) ([]Message, error) {
	stored, err := tx.Community().ReplyMessages(ctx, courseID, replyIDs(messages))
	if err != nil {
		return nil, err
	}
	return assembleCommunityMessages(stored, guild)
}

func replyIDs(messages []Message) []int64 {
	replies := []int64{}
	for _, m := range messages {
		if m.Reply != nil {
			value, err := strconv.ParseInt(*m.Reply, 10, 64)
			if err == nil && !slices.Contains(replies, value) {
				replies = append(replies, value)
			}
		}
	}
	return replies
}

func (r *Reading) foldTruncated() {
	for _, rows := range [][]Message{r.Messages, r.Before, r.After, r.Replies} {
		for _, m := range rows {
			r.Truncated = r.Truncated || m.Truncated
		}
	}
}

// assembleCommunityMessages converts bounded typed rows to the Message shape.
// IDs stay text; NULL reply_to stays nil; attachments parse from their JSON
// text. Integer flags become bools here so both drivers return plain integers.
func assembleCommunityMessages(rows []database.CommunityMessage, guild *int64) ([]Message, error) {
	result := []Message{}
	for _, row := range rows {
		m := Message{
			ID:        row.MessageID,
			Channel:   row.ChannelID,
			Timestamp: row.Timestamp,
			Author:    row.AuthorName,
			Content:   row.Content,
			Type:      row.MessageType,
			Pinned:    row.Pinned,
			Reactions: row.ReactionCount,
			Truncated: row.Truncated,
			Reply:     row.ReplyTo,
		}
		raw := row.AttachmentsJSON
		if raw == "" {
			raw = "[]"
		}
		if err := decode([]byte(raw), &m.Attachments); err != nil {
			return nil, err
		}
		decodeMessage(&m, guild)
		result = append(result, m)
	}
	return result, nil
}
