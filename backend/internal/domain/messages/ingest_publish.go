package messages

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

func lockArchive(ctx context.Context, tx database.Tx, s Archive, path string) error {
	if err := tx.DiscordImports().LockDiscordMapping(ctx); err != nil {
		return err
	}
	mapped, err := tx.DiscordImports().MappedCourseForRoot(ctx, fmt.Sprint(s.Root))
	if err != nil {
		return err
	}
	if mapped != s.Course {
		return errors.New("Discord channel mapping changed during import")
	}
	courses := []int64{s.Course}
	previous, err := tx.DiscordImports().ArchiveSourceCourse(ctx, path)
	if err != nil && !database.IsNoRows(err) {
		return err
	}
	if err == nil && previous != s.Course {
		courses = append(courses, previous)
	}
	slices.Sort(courses)
	for _, id := range courses {
		if err = tx.Jobs().QueueLock(ctx, fmt.Sprintf("course:%d", id)); err != nil {
			return err
		}
	}
	return nil
}

func publishArchive(ctx context.Context, tx database.Tx, s Archive, h exportHeader, result ImportResult) error {
	var parent *int64
	if h.Channel.CategoryID > 0 {
		v := int64(h.Channel.CategoryID)
		parent = &v
	}
	return tx.DiscordImports().PublishArchive(ctx, database.DiscordArchivePublication{
		Path:            result.Path,
		RootID:          s.Root,
		ChannelID:       s.Channel,
		CourseID:        s.Course,
		GuildID:         int64(h.Guild.ID),
		ChannelName:     identity.Encode(h.Channel.Name),
		ChannelType:     h.Channel.Type,
		ChannelTopic:    identity.Encode(h.Channel.Topic),
		ParentChannelID: parent,
		ExportedAt:      h.Exported,
		ObjectID:        result.Object.SHA256,
	})
}
