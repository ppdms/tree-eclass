package synchronization

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/html"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/notifications"
	"tree-eclass/internal/integrations/eclass"
)

func courseHeader(ctx context.Context, tx database.Tx, id int64, title string) (string, error) {
	name, err := tx.Sync().CourseName(ctx, id)
	return "**" + title + " — " + notifications.Plain(identity.Decode(name)) + "**", err
}
func announceChanges(ctx context.Context, tx database.Tx, course, record int64, changes []Change) error {
	header, err := courseHeader(ctx, tx, course, "Course changes")
	if err != nil {
		return err
	}
	lines := make([]string, 0, len(changes))
	labels := map[string]string{
		"added_file":        "Added file",
		"modified_file":     "Modified file",
		"deleted_file":      "Deleted file",
		"added_directory":   "Added directory",
		"deleted_directory": "Deleted directory",
	}
	for _, c := range changes {
		lines = append(lines, "• "+labels[c.Type]+": "+notifications.Plain(c.Path))
	}
	return notifications.EnqueueTx(
		ctx,
		tx,
		notifications.Event{Key: fmt.Sprintf("changes:%d", record), Header: header, Lines: lines},
	)
}
func announcementLine(a eclass.Announcement) string {
	line := "• **" + notifications.Plain(a.Title) + "**"
	if a.Published != nil && *a.Published != "" {
		line += " (" + notifications.Plain(*a.Published) + ")"
	}
	if a.Link != "" {
		line += "\n" + a.Link
	}
	if a.Description != "" {
		line += "\n" + notifications.Plain(htmlText(a.Description))
	}
	return line
}
func htmlText(source string) string {
	tokens := html.NewTokenizer(strings.NewReader(source))
	var text strings.Builder
	hidden := 0
	for {
		kind := tokens.Next()
		if kind == html.ErrorToken {
			break
		}
		token := tokens.Token()
		switch kind {
		case html.StartTagToken:
			if token.Data == "script" || token.Data == "style" {
				hidden++
			}
		case html.EndTagToken:
			if (token.Data == "script" || token.Data == "style") && hidden > 0 {
				hidden--
			}
		case html.TextToken:
			if hidden == 0 {
				text.WriteString(token.Data)
				text.WriteByte(' ')
			}
		}
		if text.Len() > 16000 {
			break
		}
	}
	return strings.Join(strings.Fields(text.String()), " ")
}
func announceExercises(ctx context.Context, tx database.Tx, course int64, lines []string) error {
	if len(lines) == 0 {
		return nil
	}
	header, err := courseHeader(ctx, tx, course, "Exercise updates")
	if err != nil {
		return err
	}
	return notifications.EnqueueTx(
		ctx,
		tx,
		notifications.Event{
			Key:    fmt.Sprintf("exercises:%d:%s", course, time.Now().UTC().Format(time.RFC3339Nano)),
			Header: header,
			Lines:  lines,
		},
	)
}
func exerciseEvents(ctx context.Context, tx database.Tx, course int64, ex eclass.Exercise) ([]string, error) {
	state, err := tx.Sync().ExerciseState(ctx, course, identity.Encode(ex.ID))
	grade, comments, file, url := state.Grade, state.GradeComments, state.AssignmentFileName, state.AssignmentFileURL
	title := notifications.Plain(ex.Title)
	lines := []string{}
	if errors.Is(err, database.ErrNoRows) {
		line := "• New exercise: **" + title + "**"
		if ex.Deadline != "" {
			line += "\nDeadline: " + notifications.Plain(ex.Deadline)
		}
		if ex.AssignmentFileName != "" {
			line += "\nFile: " + notifications.Plain(ex.AssignmentFileName)
		}
		return []string{line + "\n" + ex.Link}, nil
	}
	if err != nil {
		return nil, err
	}
	if ex.Grade != "" && (ex.Grade != identity.Decode(grade) || ex.GradeComments != identity.Decode(comments)) {
		line := "• Grade received: **" + title + "**: " + notifications.Plain(ex.Grade)
		if ex.MaxGrade != "" {
			line += "/" + notifications.Plain(ex.MaxGrade)
		}
		if ex.GradeComments != "" {
			line += "\n" + notifications.Plain(ex.GradeComments)
		}
		lines = append(lines, line+"\n"+ex.Link)
	}
	if ex.AssignmentFileName != identity.Decode(file) || ex.AssignmentFileURL != url {
		lines = append(
			lines,
			"• Exercise file updated: **"+title+"**\n"+notifications.Plain(ex.AssignmentFileName)+"\n"+ex.Link,
		)
	}
	return lines, nil
}
