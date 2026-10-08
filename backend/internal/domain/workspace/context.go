package workspace

import (
	"context"

	"tree-eclass/internal/domain/annotations"
	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/practice"
	"tree-eclass/internal/domain/settings"
)

type ContextRequest struct {
	CourseID         int64
	Action, Document string
	Practice         bool
}
type ContextView struct {
	Course             map[string]any           `json:"course"`
	Action             map[string]any           `json:"action"`
	Unit               string                   `json:"unit_key"`
	Revision           string                   `json:"plan_revision"`
	Documents          []map[string]any         `json:"documents"`
	Annotations        []annotations.Annotation `json:"annotations"`
	AnnotationDocument string                   `json:"annotations_document_id,omitempty"`
	Reading            Reading                  `json:"reading"`
	Practice           any                      `json:"practice"`
	PracticeError      any                      `json:"practice_error"`
	Actions            []map[string]any         `json:"actions"`
	Notice             string                   `json:"notice,omitempty"`
	Coverage           any                      `json:"coverage,omitempty"`
}

func (s Service) Context(ctx context.Context, in ContextRequest) (ContextView, error) {
	view := ContextView{
		Documents:   []map[string]any{},
		Annotations: []annotations.Annotation{},
		Actions:     []map[string]any{},
	}
	tx, err := s.Pool.BeginTx(ctx, database.Options{Isolation: database.RepeatableRead, AccessMode: database.ReadOnly})
	if err != nil {
		return view, err
	}
	defer tx.Rollback(ctx)
	_, c, err := courses.SnapshotCourses(ctx, tx, &in.CourseID)
	if err != nil {
		return view, err
	}
	view.Course = map[string]any{"course_id": c.ID, "name": c.Name, "short_name": c.ShortName}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return view, err
	}
	if in.Document != "" {
		err = s.documentContext(ctx, tx, in, a, &view)
	} else {
		err = s.actionContext(ctx, tx, in, a, &view)
	}
	if err != nil {
		return view, err
	}
	return view, tx.Commit(ctx)
}

func (s Service) documentContext(
	ctx context.Context,
	tx database.Tx,
	in ContextRequest,
	a settings.AI,
	view *ContextView,
) error {
	document, err := workspaceDocument(ctx, tx, in.CourseID, in.Document, a)
	if err != nil {
		return err
	}
	view.Documents = append(view.Documents, document)
	view.Annotations, err = annotations.Snapshot(ctx, tx, in.CourseID, in.Document, "", true)
	if err != nil {
		return err
	}
	view.Reading, err = readingTotals(ctx, tx, in.CourseID, "", in.Document)
	view.AnnotationDocument = in.Document
	view.Practice = map[string]any{"units": []any{}}
	view.Notice = "Opened from the file tree. Time and marks are recorded against the document; " +
		"this sitting is not attached to a planned action."
	return err
}

func (s Service) actionContext(
	ctx context.Context,
	tx database.Tx,
	in ContextRequest,
	a settings.AI,
	view *ContextView,
) error {
	nav, err := (navigation.Service{Pool: s.Pool}).ReadTx(
		ctx,
		tx,
		navigation.Request{CourseID: in.CourseID, Roadmap: true, IncludeHidden: true, IncludeActions: true},
	)
	if err != nil {
		return err
	}
	actions, _ := nav.Blueprint["actions"].([]map[string]any)
	if nav.Blueprint["usable"] != true || len(actions) == 0 {
		return ErrConflict
	}
	wanted := in.Action
	if wanted == "" {
		progress, _ := nav.Blueprint["progress"].(map[string]any)
		wanted, _ = progress["next_action_id"].(string)
		if wanted == "" {
			wanted, _ = actions[0]["action_id"].(string)
		}
	}
	for _, action := range actions {
		if action["action_id"] == wanted {
			view.Action = action
		}
		compact := map[string]any{}
		for _, key := range []string{"action_id", "title", "unit_key", "unit_title", "action_type",
			"estimated_minutes", "status"} {
			compact[key] = action[key]
		}
		view.Actions = append(view.Actions, compact)
	}
	if view.Action == nil {
		return ErrConflict
	}
	view.Unit, _ = view.Action["unit_key"].(string)
	view.Revision, _ = nav.Blueprint["revision_id"].(string)
	view.Coverage = nav.Blueprint["coverage"]
	view.Documents, err = actionDocuments(ctx, tx, in.CourseID, view.Action, a)
	if err != nil {
		return err
	}
	view.Annotations, err = annotations.Snapshot(ctx, tx, in.CourseID, "", wanted, true)
	if err != nil {
		return err
	}
	view.Reading, err = readingTotals(ctx, tx, in.CourseID, wanted, "")
	if err != nil {
		return err
	}
	if in.Practice {
		view.Practice, err = (practice.Service{Pool: s.Pool}).ReadTx(ctx, tx, in.CourseID, view.Unit)
		// Invalid saved practice is optional; retain the readable course sources.
		// Database errors leave this transaction aborted and still fail Commit.
		if err != nil {
			view.Practice = map[string]any{"units": []any{}}
			view.PracticeError = "Practice is unavailable for this sitting."
		}
	}
	return nil
}
