package workflow

import (
	"fmt"
	"net/url"
	"testing"

	"tree-eclass/internal/domain/annotations"
	"tree-eclass/internal/domain/workspace"
)

func workspaceContextChecks(t *testing.T, pool *fixtureStore, base, document, action string) {
	t.Helper()
	ctx := t.Context()
	service := annotations.Service{Pool: pool}
	bookmark, err := service.Create(
		ctx,
		annotations.Create{
			Annotation: annotations.Annotation{
				CourseID:   101,
				DocumentID: document,
				PageNumber: 1,
				Kind:       "bookmark",
			},
			IdempotencyKey: "context-bookmark-001",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	deleted := "deleted"
	if _, err = service.Update(ctx, bookmark.ID, annotations.Update{Status: &deleted}); err != nil {
		t.Fatal(err)
	}
	live, err := service.Create(
		ctx,
		annotations.Create{
			Annotation: annotations.Annotation{
				CourseID:   101,
				DocumentID: document,
				PageNumber: 1,
				Kind:       "bookmark",
				ActionID:   action,
			},
			IdempotencyKey: "context-bookmark-002",
		},
	)
	if err != nil || live.ID == bookmark.ID {
		t.Fatal("deleted bookmark blocked fresh bookmark", err)
	}
	root := base + "/api/study/session/context?course_id=101"
	var view workspace.ContextView
	apiJSON(t, "GET", root+"&document_id="+url.QueryEscape(document), nil, 200, &view)
	if view.Action != nil || view.AnnotationDocument != document || len(view.Documents) != 1 ||
		len(view.Annotations) != 2 ||
		view.Revision != "" {
		t.Fatal("standalone reader context", view)
	}
	if view.Documents[0]["content_url"] != fmt.Sprintf("/api/study/document/%s/content?course_id=101", document) {
		t.Fatal("reader content not course-scoped")
	}
	apiJSON(t, "GET", root+"&action_id="+url.QueryEscape(action)+"&include_practice=false", nil, 200, &view)
	if view.Action == nil || view.Unit != "unit_one" || view.Revision != "build-r1" || len(view.Documents) != 1 ||
		view.Practice != nil ||
		len(view.Annotations) != 1 {
		t.Fatal("action context", view)
	}
	apiJSON(t, "GET", root+"&action_id=missing", nil, 409, nil)
	apiJSON(
		t,
		"GET",
		base+"/api/study/session/context?course_id=102&document_id="+url.QueryEscape(document),
		nil,
		404,
		nil,
	)
	apiJSON(t, "GET", root+"&document_id=missing", nil, 404, nil)
	if _, err = pool.Native.Exec(ctx, `UPDATE knowledge.documents SET status='pending' WHERE id=$1`, document); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", root+"&document_id="+url.QueryEscape(document), nil, 409, nil)
	if _, err = pool.Native.Exec(ctx, `UPDATE knowledge.documents SET status='ready' WHERE id=$1`, document); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Native.Exec(ctx, `UPDATE app.study_annotations SET source_hash='older-revision' WHERE id=$1`, live.ID); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", root+"&document_id="+url.QueryEscape(document), nil, 200, &view)
	if view.Annotations[1].Status != "orphaned" || view.Documents[0]["orphaned_annotations"] != float64(1) {
		t.Fatal("source-bound orphan context", view)
	}
	listing, err := service.List(ctx, 101, document, "", true)
	if err != nil || len(listing.Annotations) != 2 {
		t.Fatal("deleted bookmark hid live bookmark", listing, err)
	}
	if _, err = pool.Native.Exec(ctx, `DELETE FROM app.study_annotations WHERE id=ANY($1::bigint[])`, []int64{bookmark.ID, live.ID}); err != nil {
		t.Fatal(err)
	}
}
