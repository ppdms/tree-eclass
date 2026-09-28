package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/annotations"
	"tree-eclass/internal/domain/exercises"
)

func annotationChecks(t *testing.T, db *pgx.Conn, base, document string) {
	t.Helper()
	var sourceHash string
	if err := db.QueryRow(t.Context(), `SELECT source_hash FROM knowledge.documents WHERE id=$1`, document).Scan(&sourceHash); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{
		"course_id":       "101",
		"document_id":     document,
		"page_number":     1,
		"quote":           "Δένδρα\x00\ue000",
		"idempotency_key": "synthetic-highlight-1",
		"rects":           []any{},
		"tags":            []string{"δένδρα"},
	}
	var first struct {
		Annotation annotations.Annotation `json:"annotation"`
	}
	apiJSON(t, "POST", base+"/api/study/annotations", body, 200, &first)
	if first.Annotation.ID == 0 || first.Annotation.Quote != body["quote"] {
		t.Fatalf("annotation round trip: %+v", first)
	}
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			var retry struct {
				Annotation annotations.Annotation `json:"annotation"`
			}
			apiJSON(t, "POST", base+"/api/study/annotations", body, 200, &retry)
			if retry.Annotation.ID != first.Annotation.ID {
				t.Errorf("idempotent retry duplicated annotation")
			}
		})
	}
	wg.Wait()
	var result annotations.Listing
	url := base + "/api/study/annotations?course_id=101&document_id=" + document
	apiJSON(t, "GET", url, nil, 200, &result)
	if len(result.Annotations) != 1 || result.NewlyOrphaned != 0 {
		t.Fatalf("annotation listing: %+v", result)
	}
	if _, err := db.Exec(context.Background(), `UPDATE knowledge.documents SET source_hash='changed-source' WHERE id=$1`, document); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &result)
	if result.NewlyOrphaned != 1 || result.Annotations[0].Status != "orphaned" ||
		result.Annotations[0].SourceHash == "changed-source" {
		t.Fatal("source replacement rewrote or lost learner evidence")
	}
	apiJSON(t, "DELETE", fmt.Sprintf("%s/api/study/annotations/%d", base, first.Annotation.ID), nil, 200, nil)
	apiJSON(t, "GET", url, nil, 200, &result)
	if len(result.Annotations) != 0 {
		t.Fatal("deleted annotation still visible")
	}
	apiJSON(t, "GET", url+"&include_deleted=true", nil, 200, &result)
	if len(result.Annotations) != 1 || result.Annotations[0].Status != "deleted" {
		t.Fatal("deleted annotation history lost")
	}
	body["quote"] = ""
	apiJSON(t, "POST", base+"/api/study/annotations", body, 422, nil)
	body["course_id"] = true
	apiJSON(t, "POST", base+"/api/study/annotations", body, 422, nil)
	if _, err := db.Exec(t.Context(), `UPDATE knowledge.documents SET source_hash=$2 WHERE id=$1`, document, sourceHash); err != nil {
		t.Fatal(err)
	}
}

func exerciseChecks(t *testing.T, db *pgx.Conn, base string) {
	t.Helper()
	_, err := db.Exec(
		context.Background(),
		`INSERT INTO app.exercises(course_id,exercise_id,title,link,deadline,description) VALUES(101,'assignment-1','Εργασία','https://example.invalid','25 Μαρτίου 2026 - 11:55 μ.μ.','Detailed synthetic assignment')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Exercises []exercises.Exercise `json:"exercises"`
	}
	apiJSON(t, "GET", base+"/api/v1/exercises?include_details=false", nil, 200, &list)
	if len(list.Exercises) != 1 || list.Exercises[0].Description != nil || list.Exercises[0].Urgency != "ex-overdue" {
		t.Fatalf("exercise summary: %+v", list)
	}
	apiJSON(t, "POST", base+"/api/v1/courses/101/exercises/assignment-1/ignore", map[string]any{}, 200, nil)
	apiJSON(t, "GET", base+"/api/v1/exercises", nil, 200, &list)
	if len(list.Exercises) != 0 {
		t.Fatal("ignored exercise visible")
	}
	apiJSON(t, "GET", base+"/api/v1/exercises?include_ignored=true", nil, 200, &list)
	if len(list.Exercises) != 1 || list.Exercises[0].Description == nil {
		t.Fatal("exercise detail lost")
	}
	apiJSON(t, "GET", base+"/api/v1/courses/102/exercises/assignment-1", nil, 404, nil)
}

func apiJSON(t *testing.T, method, url string, body any, status int, target any) {
	t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s: %d %s", method, url, response.StatusCode, data)
	}
	if target != nil {
		if err = json.Unmarshal(data, target); err != nil {
			t.Fatal(err)
		}
	}
}
