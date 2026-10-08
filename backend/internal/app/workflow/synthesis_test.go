package workflow

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/navigation"
	"tree-eclass/internal/domain/practice"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/services/synthesis"
)

func TestNativeSourceBoundSynthesis(t *testing.T) {
	t.Parallel()
	pool, service, doc, a, courseOutput, calls := newSynthesisFixture(t)
	synthesisRunChecks(t, pool, service, doc, a, courseOutput, calls)
}

func newSynthesisFixture(t *testing.T) (*fixtureStore, synthesis.Service, string, settings.AI, string, *int) {
	t.Helper()
	c := nativeSharedController(t)
	ctx := t.Context()
	conn, objects := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	pool := newFixtureStore(t, ctx, c)
	t.Cleanup(pool.Close)
	_, err := pool.Native.Exec(
		ctx,
		`INSERT INTO app.courses(id,name,webdav_folder) VALUES(781,'Synthetic synthesis','/Courses/781'); INSERT INTO app.course_exam_plans(course_id,enabled,exam_at) VALUES(781,1,'2026-09-20T10:00')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	a, doc := seedSynthesisSource(t, ctx, pool, objects)
	courseOutput := synthesisFixture(t, c.Repo, "validation")
	practiceOutput := synthesisFixture(t, c.Repo, "practice")
	calls := 0
	fake := syntheticInference(
		func(_ context.Context, c inference.Candidate, in inference.Request, emit func(inference.Delta) error) error {
			calls++
			if c.Provider == "synthetic" {
				return &inference.Error{Status: 429}
			}
			output := courseOutput
			if strings.Contains(in.Messages[0].Content.(string), "active-recall") {
				output = practiceOutput
			}
			if err := emit(inference.Delta{Text: output}); err != nil {
				return err
			}
			return emit(inference.Delta{Finish: "stop"})
		},
	)
	service := synthesis.Service{
		Pool:      pool,
		Keys:      map[string]string{"SYNTHETIC_API_KEY": "fixture", "OLLAMA_API_KEY": "fixture"},
		Generator: inference.Generator{Client: fake},
	}
	return pool, service, doc, a, courseOutput, &calls
}

// seedSynthesisSource saves the fixture AI settings and publishes one ready,
// indexed source document with a ready grounded analysis.
func seedSynthesisSource(
	t *testing.T,
	ctx context.Context,
	pool *fixtureStore,
	objects *blob.Store,
) (settings.AI, string) {
	t.Helper()
	a := settings.DefaultAI()
	a.EnrichmentEnabled = true
	a.CourseEnabled = true
	a.PracticeEnabled = true
	a.CourseModel = "syn:fixture"
	a.CourseFallbacks = []string{"fixture-cloud"}
	a.PracticeModel = "fixture-cloud"
	a.PracticeFallbacks = []string{}
	if err := (settings.Service{Pool: pool}).SaveAI(ctx, a); err != nil {
		t.Fatal(err)
	}
	material := materials.Service{Pool: pool, Objects: objects, Temp: t.TempDir()}
	doc, err := material.Upload(
		ctx,
		materials.Upload{
			CourseID:  781,
			Name:      "source.txt",
			MediaType: "text/plain",
			Body:      strings.NewReader("Δένδρα: a tree is a connected acyclic graph."),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Native.Exec(ctx, `UPDATE knowledge.documents SET status='ready',content_hash_verified=1 WHERE id=$1`, doc.DocumentID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Native.Exec(ctx, `INSERT INTO knowledge.document_enrichments(document_id,source_hash,analysis_version,status,model,requested_model,payload_json,available_at)
 SELECT id,source_hash,$2,'ready','served-fallback',$3,'{"summary":"Tree definitions grounded in the source","importance":"essential","topics":["Trees"]}','now' FROM knowledge.documents WHERE id=$1`, doc.DocumentID, settings.DocumentAnalysisVersion, a.Model); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Native.Exec(ctx, `INSERT INTO knowledge.chunks(id,document_id,ordinal,locator_type,locator_start,text,normalized_text,content_hash) VALUES('fixture-chunk',$1,0,'section','1','A tree is connected and acyclic.','a tree is connected and acyclic.','fixture')`, doc.DocumentID); err != nil {
		t.Fatal(err)
	}
	return a, doc.DocumentID
}

func synthesisRunChecks(
	t *testing.T,
	pool *fixtureStore,
	service synthesis.Service,
	doc string,
	a settings.AI,
	courseOutput string,
	calls *int,
) {
	t.Helper()
	ctx := t.Context()
	var err error
	var view navigation.View
	if worked, err := service.RunOne(ctx, "course"); err != nil || !worked {
		t.Fatal("course generation", worked, err)
	}
	nav := navigation.Service{Pool: pool}
	if _, err = nav.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	view, err = nav.Read(ctx, navigation.Request{CourseID: 781, Roadmap: true, IncludeActions: true})
	if err != nil || view.Blueprint["usable"] != true || *calls != 2 {
		t.Fatal("generated plan", view.Blueprint, *calls, err)
	}
	if view.Blueprint["model"] == a.CourseModel {
		t.Fatal("lost served fallback model")
	}
	if worked, err := service.RunOne(ctx, "practice"); err != nil || !worked {
		t.Fatal("practice generation", worked, err)
	}
	if _, err = nav.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	questions, err := (practice.Service{Pool: pool}).Read(ctx, 781, "")
	if err != nil || questions.Totals["questions"] != 1 {
		t.Fatal("published practice", questions, err)
	}
	for _, lane := range []string{"course", "practice"} {
		if worked, err := service.RunOne(ctx, lane); err != nil || worked {
			t.Fatal("repeated completed work", lane, worked, err)
		}
	}
	synthesisReuseChecks(t, pool, service)
	synthesisRecoveryChecks(t, pool, service, doc, courseOutput)
}
func synthesisFixture(t *testing.T, repo, kind string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repo, "backend/internal/domain/blueprints/testdata", kind+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Payload map[string]any `json:"payload"`
	}
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	payload := fixtures[0].Payload
	if kind == "validation" {
		payload["conflicts"] = []any{}
	}
	raw, err = json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(raw), "document:one", "E1")
}
func synthesisRecoveryChecks(t *testing.T, pool *fixtureStore, service synthesis.Service, doc, output string) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Native.Exec(ctx, `UPDATE app.course_exam_plans SET planning_notes='Changed goal' WHERE course_id=781`); err != nil {
		t.Fatal(err)
	}
	service.Generator.Client = syntheticInference(
		func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
			return &inference.Error{Status: 429}
		},
	)
	if worked, err := service.RunOne(ctx, "course"); err != nil || !worked {
		t.Fatal("paused successor", worked, err)
	}
	var attempts, count int64
	if err := pool.Native.QueryRow(ctx, `SELECT attempts FROM knowledge.course_blueprints WHERE status='pending'`).Scan(&attempts); err != nil ||
		attempts != 0 {
		t.Fatal("quota spent attempts", attempts, err)
	}
	if err := pool.Native.QueryRow(ctx, `SELECT count(*) FROM knowledge.course_blueprints WHERE status='ready'`).Scan(&count); err != nil ||
		count != 1 {
		t.Fatal("lost usable prior plan", count, err)
	}
	if _, err := pool.Native.Exec(ctx, `UPDATE knowledge.course_blueprints SET status='running',attempts=1 WHERE status='pending'`); err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	called := false
	service.Generator.Client = syntheticInference(
		func(ctx context.Context, _ inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
			called = true
			if _, err := pool.Native.Exec(ctx, `UPDATE knowledge.document_enrichments SET payload_json='{"summary":"Source insight changed during generation"}' WHERE document_id=$1`, doc); err != nil {
				return err
			}
			if err := emit(inference.Delta{Text: output}); err != nil {
				return err
			}
			return emit(inference.Delta{Finish: "stop"})
		},
	)
	if worked, err := service.RunOne(ctx, "course"); err != nil || !worked || !called {
		t.Fatal("stale completion", worked, called, err)
	}
	if err := pool.Native.QueryRow(ctx, `SELECT count(*) FROM knowledge.course_blueprints WHERE revision=(SELECT max(revision) FROM knowledge.course_blueprints) AND status='ready'`).Scan(&count); err != nil ||
		count != 0 {
		t.Fatal("stale result published", count, err)
	}
	nav := navigation.Service{Pool: pool}
	if _, err := nav.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := nav.Read(ctx, navigation.Request{CourseID: 781})
	if err != nil || view.Blueprint["usable"] != false {
		t.Fatal("stale prior evidence remained usable", view, err)
	}
}
