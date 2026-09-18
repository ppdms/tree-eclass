package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/integrations/parser"
	"tree-eclass/internal/services/analysis"
)

type sourceBoundAnalysisFixture struct {
	c        *Controller
	ctx      context.Context
	pool     *pgxpool.Pool
	material materials.Service
	indexer  knowledge.Indexer
	service  analysis.Service
	reader   knowledge.Reader
	document string
	model    string
	calls    *int
	images   *int
}

func TestNativeSourceBoundAnalysis(t *testing.T) {
	t.Parallel()
	fixture := newSourceBoundAnalysisFixture(t)
	sourceAnalysisChecks(t, fixture)
	visualAnalysisChecks(t, fixture)
	analysisRecoveryChecks(t, fixture.pool, fixture.service, fixture.document)
}

func newSourceBoundAnalysisFixture(t *testing.T) *sourceBoundAnalysisFixture {
	t.Helper()
	c := nativeSharedController(t)
	ctx := t.Context()
	conn, objects := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	pool, err := pgxpool.New(ctx, c.databaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err = pool.Exec(ctx, `INSERT INTO app.courses(id,name,webdav_folder) VALUES(771,'Synthetic analysis','/Courses/771')`); err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	runner := parser.New(filepath.Join(c.Repo, ".venv/bin/python"), c.Repo, temp)
	runner.Tessdata = c.Config.Tessdata
	material := materials.Service{Pool: pool, Objects: objects, Temp: temp}
	indexed := knowledge.Indexer{Pool: pool, Objects: objects, Temp: temp, Parser: runner}
	doc, err := material.Upload(
		ctx,
		materials.Upload{
			CourseID:  771,
			Name:      "notes.txt",
			MediaType: "text/plain",
			Body:      strings.NewReader("Δένδρα και αλγόριθμοι. Source evidence."),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = indexed.Index(ctx, doc.DocumentID); err != nil {
		t.Fatal(err)
	}
	a := settings.DefaultAI()
	a.EnrichmentEnabled = true
	config := settings.Service{Pool: pool}
	if err = config.SaveAI(ctx, a); err != nil {
		t.Fatal(err)
	}
	calls, images := 0, 0
	service := analysis.Service{
		Pool:      pool,
		Objects:   objects,
		Parser:    runner,
		Temp:      temp,
		Generator: inference.Generator{Client: analysisFixtureInference(&calls, &images)},
		Keys:      map[string]string{"SYNTHETIC_API_KEY": "fixture", "OLLAMA_API_KEY": "fixture"},
	}
	return &sourceBoundAnalysisFixture{
		c: c, ctx: ctx, pool: pool, material: material, indexer: indexed, service: service,
		reader: knowledge.Reader{Pool: pool}, document: doc.DocumentID, model: a.Model,
		calls: &calls, images: &images,
	}
}

func analysisFixtureInference(calls, images *int) syntheticInference {
	return func(_ context.Context, c inference.Candidate, in inference.Request, emit func(inference.Delta) error) error {
		*calls = *calls + 1
		if c.Provider == "synthetic" {
			return &inference.Error{Status: 429, Retryable: true}
		}
		for _, m := range in.Messages {
			*images += len(m.Images)
		}
		if err := emit(inference.Delta{Text: `{"summary":"Grounded fixture summary","material_type":"lecture_notes","page_type":"lecture_content","visuals":["Visible fixture"],"topics":["Δένδρα"]}`}); err != nil {
			return err
		}
		return emit(inference.Delta{Finish: "stop"})
	}
}

func sourceAnalysisChecks(t *testing.T, fixture *sourceBoundAnalysisFixture) {
	t.Helper()
	ctx := fixture.ctx
	if worked, err := fixture.service.RunOne(ctx); err != nil || !worked {
		t.Fatal("document analysis", worked, err)
	}
	insight, err := fixture.reader.MaterialInsight(ctx, fixture.document)
	if err != nil {
		t.Fatal(err)
	}
	saved := insight["study_analysis"].(map[string]any)
	if saved["ready"] != true || saved["model"] == fixture.model || saved["requested_model"] != fixture.model || *fixture.calls != 2 {
		t.Fatal("fallback guide incorrectly stale", saved, *fixture.calls)
	}
	if worked, err := fixture.service.RunOne(ctx); err != nil || worked {
		t.Fatal("ready analysis repeated", worked, err)
	}
}

func visualAnalysisChecks(t *testing.T, fixture *sourceBoundAnalysisFixture) {
	t.Helper()
	ctx := fixture.ctx
	imageFile, err := os.Open(filepath.Join(fixture.c.Repo, "backend/internal/integrations/parser/testdata/greek-ocr.png"))
	if err != nil {
		t.Fatal(err)
	}
	visual, err := fixture.material.Upload(
		ctx,
		materials.Upload{CourseID: 771, Name: "page.png", MediaType: "image/png", Body: imageFile},
	)
	imageFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.indexer.Index(ctx, visual.DocumentID); err != nil {
		t.Fatal(err)
	}
	if worked, err := fixture.service.RunOne(ctx); err != nil || !worked {
		t.Fatal("page analysis", worked, err)
	}
	page, err := fixture.reader.PageInsight(ctx, visual.DocumentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	p := page["page_analysis"].(map[string]any)
	if p["ready"] != true || *fixture.images != 1 || p["insight"].(map[string]any)["evidence_mode"] != "rendered_page" {
		t.Fatal("visual evidence", page, *fixture.images)
	}
	if worked, err := fixture.service.RunOne(ctx); err != nil || !worked {
		t.Fatal("complete page synthesis", worked, err)
	}
	insight, err := fixture.reader.MaterialInsight(ctx, visual.DocumentID)
	if err != nil || insight["study_analysis"].(map[string]any)["ready"] != true {
		t.Fatal("visual document guide", insight, err)
	}
}
func analysisRecoveryChecks(t *testing.T, pool *pgxpool.Pool, service analysis.Service, document string) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `UPDATE knowledge.document_enrichments SET status='pending',available_at='now',attempts=0 WHERE document_id=$1`, document); err != nil {
		t.Fatal(err)
	}
	service.Generator.Client = syntheticInference(
		func(context.Context, inference.Candidate, inference.Request, func(inference.Delta) error) error {
			return &inference.Error{Status: 429, Retryable: true}
		},
	)
	if worked, err := service.RunOne(ctx); err != nil || !worked {
		t.Fatal(worked, err)
	}
	var attempts int64
	var status string
	if err := pool.QueryRow(ctx, `SELECT attempts,status FROM knowledge.document_enrichments WHERE document_id=$1`, document).Scan(&attempts, &status); err != nil ||
		attempts != 0 ||
		status != "pending" {
		t.Fatal("quota pause exhausted retry budget", attempts, status, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE knowledge.document_enrichments SET status='running',attempts=1 WHERE document_id=$1`, document); err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	var called bool
	service.Generator.Client = syntheticInference(
		func(ctx context.Context, _ inference.Candidate, _ inference.Request, emit func(inference.Delta) error) error {
			called = true
			if _, err := pool.Exec(ctx, `UPDATE knowledge.documents SET source_hash='changed' WHERE id=$1`, document); err != nil {
				return err
			}
			if err := emit(inference.Delta{Text: `{"summary":"must never publish"}`}); err != nil {
				return err
			}
			return emit(inference.Delta{Finish: "stop"})
		},
	)
	if worked, err := service.RunOne(ctx); err != nil || !worked || !called {
		t.Fatal("stale completion", worked, called, err)
	}
	var payload *string
	if err := pool.QueryRow(ctx, `SELECT status,payload_json FROM knowledge.document_enrichments WHERE document_id=$1`, document).Scan(&status, &payload); err != nil ||
		status == "ready" ||
		(payload != nil && strings.Contains(*payload, "must never publish")) {
		t.Fatal("stale source analysis published", status, err)
	}
	if _, err := (knowledge.Reader{Pool: pool}).Read(ctx, knowledge.ReadRequest{DocumentID: document}); !errors.Is(
		err,
		knowledge.ErrUnavailable,
	) {
		t.Fatal("unregistered source still read", err)
	}
}
