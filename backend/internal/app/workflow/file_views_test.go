package workflow

import (
	"testing"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/settings"
)

func fileViewChecks(t *testing.T, pool *fixtureStore, base, document string) {
	t.Helper()
	prepareFileAnalysis(t, pool, document)
	var metadata struct {
		Files map[string]knowledge.FileMetadata `json:"file_insights"`
	}
	apiJSON(t, "GET", base+"/api/v1/courses/101/file-metadata", nil, 200, &metadata)
	if len(metadata.Files) != 1 {
		t.Fatal("file metadata", metadata)
	}
	var path string
	for key, item := range metadata.Files {
		path = key
		if !item.Guide || item.ID != document || item.Unit != "sections" || item.PagesReady != 1 {
			t.Fatal("metadata generation", item)
		}
	}
	var guide knowledge.FileGuide
	guideURL := base + "/api/v1/courses/101/files/" + document + "/guide"
	apiJSON(t, "GET", guideURL, nil, 200, &guide)
	if guide.AI["summary"] != "Ελληνικός οδηγός" {
		t.Fatal("current guide missing", guide)
	}
	pagesURL := base + "/api/study/document/" + document + "/pages?course_id=101"
	filePageSourceChecks(t, pool, document, pagesURL)
	apiJSON(t, "GET", base+"/api/v1/courses/102/files/"+document+"/guide", nil, 404, nil)
	apiJSON(t, "GET", base+"/api/v1/courses/101/files?include_knowledge=invalid", nil, 422, nil)
	var files courses.Files
	apiJSON(t, "GET", base+"/api/v1/courses/101/files?include_knowledge=false", nil, 200, &files)
	if files.Versions == nil || files.Deleted == nil || files.Collapsed == nil || files.Expanded == nil ||
		files.Study == nil {
		t.Fatal("empty file collections must be arrays and objects")
	}
	coverageChecks(t, pool, base, path)
	analysisGenerationChecks(t, pool, base, document, guideURL, pagesURL)
}

func prepareFileAnalysis(t *testing.T, pool *fixtureStore, document string) {
	t.Helper()
	ctx := t.Context()
	a := settings.DefaultAI()
	_, err := pool.Native.ExecContext(
		ctx,
		`INSERT INTO document_enrichments(
 document_id,source_hash,analysis_version,status,model,payload_json,available_at)
 SELECT id,source_hash,?,'ready',?,
 '{"summary":"Ελληνικός οδηγός","related_paths":[]}',strftime('%Y-%m-%dT%H:%M:%fZ','now')
 FROM documents WHERE id=?
 ON CONFLICT(document_id) DO UPDATE SET
 source_hash=excluded.source_hash,analysis_version=excluded.analysis_version,
 model=excluded.model,payload_json=excluded.payload_json`,
		settings.DocumentAnalysisVersion,
		a.Model,
		document,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Native.ExecContext(
		ctx,
		`UPDATE page_enrichments SET analysis_version=?,requested_model=?,model='fallback-model' WHERE document_id=?`,
		settings.PageAnalysisVersion,
		a.Model,
		document,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func filePageSourceChecks(t *testing.T, pool *fixtureStore, document, pagesURL string) {
	t.Helper()
	var hash string
	if err := pool.Native.QueryRowContext(t.Context(),
		`SELECT source_hash FROM documents WHERE id=?`, document).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	var pages knowledge.PageInsights
	apiJSON(t, "GET", pagesURL, nil, 200, &pages)
	if pages.SourceHash != hash || len(pages.Pages) != 1 || pages.Pages[0].Stale ||
		pages.Pages[0].Insight["summary"] == nil {
		t.Fatal("current requested-model fallback rejected", pages)
	}
	if _, err := pool.Native.ExecContext(t.Context(),
		`UPDATE page_enrichments SET source_hash='older-page-source' WHERE document_id=?`, document); err != nil {
		t.Fatal(err)
	}
	pages = knowledge.PageInsights{}
	apiJSON(t, "GET", pagesURL, nil, 200, &pages)
	if pages.SourceHash != hash || !pages.Pages[0].Stale || pages.Pages[0].Insight != nil {
		t.Fatal("page source metadata came from stale aid rather than admitted document", pages)
	}
	if _, err := pool.Native.ExecContext(t.Context(),
		`UPDATE page_enrichments SET source_hash=? WHERE document_id=?`, hash, document); err != nil {
		t.Fatal(err)
	}
}

func analysisGenerationChecks(t *testing.T, pool *fixtureStore, base, document, guideURL, pagesURL string) {
	t.Helper()
	ctx := t.Context()
	s := settings.Service{Pool: pool}
	a := settings.DefaultAI()
	a.Model = "different-model"
	if err := s.SaveAI(ctx, a); err != nil {
		t.Fatal(err)
	}
	var guide knowledge.FileGuide
	apiJSON(t, "GET", guideURL, nil, 200, &guide)
	if guide.AI != nil || guide.Status != "not_queued" {
		t.Fatal("old model guide displayed", guide)
	}
	var pages knowledge.PageInsights
	apiJSON(t, "GET", pagesURL, nil, 200, &pages)
	if !pages.Pages[0].Stale || pages.Pages[0].Insight != nil {
		t.Fatal("old requested model page displayed")
	}
	if err := s.SaveAI(ctx, settings.DefaultAI()); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"analysis_version='obsolete'", "source_hash='different-source'"} {
		if _, err := pool.Native.ExecContext(ctx, `UPDATE document_enrichments SET `+change+` WHERE document_id=?`, document); err != nil {
			t.Fatal(err)
		}
		apiJSON(t, "GET", guideURL, nil, 200, &guide)
		if guide.AI != nil {
			t.Fatal("stale contract/source guide displayed")
		}
	}
	if _, err := pool.Native.ExecContext(ctx, `UPDATE document_enrichments SET analysis_version=?,source_hash=(SELECT source_hash FROM documents WHERE id=?) WHERE document_id=?`, settings.DocumentAnalysisVersion, document, document); err != nil {
		t.Fatal(err)
	}
	// Configuration invalidation is visible before the processor runs again.
	var shelf courses.Shelf
	apiJSON(t, "GET", base+"/api/v1/courses/coverage", nil, 200, &shelf)
	if !shelf.Stale {
		t.Fatal("native settings failed to invalidate projection")
	}
}

func coverageChecks(t *testing.T, pool *fixtureStore, base, path string) {
	t.Helper()
	ctx := t.Context()
	s := courses.Service{Pool: pool}
	refresh := func() {
		for range 100 {
			changed, err := s.RefreshCoverage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !changed {
				return
			}
		}
		t.Fatal("coverage never converged")
	}
	var shelf courses.Shelf
	url := base + "/api/v1/courses/coverage"
	apiJSON(t, "GET", url, nil, 200, &shelf)
	if !shelf.Stale {
		t.Fatal("unbuilt coverage must be pending")
	}
	refresh()
	apiJSON(t, "GET", url, nil, 200, &shelf)
	if shelf.Stale || len(shelf.Courses) != 1 || shelf.Courses[0].Total != 1 || shelf.Courses[0].Indexed != 1 ||
		len(shelf.Recent) != 1 {
		t.Fatal("initial coverage", shelf)
	}
	var generation int64
	if err := pool.Native.QueryRowContext(ctx, `SELECT generation FROM course_generation WHERE course_id=101`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if err := s.StudyLevel(ctx, 101, path, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.StudyLevel(ctx, 101, "/historical-deleted.txt", 4); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &shelf)
	if !shelf.Stale || len(shelf.Recent) != 0 {
		t.Fatal("unrefreshed study edit called current")
	}
	var after int64
	if err := pool.Native.QueryRowContext(ctx, `SELECT generation FROM course_generation WHERE course_id=101`).Scan(&after); err != nil ||
		after != generation {
		t.Fatal("learner edit invalidated immutable evidence", err)
	}
	refresh()
	apiJSON(t, "GET", url, nil, 200, &shelf)
	if shelf.Courses[0].Completion != 1 || shelf.Courses[0].Distribution["4"] != 1 ||
		shelf.Levels["101"]["/historical-deleted.txt"] != 4 {
		t.Fatal("historical progress inflated completion or was lost", shelf)
	}
	if err := s.StudyLevel(ctx, 101, path, 5); err != nil {
		t.Fatal(err)
	}
	refresh()
	apiJSON(t, "GET", url, nil, 200, &shelf)
	if shelf.Courses[0].Total != 0 || shelf.Courses[0].Completion != 0 || shelf.Courses[0].Distribution["5"] != 1 {
		t.Fatal("ignored file counted as study work", shelf)
	}
	if err := s.StudyLevel(ctx, 101, path, 0); err != nil {
		t.Fatal(err)
	}
}

func coverageScopeChecks(t *testing.T, pool *fixtureStore) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO courses(id,name,webdav_folder) VALUES(909,'Coverage one','/Courses/909'),(910,'Coverage two','/Courses/910')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO nodes(id,course_id,name,url,local_path) VALUES(9909,909,'root','https://example.invalid/909','/Courses/909/eclass')`); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err := pool.Native.QueryRowContext(ctx, `SELECT generation FROM course_generation WHERE course_id=910`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO files(node_id,url,name,local_path) VALUES(9909,'https://example.invalid/file','file.txt','/Courses/909/eclass/file.txt')`); err != nil {
		t.Fatal(err)
	}
	if err := pool.Native.QueryRowContext(ctx, `SELECT generation FROM course_generation WHERE course_id=910`).Scan(&after); err != nil ||
		after != before {
		t.Fatal("file write invalidated unrelated course", err)
	}
	service := courses.Service{Pool: pool}
	for range 100 {
		changed, err := service.RefreshCoverage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			break
		}
	}
	if _, err := pool.Native.ExecContext(ctx, `DELETE FROM courses WHERE id=909`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO courses(id,name,webdav_folder) VALUES(909,'Recreated course','/Courses/909')`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.Native.QueryRowContext(ctx, `SELECT count(*) FROM course_coverage WHERE course_id=909`).Scan(&count); err != nil ||
		count != 0 {
		t.Fatal("recreated course inherited coverage", count, err)
	}
	if _, err := pool.Native.ExecContext(ctx, `DELETE FROM courses WHERE id IN(909,910)`); err != nil {
		t.Fatal(err)
	}
}
