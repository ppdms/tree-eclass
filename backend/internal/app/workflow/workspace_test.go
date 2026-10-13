package workflow

import (
	"fmt"
	"sync"
	"testing"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/domain/workspace"
)

func workspaceChecks(t *testing.T, pool *fixtureStore, base, document string) {
	t.Helper()
	ctx := t.Context()
	service := workspace.Service{Pool: pool}
	start := map[string]any{"course_id": "101", "session_key": "synthetic-reader-001"}
	var response workspace.StartResult
	apiJSON(t, "POST", base+"/api/study/session/start", start, 200, &response)
	id := response.Session.ID
	if response.NextSequence != 0 {
		t.Fatal("new sitting has a heartbeat watermark", response)
	}
	apiJSON(t, "POST", base+"/api/study/session/start", start, 200, &response)
	if response.Session.ID != id {
		t.Fatal("start retry created another session")
	}
	start["action_id"] = "changed-action"
	apiJSON(t, "POST", base+"/api/study/session/start", start, 409, nil)
	var sourceHash string
	if err := pool.Native.QueryRowContext(ctx,
		`SELECT source_hash FROM documents WHERE id=?`, document).Scan(&sourceHash); err != nil {
		t.Fatal(err)
	}
	beat := workspace.Beat{SessionID: id, Sequence: 0, Page: 1, Interval: 3600,
		Document: document, SourceHash: sourceHash, Active: true}
	var group sync.WaitGroup
	errs := make(chan error, 12)
	for range 12 {
		group.Go(func() {
			result, err := service.Heartbeat(ctx, beat)
			if err == nil && result.Session.Active != 90 {
				err = fmt.Errorf("heartbeat retry inflated time: %d", result.Session.Active)
			}
			errs <- err
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	workspaceResumeChecks(t, service, id, beat)
	workspaceProgressChecks(t, pool, base, document, service, id, beat)
}

func workspaceResumeChecks(t *testing.T, service workspace.Service, id int64, beat workspace.Beat) {
	t.Helper()
	ctx := t.Context()
	in := workspace.Start{CourseID: 101, Key: "synthetic-reader-001"}
	resumed, err := service.Start(ctx, in)
	if err != nil || resumed.Session.ID != id || resumed.NextSequence != 1 {
		t.Fatal("resume did not report committed sequence watermark", resumed, err)
	}
	beat.Sequence = 1
	beat.Active = false
	var group sync.WaitGroup
	errs := make(chan error, 13)
	group.Go(func() {
		_, err := service.Heartbeat(ctx, beat)
		errs <- err
	})
	for range 12 {
		group.Go(func() {
			result, err := service.Start(ctx, in)
			if err == nil && (result.Session.ID != id ||
				result.Session.Visible != result.NextSequence*90) {
				err = fmt.Errorf("resume watermark and session snapshot disagree: %+v", result)
			}
			errs <- err
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	resumed, err = service.Start(ctx, in)
	if err != nil || resumed.NextSequence != 2 || resumed.Session.Visible != 180 {
		t.Fatal("resume missed concurrent committed beat", resumed, err)
	}
}

func workspaceProgressChecks(
	t *testing.T,
	pool *fixtureStore,
	base, document string,
	service workspace.Service,
	id int64,
	beat workspace.Beat,
) {
	t.Helper()
	var recorded workspace.BeatResult
	url := base + "/api/study/session/heartbeat"
	payload := map[string]any{
		"session_id":       fmt.Sprint(id),
		"sequence":         0,
		"page_number":      1,
		"interval_seconds": 3600,
		"document_id":      document,
		"source_hash":      beat.SourceHash,
		"active":           true,
	}
	apiJSON(t, "POST", url, payload, 200, &recorded)
	if recorded.Status != "duplicate" || recorded.Session.Active != 90 {
		t.Fatal("HTTP heartbeat retry", recorded)
	}
	payload["active"] = false
	apiJSON(t, "POST", url, payload, 409, nil)
	payload["sequence"] = 1
	apiJSON(t, "POST", url, payload, 200, &recorded)
	if recorded.Session.Active != 90 || recorded.Session.Visible != 180 {
		t.Fatal("inactive attention counted as study")
	}
	payload["sequence"] = 2
	payload["page_number"] = 100001
	apiJSON(t, "POST", url, payload, 422, nil)
	payload["page_number"] = 1
	payload["document_id"] = "other-course-document"
	apiJSON(t, "POST", url, payload, 404, nil)
	payload["document_id"] = document
	payload["source_hash"] = ""
	apiJSON(t, "POST", url, payload, 422, nil)
	beat = workspaceSourceChecks(t, pool, document, service, id, beat)
	workspaceFinishChecks(t, pool, base, document, service, id, beat)
}

func workspaceSourceChecks(
	t *testing.T, pool *fixtureStore, document string, service workspace.Service, id int64, beat workspace.Beat,
) workspace.Beat {
	t.Helper()
	ctx := t.Context()
	var original string
	if err := pool.Native.QueryRowContext(ctx,
		`SELECT source_hash FROM documents WHERE id=?`, document).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.ExecContext(ctx,
		`UPDATE documents SET source_hash='synthetic-second-revision' WHERE id=?`, document); err != nil {
		t.Fatal(err)
	}
	if result, err := service.Heartbeat(ctx, beat); err != nil || result.Status != "duplicate" {
		t.Fatal("committed old-source beat could not be replayed after replacement", result, err)
	}
	beat.Sequence = 2
	beat.Interval = 30
	if _, err := service.Heartbeat(ctx, beat); err != workspace.ErrSourceChanged {
		t.Fatal("old-source heartbeat was admitted against replaced bytes", err)
	}
	beat.SourceHash = "synthetic-second-revision"
	if _, err := service.Heartbeat(ctx, beat); err != nil {
		t.Fatal(err)
	}
	var spans int
	if err := pool.Native.QueryRowContext(ctx,
		`SELECT count(DISTINCT source_hash) FROM study_reading_spans WHERE session_id=?`, id).Scan(&spans); err != nil ||
		spans != 2 {
		t.Fatal("source revisions merged in reading ledger", spans, err)
	}
	if _, err := pool.Native.ExecContext(ctx,
		`UPDATE documents SET source_hash=? WHERE id=?`, original, document); err != nil {
		t.Fatal(err)
	}
	return beat
}

func workspaceFinishChecks(
	t *testing.T, pool *fixtureStore, base, document string, service workspace.Service, id int64, beat workspace.Beat,
) {
	t.Helper()
	ctx := t.Context()
	var finished workspace.FinishResult
	finish := map[string]any{"session_id": id, "outcome": "completed", "note": "  Ελληνικό\x00σημείωμα  "}
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 200, &finished)
	if finished.EventID != nil || finished.Minutes != 2 || finished.Session.Note == nil ||
		*finished.Session.Note != "Ελληνικό\x00σημείωμα" ||
		finished.Reading.Active != 120 {
		t.Fatal("standalone reading invented action progress or lost time", finished)
	}
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 200, &finished)
	finish["outcome"] = "abandoned"
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 409, nil)
	if result, err := service.Heartbeat(ctx, beat); err != nil || result.Status != "duplicate" {
		t.Fatal("already recorded beat retry failed after close", err)
	}
	beat.Sequence = 3
	if _, err := service.Heartbeat(ctx, beat); err != workspace.ErrConflict {
		t.Fatal("closed session accepted new reading", err)
	}
	workspaceActionChecks(t, pool, base, document)
	workspaceHiddenCourseChecks(t, pool, base)
}

func workspaceHiddenCourseChecks(t *testing.T, pool *fixtureStore, base string) {
	t.Helper()
	ctx := t.Context()
	if _, err := pool.Native.ExecContext(ctx, `UPDATE courses SET hidden=1 WHERE id=101`); err != nil {
		t.Fatal(err)
	}
	in := map[string]any{"course_id": 101, "session_key": "synthetic-hidden-reader"}
	apiJSON(t, "POST", base+"/api/study/session/start", in, 404, nil)
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO course_exam_plans(course_id,exam_at,enabled) VALUES(101,'2026-09-20',1)`); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "POST", base+"/api/study/session/start", in, 200, nil)
	if _, err := pool.Native.ExecContext(ctx, `UPDATE courses SET hidden=0 WHERE id=101`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.ExecContext(ctx, `DELETE FROM course_exam_plans WHERE course_id=101`); err != nil {
		t.Fatal(err)
	}
}

func workspaceActionChecks(t *testing.T, pool *fixtureStore, base, document string) {
	t.Helper()
	ctx := t.Context()
	// A synthetic published action exercises revision admission independently
	// of providers. The final navigation builder must satisfy the same contract.
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO roadmap_content(content_id,payload) VALUES('synthetic-roadmap','{}') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Native.ExecContext(ctx, `INSERT INTO roadmap_actions(course_id,action_id,ordinal,unit_key,payload) VALUES(101,'action-001',0,'unit-001','{"estimated_minutes":20}')`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Native.ExecContext(
		ctx,
		`INSERT INTO navigation(course_id,source_generation,config_generation,revision_id,overview,content_id)
 SELECT course_id,generation,?,'revision-001','{"usable":true}','synthetic-roadmap' FROM course_generation WHERE course_id=101
 ON CONFLICT(course_id) DO UPDATE SET source_generation=excluded.source_generation,config_generation=excluded.config_generation,revision_id=excluded.revision_id,overview=excluded.overview,content_id=excluded.content_id`,
		settings.DefaultAI().AnalysisGeneration(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Session workspace.Session `json:"session"`
	}
	start := map[string]any{
		"course_id":     101,
		"session_key":   "synthetic-action-001",
		"action_id":     "action-001",
		"unit_key":      "unit-001",
		"plan_revision": "old-revision",
	}
	apiJSON(t, "POST", base+"/api/study/session/start", start, 409, nil)
	start["plan_revision"] = "revision-001"
	apiJSON(t, "POST", base+"/api/study/session/start", start, 200, &response)
	id := response.Session.ID
	// No measured minutes means a partial outcome honestly becomes deferred.
	var finished workspace.FinishResult
	finish := map[string]any{"session_id": id, "outcome": "partial", "confidence": 3}
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 200, &finished)
	if finished.EventID == nil {
		t.Fatal("action outcome event missing")
	}
	event := *finished.EventID
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 200, &finished)
	if *finished.EventID != event {
		t.Fatal("finish replay duplicated progress")
	}
	var kind string
	if err = pool.Native.QueryRowContext(ctx, `SELECT event_type FROM study_unit_events WHERE id=?`, event).Scan(&kind); err != nil ||
		kind != "deferred" {
		t.Fatal("unmeasured partial invented progress", kind, err)
	}
	finish["confidence"] = 4
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 409, nil)
	workspaceFinishFaultCheck(t, pool, base, start)
}

func workspaceFinishFaultCheck(t *testing.T, pool *fixtureStore, base string, start map[string]any) {
	t.Helper()
	ctx := t.Context()
	start["session_key"] = "synthetic-action-atomic"
	var response struct {
		Session workspace.Session `json:"session"`
	}
	apiJSON(t, "POST", base+"/api/study/session/start", start, 200, &response)
	id := response.Session.ID
	_, err := pool.Native.ExecContext(
		ctx,
		`CREATE TRIGGER synthetic_finish_failure BEFORE UPDATE ON study_workspace_sessions WHEN NEW.ended_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'synthetic interrupted close'); END`,
	)
	if err != nil {
		t.Fatal(err)
	}
	finish := map[string]any{"session_id": id, "outcome": "completed"}
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 500, nil)
	var closed bool
	var events int64
	if err = pool.Native.QueryRowContext(ctx, `SELECT ended_at IS NOT NULL FROM study_workspace_sessions WHERE id=?`, id).Scan(&closed); err != nil ||
		closed {
		t.Fatal("failed finish closed sitting", err)
	}
	if err = pool.Native.QueryRowContext(ctx, `SELECT count(*) FROM study_unit_events WHERE idempotency_key=?`, fmt.Sprintf("workspace:%d:completed", id)).Scan(&events); err != nil ||
		events != 0 {
		t.Fatal("failed finish leaked progress", events, err)
	}
	if _, err = pool.Native.ExecContext(ctx, `DROP TRIGGER synthetic_finish_failure`); err != nil {
		t.Fatal(err)
	}
	var finished workspace.FinishResult
	apiJSON(t, "POST", base+"/api/study/session/finish", finish, 200, &finished)
	if finished.EventID == nil || finished.Session.Ended == nil {
		t.Fatal("finish did not recover after transactional failure")
	}
}
