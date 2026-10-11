package workflow

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"

	"tree-eclass/internal/domain/blueprints"
	"tree-eclass/internal/domain/practice"
	"tree-eclass/internal/domain/settings"
)

func practiceChecks(t *testing.T, pool *fixtureStore, base, document string, refresh func()) {
	t.Helper()
	ctx := t.Context()
	raw, err := os.ReadFile("../../domain/blueprints/testdata/practice.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct{ Expected blueprints.PracticeSet }
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	question := fixtures[0].Expected.Questions[0]
	question.Evidence = []string{"document:" + document}
	encoded, _ := json.Marshal(question)
	a := settings.DefaultAI()
	var setID int64
	err = pool.Native.QueryRowContext(ctx, `INSERT INTO practice_question_sets(course_id,unit_key,set_hash,blueprint_revision_hash,evidence_hash,evidence_packet_json,analysis_version,status,requested_model,model,available_at,created_at)
 VALUES(101,'unit_one','practice-set-1','build-r1','evidence-1','{}',?,'ready',?,'fallback-practice-model',strftime('%Y-%m-%dT%H:%M:%fZ','now'),strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING id`, settings.PracticeAnalysisVersion, a.PracticeModel).
		Scan(&setID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Native.ExecContext(
		ctx,
		`INSERT INTO practice_questions(question_id,set_id,course_id,unit_key,ordinal,question_key,response_mode,difficulty,estimated_minutes,payload_json)
 VALUES(?,?,101,'unit_one',1,?,?,?,?,?)`,
		question.ID,
		setID,
		question.Key,
		question.Mode,
		question.Difficulty,
		question.Minutes,
		string(encoded),
	)
	if err != nil {
		t.Fatal(err)
	}
	refresh()
	var view practice.View
	url := base + "/api/study/practice?course_id=101"
	apiJSON(t, "GET", url, nil, 200, &view)
	if len(view.Units) != 1 || view.Totals["questions"] != 1 || view.Units[0].Questions[0].ID != question.ID ||
		view.Units[0].Questions[0].State != "unattempted" {
		t.Fatal("practice current set", view)
	}
	apiJSON(t, "GET", url+"&unit_key=missing", nil, 200, &view)
	if len(view.Units) != 0 {
		t.Fatal("unit filter leaked questions")
	}
	practiceAttemptChecks(t, pool, base, question.ID)
	practiceStaleChecks(t, pool, base, refresh, setID, question.ID)
}

func practiceStaleChecks(
	t *testing.T,
	pool *fixtureStore,
	base string,
	refresh func(),
	setID int64,
	question string,
) {
	t.Helper()
	ctx := t.Context()
	var view practice.View
	url := base + "/api/study/practice?course_id=101"
	var err error
	if _, err = pool.Native.ExecContext(ctx, `UPDATE practice_question_sets SET requested_model='obsolete-model' WHERE id=?`, setID); err != nil {
		t.Fatal(err)
	}
	apiJSON(t, "GET", url, nil, 200, &view)
	if len(view.Units) != 0 {
		t.Fatal("stale navigation exposed practice")
	}
	refresh()
	apiJSON(t, "GET", url, nil, 200, &view)
	if len(view.Units) != 0 {
		t.Fatal("wrong-model practice exposed")
	}
	apiJSON(
		t,
		"POST",
		base+"/api/study/practice/attempt",
		practice.Attempt{CourseID: 101, Question: question, Outcome: "correct", Key: "obsolete-question-attempt"},
		409,
		nil,
	)
	if _, err = pool.Native.ExecContext(ctx, `DELETE FROM practice_question_sets WHERE id=?`, setID); err != nil {
		t.Fatal(err)
	}
	refresh()
}

func practiceAttemptChecks(t *testing.T, pool *fixtureStore, base, question string) {
	t.Helper()
	ctx := t.Context()
	service := practice.Service{Pool: pool}
	answer := "Δένδρα\x00\ue0000"
	in := practice.Attempt{
		CourseID: 101,
		Question: question,
		Outcome:  "correct",
		Key:      "practice-retry-001",
		Answer:   &answer,
	}
	var wg sync.WaitGroup
	results := make(chan practice.Attempt, 12)
	failures := make(chan error, 12)
	for range 12 {
		wg.Go(func() { result, err := service.Record(ctx, in); results <- result; failures <- err })
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id, event int64
	for result := range results {
		if id == 0 {
			id, event = result.ID, result.EventID
		}
		if result.ID != id || result.EventID != event || result.Answer == nil || *result.Answer != answer {
			t.Fatal("practice retry duplicated identity/text", result)
		}
	}
	var view practice.View
	url := base + "/api/study/practice?course_id=101"
	apiJSON(t, "GET", url, nil, 200, &view)
	if q := view.Units[0].Questions[0]; q.Attempts != 1 || q.State != "review" {
		t.Fatal("first correct answer progress", q)
	}
	apiJSON(t, "POST", base+"/api/study/practice/attempt", in, 200, nil)
	in.Outcome = "incorrect"
	apiJSON(t, "POST", base+"/api/study/practice/attempt", in, 409, nil)
	in.Outcome = "correct"
	in.Key = strings.Repeat("a", 127) + "1"
	apiJSON(t, "POST", base+"/api/study/practice/attempt", in, 200, nil)
	apiJSON(t, "GET", url, nil, 200, &view)
	if q := view.Units[0].Questions[0]; q.Attempts != 2 || q.State != "learned" || q.Streak != 2 {
		t.Fatal("correct streak", q)
	}
	in.Outcome = "incorrect"
	in.Key = strings.Repeat("a", 127) + "2"
	apiJSON(t, "POST", base+"/api/study/practice/attempt", in, 200, nil)
	apiJSON(t, "GET", url, nil, 200, &view)
	if q := view.Units[0].Questions[0]; q.Attempts != 3 || q.State != "needs_work" || q.Streak != 0 {
		t.Fatal("failed recall did not return question to queue", q)
	}
	practiceAtomicityChecks(t, pool, service, in)
	for _, query := range []string{`DELETE FROM practice_attempts WHERE question_id=?`, `DELETE FROM study_unit_events WHERE action_id=?`} {
		if _, err := pool.Native.ExecContext(ctx, query, question); err != nil {
			t.Fatal(err)
		}
	}
}

func practiceAtomicityChecks(t *testing.T, pool *fixtureStore, service practice.Service, in practice.Attempt) {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Native.ExecContext(
		ctx,
		`CREATE TRIGGER synthetic_attempt_failure BEFORE INSERT ON practice_attempts BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END`,
	)
	if err != nil {
		t.Fatal(err)
	}
	in.Key = "practice-atomic-001"
	if _, err = service.Record(ctx, in); err == nil {
		t.Fatal("injected attempt failure succeeded")
	}
	var attempts, events int64
	err = pool.Native.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM practice_attempts WHERE question_id=?),(SELECT count(*) FROM study_unit_events WHERE action_id=?)`, in.Question, in.Question).
		Scan(&attempts, &events)
	if err != nil || attempts != 3 || events != 3 {
		t.Fatal("failed attempt left an orphan event", attempts, events, err)
	}
	if _, err = pool.Native.ExecContext(ctx, `DROP TRIGGER synthetic_attempt_failure`); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Record(ctx, in); err != nil {
		t.Fatal("retry after interrupted transaction", err)
	}
}
