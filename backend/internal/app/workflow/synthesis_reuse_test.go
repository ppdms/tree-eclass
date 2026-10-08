package workflow

import (
	"testing"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/services/synthesis"
)

func synthesisReuseChecks(t *testing.T, pool rdbms.Pool, s synthesis.Service) {
	t.Helper()
	ctx := t.Context()
	var first, question string
	if err := pool.QueryRow(ctx, `SELECT revision_hash FROM knowledge.course_blueprints WHERE status='ready'`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT question_id FROM knowledge.practice_questions LIMIT 1`).Scan(&question); err != nil {
		t.Fatal(err)
	}
	for _, notes := range []any{"Alternative goal", nil} {
		if _, err := pool.Exec(ctx, `UPDATE app.course_exam_plans SET planning_notes=$1 WHERE course_id=781`, notes); err != nil {
			t.Fatal(err)
		}
		for _, lane := range []string{"course", "practice"} {
			if worked, err := s.RunOne(ctx, lane); err != nil || !worked {
				t.Fatal("revision reactivation", notes, lane, worked, err)
			}
		}
	}
	var current, currentQuestion string
	var revision int64
	if err := pool.QueryRow(ctx, `SELECT revision_hash,revision FROM knowledge.course_blueprints WHERE status='ready'`).Scan(&current, &revision); err != nil ||
		current != first ||
		revision != 3 {
		t.Fatal("exact revision reuse", current, revision, err)
	}
	if err := pool.QueryRow(ctx, `SELECT q.question_id FROM knowledge.practice_questions q JOIN knowledge.practice_question_sets s ON s.id=q.set_id WHERE s.status='ready'`).Scan(&currentQuestion); err != nil ||
		currentQuestion != question {
		t.Fatal("question identity changed across regeneration", currentQuestion, err)
	}
}
