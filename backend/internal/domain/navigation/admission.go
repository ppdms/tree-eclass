package navigation

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

var ErrActionConflict = errors.New("action is not in the current published roadmap")

// AdmitAction keeps the generation and exact action membership locked through
// the caller's write. Callers first lock the course and verify study visibility.
func AdmitAction(ctx context.Context, tx rdbms.Tx, course int64, action, revision string) (string, error) {
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=$1 FOR SHARE`, course).Scan(&generation); err != nil {
		return "", err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return "", err
	}
	var unit string
	err = tx.QueryRow(ctx, `SELECT a.unit_key FROM read_model.navigation n JOIN read_model.roadmap_actions a USING(course_id)
 WHERE n.course_id=$1 AND a.action_id=$2 AND n.source_generation=$3 AND n.config_generation=$4 AND n.revision_id=$5
 AND n.overview->>'usable'='true' FOR SHARE OF n,a`, course, identity.Encode(action), generation, a.AnalysisGeneration(), identity.Encode(revision)).
		Scan(&unit)
	if errors.Is(err, rdbms.ErrNoRows) {
		return "", ErrActionConflict
	}
	return identity.Decode(unit), err
}
