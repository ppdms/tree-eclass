package navigation

import (
	"context"
	"errors"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
)

var ErrActionConflict = errors.New("action is not in the current published roadmap")

// AdmitAction keeps the generation and exact action membership locked through
// the caller's write. Callers first lock the course and verify study visibility.
func AdmitAction(ctx context.Context, tx database.Tx, course int64, action, revision string) (string, error) {
	generation, err := tx.Navigation().LockedGeneration(ctx, course)
	if err != nil {
		return "", err
	}
	a, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return "", err
	}
	unit, err := tx.Navigation().AdmitActionUnit(ctx, database.NavigationAdmission{
		CourseID:   course,
		Action:     identity.Encode(action),
		Generation: generation,
		Config:     a.AnalysisGeneration(),
		Revision:   identity.Encode(revision),
	})
	if errors.Is(err, database.ErrNoRows) {
		return "", ErrActionConflict
	}
	if err != nil {
		return "", err
	}
	return identity.Decode(unit), nil
}
