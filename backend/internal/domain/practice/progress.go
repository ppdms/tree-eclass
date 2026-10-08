package practice

import (
	"context"
	"sort"

	"tree-eclass/internal/domain/database"
)

func questionState(attempts, streak int64, last *string) (string, int) {
	if attempts == 0 {
		return "unattempted", 1
	}
	if last == nil || *last != "correct" {
		return "needs_work", 0
	}
	if streak >= 2 {
		return "learned", 3
	}
	return "review", 2
}

func readAttempts(ctx context.Context, tx database.Tx, course int64, units []*Unit) error {
	ids := []string{}
	byID := map[string]*Question{}
	for _, unit := range units {
		for _, q := range unit.Questions {
			ids = append(ids, q.ID)
			byID[q.ID] = q
		}
	}
	progress, err := tx.Practice().QuestionProgress(ctx, course, ids)
	if err != nil {
		return err
	}
	for i := range progress {
		row := progress[i]
		q := byID[row.Question]
		q.Attempts, q.Streak, q.Last, q.Attempted = row.Attempts, row.Streak, row.Last, row.AttemptedAt
		q.State, q.Rank = questionState(row.Attempts, row.Streak, row.Last)
	}
	return nil
}

func summarize(view *View) {
	view.Totals["units"] = int64(len(view.Units))
	for _, unit := range view.Units {
		sort.SliceStable(unit.Questions, func(i, j int) bool {
			a, b := unit.Questions[i], unit.Questions[j]
			if a.Rank != b.Rank {
				return a.Rank < b.Rank
			}
			return a.Key < b.Key
		})
		unit.Count = len(unit.Questions)
		for _, q := range unit.Questions {
			view.Totals["questions"]++
			if q.State == "needs_work" || q.State == "unattempted" {
				unit.Due++
				view.Totals["due"]++
			}
			if q.Attempts > 0 {
				view.Totals["attempted"]++
			}
			if q.State == "learned" {
				view.Totals["learned"]++
			}
		}
	}
}
