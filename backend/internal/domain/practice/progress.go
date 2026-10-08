package practice

import (
	"context"
	"sort"

	"tree-eclass/internal/infrastructure/rdbms"
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

func readAttempts(ctx context.Context, tx rdbms.Tx, course int64, units []*Unit) error {
	ids := []string{}
	byID := map[string]*Question{}
	for _, unit := range units {
		for _, q := range unit.Questions {
			ids = append(ids, q.ID)
			byID[q.ID] = q
		}
	}
	rows, err := tx.Query(ctx, `WITH history AS (
 SELECT question_id,outcome,attempted_at,row_number() OVER(PARTITION BY question_id ORDER BY attempted_at DESC,id DESC) rn
 FROM app.practice_attempts WHERE course_id=$1 AND question_id=ANY($2::text[])
) SELECT question_id,count(*),coalesce(min(rn) FILTER(WHERE outcome<>'correct'),count(*)+1)-1,
 max(outcome) FILTER(WHERE rn=1),max(attempted_at) FILTER(WHERE rn=1) FROM history GROUP BY question_id`, course, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var attempts, streak int64
		var last, at *string
		if err = rows.Scan(&id, &attempts, &streak, &last, &at); err != nil {
			return err
		}
		q := byID[id]
		q.Attempts, q.Streak, q.Last, q.Attempted = attempts, streak, last, at
		q.State, q.Rank = questionState(attempts, streak, last)
	}
	return rows.Err()
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
