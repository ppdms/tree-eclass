package practice

import "testing"

func TestQuestionProgressTransitions(t *testing.T) {
	correct, wrong := "correct", "incorrect"
	for _, item := range []struct {
		Attempts, Streak int64
		Last             *string
		State            string
		Rank             int
	}{
		{0, 0, nil, "unattempted", 1}, {1, 0, &wrong, "needs_work", 0}, {1, 1, &correct, "review", 2}, {2, 2, &correct, "learned", 3}, {3, 0, &wrong, "needs_work", 0},
	} {
		if state, rank := questionState(item.Attempts, item.Streak, item.Last); state != item.State ||
			rank != item.Rank {
			t.Fatal(item, state, rank)
		}
	}
}
