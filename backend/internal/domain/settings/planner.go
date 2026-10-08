package settings

import (
	"context"
	"encoding/json"
	"strconv"

	"tree-eclass/internal/domain/database"
)

type Planner struct {
	DailyBlocks  int64            `json:"daily_blocks"`
	BlockMinutes int64            `json:"block_minutes"`
	Weekly       map[string]int64 `json:"weekly_minutes"`
	Blackouts    []string         `json:"blackout_dates"`
	MaxCourses   int64            `json:"max_courses_per_day"`
}

func defaultPlanner() Planner {
	p := Planner{DailyBlocks: 6, BlockMinutes: 50, Weekly: map[string]int64{}, Blackouts: []string{}, MaxCourses: 2}
	for i := range 7 {
		minutes := int64(150)
		if i >= 5 {
			minutes = 400
		}
		p.Weekly[strconv.Itoa(i)] = minutes
	}
	return p
}

// ReadPlanner accepts store or transaction operations so derived reads
// share their source snapshot.
func ReadPlanner(ctx context.Context, db database.Operations) (Planner, error) {
	p := defaultPlanner()
	stored, err := db.Settings().LoadPlannerSettings(ctx)
	if database.IsNoRows(err) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	p.DailyBlocks = stored.DailyBlocks
	p.BlockMinutes = stored.BlockMinutes
	p.MaxCourses = stored.MaxCourses
	values := map[string]int64{}
	_ = json.Unmarshal([]byte(stored.WeeklyJSON), &values)
	for i := range 7 {
		key := strconv.Itoa(i)
		p.Weekly[key] = max(0, values[key])
	}
	if json.Unmarshal([]byte(stored.BlackoutsJSON), &p.Blackouts) != nil || p.Blackouts == nil {
		p.Blackouts = []string{}
	}
	p.MaxCourses = max(1, p.MaxCourses)
	return p, nil
}
