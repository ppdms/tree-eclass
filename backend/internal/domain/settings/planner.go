package settings

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"tree-eclass/internal/infrastructure/rdbms"
)

type Planner struct {
	DailyBlocks  int64            `json:"daily_blocks"`
	BlockMinutes int64            `json:"block_minutes"`
	Weekly       map[string]int64 `json:"weekly_minutes"`
	Blackouts    []string         `json:"blackout_dates"`
	MaxCourses   int64            `json:"max_courses_per_day"`
}

func ReadPlanner(ctx context.Context, db queryer) (Planner, error) {
	p := Planner{DailyBlocks: 6, BlockMinutes: 50, Weekly: map[string]int64{}, Blackouts: []string{}, MaxCourses: 2}
	for i := range 7 {
		minutes := int64(150)
		if i >= 5 {
			minutes = 400
		}
		p.Weekly[strconv.Itoa(i)] = minutes
	}
	var weekly, blackouts string
	err := db.QueryRow(ctx, `SELECT daily_blocks,block_minutes,weekly_minutes_json,blackout_dates_json,max_courses_per_day FROM app.study_planner_settings WHERE id=1`).
		Scan(&p.DailyBlocks, &p.BlockMinutes, &weekly, &blackouts, &p.MaxCourses)
	if errors.Is(err, rdbms.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	values := map[string]int64{}
	_ = json.Unmarshal([]byte(weekly), &values)
	for i := range 7 {
		key := strconv.Itoa(i)
		p.Weekly[key] = max(0, values[key])
	}
	if json.Unmarshal([]byte(blackouts), &p.Blackouts) != nil || p.Blackouts == nil {
		p.Blackouts = []string{}
	}
	p.MaxCourses = max(1, p.MaxCourses)
	return p, nil
}
