package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func Build(ctx context.Context, in Input) (Result, error) {
	result := Result{
		Start:    in.Start,
		End:      in.Start,
		Days:     []Day{},
		Runways:  []map[string]any{},
		Warnings: []string{},
		Feasible: true,
	}
	today, err := prepare(&in)
	if err != nil {
		return result, err
	}
	actions, courses, warnings, err := normalize(in, today)
	if err != nil {
		return result, err
	}
	if len(in.Stuck) > 0 {
		warnings = append(
			warnings,
			"One or more actions are marked stuck; use their evidence links to diagnose the gap before retrying.",
		)
	}
	if len(actions) == 0 {
		result.Warnings = unique(warnings)
		result.Feasible = len(warnings) == 0
		return result, nil
	}
	end, err := horizon(actions, courses, &result, today)
	if err != nil {
		return result, err
	}
	blackouts := blackoutDays(in)
	deferred, stuck := set(in.Deferred), set(in.Stuck)
	done := completedUnits(in, courses)
	warnings = feasibilityWarnings(actions, today, in, blackouts, warnings)
	err = scheduleDays(ctx, &result, in, actions, courses, today, end, blackouts, deferred, stuck, done)
	if err != nil {
		return result, err
	}
	report(&result, in, courses, today, blackouts, warnings)
	return result, nil
}

// prepare parses the start date and clamps the request bounds in place.
func prepare(in *Input) (time.Time, error) {
	today, err := date(in.Start)
	if err != nil {
		return time.Time{}, err
	}
	in.Session = max(15, in.Session)
	in.MaxCourses = max(1, in.MaxCourses)
	if in.Session > 1440 || len(in.Blackouts) > 3660 {
		return time.Time{}, errors.New("scheduler availability exceeds its bounds")
	}
	for _, minutes := range in.Weekly {
		if minutes > 1440 {
			return time.Time{}, errors.New("daily availability cannot exceed 1440 minutes")
		}
	}
	return today, nil
}

// horizon totals required minutes and validates the exam horizon.
func horizon(actions []*work, courses map[int64]*course, result *Result, today time.Time) (time.Time, error) {
	end := today
	for _, a := range actions {
		if a.ExamDate.After(end) {
			end = a.ExamDate
		}
		courses[a.CourseID].Remaining += a.Remaining
		courses[a.CourseID].Required += a.Remaining
		result.Required += a.Remaining
	}
	end = end.AddDate(0, 0, -1)
	if end.After(today.AddDate(10, 0, 0)) {
		return time.Time{}, errors.New("scheduler exam horizon exceeds ten years")
	}
	result.End = end.Format(time.DateOnly)
	return end, nil
}

func blackoutDays(in Input) map[string]bool {
	blackouts := map[string]bool{}
	for _, value := range in.Blackouts {
		if day, err := date(value); err == nil {
			blackouts[day.Format(time.DateOnly)] = true
		}
	}
	return blackouts
}

func scheduleDays(
	ctx context.Context,
	result *Result,
	in Input,
	actions []*work,
	courses map[int64]*course,
	today, end time.Time,
	blackouts, deferred, stuck map[string]bool,
	done map[int64]map[string]bool,
) error {
	slots := 0
	for day := today; !day.After(end); day = day.AddDate(0, 0, 1) {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, err := fillDay(ctx, actions, courses, day, today, in, blackouts, deferred, stuck, done)
		if err != nil {
			return err
		}
		slots += len(row.Sessions)
		if slots > 64000 {
			return errors.New("scheduler exceeds 64000 sessions")
		}
		if len(row.Sessions) > 0 {
			result.Days = append(result.Days, row)
			result.Scheduled += row.Scheduled
		}
		if result.Scheduled == result.Required {
			break
		}
	}
	return nil
}

func completedUnits(in Input, courses map[int64]*course) map[int64]map[string]bool {
	done := map[int64]map[string]bool{}
	complete := set(in.Completed)
	for _, p := range in.Plans {
		if courses[p.ID] == nil {
			continue
		}
		done[p.ID] = map[string]bool{}
		for _, u := range p.Units {
			all := len(u.Actions) > 0
			for i, a := range u.Actions {
				id := a.ID
				if id == "" {
					id = fmt.Sprintf("%d:%s:%d", p.ID, u.Key, i+1)
				}
				if !complete[id] {
					all = false
				}
			}
			if all {
				done[p.ID][u.Key] = true
			}
		}
	}
	return done
}

func fillDay(
	ctx context.Context,
	actions []*work,
	courses map[int64]*course,
	day, today time.Time,
	in Input,
	blackouts, deferred, stuck map[string]bool,
	done map[int64]map[string]bool,
) (Day, error) {
	row := Day{
		Date:      day.Format(time.DateOnly),
		Available: availability(day, in, blackouts),
		Sessions:  []Session{},
		Blackout:  blackouts[day.Format(time.DateOnly)],
	}
	left := row.Available
	todayCourses := map[int64]bool{}
	unusable := map[int]bool{}
	for left >= 15 {
		if err := ctx.Err(); err != nil {
			return row, err
		}
		index := pick(actions, courses, day, today, in, blackouts, deferred, stuck, done, todayCourses, unusable)
		if index < 0 {
			break
		}
		a := actions[index]
		minutes := allocation(a.Remaining, in.Session, left)
		if minutes < 15 {
			unusable[index] = true
			continue
		}
		a.Remaining -= minutes
		left -= minutes
		row.Scheduled += minutes
		courses[a.CourseID].Remaining -= minutes
		courses[a.CourseID].Scheduled += minutes
		todayCourses[a.CourseID] = true
		session := a.Session
		session.Minutes = minutes
		session.Days = (a.ExamDate.Unix() - day.Unix()) / 86400
		row.Sessions = append(row.Sessions, session)
		markUnitDone(actions, a, done)
	}
	return row, nil
}

func markUnitDone(actions []*work, a *work, done map[int64]map[string]bool) {
	if a.Remaining != 0 {
		return
	}
	for _, peer := range actions {
		if peer.CourseID == a.CourseID && peer.UnitKey == a.UnitKey && peer.Remaining > 0 {
			return
		}
	}
	done[a.CourseID][a.UnitKey] = true
}
