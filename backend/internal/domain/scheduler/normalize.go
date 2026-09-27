package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

func normalize(in Input, today time.Time) ([]*work, map[int64]*course, []string, error) {
	if len(in.Plans) > 200 {
		return nil, nil, nil, errors.New("scheduler exceeds 200 courses")
	}
	actions := []*work{}
	courses := map[int64]*course{}
	warnings := []string{}
	complete := set(in.Completed)
	seen := map[string]bool{}
	for _, p := range in.Plans {
		if p.ID < 1 || courses[p.ID] != nil {
			return nil, nil, nil, errors.New("invalid or duplicate scheduler course")
		}
		if p.Name == "" {
			p.Name = fmt.Sprint(p.ID)
		}
		c, w, err := planCourse(p, today, warnings)
		if err != nil {
			return nil, nil, nil, err
		}
		warnings = w
		if c == nil {
			continue
		}
		courses[p.ID] = c
		actions, err = appendWorks(actions, c.Plan, c.Date, complete, seen, in)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	sort.SliceStable(actions, func(i, j int) bool { return lessAction(actions[i], actions[j]) })
	return actions, courses, warnings, nil
}

// planCourse validates one plan, applies commitment and importance defaults and
// reports unusable plans through warnings. A nil course means the plan is skipped.
func planCourse(p Plan, today time.Time, warnings []string) (*course, []string, error) {
	exam, err := date(p.Exam)
	if err != nil {
		warnings = append(warnings, p.Name+": missing or invalid exam date.")
		return nil, warnings, nil
	}
	if !exam.After(today) {
		warnings = append(warnings, p.Name+": no preparation time remains before the exam.")
		return nil, warnings, nil
	}
	if _, ok := commitments[p.Commitment]; !ok {
		p.Commitment = "committed"
	}
	if p.Commitment == "skipped" {
		return nil, warnings, nil
	}
	if math.IsNaN(p.Importance) || math.IsInf(p.Importance, 0) {
		return nil, warnings, errors.New("non-finite course importance")
	}
	if p.Importance == 0 {
		p.Importance = 1
	}
	p.Importance = max(.25, p.Importance)
	return &course{Plan: p, Date: exam}, warnings, nil
}

// appendWorks maps one plan's units and actions into planning work entries.
func appendWorks(actions []*work, p Plan, exam time.Time, complete, seen map[string]bool, in Input) ([]*work, error) {
	for i, u := range p.Units {
		if u.Key == "" {
			u.Key = fmt.Sprintf("unit-%d", i+1)
		}
		if u.Title == "" {
			u.Title = u.Key
		}
		if u.Order == 0 {
			u.Order = i + 1
		}
		if math.IsNaN(u.Value) || math.IsInf(u.Value, 0) {
			return nil, errors.New("non-finite exam value")
		}
		u.Value = min(1, max(0, u.Value))
		for j, a := range u.Actions {
			w, err := actionWork(p, u, a, j, exam, complete, seen, in)
			if err != nil {
				return nil, err
			}
			if w == nil {
				continue
			}
			actions = append(actions, w)
			if len(actions) > 4000 {
				return nil, errors.New("scheduler exceeds 4000 active actions")
			}
		}
	}
	return actions, nil
}

// actionWork applies action defaults and builds the planning entry. A nil work
// with no error means the action is already completed.
func actionWork(
	p Plan,
	u Unit,
	a Action,
	j int,
	exam time.Time,
	complete, seen map[string]bool,
	in Input,
) (*work, error) {
	if a.ID == "" {
		a.ID = fmt.Sprintf("%d:%s:%d", p.ID, u.Key, j+1)
	}
	if seen[a.ID] {
		return nil, errors.New("duplicate scheduler action")
	}
	seen[a.ID] = true
	if complete[a.ID] {
		return nil, nil
	}
	if a.Minutes == 0 {
		a.Minutes = 50
	}
	if a.Minutes > 1440 {
		return nil, errors.New("scheduler action exceeds 1440 minutes")
	}
	a.Minutes = max(15, a.Minutes)
	if _, ok := weights[a.Kind]; !ok {
		a.Kind = "learn"
	}
	if a.BlueprintKind == "" {
		a.BlueprintKind = a.Kind
	}
	if a.Instruction == "" {
		a.Instruction = u.Title
	}
	evidence := a.Evidence
	if len(evidence) == 0 {
		evidence = u.Evidence
	}
	if evidence == nil {
		evidence = []string{}
	}
	progress := max(0, min(max(0, a.Minutes-15), in.Progress[a.ID]))
	return newWork(p, u, a, j, exam, evidence, progress), nil
}

func newWork(p Plan, u Unit, a Action, j int, exam time.Time, evidence []string, progress int64) *work {
	return &work{
		Session: Session{
			ActionID:      a.ID,
			Revision:      p.Revision,
			CourseID:      p.ID,
			CourseName:    p.Name,
			UnitKey:       u.Key,
			UnitTitle:     u.Title,
			UnitObjective: u.Objective,
			UnitPriority:  u.Priority,
			Kind:          a.Kind,
			BlueprintKind: a.BlueprintKind,
			Instruction:   a.Instruction,
			Success:       a.Success,
			Exam:          exam.Format(time.DateOnly),
			Evidence:      evidence,
		},
		ExamDate:      exam,
		Commitment:    p.Commitment,
		Importance:    p.Importance,
		Value:         u.Value,
		Order:         u.Order,
		ActionOrder:   j,
		Prerequisites: u.Prerequisites,
		Remaining:     a.Minutes - progress,
	}
}

func lessAction(a, b *work) bool {
	if !a.ExamDate.Equal(b.ExamDate) {
		return a.ExamDate.Before(b.ExamDate)
	}
	if a.CourseID != b.CourseID {
		return a.CourseID < b.CourseID
	}
	if a.Order != b.Order {
		return a.Order < b.Order
	}
	if a.ActionOrder != b.ActionOrder {
		return a.ActionOrder < b.ActionOrder
	}
	return a.ActionID < b.ActionID
}
