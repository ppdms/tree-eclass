package blueprints

import "errors"

func (u *Unit) validate(known, earlier map[string]bool) error {
	if err := key(&u.Key); err != nil {
		return err
	}
	if earlier[u.Key] {
		return errors.New("duplicate blueprint unit")
	}
	if err := textList(u.Depends, 0, 30, true); err != nil {
		return err
	}
	u.Depends = resolveKeys(u.Depends, earlier)
	if len(u.Actions) < 1 || len(u.Actions) > 20 {
		return errors.New("unit requires 1 to 20 actions")
	}
	u.Estimated = 0
	for i := range u.Actions {
		if err := u.Actions[i].validate(known); err != nil {
			return err
		}
		u.Estimated += u.Actions[i].Estimated
	}
	if u.Estimated < 5 || u.Estimated > 720 {
		return errors.New("unit actions must total 5 to 720 minutes")
	}
	if err := text(&u.Title, 300); err != nil {
		return err
	}
	if err := text(&u.Objective, 1000); err != nil {
		return err
	}
	if err := enum(u.Priority, priorities); err != nil {
		return err
	}
	return references(u.Evidence, known, 1)
}

func (a *Action) validate(known map[string]bool) error {
	if a.Estimated < 5 || a.Estimated > 240 {
		return errors.New("action requires 5 to 240 minutes")
	}
	if err := enum(a.Kind, []string{"read", "recall", "solve", "write", "draw", "implement", "compare", "review", "diagnostic"}); err != nil {
		return err
	}
	if err := text(&a.Instruction, 1200); err != nil {
		return err
	}
	if err := text(&a.Success, 800); err != nil {
		return err
	}
	return references(a.Evidence, known, 1)
}

func (s *Strategy) validate(known, units map[string]bool) error {
	if err := textList(s.Priorities, 1, 100, true); err != nil {
		return err
	}
	s.Priorities = resolveKeys(s.Priorities, units)
	if len(s.Priorities) == 0 {
		return errors.New("exam strategy names no existing unit")
	}
	if err := text(&s.Objective, 1500); err != nil {
		return err
	}
	if err := text(&s.Approach, 3000); err != nil {
		return err
	}
	if !(s.Target >= 0 && s.Target <= 100) {
		return errors.New("invalid target score percentage")
	}
	if err := enum(s.Confidence, confidences); err != nil {
		return err
	}
	return references(s.Evidence, known, 1)
}
