package blueprints

import "errors"

func (b *Blueprint) validateCollections(known, units map[string]bool) error {
	if err := b.validateFamilies(known, units); err != nil {
		return err
	}
	if err := b.validateConflicts(known); err != nil {
		return err
	}
	for i := range b.Gaps {
		g := &b.Gaps[i]
		if err := text(&g.Summary, 1500); err != nil {
			return err
		}
		if err := text(&g.Action, 1500); err != nil {
			return err
		}
		if err := enum(g.Severity, []string{"blocking", "important", "minor"}); err != nil {
			return err
		}
		if err := references(g.Evidence, known, 0); err != nil {
			return err
		}
	}
	return nil
}

func (b *Blueprint) validateFamilies(known, units map[string]bool) error {
	families := []Family{}
	seen := map[string]bool{}
	for _, family := range b.Families {
		if err := key(&family.Key); err != nil {
			return err
		}
		if seen[family.Key] {
			return errors.New("duplicate question family")
		}
		if family.Observed < 0 || family.OutOf < 0 || family.OutOf > 100 || family.Observed > family.OutOf {
			return errors.New("invalid question observation counts")
		}
		if err := textList(family.Units, 1, 100, true); err != nil {
			return err
		}
		family.Units = resolveKeys(family.Units, units)
		if len(family.Units) == 0 {
			continue
		}
		seen[family.Key] = true
		if err := family.validate(known); err != nil {
			return err
		}
		families = append(families, family)
	}
	b.Families = families
	return nil
}

func (b *Blueprint) validateConflicts(known map[string]bool) error {
	conflicts := []Conflict{}
	for _, conflict := range b.Conflicts {
		if err := references(conflict.Evidence, known, 0); err != nil {
			return err
		}
		if len(conflict.Evidence) < 2 {
			continue
		}
		if err := text(&conflict.Summary, 1500); err != nil {
			return err
		}
		if err := text(&conflict.Resolution, 1500); err != nil {
			return err
		}
		if err := enum(conflict.Status, []string{"unresolved", "resolved", "superseded"}); err != nil {
			return err
		}
		conflicts = append(conflicts, conflict)
	}
	b.Conflicts = conflicts
	return nil
}

func (f *Family) validate(known map[string]bool) error {
	if err := text(&f.Name, 300); err != nil {
		return err
	}
	if err := enum(f.Mode, []string{"multiple_choice", "short_answer", "essay", "calculation", "proof", "diagram", "code", "mixed"}); err != nil {
		return err
	}
	if err := enum(f.Priority, priorities); err != nil {
		return err
	}
	if err := enum(f.Confidence, confidences); err != nil {
		return err
	}
	if !(f.Marks >= 0 && f.Marks <= 100) {
		return errors.New("invalid estimated marks percentage")
	}
	return references(f.Evidence, known, 1)
}
