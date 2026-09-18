package blueprints

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

type PracticeQuestion struct {
	Key        string   `json:"key"`
	Prompt     string   `json:"prompt"`
	Mode       string   `json:"response_mode"`
	Difficulty string   `json:"difficulty"`
	Minutes    int64    `json:"estimated_minutes"`
	Answer     string   `json:"expected_answer"`
	Points     []string `json:"answer_points"`
	Mistakes   []string `json:"common_mistakes"`
	Evidence   []string `json:"evidence_refs"`
}
type Question struct {
	PracticeQuestion
	ID string `json:"question_id"`
}
type PracticeSet struct {
	Version   string     `json:"practice_set_version"`
	Unit      string     `json:"unit_key"`
	Questions []Question `json:"questions"`
	Coverage  string     `json:"coverage_note"`
}

func QuestionIdentity(course int64, unit, prompt, mode string) string {
	words := strings.FieldsFunc(prompt, func(r rune) bool { return unicode.IsSpace(r) || r >= 0x1c && r <= 0x1f })
	raw, _ := json.Marshal(
		map[string]any{
			"course_id":     fmt.Sprint(course),
			"unit_key":      strings.TrimSpace(unit),
			"prompt":        strings.Join(words, " "),
			"response_mode": strings.TrimSpace(mode),
		},
	)
	hash, _ := PayloadHash(raw)
	return fmt.Sprintf("practice-question:%d:%s:%s", course, unit, hash[:24])
}

func (q *PracticeQuestion) Validate(known map[string]bool) error {
	if err := key(&q.Key); err != nil {
		return err
	}
	for _, field := range []struct {
		Text *string
		Max  int
	}{{&q.Prompt, 2000}, {&q.Answer, 4000}} {
		if err := text(field.Text, field.Max); err != nil {
			return err
		}
	}
	if err := enum(q.Mode, []string{"multiple_choice", "short_answer", "essay", "calculation", "proof", "diagram", "code", "mixed"}); err != nil {
		return err
	}
	if err := enum(q.Difficulty, []string{"core", "standard", "stretch"}); err != nil {
		return err
	}
	if q.Minutes < 1 || q.Minutes > 60 {
		return errors.New("question duration must be 1 to 60 minutes")
	}
	if err := textList(q.Points, 1, 12, false); err != nil {
		return err
	}
	if err := textList(q.Mistakes, 0, 8, false); err != nil {
		return err
	}
	return references(q.Evidence, known, 1)
}

func ValidatePractice(raw []byte, packet map[string]any, course int64, unit string) (PracticeSet, error) {
	result := PracticeSet{Version: "practice-set-v1", Unit: unit, Questions: []Question{}}
	known, err := packetRefs(packet)
	if err != nil {
		return result, err
	}
	courseMeta, _ := packet["course"].(map[string]any)
	unitMeta, _ := packet["unit"].(map[string]any)
	if course < 1 || fmt.Sprint(courseMeta["course_id"]) != fmt.Sprint(course) || unitMeta["key"] != unit ||
		!keyPattern.MatchString(unit) {
		return result, errors.New("practice packet identity mismatch")
	}
	var wire struct {
		Version   json.RawMessage   `json:"practice_set_version"`
		Questions []json.RawMessage `json:"questions"`
		Coverage  string            `json:"coverage_note"`
	}
	if len(raw) > 1024*1024 {
		return result, errors.New("practice set exceeds 1 MiB")
	}
	if err = strict(raw, &wire, "questions coverage_note"); err != nil {
		return result, err
	}
	if len(wire.Version) > 0 {
		var version string
		if json.Unmarshal(wire.Version, &version) != nil || version != result.Version {
			return result, errors.New("unsupported practice version")
		}
	}
	if len(wire.Questions) < 1 || len(wire.Questions) > 12 {
		return result, errors.New("invalid practice set version/count")
	}
	if err = text(&wire.Coverage, 1500); err != nil {
		return result, err
	}
	result.Coverage = wire.Coverage
	keys, ids := map[string]bool{}, map[string]bool{}
	for _, raw := range wire.Questions {
		var q PracticeQuestion
		if err = strict(raw, &q, "key prompt response_mode difficulty estimated_minutes expected_answer answer_points common_mistakes evidence_refs"); err != nil {
			return result, err
		}
		if err = q.Validate(known); err != nil {
			return result, err
		}
		id := QuestionIdentity(course, unit, q.Prompt, q.Mode)
		if keys[q.Key] || ids[id] {
			return result, errors.New("duplicate practice question")
		}
		keys[q.Key], ids[id] = true, true
		result.Questions = append(result.Questions, Question{q, id})
	}
	return result, nil
}
