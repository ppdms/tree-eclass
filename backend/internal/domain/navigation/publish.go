package navigation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

type immutableAction struct {
	ID, Unit string
	Payload  map[string]any
}

// Publish atomically replaces the immutable navigation content and action
// membership only if the processor's captured inputs still describe this course.
// Its caller must validate source evidence and the synthesis contract first.
func (s Service) Publish(
	ctx context.Context,
	course, generation int64,
	a settings.AI,
	view map[string]any,
) (bool, error) {
	content, actions, err := splitContent(course, view)
	if err != nil {
		return false, err
	}
	encoded, overview, contentID, err := encodeContent(content)
	if err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if skip, err := publishGate(ctx, tx, course, generation, a); err != nil || skip {
		return false, err
	}
	previous, err := upsertNavigation(ctx, tx, course, generation, a, content["revision_id"], contentID, encoded, overview)
	if err != nil {
		return false, err
	}
	if err = publishActions(ctx, tx, course, actions); err != nil {
		return false, err
	}
	if err = pruneReplacedContent(ctx, tx, previous, contentID); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func encodeContent(content map[string]any) (encoded, overview []byte, contentID string, err error) {
	encoded, err = json.Marshal(identity.EncodeJSON(content))
	if err != nil {
		return nil, nil, "", err
	}
	hash := sha256.Sum256(encoded)
	contentID = hex.EncodeToString(hash[:])
	overview, err = json.Marshal(identity.EncodeJSON(compactOverview(content)))
	if err != nil {
		return nil, nil, "", err
	}
	return encoded, overview, contentID, nil
}

func publishGate(ctx context.Context, tx rdbms.Tx, course, generation int64, a settings.AI) (bool, error) {
	var current int64
	err := tx.QueryRow(ctx, `SELECT generation FROM read_model.course_generation WHERE course_id=$1 FOR UPDATE`, course).Scan(&current)
	if errors.Is(err, rdbms.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	active, err := settings.ReadAI(ctx, tx)
	if err != nil {
		return false, err
	}
	return current != generation || active.AnalysisGeneration() != a.AnalysisGeneration(), nil
}

func upsertNavigation(
	ctx context.Context,
	tx rdbms.Tx,
	course, generation int64,
	a settings.AI,
	revision any,
	contentID string,
	encoded, overview []byte,
) (*string, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO read_model.roadmap_content(content_id,payload) VALUES($1,$2) ON CONFLICT DO NOTHING`, contentID, encoded); err != nil {
		return nil, err
	}
	var previous *string
	if err := tx.QueryRow(ctx, `SELECT content_id FROM read_model.navigation WHERE course_id=$1`, course).Scan(&previous); err != nil &&
		!errors.Is(err, rdbms.ErrNoRows) {
		return nil, err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO read_model.navigation(course_id,source_generation,config_generation,revision_id,overview,content_id)
 VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(course_id) DO UPDATE SET source_generation=excluded.source_generation,config_generation=excluded.config_generation,revision_id=excluded.revision_id,overview=excluded.overview,content_id=excluded.content_id,generated_at=now()`,
		course,
		generation,
		a.AnalysisGeneration(),
		revision,
		overview,
		contentID,
	); err != nil {
		return nil, err
	}
	return previous, nil
}

func pruneReplacedContent(ctx context.Context, tx rdbms.Tx, previous *string, contentID string) error {
	if previous == nil || *previous == contentID {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM read_model.roadmap_content c WHERE content_id=$1 AND NOT EXISTS(SELECT 1 FROM read_model.navigation n WHERE n.content_id=c.content_id)`, *previous)
	return err
}

func publishActions(ctx context.Context, tx rdbms.Tx, course int64, actions []immutableAction) error {
	if _, err := tx.Exec(ctx, `DELETE FROM read_model.roadmap_actions WHERE course_id=$1`, course); err != nil {
		return err
	}
	for ordinal, action := range actions {
		payload, err := json.Marshal(identity.EncodeJSON(action.Payload))
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO read_model.roadmap_actions(course_id,action_id,ordinal,unit_key,payload) VALUES($1,$2,$3,$4,$5)`, course, action.ID, ordinal, action.Unit, payload); err != nil {
			return err
		}
	}
	return nil
}

func splitContent(course int64, view map[string]any) (map[string]any, []immutableAction, error) {
	raw, err := json.Marshal(view)
	if err != nil {
		return nil, nil, err
	}
	if len(raw) > 8*1024*1024 {
		return nil, nil, errors.New("navigation content exceeds 8 MiB")
	}
	var content map[string]any
	if err = decodeJSON(raw, &content); err != nil {
		return nil, nil, err
	}
	if content == nil || integer(content["course_id"]) != course {
		return nil, nil, errors.New("navigation course identity mismatch")
	}
	actions := []immutableAction{}
	seen := map[string]bool{}
	blueprint, _ := content["blueprint"].(map[string]any)
	units, _ := blueprint["units"].([]any)
	if content["usable"] == true {
		revision, _ := content["revision_id"].(string)
		if revision == "" || utf8.RuneCountInString(revision) > 128 || len(units) == 0 {
			return nil, nil, errors.New("usable navigation requires a revision and units")
		}
	}
	if len(units) > 200 {
		return nil, nil, errors.New("navigation exceeds 200 units")
	}
	for _, item := range units {
		unit, ok := item.(map[string]any)
		if !ok {
			return nil, nil, errors.New("invalid roadmap unit")
		}
		key, _ := unit["key"].(string)
		if key == "" || utf8.RuneCountInString(key) > 160 || seen["unit:"+key] {
			return nil, nil, errors.New("invalid or duplicate roadmap unit key")
		}
		seen["unit:"+key] = true
		rows, valid := unit["actions"].([]any)
		if !valid || len(rows) == 0 {
			return nil, nil, errors.New("roadmap unit requires actions")
		}
		ids := []string{}
		for _, raw := range rows {
			action, ok := raw.(map[string]any)
			if !ok {
				return nil, nil, errors.New("invalid roadmap action")
			}
			id, _ := action["action_id"].(string)
			if minutes := integer(action["estimated_minutes"]); minutes < 1 || minutes > 240 {
				return nil, nil, errors.New("invalid roadmap action duration")
			}
			if id == "" || utf8.RuneCountInString(id) > 240 || seen["action:"+id] || len(actions) >= 2000 {
				return nil, nil, fmt.Errorf("invalid or duplicate roadmap action %q", id)
			}
			seen["action:"+id] = true
			for _, field := range []string{"status", "progress_minutes", "remaining_minutes"} {
				delete(action, field)
			}
			action["unit_key"], action["unit_title"], action["course_id"] = key, unit["title"], course
			actions = append(actions, immutableAction{id, key, action})
			ids = append(ids, id)
		}
		unit["action_ids"] = ids
		for _, field := range []string{"actions", "completed_actions", "total_actions", "progress_percent"} {
			delete(unit, field)
		}
	}
	for _, field := range []string{"actions", "action_membership", "action_states", "progress"} {
		delete(content, field)
	}
	return content, actions, nil
}

func compactOverview(content map[string]any) map[string]any {
	result := map[string]any{}
	for _, key := range []string{"course_id", "usable", "readiness", "revision_id", "revision_number", "validation_reason"} {
		result[key] = content[key]
	}
	blueprint, _ := content["blueprint"].(map[string]any)
	units, _ := blueprint["units"].([]any)
	result["total_units"] = len(units)
	summary := []map[string]any{}
	for _, item := range units[:min(6, len(units))] {
		unit := item.(map[string]any)
		summary = append(summary, map[string]any{"key": unit["key"], "title": unit["title"]})
	}
	result["blueprint"] = map[string]any{"units": summary}
	return result
}
