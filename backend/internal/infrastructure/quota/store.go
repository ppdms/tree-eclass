package quota

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Postgres struct{ Pool rdbms.Pool }

func (s Postgres) Load(ctx context.Context, provider string) (State, error) {
	var state State
	if !slices.Contains(settings.Providers, provider) {
		return state, errors.New("unknown quota provider")
	}
	var raw string
	err := s.Pool.QueryRow(ctx, `SELECT CASE WHEN octet_length(value)<=65536 THEN value END FROM knowledge.knowledge_state WHERE key=$1`, provider+"_quota").
		Scan(&raw)
	if errors.Is(err, rdbms.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err = json.Unmarshal([]byte(raw), &state); err != nil {
		return State{}, errors.New("invalid saved quota state")
	}
	return state, nil
}
func (s Postgres) Save(ctx context.Context, provider string, state State) error {
	if !slices.Contains(settings.Providers, provider) {
		return errors.New("unknown quota provider")
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(
		ctx,
		`INSERT INTO knowledge.knowledge_state(key,value,updated_at) VALUES($1,$2,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`,
		provider+"_quota",
		string(raw),
	)
	return err
}
