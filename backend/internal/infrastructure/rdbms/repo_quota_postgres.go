package rdbms

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"tree-eclass/internal/domain/database"
)

type postgresQuota struct{ db nativeDBTX }

var quotaProviders = []string{"synthetic", "ollama", "huggingface", "alibaba", "zai", "opencode-go"}

func quotaKey(provider string) (string, error) {
	if !slices.Contains(quotaProviders, provider) {
		return "", errors.New("unknown quota provider")
	}
	return provider + "_quota", nil
}

func decodeQuotaState(raw *string) (database.QuotaState, error) {
	var state database.QuotaState
	if raw == nil {
		return state, nil
	}
	if err := json.Unmarshal([]byte(*raw), &state); err != nil {
		return database.QuotaState{}, errors.New("invalid saved quota state")
	}
	return state, nil
}

func (q postgresQuota) LoadState(ctx context.Context, provider string) (database.QuotaState, error) {
	key, err := quotaKey(provider)
	if err != nil {
		return database.QuotaState{}, err
	}
	var raw *string
	err = q.db.QueryRow(ctx, `SELECT CASE WHEN octet_length(value)<=65536 THEN value END`+
		` FROM knowledge.knowledge_state WHERE key=$1`, key).Scan(&raw)
	if err != nil {
		if errors.Is(err, database.ErrNoRows) {
			return database.QuotaState{}, nil
		}
		return database.QuotaState{}, err
	}
	return decodeQuotaState(raw)
}

func (q postgresQuota) SaveState(ctx context.Context, provider string, state database.QuotaState) error {
	key, err := quotaKey(provider)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = q.db.Exec(ctx, `INSERT INTO knowledge.knowledge_state(key,value,updated_at)`+
		` VALUES($1,$2,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS'))`+
		` ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`,
		key, string(raw))
	return err
}

func (q postgresQuota) LoadStatuses(ctx context.Context) ([]database.QuotaStatus, error) {
	rows, err := q.db.Query(ctx, `SELECT key,value FROM knowledge.knowledge_state WHERE key LIKE '%\_quota'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	known := map[string]bool{}
	for _, provider := range quotaProviders {
		known[provider] = true
	}
	out := []database.QuotaStatus{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(key, "_quota") {
			continue
		}
		provider := strings.TrimSuffix(key, "_quota")
		if !known[provider] {
			continue
		}
		var data struct {
			Status string `json:"status"`
		}
		if json.Unmarshal([]byte(value), &data) != nil || data.Status == "" {
			continue
		}
		out = append(out, database.QuotaStatus{Provider: provider, Status: data.Status})
	}
	return out, rows.Err()
}
