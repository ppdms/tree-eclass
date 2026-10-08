package rdbms

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"tree-eclass/internal/domain/database"
)

type sqliteQuota struct{ db nativeDBTX }

func (q sqliteQuota) LoadState(ctx context.Context, provider string) (database.QuotaState, error) {
	key, err := quotaKey(provider)
	if err != nil {
		return database.QuotaState{}, err
	}
	var raw *string
	err = q.db.QueryRow(ctx, `SELECT CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value END`+
		` FROM knowledge_state WHERE key=?`, key).Scan(&raw)
	if err != nil {
		if errors.Is(err, database.ErrNoRows) {
			return database.QuotaState{}, nil
		}
		return database.QuotaState{}, err
	}
	return decodeQuotaState(raw)
}

func (q sqliteQuota) SaveState(ctx context.Context, provider string, state database.QuotaState) error {
	key, err := quotaKey(provider)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = q.db.Exec(ctx, `INSERT INTO knowledge_state(key,value,updated_at)`+
		` VALUES(?,?,strftime('%Y-%m-%d %H:%M:%S','now'))`+
		` ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`,
		key, string(raw))
	return err
}

func (q sqliteQuota) LoadStatuses(ctx context.Context) ([]database.QuotaStatus, error) {
	rows, err := q.db.Query(ctx, `SELECT key,value FROM knowledge_state WHERE key LIKE '%\_quota' ESCAPE '\'`)
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
