package rdbms

import (
	"context"
	"encoding/json"
)

type sqliteRuntime struct{ db nativeDBTX }

func (r sqliteRuntime) SetRuntimeCode(ctx context.Context, code string) error {
	encoded, err := json.Marshal(code)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `INSERT INTO native_settings(key,value)
        VALUES('_runtime_code',json(?))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value,
            updated_at=strftime('%Y-%m-%d %H:%M:%S','now')
        WHERE native_settings.value IS NOT excluded.value`, string(encoded))
	return err
}
