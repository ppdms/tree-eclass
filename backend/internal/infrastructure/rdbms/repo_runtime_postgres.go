package rdbms

import (
	"context"
	"encoding/json"
)

type postgresRuntime struct{ db nativeDBTX }

func (r postgresRuntime) SetRuntimeCode(ctx context.Context, code string) error {
	encoded, err := json.Marshal(code)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `INSERT INTO app.native_settings(key,value)
        VALUES('_runtime_code',$1::jsonb)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=clock_timestamp()
        WHERE app.native_settings.value IS DISTINCT FROM excluded.value`, encoded)
	return err
}
