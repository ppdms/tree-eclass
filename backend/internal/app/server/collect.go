package server

import (
	"context"
	"encoding/json"
	"os"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/objectgc"
	"tree-eclass/internal/infrastructure/storage"
)

func collect(ctx context.Context, cfg Config) error {
	db, err := storage.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	store, err := blob.New(cfg.S3Endpoint, cfg.S3Access, cfg.S3Secret)
	if err != nil {
		return err
	}
	result, err := objectgc.Collect(ctx, db, store)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
