package rdbms

import (
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

//go:embed sqlite_migrations/*.sql
var sqliteFS embed.FS

type migration struct {
	Version         int64
	Name, SQL, Hash string
}

func readMigrations(fsys embed.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	result := make([]migration, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		version, err := strconv.ParseInt(strings.SplitN(name, "_", 2)[0], 10, 64)
		if err != nil {
			return nil, err
		}
		b, err := fsys.ReadFile(dir + "/" + name)
		if err != nil {
			return nil, err
		}
		result = append(result, migration{version, name, string(b), fmt.Sprintf("%x", sha256.Sum256(b))})
	}
	return result, nil
}

// sqliteManifest returns the name->sha256 ledger for the sqlite chain.
func sqliteManifest() (map[string]string, error) {
	ms, err := readMigrations(sqliteFS, "sqlite_migrations")
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, m := range ms {
		result[m.Name] = m.Hash
	}
	return result, nil
}
