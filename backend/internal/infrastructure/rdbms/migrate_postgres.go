package rdbms

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed postgres_migrations/*.sql
var postgresFS embed.FS

//go:embed sqlite_migrations/*.sql
var sqliteFS embed.FS

const migrationLock int64 = 87422101

const runtimeLock int64 = 87422102

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

// postgresManifest returns the name->sha256 ledger for the postgres chain.
func postgresManifest() (map[string]string, error) {
	ms, err := readMigrations(postgresFS, "postgres_migrations")
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, m := range ms {
		result[m.Name] = m.Hash
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

// migratePostgres uses Goose for ordering and records each source checksum in
// the same transaction as the SQL and Goose version. A crash cannot bless
// modified SQL. Unchanged from the pre-abstraction storage.Migrate.
func migratePostgres(ctx context.Context, url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLock); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLock)
	var exclusive bool
	if err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", runtimeLock).Scan(&exclusive); err != nil {
		return err
	}
	if !exclusive {
		return errors.New("another application owns this database; stop all writers before migration")
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", runtimeLock)
	if err = admitPostgres(ctx, conn); err != nil {
		return err
	}
	ms, err := readMigrations(postgresFS, "postgres_migrations")
	if err != nil {
		return err
	}
	if err = validatePostgresLedger(ctx, conn, ms, false); err != nil {
		return err
	}
	goMigrations := make([]*goose.Migration, 0, len(ms))
	for _, m := range ms {
		goMigrations = append(goMigrations, goPostgresMigration(m))
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, nil, goose.WithGoMigrations(goMigrations...))
	if err != nil {
		return err
	}
	if _, err = provider.Up(ctx); err != nil {
		return err
	}
	return validatePostgresLedger(ctx, conn, ms, true)
}

func goPostgresMigration(m migration) *goose.Migration {
	up := &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, m.SQL+"\nSET search_path TO public;"); err != nil {
			return err
		}
		_, err := tx.ExecContext(
			ctx,
			"INSERT INTO public.tree_go_migrations(version,name,sha256) VALUES($1,$2,$3)",
			m.Version,
			m.Name,
			m.Hash,
		)
		return err
	}}
	down := &goose.GoFunc{RunTx: func(context.Context, *sql.Tx) error {
		return errors.New("schema downgrade is disabled; restore a cold checkpoint")
	}}
	return goose.NewGoMigration(m.Version, up, down)
}

func admitPostgres(ctx context.Context, conn *sql.Conn) error {
	var ledger bool
	if err := conn.QueryRowContext(ctx, "SELECT to_regclass('public.tree_go_migrations') IS NOT NULL").Scan(&ledger); err != nil {
		return err
	}
	if ledger {
		return nil
	}
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p')").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("database is not empty and has no Go migration ledger; refusing initialization")
	}
	_, err := conn.ExecContext(
		ctx,
		"CREATE TABLE public.tree_go_migrations(version bigint PRIMARY KEY,name text NOT NULL UNIQUE,sha256 text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())",
	)
	return err
}

func validatePostgresLedger(ctx context.Context, conn *sql.Conn, ms []migration, exact bool) error {
	expected := map[int64]migration{}
	for _, m := range ms {
		expected[m.Version] = m
	}
	rows, err := conn.QueryContext(ctx, "SELECT version,name,sha256 FROM public.tree_go_migrations ORDER BY version")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var version int64
		var name, hash string
		if err = rows.Scan(&version, &name, &hash); err != nil {
			return err
		}
		m, ok := expected[version]
		if !ok || m.Name != name || m.Hash != hash {
			return fmt.Errorf("incompatible migration %s (%d); use its matching release or checkpoint", name, version)
		}
		if count >= len(ms) || ms[count].Version != version {
			return errors.New("migration ledger has a gap")
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if exact && count != len(ms) {
		return errors.New("schema migration required before application startup")
	}
	return nil
}

func requirePostgres(ctx context.Context, url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	ms, err := readMigrations(postgresFS, "postgres_migrations")
	if err != nil {
		return err
	}
	return validatePostgresLedger(ctx, conn, ms, true)
}
