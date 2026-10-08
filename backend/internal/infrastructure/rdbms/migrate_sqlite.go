package rdbms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

// migrateSQLite applies the sqlite schema chain with the same ledger contract
// as postgres: goose ordering plus a per-source sha256 ledger written in the
// same transaction as the DDL. Runtime and migration admission hold the same
// exclusive kernel ownership lock; schema changes never race an application.
func migrateSQLite(ctx context.Context, path string) error {
	path, err := canonicalSQLitePath(path)
	if err != nil {
		return err
	}
	owner, err := acquireSQLiteOwner(path)
	if err != nil {
		return err
	}
	defer owner.Close()
	return migrateSQLiteOwned(ctx, path)
}

// migrateSQLiteOwned runs only after the runtime ownership lock is acquired.
func migrateSQLiteOwned(ctx context.Context, path string) error {
	dsn := "file:" + path + "?_pragma=busy_timeout%3D30000&_pragma=journal_mode%3DWAL&_pragma=foreign_keys%3D1"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	// Goose runs Go migrations inside a transaction while holding a separate
	// lock connection; a single connection would deadlock that handshake. The
	// runtime pool allows a few WAL-reader connections after migration.
	db.SetMaxOpenConns(2)
	if err = admitSQLite(ctx, db); err != nil {
		return err
	}
	ms, err := readMigrations(sqliteFS, "sqlite_migrations")
	if err != nil {
		return err
	}
	if err = validateSQLiteLedger(ctx, db, ms, false); err != nil {
		return err
	}
	goMigrations := make([]*goose.Migration, 0, len(ms))
	for _, m := range ms {
		goMigrations = append(goMigrations, goSQLiteMigration(m))
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, nil, goose.WithGoMigrations(goMigrations...))
	if err != nil {
		return err
	}
	if _, err = provider.Up(ctx); err != nil {
		return err
	}
	return validateSQLiteLedger(ctx, db, ms, true)
}

func goSQLiteMigration(m migration) *goose.Migration {
	up := &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
		for _, stmt := range splitStatements(m.SQL) {
			if isCommentOnly(stmt) {
				continue
			}
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				preview := strings.TrimSpace(stmt)
				if len(preview) > 160 {
					preview = preview[:160]
				}
				return fmt.Errorf("sqlite migration %s: %w :: %s", m.Name, err, preview)
			}
		}
		_, err := tx.ExecContext(
			ctx,
			"INSERT INTO tree_go_migrations(version,name,sha256) VALUES(?,?,?)",
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

// isCommentOnly reports statements holding nothing but comments/whitespace.
func isCommentOnly(stmt string) bool {
	trimmed := strings.TrimSpace(stmt)
	for trimmed != "" {
		if strings.HasPrefix(trimmed, "--") {
			if idx := strings.IndexByte(trimmed, '\n'); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+1:])
				continue
			}
			return true
		}
		if strings.HasPrefix(trimmed, "/*") {
			if idx := strings.Index(trimmed, "*/"); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+2:])
				continue
			}
			return true
		}
		return false
	}
	return true
}

// stmtSplitter holds splitStatements scan state.
type stmtSplitter struct {
	body           string
	upper          string
	out            []string
	cur            strings.Builder
	inString       bool
	inLineComment  bool
	inBlockComment bool
	depth          int
}

// splitStatements splits migration SQL on semicolons outside string literals
// and outside CREATE TRIGGER bodies. SQLite via database/sql executes one
// statement at a time, but a trigger body holds several semicolon-terminated
// statements between BEGIN and END that MUST stay in one Exec.
func splitStatements(body string) []string {
	s := &stmtSplitter{body: body, upper: strings.ToUpper(body)}
	for i := range len(body) {
		c := body[i]
		if s.scanComment(c, i) {
			continue
		}
		if s.scanString(c, i) {
			continue
		}
		s.trackTriggerDepth(i)
		if s.flushTriggerEnd(c, i) {
			continue
		}
		if s.flushStatement(c) {
			continue
		}
		s.cur.WriteByte(c)
	}
	if rest := strings.TrimSpace(s.cur.String()); rest != "" {
		s.out = append(s.out, s.cur.String())
	}
	return s.out
}

// scanComment consumes comment bytes, entering comments outside strings.
// It reports whether the byte was consumed by comment handling.
func (s *stmtSplitter) scanComment(c byte, i int) bool {
	if s.inLineComment {
		if c == '\n' {
			s.inLineComment = false
			s.cur.WriteByte(c)
		}
		return true
	}
	if s.inBlockComment {
		if c == '/' && i > 0 && s.body[i-1] == '*' {
			s.inBlockComment = false
		}
		return true
	}
	if !s.inString && c == '-' && i+1 < len(s.body) && s.body[i+1] == '-' {
		s.inLineComment = true
		return true
	}
	if !s.inString && c == '/' && i+1 < len(s.body) && s.body[i+1] == '*' {
		s.inBlockComment = true
		return true
	}
	return false
}

// scanString consumes quote bytes, folding escaped ” pairs.
// It reports whether the byte was consumed by string handling.
func (s *stmtSplitter) scanString(c byte, i int) bool {
	if c != '\'' {
		return false
	}
	if s.inString && i+1 < len(s.body) && s.body[i+1] == '\'' {
		s.cur.WriteByte(c)
		s.cur.WriteByte(s.body[i+1])
		return true
	}
	s.inString = !s.inString
	s.cur.WriteByte(c)
	return true
}

// trackTriggerDepth maintains the CREATE TRIGGER BEGIN/END nesting depth.
func (s *stmtSplitter) trackTriggerDepth(i int) {
	if s.inString {
		return
	}
	if s.depth == 0 && isTriggerStart(s.upper, i) {
		s.depth = 1
	} else if s.depth > 0 && isWordAt(s.upper, i, "BEGIN") {
		s.depth++
	} else if s.depth > 0 && isWordAt(s.upper, i, "END") {
		s.depth--
	}
}

// flushTriggerEnd emits the closing END; of a top-level trigger body.
func (s *stmtSplitter) flushTriggerEnd(c byte, i int) bool {
	if c == ';' && !s.inString && s.depth <= 1 && atTriggerEnd(s.depth, s.upper, i) {
		s.cur.WriteByte(c)
		s.out = append(s.out, s.cur.String())
		s.cur.Reset()
		s.depth = 0
		return true
	}
	return false
}

// flushStatement emits a top-level semicolon-terminated statement.
func (s *stmtSplitter) flushStatement(c byte) bool {
	if c == ';' && !s.inString && s.depth == 0 {
		s.out = append(s.out, s.cur.String())
		s.cur.Reset()
		return true
	}
	return false
}

// isTriggerStart reports a CREATE TRIGGER opening at offset i.
func isTriggerStart(upper string, i int) bool {
	return isWordAt(upper, i, "CREATE TRIGGER") || isWordAt(upper, i, "CREATE TEMP TRIGGER") ||
		isWordAt(upper, i, "CREATE TEMPORARY TRIGGER")
}

// atTriggerEnd reports the semicolon closing a top-level trigger body: the
// END keyword immediately precedes it (END;).
func atTriggerEnd(depth int, upper string, i int) bool {
	if depth != 1 {
		return false
	}
	j := i - 1
	for j >= 0 && (upper[j] == ' ' || upper[j] == '\t' || upper[j] == '\n' || upper[j] == '\r') {
		j--
	}
	return j >= 2 && upper[j-2:j+1] == "END"
}

// isWordAt reports keyword match with non-identifier boundaries.
func isWordAt(upper string, i int, word string) bool {
	if !strings.HasPrefix(upper[i:], word) {
		return false
	}
	before := i - 1
	if before >= 0 && isIdentChar(upper[before]) {
		return false
	}
	after := i + len(word)
	if after < len(upper) && isIdentChar(upper[after]) {
		return false
	}
	return true
}

func isIdentChar(c byte) bool {
	return c == '_' || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func admitSQLite(ctx context.Context, db *sql.DB) error {
	var ledger bool
	err := db.QueryRowContext(ctx, "SELECT count(*)>0 FROM sqlite_master WHERE type='table' AND name='tree_go_migrations'").Scan(&ledger)
	if err != nil {
		return err
	}
	if ledger {
		return nil
	}
	var count int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'").Scan(&count)
	if err != nil {
		return err
	}
	if count != 0 {
		return errors.New("database is not empty and has no Go migration ledger; refusing initialization")
	}
	_, err = db.ExecContext(
		ctx,
		"CREATE TABLE tree_go_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE,sha256 TEXT NOT NULL,applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')))",
	)
	return err
}

func validateSQLiteLedger(ctx context.Context, db *sql.DB, ms []migration, exact bool) error {
	expected := map[int64]migration{}
	for _, m := range ms {
		expected[m.Version] = m
	}
	rows, err := db.QueryContext(ctx, "SELECT version,name,sha256 FROM tree_go_migrations ORDER BY version")
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

func requireSQLite(ctx context.Context, path string) error {
	path, err := canonicalSQLitePath(path)
	if err != nil {
		return err
	}
	dsn := "file:" + path + "?_pragma=busy_timeout%3D30000&_pragma=journal_mode%3DWAL&_pragma=foreign_keys%3D1"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ms, err := readMigrations(sqliteFS, "sqlite_migrations")
	if err != nil {
		return err
	}
	return validateSQLiteLedger(ctx, db, ms, true)
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o700)
}
