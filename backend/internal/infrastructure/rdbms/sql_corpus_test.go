package rdbms

import (
	"context"
	"database/sql"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestSQLCorpusSQLiteCompat extracts every SQL string literal in backend/
// (generated consts plus inline Exec/QueryRow literals), runs each through
// the sqlite translate() pipeline, and EXPLAINs the result against a
// freshly-migrated scratch database. EXPLAIN executes nothing and needs no
// real argument values: it validates syntax plus schema (tables, columns).
//
// This is the fail-fast gate for the sqlite port: postgres-only SQL must
// fail HERE, not in production. Statements that are genuinely out of scope
// for sqlite (maintenance paths with no sqlite equivalent yet) are listed
// in corpusAllowlist with their owning issue; the sync/nav/coverage/claim
// hot path must be fully clean.
func TestSQLCorpusSQLiteCompat(t *testing.T) {
	root, err := moduleRoot()
	if err != nil {
		t.Fatal(err)
	}
	literals := collectSQLLiterals(t, root)
	if len(literals) == 0 {
		t.Fatal("no SQL literals collected; extractor is broken")
	}
	db := migratedScratch(t)
	ctx := context.Background()
	failures := 0
	for _, lit := range literals {
		if corpusAllowlisted(lit) {
			continue
		}
		if isFragment(lit.sql) {
			continue
		}
		// The extractor saw the call site: placeholders must match the
		// argument count, or the call fails at runtime on every backend.
		if lit.hasArity && lit.placeholders != lit.arity {
			t.Errorf("%s: want %d args for %d placeholders\ninput:  %.200q",
				lit.origin, lit.arity, lit.placeholders, lit.sql)
			failures++
			continue
		}
		// database/sql); EXPLAIN it verbatim instead of translating.
		// Everything else goes through translate (schema strip, lock
		// strip) even without placeholders.
		if !strings.Contains(lit.sql, "$") && strings.Contains(lit.sql, "?") {
			if err := explain(ctx, db, lit.sql, dummyArgs(lit.sql)); err != nil {
				t.Errorf("%s: native EXPLAIN failed: %v\ninput:  %.200q",
					lit.origin, err, lit.sql)
				failures++
			}
			continue
		}
		out, expanded, lock, err := translate(lit.sql, dummyArgs(lit.sql))
		if err != nil {
			t.Errorf("%s: fail-closed translate rejected: %v\ninput:  %.200q",
				lit.origin, err, lit.sql)
			failures++
			continue
		}
		if lock != nil {
			continue
		}
		if err := explain(ctx, db, out, expanded); err != nil {
			t.Errorf("%s: translate+EXPLAIN failed: %v\ninput:  %.200q\noutput: %.200q",
				lit.origin, err, lit.sql, out)
			failures++
		}
	}
	t.Logf("corpus: %d literals, %d failures", len(literals), failures)
}

type sqlLiteral struct {
	sql          string
	placeholders int
	origin       string
	arity        int
	hasArity     bool
}

var sqlKeyword = regexp.MustCompile(`(?i)\b(SELECT|INSERT|UPDATE|DELETE|WITH)\b`)

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}

// collectSQLLiterals parses every non-test .go file and keeps SQL text:
// standalone string literals plus `+`-concatenations whose every part is a
// literal or a package-level string constant (resolved via constTable).
// Runtime-built fragments (fmt.Sprintf, variables) are skipped: they need
// the dual-backend runtime harness, not static EXPLAIN.
// literalCollector accumulates deduplicated SQL text across the module walk.
type literalCollector struct {
	root   string
	consts map[string]string
	seen   map[string]bool
	out    []sqlLiteral
}

func collectSQLLiterals(t *testing.T, root string) []sqlLiteral {
	t.Helper()
	c := &literalCollector{root: root, consts: collectStringConsts(t, root), seen: map[string]bool{}}
	c.walkDir(t, root)
	sort.Slice(c.out, func(i, j int) bool { return c.out[i].origin < c.out[j].origin })
	return c.out
}

// collectDirLiterals walks one directory, inspecting every non-test Go file
// for SQL text and recursing into subdirectories.
// walkDir inspects every non-test Go file below dir for SQL text.
func (c *literalCollector) walkDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			name := entry.Name()
			if name == "testdata" || strings.HasPrefix(name, ".") {
				continue
			}
			c.walkDir(t, path)
			continue
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			continue
		}
		c.inspectFile(t, path)
	}
}

// inspectFileLiterals extracts SQL text from one Go file: standalone
// literals plus const-resolved concatenations.
// inspectFile extracts SQL text from one Go file: standalone literals
// plus const-resolved concatenations.
func (c *literalCollector) inspectFile(t *testing.T, path string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if ok {
			c.inspectCall(fset, path, call)
			return true
		}
		trimmed, pos, ok := nodeSQLText(n, c.consts)
		if !ok {
			return true
		}
		c.addLiteral(fset, path, trimmed, pos, -1)
		return false
	})
}

// inspectCall records SQL passed directly to Exec/Query/QueryRow with its
// call-site argument count, so placeholder/argument mismatches fail here.
func (c *literalCollector) inspectCall(fset *token.FileSet, path string, call *ast.CallExpr) {
	name := ""
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		name = sel.Sel.Name
	}
	if name != "Exec" && name != "Query" && name != "QueryRow" {
		return
	}
	if len(call.Args) < 2 {
		return
	}
	trimmed, pos, ok := nodeSQLText(call.Args[1], c.consts)
	if !ok {
		return
	}
	c.addLiteral(fset, path, trimmed, pos, len(call.Args)-2)
}

func (c *literalCollector) addLiteral(fset *token.FileSet, path, trimmed string, pos token.Pos, arity int) {
	if len(trimmed) < 20 || !sqlKeyword.MatchString(trimmed) {
		return
	}
	upper := strings.ToUpper(trimmed)
	if !strings.Contains(upper, "FROM") && !strings.Contains(upper, "INTO") &&
		!strings.Contains(upper, "TABLE") && !strings.Contains(upper, "UPDATE") {
		return
	}
	rel, _ := filepath.Rel(c.root, path)
	position := fset.Position(pos)
	key := trimmed
	if c.seen[key] {
		if arity >= 0 {
			for i := range c.out {
				if c.out[i].sql == key {
					c.out[i].arity = arity
					c.out[i].hasArity = true
				}
			}
		}
		return
	}
	c.seen[key] = true
	c.out = append(c.out, sqlLiteral{
		sql:          trimmed,
		placeholders: maxPlaceholder(trimmed),
		origin:       fmt.Sprintf("%s:%d", rel, position.Line),
		arity:        arity,
		hasArity:     arity >= 0,
	})
}

// nodeSQLText extracts SQL text from a literal or const-resolved concat.
func nodeSQLText(n ast.Node, consts map[string]string) (string, token.Pos, bool) {
	switch node := n.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return "", 0, false
		}
		return strings.TrimSpace(literalValue(node.Value)), node.Pos(), true
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return "", 0, false
		}
		joined, ok := joinConcat(node, consts)
		if !ok {
			return "", 0, false
		}
		return strings.TrimSpace(joined), node.Pos(), true
	}
	return "", 0, false
}

func migratedScratch(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corpus.db")
	if err := migrateSQLite(context.Background(), path); err != nil {
		t.Fatalf("migrate scratch: %v", err)
	}
	dsn := "file:" + path + "?_pragma=busy_timeout%3D5000"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// Runtime TEMP staging table for the Discord ingest path: EXPLAIN must
	// resolve it like the ingest connection does.
	if _, err := db.Exec(`CREATE TEMP TABLE tree_discord_stage(message_id BIGINT PRIMARY KEY,timestamp TEXT NOT NULL,timestamp_epoch DOUBLE PRECISION NOT NULL,author_key TEXT,author_name TEXT NOT NULL,content TEXT NOT NULL,searchable_text TEXT NOT NULL,reply_to_message_id BIGINT,message_type TEXT NOT NULL,is_pinned BIGINT NOT NULL,reaction_count BIGINT NOT NULL,attachment_metadata_json TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func explain(ctx context.Context, db *sql.DB, query string, args []any) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return fmt.Errorf("empty translation")
	}
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "SELECT 1") {
		return nil
	}
	rows, err := db.QueryContext(ctx, "EXPLAIN "+trimmed, args...)
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}

// corpusAllowlist names statements with no sqlite equivalent yet. Each
// entry must name the owning area; hot-path statements (sync, feeds,
// nav, coverage, claims, queue) are NEVER allowlisted.
func corpusAllowlisted(lit sqlLiteral) bool {
	lower := strings.ToLower(lit.sql)
	for _, marker := range []string{
		"gen_random_uuid",
		"for update skip locked",
		"string_agg",
		"to_tsvector",
		"plainto_tsquery",
		"ts_rank",
		"ts_headline",
		// Postgres-only sources and admin paths: pg catalog probes and
		// CREATE DATABASE never execute on sqlite.
		"pg_class",
		"pg_namespace",
		"pg_database",
		"create database",
		// Runtime-composed CTE reference: the enclosing WITH recent AS
		// is assembled in Go; the static union arm cannot EXPLAIN alone.
		"from recent",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
