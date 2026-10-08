package rdbms

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// placeholder rewrites postgres $N placeholders to positional ?. modernc
// sqlite binds strictly positionally: every ? consumes the next arg, so each
// $N occurrence appends its arg (repeats duplicate it).
func placeholder(query string, args []any) (string, []any) {
	var out strings.Builder
	out.Grow(len(query))
	// modernc sqlite binds strictly positionally: every ? consumes the next
	// arg, so each $N occurrence appends its arg (repeats duplicate it).
	final := []any{}
	for i := 0; i < len(query); {
		if query[i] != '$' {
			out.WriteByte(query[i])
			i++
			continue
		}
		j := i + 1
		for j < len(query) && query[j] >= '0' && query[j] <= '9' {
			j++
		}
		if j == i+1 {
			out.WriteByte(query[i])
			i++
			continue
		}
		var num int
		fmt.Sscanf(query[i+1:j], "%d", &num)
		if num <= 0 || num > len(args) {
			out.WriteString(query[i:j])
			i = j
			continue
		}
		final = append(final, args[num-1])
		out.WriteByte('?')
		i = j
	}
	return out.String(), final
}

var castPattern = regexp.MustCompile(`::\s*(bigint\[\]|int8\[\]|integer\[\]|int4\[\]|int\[\]|text\[\]|bigint\b|int8\b|integer\b|int4\b|int\b|text\b|boolean\b|bool\b|timestamptz\b|timestamp\b|jsonb\b|json\b|bytea\b|double precision\b|double\b|real\b|numeric\b)`)

// stripCasts removes postgres ::type casts. The schema stores timestamps as
// TEXT and booleans as integers on both backends, so casts are cosmetic.
func stripCasts(query string) string {
	return castPattern.ReplaceAllString(query, "")
}

// forUpdatePattern matches trailing row-locking clauses. FOR SHARE/UPDATE are
// read-your-writes hints on postgres; sqlite serializes writers, so no-ops
// preserve the semantics without the syntax. SKIP LOCKED / NOWAIT are vacuous
// under the pool's writer serialization (no two writer transactions overlap,
// so nothing is ever skipped or waited on); stripping them keeps the
// admission order while the single-writer mutex keeps claims atomic.
var forUpdatePattern = regexp.MustCompile(`(?i)\s+FOR\s+(UPDATE|SHARE)(\s+OF\s+[a-zA-Z0-9_.,\s]+?)?(\s+(SKIP\s+LOCKED|NOWAIT))?\s*$`)

// stripRowLock removes a trailing FOR UPDATE / FOR SHARE clause, including
// SKIP LOCKED / NOWAIT variants.
func stripRowLock(query string) (string, bool) {
	upper := strings.ToUpper(query)
	stripped := forUpdatePattern.ReplaceAllString(query, "")
	return stripped, stripped != query || !strings.Contains(upper, " FOR ")
}

// LockFunc names the advisory-lock shim a statement requests. Advisory locks
// serialize same-key writers inside one transaction on postgres; sqlite holds
// a process-local mutex per key for the transaction duration.
type lockRequest struct {
	key  string
	args []any
}

// Advisory lock statements all share one shape:
//
//	SELECT pg_advisory_xact_lock(hashtextextended('prefix:'||$N,0))
//
// with an optional second argument or a literal key. parseLock extracts the
// key expression so the driver can substitute its local-mutex equivalent.
func parseLock(query string, args []any) (lockRequest, bool) {
	lower := strings.ToLower(query)
	if !strings.Contains(lower, "pg_advisory_xact_lock") && !strings.Contains(lower, "pg_try_advisory_xact_lock") {
		return lockRequest{}, false
	}
	// Key is either a literal 'name' inside the call or a $N argument.
	if m := lockLiteral.FindStringSubmatch(query); m != nil {
		key := m[1]
		if idx := strings.Index(key, ":'||"); idx >= 0 {
			key = key[:idx+1]
		}
		if len(args) > 0 {
			if s, ok := args[0].(string); ok {
				key += s
			} else {
				key += strconv.FormatUint(uint64(len(args)), 10)
			}
		}
		return lockRequest{key: key}, true
	}
	if len(args) > 0 {
		if s, ok := args[0].(string); ok {
			return lockRequest{key: s}, true
		}
	}
	return lockRequest{key: query}, true
}

var lockLiteral = regexp.MustCompile(`hashtext\w*\(\s*'((?:[^']|'')+)'`)

// nowFuncs lists the postgres clock expressions the sqlite driver rewrites to
// a bound timestamp. Rewrites bind NowUTC() at execution time so stored values
// keep the schema's TEXT format.
var nowRewrites = []string{
	"clock_timestamp() AT TIME ZONE 'UTC'",
	"clock_timestamp()",
	"now()",
	"CURRENT_TIMESTAMP",
}

// rewriteNow reports whether the query references a postgres clock expression.
func usesNow(query string) bool {
	lower := strings.ToLower(query)
	for _, fn := range nowRewrites {
		if strings.Contains(lower, strings.ToLower(fn)) {
			return true
		}
	}
	return false
}
