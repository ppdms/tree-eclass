package rdbms

import (
	"fmt"
	"regexp"
	"strings"
)

// translate converts caller SQL ($N placeholders, ::casts, row locks,
// advisory locks, clock expressions) to sqlite dialect. Advisory-lock
// statements are consumed here (translated to no-op SELECT) with the key
// recorded for mutex acquisition by the executor. Unknown postgres dialect
// fails here instead of reaching sqlite.
func translate(query string, args []any) (string, []any, *lockRequest, error) {
	trimmed := strings.TrimSpace(query)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "SELECT PG_ADVISORY") || strings.Contains(upper, "PG_ADVISORY_XACT_LOCK") ||
		strings.Contains(upper, "PG_TRY_ADVISORY") {
		if req, ok := parseLock(query, args); ok {
			return "SELECT 1", args, &req, nil
		}
	}
	out, args := expandArrayPosition(query, args)
	out, args = expandAny(out, args)
	out = stripCasts(out)
	out = stripSchemas(out)
	out = rewriteArrow(out)
	out = rewriteExtract(out)
	out = rewriteClockZone(out)
	out = rewriteUpdateAlias(out)
	out = rewriteDistinctOn(out)
	out = rewriteInterval(out)
	out, args = placeholder(out, args)
	if stripped, ok := stripRowLock(out); ok {
		out = stripped
	}
	if marker := residualDialect(out); marker != "" {
		return "", nil, nil, fmt.Errorf("rdbms: untranslated postgres dialect %q in %.120q", marker, query)
	}
	return out, args, nil, nil
}

// Interval shapes in the codebase rewrite to sqlite datetime(). Its '+N
// unit' modifiers compose over the schema's UTC text timestamps.
var intervalMulPattern = regexp.MustCompile(`(?i)([\w().]+)\s*\+\s*(\$\d+)\s*\*\s*interval\s+'1 second'`)
var intervalLitPattern = regexp.MustCompile(`(?i)(clock_timestamp\(\)|now\(\))\s*\+\s*interval\s+'([^']+)'`)
var intervalSubPattern = regexp.MustCompile(`(?i)(clock_timestamp\(\))\s*-\s*interval\s+'([^']+)'`)

// rewriteInterval converts postgres clock±interval arithmetic to sqlite
// datetime(). Supported shapes (the only ones in the codebase):
// `<base> + $N * interval '1 second'` → `datetime(<base>, '+' || $N || ' seconds')`
// `now()/clock_timestamp() + interval '<lit>'` → `datetime(<base>, '+<lit>')`
// `clock_timestamp() - interval '<lit>'` → `datetime(<base>, '-<lit>')`
func rewriteInterval(query string) string {
	out := intervalMulPattern.ReplaceAllString(query,
		`datetime(strftime('%Y-%m-%d %H:%M:%S', $1), '+' || $2 || ' seconds')`)
	// now()/clock_timestamp() are registered scalar shims returning UTC
	// text; wrap in strftime so datetime() parses the value instead of the
	// literal expression text.
	out = intervalLitPattern.ReplaceAllString(out, `datetime(strftime('%Y-%m-%d %H:%M:%S', $1), '+$2')`)
	out = intervalSubPattern.ReplaceAllString(out, `datetime(strftime('%Y-%m-%d %H:%M:%S', $1), '-$2')`)
	return out
}

// clockZonePattern matches `<clock> AT TIME ZONE 'UTC'` where clock is a
// registered zero-arg UTC shim (clock_timestamp()/now()). Postgres returns
// timestamptz; the shims already return UTC text, so the zone conversion is
// a no-op and the call collapses to the bare shim. Must run before
// rewriteInterval so interval arithmetic sees the plain clock call.
var clockZonePattern = regexp.MustCompile(`(?i)(clock_timestamp\(\)|now\(\))\s+AT\s+TIME\s+ZONE\s+'[^']*'`)

func rewriteClockZone(query string) string {
	return clockZonePattern.ReplaceAllString(query, `$1`)
}

// updateAliasPattern matches `UPDATE <table> <alias> SET`; deleteAliasPattern
// matches `DELETE FROM <table> <alias> WHERE`. Sqlite supports a target alias
// in neither. The codebase only aliases the modified table to qualify its own
// columns (single-table writes), so dropping the alias and its `alias.`
// prefixes preserves semantics. Subquery aliases for other tables survive:
// only the write target alias is removed.
var updateAliasPattern = regexp.MustCompile(`(?i)\bUPDATE\s+([a-zA-Z0-9_."]+)\s+([a-zA-Z0-9_"]+)\s+SET\b`)
var deleteAliasPattern = regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+([a-zA-Z0-9_."]+)\s+([a-zA-Z0-9_"]+)\s+WHERE\b`)

func rewriteUpdateAlias(query string) string {
	if m := updateAliasPattern.FindStringSubmatch(query); m != nil {
		out := updateAliasPattern.ReplaceAllString(query, `UPDATE $1 SET`)
		return regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(m[2])+`\.`).ReplaceAllString(out, "")
	}
	if m := deleteAliasPattern.FindStringSubmatch(query); m != nil {
		out := deleteAliasPattern.ReplaceAllString(query, `DELETE FROM $1 WHERE`)
		return regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(m[2])+`\.`).ReplaceAllString(out, "")
	}
	return query
}

// extractPattern matches extract(epoch FROM <expr>). Only epoch over a
// timestamp difference occurs; it becomes extract_epoch(<expr>) handled by
// the registered scalar function.
var extractPattern = regexp.MustCompile(`(?i)extract\s*\(\s*epoch\s+FROM\s+`)

// rewriteExtract converts extract(epoch FROM expr) to extract_epoch(expr).
func rewriteExtract(query string) string {
	var out strings.Builder
	out.Grow(len(query))
	inString := false
	for i := 0; i < len(query); {
		c := query[i]
		if c == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				out.WriteString("''")
				i += 2
				continue
			}
			inString = !inString
			out.WriteByte(c)
			i++
			continue
		}
		if !inString {
			if loc := extractPattern.FindStringIndex(query[i:]); loc != nil && loc[0] == 0 {
				out.WriteString("extract_epoch(")
				i += loc[1]
				continue
			}
		}
		out.WriteByte(c)
		i++
	}
	return out.String()
}

// stripSchemas removes postgres schema qualifiers (app., knowledge.,
// messages., read_model., public.). The sqlite migrations create the same
// tables unqualified in one database; qualifier names never collide across
// schemas except the already-distinct *schema_version tables.
func stripSchemas(query string) string {
	schemas := []string{"app.", "knowledge.", "messages.", "read_model.", "public."}
	var out strings.Builder
	out.Grow(len(query))
	inString := false
	for i := 0; i < len(query); {
		c := query[i]
		if c == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				out.WriteString("''")
				i += 2
				continue
			}
			inString = !inString
			out.WriteByte(c)
			i++
			continue
		}
		if !inString {
			matched := false
			for _, schema := range schemas {
				if strings.HasPrefix(query[i:], schema) && (i == 0 || !isSchemaIdent(query[i-1])) {
					i += len(schema)
					matched = true
					break
				}
			}
			if matched {
				continue
			}
		}
		out.WriteByte(c)
		i++
	}
	return out.String()
}

func isSchemaIdent(c byte) bool {
	return c == '_' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
