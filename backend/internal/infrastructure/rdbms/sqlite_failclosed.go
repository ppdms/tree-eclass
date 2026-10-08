package rdbms

import (
	"regexp"
	"strings"
)

// residualMarkers names postgres constructs with no sqlite form. Anything
// surviving the pipeline fails here with a clear error instead of a
// cryptic sqlite syntax failure (or worse, silently different semantics).
var residualMarkers = []string{
	"AT TIME ZONE",
	"DISTINCT ON",
	"FOR UPDATE",
	"FOR SHARE",
	"ON CONFLICT ON CONSTRAINT",
	"generate_series",
	"unnest(",
	"jsonb_array_elements",
	"->>",
	"->",
	"::",
}

// residualRegistered lists pg_ shims that ARE registered and must not trip
// the pg_* scan.
var residualRegistered = []string{"pg_input_is_valid", "pg_arrow_text", "pg_arrow_json"}

var residualWordMarkers = []string{"ANY("}

// residualDialect scans outside string literals for leftover postgres
// dialect: unexpanded $N placeholders plus every residual marker.
func residualDialect(query string) string {
	code := stripLiterals(query)
	upper := strings.ToUpper(code)
	for i := 0; i < len(code); i++ {
		if code[i] == '$' && i+1 < len(code) && code[i+1] >= '0' && code[i+1] <= '9' {
			return "$N placeholder"
		}
	}
	for _, marker := range residualMarkers {
		if strings.Contains(upper, marker) {
			return marker
		}
	}
	for _, marker := range residualWordMarkers {
		if regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(marker[:len(marker)-1]) + `\s*\(`).MatchString(code) {
			return marker
		}
	}
	for _, match := range regexp.MustCompile(`(?i)\b(pg_[a-z_]+)\s*\(`).FindAllStringSubmatch(code, -1) {
		registered := false
		for _, name := range residualRegistered {
			if strings.EqualFold(match[1], name) {
				registered = true
				break
			}
		}
		if !registered {
			return "pg_* function"
		}
	}
	return ""
}

// stripLiterals blanks single-quoted literals so dialect scans never fire
// on text content.
func stripLiterals(query string) string {
	var out strings.Builder
	out.Grow(len(query))
	inString := false
	for i := 0; i < len(query); i++ {
		c := query[i]
		if c == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				out.WriteString("  ")
				i++
				continue
			}
			inString = !inString
			out.WriteByte(' ')
			continue
		}
		if inString {
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(c)
	}
	return out.String()
}

// arrowTextPattern matches `expr->>'key'` (postgres text extraction) and
// arrowJSONPattern matches `expr->'key'` (postgres JSON extraction). Both
// become registered scalar shims with exact postgres semantics; the LHS is
// an identifier chain (column refs, possibly qualified).
var arrowTextPattern = regexp.MustCompile(`([A-Za-z_][\w."]*)->>'((?:[^']|'')*)'`)
var arrowJSONPattern = regexp.MustCompile(`([A-Za-z_][\w."]*)->'((?:[^']|'')*)'`)

func rewriteArrow(query string) string {
	out := arrowTextPattern.ReplaceAllString(query, `pg_arrow_text($1,'$2')`)
	return arrowJSONPattern.ReplaceAllString(out, `pg_arrow_json($1,'$2')`)
}
