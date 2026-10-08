package rdbms

import (
	"regexp"
	"strconv"
	"strings"
)

var placeholderRef = regexp.MustCompile(`\$(\d+)`)

func maxPlaceholder(query string) int {
	max := 0
	for _, m := range placeholderRef.FindAllStringSubmatch(query, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil && n > max {
			max = n
		}
	}
	return max
}

// dummyArgs supplies one bound value per placeholder so placeholder()
// expansion and EXPLAIN both see a complete statement. Placeholders fed to
// ANY(...) get a one-element slice so expandAny splices; everything else
// gets a scalar. Queries with native `?` and no $N are already sqlite and
// skip translation; their count still sizes EXPLAIN args. Types are
// irrelevant: EXPLAIN never executes.
func dummyArgs(query string) []any {
	n := maxPlaceholder(query)
	if !strings.Contains(query, "$") {
		return make([]any, strings.Count(query, "?"))
	}
	if q := strings.Count(query, "?"); q > n {
		n = q
	}
	slices := map[int]bool{}
	for _, m := range regexp.MustCompile(`(?i)ANY\s*\(\s*\$(\d+)`).FindAllStringSubmatch(query, -1) {
		if i, err := strconv.Atoi(m[1]); err == nil {
			slices[i] = true
		}
	}
	args := make([]any, n)
	for i := range args {
		if slices[i+1] {
			args[i] = []any{1}
			continue
		}
		args[i] = 1
	}
	return args
}

// stmtStartPattern matches a complete statement opening: optional parens or
// comments, then SELECT/INSERT/UPDATE/DELETE/WITH/EXPLAIN/VALUES.
var stmtStartPattern = regexp.MustCompile(`(?is)^\s*(\([^)]*\)\s*|(--[^\n]*\n\s*|/\*.*?\*/\s*)*)(SELECT|INSERT|UPDATE|DELETE|WITH|VALUES)\b`)

// trailingOpPattern matches text that ends mid-statement: dangling comma,
// operator, clause keyword, open paren, or schema dot.
var trailingOpPattern = regexp.MustCompile(`(?i)(,\s*|(\b(AND|OR|WHERE|JOIN|ON|SELECT|FROM|SET|VALUES|BY|IN|AS|NOT|ORDER|GROUP|HAVING|LIMIT|UNION|ALL|CASE|WHEN|THEN|ELSE)\b)\s*|\(\s*|\.\s*)$`)

// isFragment reports string literals that are query pieces, not runnable
// statements: bare predicates, prefix fragments, suffixes, unbalanced text,
// or WITH blocks without a main query. Those need the runtime harness;
// EXPLAIN would only report the truncation.
func isFragment(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	if !stmtStartPattern.MatchString(trimmed) {
		return true
	}
	if trailingOpPattern.MatchString(trimmed) {
		return true
	}
	if !balancedParens(trimmed) {
		return true
	}
	return isBareWith(trimmed)
}

// balancedParens reports balanced () outside string literals.
func balancedParens(text string) bool {
	depth := 0
	inString := false
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '\'':
			if inString && i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				depth--
				if depth < 0 {
					return false
				}
			}
		}
	}
	return depth == 0 && !inString
}

// isBareWith reports WITH blocks with no main query: after the leading
// WITH name AS (...), [,...] closes, nothing runnable follows.
func isBareWith(trimmed string) bool {
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "WITH ") {
		return false
	}
	depth := 0
	inString := false
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case '\'':
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				depth--
				if depth == 0 {
					rest := strings.TrimSpace(trimmed[i+1:])
					if rest == "" {
						return true
					}
					restUpper := strings.ToUpper(rest)
					return !strings.HasPrefix(restUpper, "SELECT") &&
						!strings.HasPrefix(restUpper, "INSERT") &&
						!strings.HasPrefix(restUpper, "UPDATE") &&
						!strings.HasPrefix(restUpper, "DELETE")
				}
			}
		}
	}
	return false
}
