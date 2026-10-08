package rdbms

import (
	"regexp"
	"strings"
)

// distinctOnPattern matches the `SELECT DISTINCT ON(keys) cols FROM` opening.
// The rewritten segment spans FROM through the ORDER BY tiebreak, ending at
// LIMIT (depth 0), a closing paren at depth 0 (CTE/subquery wrapper), or end
// of string; everything outside is preserved verbatim. Postgres keeps the
// first row per key group under the ORDER BY; sqlite expresses it with
// ROW_NUMBER over the same partition/order, with LIMIT staying on the
// rewritten segment where postgres limits distinct rows.
var distinctOnPattern = regexp.MustCompile(`(?is)\bSELECT\s+DISTINCT\s+ON\s*\(([^)]+)\)\s+`)

func rewriteDistinctOn(query string) string {
	loc := distinctOnPattern.FindStringIndex(query)
	if loc == nil {
		return query
	}
	head, rest := query[:loc[0]], query[loc[1]:]
	keys := distinctOnPattern.FindStringSubmatch(query)[1]
	fromLoc := regexp.MustCompile(`(?is)\bFROM\b`).FindStringIndex(rest)
	if fromLoc == nil {
		return query
	}
	cols, afterFrom := rest[:fromLoc[0]], rest[fromLoc[1]:]
	boundary := distinctSegmentBoundary(afterFrom)
	if boundary.orderBy == "" {
		return query
	}
	tiebreak := distinctTiebreak(boundary.orderBy, keys)
	windowOrder := strings.Join(tiebreak, ", ")
	if windowOrder == "" {
		windowOrder = keys
	}
	segment := "SELECT " + strings.TrimSpace(cols) + " FROM (SELECT " + strings.TrimSpace(cols) +
		", ROW_NUMBER() OVER (PARTITION BY " + keys + " ORDER BY " + windowOrder + ") rn FROM " +
		strings.TrimSpace(afterFrom[:boundary.end]) + ") WHERE rn=1"
	if outer := strings.Join(tiebreak, ", "); outer != "" {
		segment += " ORDER BY " + outer
	}
	segment += boundary.limit
	return head + segment + afterFrom[boundary.tail:]
}

// distinctBoundary marks the ORDER BY span inside the text after FROM: end
// is the segment end, orderBy the tiebreak text, limit the trailing LIMIT,
// tail the resume offset for the preserved suffix.
type distinctBoundary struct {
	end     int
	orderBy string
	limit   string
	tail    int
}

// distinctSegmentBoundary scans afterFrom tracking paren depth and strings,
// finding ORDER BY at depth 0, then ending at LIMIT (depth 0), `)` at depth 0,
// or end of string.
func distinctSegmentBoundary(afterFrom string) distinctBoundary {
	var boundary distinctBoundary
	orderAt := findDistinctOrder(afterFrom)
	if orderAt < 0 {
		return boundary
	}
	return scanDistinctTail(afterFrom, orderAt)
}

// distinctScanner carries the depth/string scan state for the segment walk.
type distinctScanner struct {
	text     string
	depth    int
	inString bool
	i        int
}

// isWord reports a whole-word match at the cursor.
func (s *distinctScanner) isWord(word string) bool {
	at := s.i
	if at+len(word) > len(s.text) || !strings.EqualFold(s.text[at:at+len(word)], word) {
		return false
	}
	before := at == 0 || !isSchemaIdent(s.text[at-1])
	after := at+len(word) >= len(s.text) || !isSchemaIdent(s.text[at+len(word)])
	return before && after
}

// findDistinctOrder returns the offset just past ORDER BY at depth 0, or -1.
func findDistinctOrder(afterFrom string) int {
	s := &distinctScanner{text: afterFrom}
	for s.i < len(s.text) {
		c := s.text[s.i]
		if c == '\'' {
			s.inString = !s.inString
			s.i++
			continue
		}
		if s.inString {
			s.i++
			continue
		}
		switch {
		case c == '(':
			s.depth++
		case c == ')':
			if s.depth == 0 {
				return -1
			}
			s.depth--
		case s.isWord("ORDER"):
			byMatch := regexp.MustCompile(`(?i)^ORDER\s+BY\b`).FindString(s.text[s.i:])
			if byMatch != "" {
				return s.i + len(byMatch)
			}
		}
		s.i++
	}
	return -1
}

// scanDistinctTail resolves the segment end from the ORDER BY offset.
func scanDistinctTail(afterFrom string, orderAt int) distinctBoundary {
	var boundary distinctBoundary
	s := &distinctScanner{text: afterFrom, i: orderAt}
	for s.i < len(s.text) {
		c := s.text[s.i]
		if c == '\'' {
			s.inString = !s.inString
			s.i++
			continue
		}
		if s.inString {
			s.i++
			continue
		}
		switch {
		case c == '(':
			s.depth++
		case c == ')':
			if s.depth == 0 {
				boundary.end = s.i
				boundary.orderBy = strings.TrimSpace(s.text[orderAt:s.i])
				boundary.tail = s.i
				return boundary
			}
			s.depth--
		case s.depth == 0 && s.isWord("LIMIT"):
			boundary.end = s.i
			boundary.orderBy = strings.TrimSpace(s.text[orderAt:s.i])
			tail := regexp.MustCompile(`(?i)^LIMIT\s+\S+`).FindString(s.text[s.i:])
			boundary.limit = " " + strings.TrimSpace(tail)
			boundary.tail = s.i + len(tail)
			return boundary
		}
		s.i++
	}
	boundary.end = len(s.text)
	boundary.orderBy = strings.TrimSpace(s.text[orderAt:])
	boundary.tail = len(s.text)
	return boundary
}

// distinctTiebreak drops the DISTINCT key columns from the ORDER BY items,
// leaving the tiebreak order for the window.
func distinctTiebreak(orderBy, keys string) []string {
	keySet := map[string]bool{}
	for _, key := range splitTopLevel(keys, ',') {
		keySet[strings.ToLower(strings.TrimSpace(key))] = true
	}
	tiebreak := []string{}
	for _, item := range splitTopLevel(orderBy, ',') {
		fields := strings.Fields(strings.TrimSpace(item))
		first := ""
		if len(fields) > 0 {
			first = strings.Trim(fields[0], "()\"")
			if dot := strings.LastIndex(first, "."); dot >= 0 {
				first = first[dot+1:]
			}
		}
		if keySet[strings.ToLower(first)] {
			continue
		}
		tiebreak = append(tiebreak, strings.TrimSpace(item))
	}
	return tiebreak
}

// splitTopLevel splits on a separator ignoring parenthesized nesting and
// single-quoted literals.
func splitTopLevel(text string, sep rune) []string {
	var parts []string
	depth := 0
	inString := false
	start := 0
	for i, c := range text {
		switch {
		case c == '\'':
			inString = !inString
		case inString:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == sep && depth == 0:
			parts = append(parts, text[start:i])
			start = i + 1
		}
	}
	return append(parts, text[start:])
}
