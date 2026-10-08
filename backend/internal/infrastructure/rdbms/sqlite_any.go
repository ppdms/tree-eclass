package rdbms

import (
	"fmt"
	"regexp"
	"strings"
)

// anyPattern matches `=ANY($N)` / `<>ANY($N)` / `!=ANY($N)` with optional casts.
// The bound arg is a Go slice. Expansion rewrites to IN with one fresh
// $M per element and splices args; nil/empty becomes `IN (NULL)` which never
// matches, preserving OR/AND logic shape.
var anyPattern = regexp.MustCompile(`(?i)(=|<>|!=)\s*ANY\s*\(\s*(\$\d+)(::[a-z\[\]]+)?\s*\)`)

// anySplice is one ANY(...) match rewritten to an IN list.
type anySplice struct {
	start, end int
	elems      []any
	consumes   int
	dropClose  bool
	deadTrue   bool
}

// anyCut is a dead guard span excised from the output.
type anyCut struct{ start, end int }

// expandAny rewrites ANY array comparisons to IN lists. Placeholder numbers
// in the output are reassigned sequentially in first-occurrence order and
// args reordered to match. A `($N IS NULL OR ...)` guard sharing the same
// $N is dead code after expansion (bound slices are never SQL NULL: nil
// becomes empty IN (NULL)) and is dropped so placeholder accounting stays
// exact. cardinality($N)=0 guards are likewise dead when the slice is empty
// (IN (NULL) matches nothing) and always true otherwise — but the OR other
// branch then decides; dropping just the guard keeps semantics: empty slice
// → other branch false → whole OR false. Correct: drop `cardinality($N)=0
// OR ` too.
func expandAny(query string, args []any) (string, []any) {
	splices := collectAnySplices(query, args)
	if len(splices) == 0 {
		return query, args
	}
	cuts := exciseAnyGuards(query, args, splices)
	return emitAnyExpansion(query, args, splices, cuts)
}

// arrayPositionPattern matches `array_position($N[, ::cast], col)`: postgres
// returns the 1-based index of col in the bound list (NULL when absent).
var arrayPositionPattern = regexp.MustCompile(`(?i)array_position\s*\(\s*(\$\d+)(::[a-z\[\]]+)?\s*,\s*([^)]+)\)`)

// expandArrayPosition rewrites array_position to CASE before ANY expansion.
// Fresh $M numbers continue past the current max; expandAny reassigns all
// placeholders sequentially afterward, so numbering stays exact.
func expandArrayPosition(query string, args []any) (string, []any) {
	max := maxPlaceholder(query)
	var out strings.Builder
	cursor := 0
	final := append([]any{}, args...)
	for _, m := range arrayPositionPattern.FindAllStringSubmatchIndex(query, -1) {
		var num int
		fmt.Sscanf(query[m[2]+1:m[3]], "%d", &num)
		if num <= 0 || num > len(args) {
			continue
		}
		elems := anySlice(args[num-1])
		out.WriteString(query[cursor:m[0]])
		if len(elems) == 0 {
			out.WriteString("NULL")
			cursor = m[1]
			continue
		}
		col := strings.TrimSpace(query[m[6]:m[7]])
		out.WriteString("CASE " + col)
		for i := range elems {
			max++
			fmt.Fprintf(&out, " WHEN $%d THEN %d", max, i+1)
			final = append(final, elems[i])
		}
		out.WriteString(" END")
		cursor = m[1]
	}
	out.WriteString(query[cursor:])
	return out.String(), final
}

// collectAnySplices finds ANY(...) matches bound to Go slice args.
func collectAnySplices(query string, args []any) []anySplice {
	var splices []anySplice
	for _, m := range anyPattern.FindAllStringSubmatchIndex(query, -1) {
		num := guardNum(query[m[4]:m[5]])
		idx := num - 1
		if idx < 0 || idx >= len(args) || args[idx] == nil {
			continue
		}
		switch args[idx].(type) {
		case []int64, []string, []int32, []int, []any:
			// Splicable.
		default:
			continue
		}
		splices = append(splices, anySplice{
			start:    m[0],
			end:      m[1],
			elems:    anySlice(args[idx]),
			consumes: idx,
		})
	}
	return splices
}

// exciseAnyGuards collects dead guard spans for spliced placeholders and
// marks the splices whose guard parens they close.
func exciseAnyGuards(query string, args []any, splices []anySplice) []anyCut {
	consumed := make(map[int]bool, len(splices))
	for _, s := range splices {
		consumed[s.consumes] = true
	}
	cuts := exciseNullGuards(query, consumed, splices)
	cuts = append(cuts, exciseCardinalityGuards(query, args, consumed, splices)...)
	return cuts
}

// exciseNullGuards drops `($N IS NULL OR ...)` openers whose $N feeds a
// splice. Guards over bare scalars survive: they test nullable params.
func exciseNullGuards(query string, consumed map[int]bool, splices []anySplice) []anyCut {
	guardPattern := regexp.MustCompile(`\(\s*\$\d+(::[a-z\[\]]+)?\s+IS NULL\s+OR\s+`)
	var cuts []anyCut
	for _, loc := range guardPattern.FindAllStringIndex(query, -1) {
		num := guardNum(query[loc[0]:loc[1]])
		if num <= 0 || !consumed[num-1] {
			continue
		}
		// Excise the opener whether the slice is empty or not: its $N is
		// consumed by the splice, so leaving it emits a literal $N the
		// driver reads as a named parameter. The ANY's own close paren
		// closes the guard's open paren: drop one close after the splice.
		cuts = append(cuts, anyCut{loc[0], loc[1]})
		for i := range splices {
			if splices[i].end > loc[1] {
				splices[i].dropClose = true
				break
			}
		}
	}
	return cuts
}

// exciseCardinalityGuards drops `cardinality($N)=0 OR` guards whose $N feeds
// a splice. Empty slices make the whole OR branch true (emitted as TRUE);
// non-empty slices drop just the guard.
func exciseCardinalityGuards(query string, args []any, consumed map[int]bool, splices []anySplice) []anyCut {
	cardPattern := regexp.MustCompile(`cardinality\s*\(\s*\$\d+(::[a-z\[\]]+)?\s*\)\s*=\s*0\s+OR\s+`)
	var cuts []anyCut
	for _, loc := range cardPattern.FindAllStringIndex(query, -1) {
		num := guardNum(query[loc[0]:loc[1]])
		if num <= 0 || !consumed[num-1] {
			continue
		}
		cuts = append(cuts, anyCut{loc[0], loc[1]})
		if len(anySlice(args[num-1])) != 0 {
			continue
		}
		// Empty slice: the whole `(cardinality=0 OR ...)` is true. Excise
		// the guard AND the other branch, leaving TRUE (see deadTrue).
		for i := range splices {
			if splices[i].start <= loc[1] {
				continue
			}
			cuts = append(cuts, anyCut{rewindAnyOperator(query, splices[i].start), splices[i].end})
			splices[i].dropClose = true
			splices[i].deadTrue = true
			break
		}
	}
	return cuts
}

// rewindAnyOperator backs pos over whitespace, the comparison operator and
// the guarded column to the branch start.
func rewindAnyOperator(query string, pos int) int {
	opStart := pos
	for opStart > 0 && (query[opStart-1] == ' ' || query[opStart-1] == '\t') {
		opStart--
	}
	for opStart > 0 && isAnyOperatorChar(query[opStart-1]) {
		opStart--
	}
	for opStart > 0 && (query[opStart-1] == ' ' || query[opStart-1] == '\t') {
		opStart--
	}
	for opStart > 0 && isSchemaIdent(query[opStart-1]) {
		opStart--
	}
	return opStart
}

// isAnyOperatorChar reports the comparison characters of `=`, `<>`, `!=`.
func isAnyOperatorChar(c byte) bool {
	return c == '=' || c == '<' || c == '>' || c == '!'
}

// planAnyArgs orders final args: unconsumed originals first, then splice
// elements. elemNums[i] is splices[i]'s first 1-based placeholder number.
func planAnyArgs(args []any, splices []anySplice) ([]any, []int) {
	consumed := make(map[int]bool, len(splices))
	for _, s := range splices {
		consumed[s.consumes] = true
	}
	final := make([]any, 0, len(args))
	for i, a := range args {
		if !consumed[i] {
			final = append(final, a)
		}
	}
	elemNums := make([]int, len(splices))
	for i, s := range splices {
		elemNums[i] = len(final) + 1
		final = append(final, s.elems...)
	}
	return final, elemNums
}

// emitAnyExpansion renders splices into the query, skipping excised guards.
func emitAnyExpansion(query string, args []any, splices []anySplice, cuts []anyCut) (string, []any) {
	final, elemNums := planAnyArgs(args, splices)
	inCut := func(pos int) bool {
		for _, c := range cuts {
			if pos >= c.start && pos < c.end {
				return true
			}
		}
		return false
	}
	var out strings.Builder
	out.Grow(len(query) + len(splices)*8)
	cursor := 0
	flush := func(upto int) {
		for upto > cursor {
			if inCut(cursor) {
				cursor++
				continue
			}
			out.WriteByte(query[cursor])
			cursor++
		}
	}
	for i := range splices {
		if splices[i].deadTrue {
			// Whole branch excised via cuts (guard + op + ANY): flush to
			// the splice start (cut spans skipped), then emit TRUE.
			flush(splices[i].start)
			out.WriteString(" TRUE ")
		} else {
			flush(splices[i].start)
			writeAnyList(&out, splices, elemNums, i)
			cursor = splices[i].end
		}
		if splices[i].dropClose {
			cursor = skipAnyGuardClose(query, &out, cursor)
		}
	}
	flush(len(query))
	return out.String(), final
}

// writeAnyList emits `= (NULL)` for empty slices, else ` IN ($a,$b,...)`.
func writeAnyList(out *strings.Builder, splices []anySplice, elemNums []int, i int) {
	s := splices[i]
	if len(s.elems) == 0 {
		out.WriteString("= (NULL)")
		return
	}
	parts := make([]string, len(s.elems))
	for j := range s.elems {
		parts[j] = fmt.Sprintf("$%d", elemNums[i]+j)
	}
	out.WriteString(" IN (" + strings.Join(parts, ",") + ")")
}

// skipAnyGuardClose copies whitespace after a splice, then drops the guard's
// close paren.
func skipAnyGuardClose(query string, out *strings.Builder, cursor int) int {
	for cursor < len(query) && (query[cursor] == ' ' || query[cursor] == '\t' || query[cursor] == '\n') {
		out.WriteByte(query[cursor])
		cursor++
	}
	if cursor < len(query) && query[cursor] == ')' {
		cursor++
	}
	return cursor
}

// guardNum extracts the first $N placeholder number from a guard span.
func guardNum(span string) int {
	num := 0
	for i := range len(span) {
		if span[i] == '$' {
			j := i + 1
			for j < len(span) && span[j] >= '0' && span[j] <= '9' {
				j++
			}
			if j > i+1 {
				fmt.Sscanf(span[i+1:j], "%d", &num)
				return num
			}
		}
	}
	return 0
}

// anySlice converts a bound slice arg to elements. Supported: []int64,
// []string, []int32, []any, nil (empty).
func anySlice(v any) []any {
	switch s := v.(type) {
	case nil:
		return nil
	case []int64:
		out := make([]any, len(s))
		for i, e := range s {
			out[i] = e
		}
		return out
	case []string:
		out := make([]any, len(s))
		for i, e := range s {
			out[i] = e
		}
		return out
	case []int32:
		out := make([]any, len(s))
		for i, e := range s {
			out[i] = e
		}
		return out
	case []int:
		out := make([]any, len(s))
		for i, e := range s {
			out[i] = e
		}
		return out
	case []any:
		return s
	default:
		return []any{v}
	}
}
