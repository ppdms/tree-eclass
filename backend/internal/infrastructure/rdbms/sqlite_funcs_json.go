package rdbms

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// registerJSONFuncs registers postgres JSON constructors, aggregates and
// type probes. Values encode by Go type with postgres jsonb text output
// (', ' and ': ' separators, no HTML escaping) so hash inputs stay
// byte-identical across backends. A TEXT arg holding JSON is
// indistinguishable from plain text: call sites nesting JSON values MUST be
// rewritten in Go, never passed through these shims.
func registerJSONFuncs() {
	mustRegister("pg_arrow_text", 2, true, func(args []driver.Value) (driver.Value, error) {
		return pgArrow(args[0], args[1], true)
	})
	mustRegister("pg_arrow_json", 2, true, func(args []driver.Value) (driver.Value, error) {
		return pgArrow(args[0], args[1], false)
	})
	mustRegister("jsonb_build_object", -1, true, func(args []driver.Value) (driver.Value, error) {
		return jsonbBuildObject(args)
	})
	// jsonb_agg/string_agg: NULL inputs skipped; no rows → NULL so
	// coalesce(..., '[]') keeps working. ORDER BY is honored pre-step.
	mustRegisterAggregate("jsonb_agg", 1, true, func() sqlite.AggregateFunction { return &jsonbAgg{} })
	mustRegisterAggregate("string_agg", 2, true, func() sqlite.AggregateFunction { return &stringAgg{} })
	// jsonb_typeof: TEXT/BLOB args hold JSON and are classified by parsing;
	// unparseable text → NULL.
	mustRegister("jsonb_typeof", 1, true, func(args []driver.Value) (driver.Value, error) {
		return jsonbTypeof(args[0])
	})
	// to_jsonb scalar form only (to_jsonb($1), to_jsonb(id)). Row/table
	// forms MUST be rewritten; they fail loudly at prepare time.
	mustRegister("to_jsonb", 1, true, func(args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return "null", nil
		}
		return jsonbEncode(args[0])
	})
}

// jsonbBuildArray implements jsonb_build_array(v,...).
func jsonbBuildArray(args []driver.Value) (driver.Value, error) {
	elems := make([]string, 0, len(args))
	for _, arg := range args {
		s, err := jsonbEncode(arg)
		if err != nil {
			return nil, err
		}
		elems = append(elems, s)
	}
	return "[" + strings.Join(elems, ", ") + "]", nil
}

// jsonbEncode renders one SQL value as postgres jsonb text: text→JSON
// string, integers/floats→numbers, bools→booleans, NULL→null.
func jsonbEncode(v driver.Value) (string, error) {
	switch t := v.(type) {
	case nil:
		return "null", nil
	case string:
		return quoteJSONString(t), nil
	case []byte:
		return quoteJSONString(string(t)), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	case time.Time:
		return quoteJSONString(t.UTC().Format(time.RFC3339Nano)), nil
	default:
		return "", fmt.Errorf("rdbms: cannot encode %T as jsonb", v)
	}
}

// quoteJSONString quotes text with postgres jsonb string escaping (short
// \b \f \n \r \t forms, \u00xx otherwise, raw UTF-8 passthrough, no HTML
// escaping of <>&).
func quoteJSONString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := range s {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, c)
			} else {
				b.WriteByte(c)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// jsonbBuildObject implements jsonb_build_object(k,v,...) with postgres
// text output. Later duplicates win; a NULL key errors like postgres.
func jsonbBuildObject(args []driver.Value) (driver.Value, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("rdbms: jsonb_build_object needs an even argument count, got %d", len(args))
	}
	type pair struct{ key, val string }
	pairs := make([]pair, 0, len(args)/2)
	index := make(map[string]int, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		if args[i] == nil {
			return nil, fmt.Errorf("rdbms: jsonb_build_object key must not be null")
		}
		key := sqlText(args[i])
		val, err := jsonbEncode(args[i+1])
		if err != nil {
			return nil, err
		}
		if at, ok := index[key]; ok {
			pairs[at].val = val
			continue
		}
		index[key] = len(pairs)
		pairs = append(pairs, pair{key, val})
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, p := range pairs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteJSONString(p.key))
		b.WriteString(": ")
		b.WriteString(p.val)
	}
	b.WriteByte('}')
	return b.String(), nil
}

// jsonbAgg accumulates jsonb_agg(x), skipping NULL inputs; WindowValue
// returns NULL when no rows aggregated.
type jsonbAgg struct{ elems []string }

func (a *jsonbAgg) Step(ctx *sqlite.FunctionContext, rowArgs []driver.Value) error {
	if len(rowArgs) != 1 {
		return fmt.Errorf("rdbms: jsonb_agg needs 1 argument, got %d", len(rowArgs))
	}
	if rowArgs[0] == nil {
		return nil
	}
	s, err := jsonbEncode(rowArgs[0])
	if err != nil {
		return err
	}
	a.elems = append(a.elems, s)
	return nil
}

func (a *jsonbAgg) WindowInverse(ctx *sqlite.FunctionContext, rowArgs []driver.Value) error {
	return nil
}

func (a *jsonbAgg) WindowValue(ctx *sqlite.FunctionContext) (driver.Value, error) {
	if len(a.elems) == 0 {
		return nil, nil
	}
	return "[" + strings.Join(a.elems, ", ") + "]", nil
}

func (a *jsonbAgg) Final(ctx *sqlite.FunctionContext) {}

// stringAgg accumulates string_agg(x, delim), skipping NULL values with
// each row's own delimiter separating (postgres semantics).
type stringAgg struct {
	parts strings.Builder
	rows  int
}

func (a *stringAgg) Step(ctx *sqlite.FunctionContext, rowArgs []driver.Value) error {
	if len(rowArgs) != 2 {
		return fmt.Errorf("rdbms: string_agg needs 2 arguments, got %d", len(rowArgs))
	}
	if rowArgs[0] == nil {
		return nil
	}
	if a.rows > 0 {
		a.parts.WriteString(sqlText(rowArgs[1]))
	}
	a.parts.WriteString(sqlText(rowArgs[0]))
	a.rows++
	return nil
}

func (a *stringAgg) WindowInverse(ctx *sqlite.FunctionContext, rowArgs []driver.Value) error {
	return nil
}

func (a *stringAgg) WindowValue(ctx *sqlite.FunctionContext) (driver.Value, error) {
	if a.rows == 0 {
		return nil, nil
	}
	return a.parts.String(), nil
}

func (a *stringAgg) Final(ctx *sqlite.FunctionContext) {}

// jsonbTypeof reports the postgres jsonb_typeof name for one value.
func jsonbTypeof(v driver.Value) (driver.Value, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return classifyJSON(t)
	case []byte:
		return classifyJSON(string(t))
	case int64, float64:
		return "number", nil
	case bool:
		return "boolean", nil
	case time.Time:
		return "string", nil
	default:
		return nil, fmt.Errorf("rdbms: cannot typeof %T", v)
	}
}

func classifyJSON(s string) (driver.Value, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, nil
	}
	switch v.(type) {
	case string:
		return "string", nil
	case float64:
		return "number", nil
	case bool:
		return "boolean", nil
	case nil:
		return "null", nil
	case []any:
		return "array", nil
	case map[string]any:
		return "object", nil
	default:
		return nil, nil
	}
}

// pgArrow implements postgres `json->>'key'` (text=true) and `json->'key'`
// (text=false) over a JSON object. Text form unquotes strings and renders
// numbers/booleans canonically; JSON form returns compact JSON. Missing
// keys, JSON null, non-objects and unparseable input all yield NULL.
func pgArrow(doc, key driver.Value, text bool) (driver.Value, error) {
	raw, _ := doc.(string)
	if bytes, ok := doc.([]byte); ok {
		raw = string(bytes)
	}
	name, _ := key.(string)
	if raw == "" || name == "" {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, nil
	}
	value, ok := obj[name]
	if !ok || value == nil {
		return nil, nil
	}
	if !text {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, nil
		}
		return string(encoded), nil
	}
	switch typed := value.(type) {
	case string:
		return typed, nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case bool:
		if typed {
			return "true", nil
		}
		return "false", nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil
	}
	return string(encoded), nil
}
