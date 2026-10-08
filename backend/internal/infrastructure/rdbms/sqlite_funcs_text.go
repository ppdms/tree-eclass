package rdbms

import (
	"database/sql/driver"
	"strings"
	"unicode"
)

// registerTextFuncs registers postgres text helpers used by knowledge
// status/diagnostics queries. left counts characters like postgres; strpos
// is 1-based with 0 for absent.
func registerTextFuncs() {
	mustRegister("left", 2, true, leftValue)
	mustRegister("initcap", 1, true, func(args []driver.Value) (driver.Value, error) {
		return initcapValue(sqlText(args[0])), nil
	})
	mustRegister("octet_length", 1, true, func(args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}
		if raw, ok := args[0].([]byte); ok {
			return int64(len(raw)), nil
		}
		return int64(len(sqlText(args[0]))), nil
	})
	mustRegister("strpos", 2, true, func(args []driver.Value) (driver.Value, error) {
		return int64(strings.Index(sqlText(args[0]), sqlText(args[1])) + 1), nil
	})
	mustRegister("starts_with", 2, true, func(args []driver.Value) (driver.Value, error) {
		if strings.HasPrefix(sqlText(args[0]), sqlText(args[1])) {
			return int64(1), nil
		}
		return int64(0), nil
	})
}

// leftValue implements left(text, n); negative n drops |n| trailing chars.
func leftValue(args []driver.Value) (driver.Value, error) {
	runes := []rune(sqlText(args[0]))
	n, err := sqlInt(args[1])
	if err != nil {
		return nil, err
	}
	if n < 0 {
		if drop := int(-n); drop < len(runes) {
			return string(runes[:len(runes)-drop]), nil
		}
		return "", nil
	}
	if int(n) < len(runes) {
		return string(runes[:n]), nil
	}
	return string(runes), nil
}

// initcapValue mirrors postgres initcap: first alphanumeric of each word
// upper-cased, the rest lower-cased; words split on non-alphanumerics.
func initcapValue(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	upper := true
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if upper {
				b.WriteRune(unicode.ToUpper(r))
			} else {
				b.WriteRune(unicode.ToLower(r))
			}
			upper = false
		} else {
			b.WriteRune(r)
			upper = true
		}
	}
	return b.String()
}
