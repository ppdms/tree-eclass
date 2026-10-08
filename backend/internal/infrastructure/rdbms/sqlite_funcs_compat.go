package rdbms

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

// registerCompatFuncs registers postgres builtins with no sqlite equivalent:
// greatest/least, validation probes, clock readings, timestamp helpers.
func registerCompatFuncs() {
	// greatest/least: numeric comparison; NULL propagates like postgres.
	mustRegister("greatest", -1, false, func(args []driver.Value) (driver.Value, error) {
		return extremum(args, true)
	})
	mustRegister("least", -1, false, func(args []driver.Value) (driver.Value, error) {
		return extremum(args, false)
	})
	// pg_input_is_valid: schema stores validated text; timestamptz checks
	// parseable-or-infinity, other types check non-empty.
	mustRegister("pg_input_is_valid", 2, true, func(args []driver.Value) (driver.Value, error) {
		text, _ := args[0].(string)
		typ, _ := args[1].(string)
		return isValidInput(text, typ), nil
	})
	// clock_timestamp()/now(): schema TEXT timestamps (UTC).
	mustRegister("clock_timestamp", 0, false, func(args []driver.Value) (driver.Value, error) {
		return NowUTC(), nil
	})
	mustRegister("now", 0, false, func(args []driver.Value) (driver.Value, error) {
		return NowUTC(), nil
	})
	// to_char: only the schema's two timestamp formats occur.
	mustRegister("to_char", 2, true, func(args []driver.Value) (driver.Value, error) {
		text, _ := args[0].(string)
		format, _ := args[1].(string)
		return formatTimestamp(text, format)
	})
	mustRegister("extract_epoch", -1, true, extractEpoch)
}

// extractEpoch implements extract(epoch FROM ts): day-math needs epoch days.
func extractEpoch(args []driver.Value) (driver.Value, error) {
	for _, arg := range args {
		if text, ok := arg.(string); ok && text != "" {
			t, err := parseStoredTimestamp(text)
			if err != nil {
				return nil, err
			}
			return float64(t.Unix()), nil
		}
	}
	return nil, nil
}

func extremum(args []driver.Value, wantMax bool) (driver.Value, error) {
	var best float64
	var have bool
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
		f, ok := toFloat(arg)
		if !ok {
			return nil, fmt.Errorf("rdbms: greatest/least on non-numeric %v", arg)
		}
		if !have || (wantMax && f > best) || (!wantMax && f < best) {
			best, have = f, true
		}
	}
	if !have {
		return nil, nil
	}
	return best, nil
}

func toFloat(v driver.Value) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func isValidInput(text, typ string) int64 {
	if text == "" {
		return 0
	}
	switch strings.ToLower(typ) {
	case "timestamptz", "timestamp":
		if text == "-infinity" || text == "infinity" {
			return 1
		}
		if _, err := parseStoredTimestamp(text); err == nil {
			return 1
		}
		if _, err := time.Parse(time.RFC3339Nano, text); err == nil {
			return 1
		}
		return 0
	default:
		return 1
	}
}

// parseStoredTimestamp reads the schema's UTC text format with optional
// fractional seconds.
func parseStoredTimestamp(text string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05", time.RFC3339Nano} {
		if t, err := time.ParseInLocation(layout, text, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("rdbms: unparseable timestamp %q", text)
}

func formatTimestamp(text, format string) (driver.Value, error) {
	t, err := parseStoredTimestamp(text)
	if err != nil {
		// to_char(clock_timestamp() ...) receives NowUTC text; fall back to
		// now on unparseable input (e.g. '-infinity' never reaches here).
		t = time.Now().UTC()
	}
	switch format {
	case "YYYY-MM-DD HH24:MI:SS":
		return t.Format("2006-01-02 15:04:05"), nil
	case "YYYY-MM-DD HH24:MI:SS.MS":
		return t.Format("2006-01-02 15:04:05.000"), nil
	default:
		return nil, fmt.Errorf("rdbms: unsupported to_char format %q", format)
	}
}
