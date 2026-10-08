package rdbms

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
)

func init() {
	registerCompatFuncs()
	registerJSONFuncs()
	registerHashFuncs()
	registerTextFuncs()
}

func mustRegister(
	name string,
	nArg int32,
	deterministic bool,
	fn func(args []driver.Value) (driver.Value, error),
) {
	impl := &sqlite.FunctionImpl{NArgs: nArg, Deterministic: deterministic, Scalar: func(
		ctx *sqlite.FunctionContext,
		args []driver.Value,
	) (driver.Value, error) {
		return fn(args)
	}}
	if err := sqlite.RegisterFunction(name, impl); err != nil {
		panic(fmt.Sprintf("rdbms: register %s: %v", name, err))
	}
}

func mustRegisterAggregate(
	name string,
	nArg int32,
	deterministic bool,
	make func() sqlite.AggregateFunction,
) {
	impl := &sqlite.FunctionImpl{NArgs: nArg, Deterministic: deterministic, MakeAggregate: func(
		ctx sqlite.FunctionContext,
	) (sqlite.AggregateFunction, error) {
		return make(), nil
	}}
	if err := sqlite.RegisterFunction(name, impl); err != nil {
		panic(fmt.Sprintf("rdbms: register %s: %v", name, err))
	}
}

// sqlText renders a scalar argument as text (NULL → "").
func sqlText(v driver.Value) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		return t.UTC().Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprint(v)
	}
}

// sqlInt renders an integer argument.
func sqlInt(v driver.Value) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case float64:
		return int64(t), nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("rdbms: not an integer %q", t)
		}
		return n, nil
	case []byte:
		return sqlInt(string(t))
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("rdbms: not an integer %v", v)
	}
}
