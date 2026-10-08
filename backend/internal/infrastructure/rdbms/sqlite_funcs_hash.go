package rdbms

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// registerHashFuncs registers convert_to/sha256/encode as a composing trio
// — encode(sha256(convert_to(...)),'hex') works verbatim — plus
// gen_random_uuid.
func registerHashFuncs() {
	// convert_to(text,'UTF8') is identity; sqlite TEXT is already UTF8.
	mustRegister("convert_to", 2, true, func(args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}
		enc, _ := args[1].(string)
		if !strings.EqualFold(enc, "UTF8") {
			return nil, fmt.Errorf("rdbms: unsupported convert_to encoding %q", enc)
		}
		return sqlText(args[0]), nil
	})
	// sha256(x) yields raw digest bytes for encode(...,'hex'|'base64').
	mustRegister("sha256", 1, true, func(args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}
		sum := sha256.Sum256([]byte(sqlText(args[0])))
		return append([]byte(nil), sum[:]...), nil
	})
	mustRegister("encode", 2, true, encodeValue)
	// gen_random_uuid(): v4 text via crypto/rand.
	mustRegister("gen_random_uuid", 0, false, func(args []driver.Value) (driver.Value, error) {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		b[6] = b[6]&0x0f | 0x40
		b[8] = b[8]&0x3f | 0x80
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
	})
}

// encodeValue implements encode(x,'hex'|'base64') over raw bytes or text.
func encodeValue(args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil
	}
	var raw []byte
	switch t := args[0].(type) {
	case []byte:
		raw = t
	default:
		raw = []byte(sqlText(args[0]))
	}
	enc, _ := args[1].(string)
	switch strings.ToLower(enc) {
	case "hex":
		return hex.EncodeToString(raw), nil
	case "base64":
		return base64.StdEncoding.EncodeToString(raw), nil
	default:
		return nil, fmt.Errorf("rdbms: unsupported encode %q", enc)
	}
}
