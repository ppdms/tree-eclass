package server

import (
	"bytes"
	"errors"
	"testing"
)

func TestExportPreservesDiskReserve(t *testing.T) {
	var target bytes.Buffer
	free := uint64(5*1024*1024*1024 + 8)
	spool := exportSpool{target: &target, available: func() (uint64, error) { return free, nil }}
	if _, err := spool.Write([]byte("12345")); err != nil {
		t.Fatal(err)
	}
	free -= 5
	if _, err := spool.Write([]byte("6789")); !errors.Is(err, errExportSpace) || target.String() != "12345" {
		t.Fatal("export consumed reserved disk space", err)
	}
	free = 0
	if _, err := spool.Write(bytes.Repeat([]byte("x"), 4*1024*1024+1)); !errors.Is(err, errExportSpace) {
		t.Fatal("large write bypassed reserve", err)
	}
}
