package process

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"tree-eclass/internal/infrastructure/platform"
)

func TestFiniteCompletionAndFailure(t *testing.T) {
	m := manager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, success := range []bool{true, false, true} {
		command := "/usr/bin/true"
		if !success {
			command = "/usr/bin/false"
		}
		spec := Spec{Name: "migration", Token: time.Now().String(), Command: []string{command}}
		err := m.Run(ctx, spec)
		if (err == nil) != success {
			t.Fatalf("completion success=%v: %v", success, err)
		}
		if m.Alive(spec.Name) {
			t.Fatal("finite command returned while supervisor still owns the process")
		}
		var record Record
		if err = platform.ReadJSON(filepath.Join(m.dir(spec.Name), "process.json"), &record); err != nil ||
			!record.Completed ||
			record.ChildPID != 0 ||
			record.Token != spec.Token {
			t.Fatalf("missing durable completion: %+v %v", record, err)
		}
	}
}

func TestFiniteCancellationDrainsWriter(t *testing.T) {
	m := manager(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, Spec{Name: "migration", Token: "cancel-fixture", Command: []string{"/bin/sleep", "120"}})
	}()
	record := waitFiniteChild(t, m)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation claimed completion: %v", err)
	}
	if m.Alive("migration") || syscall.Kill(-record.ChildPID, 0) == nil {
		t.Fatal("writer survived cancellation")
	}
}

func TestFiniteSupervisorLossDoesNotClaimSuccess(t *testing.T) {
	m := manager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx, Spec{Name: "migration", Token: "crash-fixture", Command: []string{"/bin/sleep", "120"}})
	}()
	record := waitFiniteChild(t, m)
	t.Cleanup(func() {
		if err := m.Stop("migration"); err != nil {
			t.Error(err)
		}
	})
	if err := syscall.Kill(record.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("unconfirmed finite writer accepted as successful")
	}
	if err := m.Stop("migration"); err != nil {
		t.Fatal(err)
	}
	if syscall.Kill(-record.ChildPID, 0) == nil {
		t.Fatal("orphan writer survived recovery")
	}
}

func waitFiniteChild(t *testing.T, m Manager) Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var record Record
		if platform.ReadJSON(filepath.Join(m.dir("migration"), "process.json"), &record) == nil && record.ChildPID > 0 {
			return record
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = m.Stop("migration")
	t.Fatal("finite child never started")
	return Record{}
}
