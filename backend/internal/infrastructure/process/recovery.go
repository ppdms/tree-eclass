package process

import (
	"fmt"
	"path/filepath"
	"syscall"
	"time"

	"tree-eclass/internal/domain/platform"
)

func (m Manager) stopOrphan(name string, r Record) error {
	var spec Spec
	if err := platform.ReadJSON(filepath.Join(m.dir(name), "intent.json"), &spec); err != nil {
		return err
	}
	birth, err := Birth(r.ChildPID)
	group, groupErr := syscall.Getpgid(r.ChildPID)
	if err != nil || groupErr != nil || r.ChildBirth == "" || birth != r.ChildBirth || group != r.ChildPID ||
		spec.Token != r.Token {
		return fmt.Errorf(
			"cannot verify orphaned %s process group %d; refusing to signal it or checkpoint",
			name,
			r.ChildPID,
		)
	}
	sig := syscall.Signal(spec.StopSignal)
	if sig == 0 {
		sig = syscall.SIGTERM
	}
	if err = syscall.Kill(-r.ChildPID, sig); err != nil && err != syscall.ESRCH {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for syscall.Kill(-r.ChildPID, 0) == nil {
		if time.Now().After(deadline) {
			return fmt.Errorf("orphaned %s did not stop gracefully; checkpoint refused", name)
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.ChildPID = 0
	r.ChildBirth = ""
	return platform.WriteJSON(filepath.Join(m.dir(name), "process.json"), r)
}
