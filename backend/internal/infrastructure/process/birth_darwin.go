package process

import (
	"fmt"
	"golang.org/x/sys/unix"
)

// Kernel start time distinguishes a recorded child from a recycled PID without
// invoking ps or relying on its second-resolution human display.
func Birth(pid int) (string, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	if info.Proc.P_pid != int32(pid) {
		return "", unix.ESRCH
	}
	return fmt.Sprintf("%d.%06d", info.Proc.P_starttime.Sec, info.Proc.P_starttime.Usec), nil
}
