package process

import (
	"fmt"
	"os"
	"strings"
)

func Birth(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	// comm may contain spaces and parentheses; fields after its closing ')' are
	// stable. Field 22 is process start time in ticks since boot.
	close := strings.LastIndexByte(string(data), ')')
	if close < 0 {
		return "", fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(data[close+1:]))
	if len(fields) < 20 {
		return "", fmt.Errorf("short process stat")
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(boot)) + ":" + fields[19], nil
}
