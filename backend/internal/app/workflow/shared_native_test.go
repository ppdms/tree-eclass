package workflow

import (
	"fmt"
	"net"
	"testing"
)

// Service tests share one sqlite layout per test via nativeSharedController.
// Lifecycle and destructive-storage tests keep their fully isolated fixtures.
func nativeSharedController(t *testing.T) *Controller {
	t.Helper()
	c := nativeController(t)
	if err := c.infrastructure(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := c.migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return c
}

func nativePorts(t *testing.T) Ports {
	t.Helper()
	ports, err := reserveNativePorts()
	if err != nil {
		t.Fatal(err)
	}
	return ports
}

func reserveNativePorts() (ports Ports, err error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return Ports{}, err
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	if port < 1024 {
		return Ports{}, fmt.Errorf("could not reserve native fixture port")
	}
	return Ports{HTTP: port}, nil
}
