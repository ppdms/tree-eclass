package rdbms_test

import (
	"sync"
	"testing"
)

func TestConcurrentHealthProbesDoNotLoseRuntimeOwnership(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			start := make(chan struct{})
			failures := make(chan error, 16)
			var workers sync.WaitGroup
			for range 16 {
				workers.Go(func() {
					<-start
					for range 20 {
						if err := store.Ping(t.Context()); err != nil {
							failures <- err
							return
						}
					}
				})
			}
			close(start)
			workers.Wait()
			close(failures)
			for err := range failures {
				t.Errorf("concurrent health probe: %v", err)
			}
		})
	}
}
