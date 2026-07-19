package aerospike

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestForEachNamespace(t *testing.T) {
	e := &AerospikeEndpoint{Namespaces: []string{"a", "b", "c"}}

	var mu sync.Mutex
	seen := map[string]int{}
	err := forEachNamespace(e, 2, func(ns string) error {
		mu.Lock()
		seen[ns]++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(seen) != 3 || seen["a"] != 1 || seen["b"] != 1 || seen["c"] != 1 {
		t.Fatalf("expected each namespace probed exactly once, got %v", seen)
	}
}

// TestForEachNamespaceErrorIsolation locks in the guarantee that an error on one namespace
// does not prevent the others from being probed.
func TestForEachNamespaceErrorIsolation(t *testing.T) {
	e := &AerospikeEndpoint{Namespaces: []string{"a", "b", "c"}}

	var ran int32
	err := forEachNamespace(e, 2, func(ns string) error {
		atomic.AddInt32(&ran, 1)
		if ns == "b" {
			return errors.New("boom")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected a non-nil error when a namespace fails")
	}
	if got := atomic.LoadInt32(&ran); got != 3 {
		t.Fatalf("expected all 3 namespaces to run despite the error, got %d", got)
	}
}

func TestForEachNamespaceEmpty(t *testing.T) {
	e := &AerospikeEndpoint{}
	called := false
	err := forEachNamespace(e, 2, func(ns string) error {
		called = true
		return nil
	})
	if err != nil || called {
		t.Fatalf("expected a no-op for no namespaces, err=%v called=%v", err, called)
	}
}

func TestForEachNamespaceHonorsParallelism(t *testing.T) {
	e := &AerospikeEndpoint{Namespaces: []string{"a", "b", "c", "d"}}

	var active int32
	var maxActive int32
	err := forEachNamespace(e, 2, func(ns string) error {
		current := atomic.AddInt32(&active, 1)
		for {
			max := atomic.LoadInt32(&maxActive)
			if current <= max || atomic.CompareAndSwapInt32(&maxActive, max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := atomic.LoadInt32(&maxActive); got > 2 {
		t.Fatalf("expected at most 2 active namespaces, got %d", got)
	}
}

func TestNamespaceCheckParallelism(t *testing.T) {
	for _, tc := range []struct {
		name       string
		namespaces int
		want       int
	}{
		{name: "none", namespaces: 0, want: 1},
		{name: "one", namespaces: 1, want: 1},
		{name: "two", namespaces: 2, want: 1},
		{name: "three", namespaces: 3, want: 2},
		{name: "four", namespaces: 4, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := namespaceCheckParallelism(tc.namespaces); got != tc.want {
				t.Fatalf("expected parallelism %d, got %d", tc.want, got)
			}
		})
	}
}
