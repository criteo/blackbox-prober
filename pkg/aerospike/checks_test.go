package aerospike

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func authTestCluster(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

// countAuthCheckSeries counts the number of auth_check_total series currently exported for a given cluster.
func countAuthCheckSeries(t *testing.T, cluster string) int {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() {
		authCheckTotal.Collect(ch)
		close(ch)
	}()
	n := 0
	for m := range ch {
		var dm dto.Metric
		if err := m.Write(&dm); err != nil {
			continue
		}
		for _, lp := range dm.GetLabel() {
			if lp.GetName() == "cluster" && lp.GetValue() == cluster {
				n++
			}
		}
	}
	return n
}

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

func TestCleanupAuthMetrics(t *testing.T) {
	cluster := authTestCluster(t)
	// A live node (kept) and a departed node with two status series (removed).
	authCheckTotal.WithLabelValues(cluster, "10.0.0.2", "B", authStatusSuccess).Inc()
	authCheckTotal.WithLabelValues(cluster, "10.0.0.1", "A", authStatusSuccess).Inc()
	authCheckTotal.WithLabelValues(cluster, "10.0.0.1", "A", authStatusAuthFail).Inc()

	if got := countAuthCheckSeries(t, cluster); got != 3 {
		t.Fatalf("precondition: expected 3 seeded series, got %d", got)
	}

	e := &AerospikeEndpoint{ClusterConfig: &AerospikeClientConfig{clusterName: cluster}}
	e.cleanupAuthMetrics(map[authNodeKey]struct{}{
		{nodeId: "B", ip: "10.0.0.2"}: {},
	})

	if got := countAuthCheckSeries(t, cluster); got != 1 {
		t.Fatalf("expected only the live node's series to remain, got %d", got)
	}
	// The surviving series must keep its accumulated value (cleanup must not reset it).
	if got := testutil.ToFloat64(authCheckTotal.WithLabelValues(cluster, "10.0.0.2", "B", authStatusSuccess)); got != 1 {
		t.Errorf("live node series should be untouched, got value %v", got)
	}
}

func TestAuthCheck(t *testing.T) {
	cluster := authTestCluster(t)
	e := &AerospikeEndpoint{
		ClusterConfig: &AerospikeClientConfig{
			clusterName:   cluster,
			authEnabled:   true,
			genericConfig: &AerospikeEndpointConfig{},
		},
		Logger: log.NewNopLogger(),
	}

	// A stale series from a node that is no longer live; AuthCheck must clean it up.
	authCheckTotal.WithLabelValues(cluster, "10.9.9.9", "Z", authStatusSuccess).Inc()

	origTargets, origLogin := authTargets, freshLogin
	defer func() { authTargets, freshLogin = origTargets, origLogin }()

	authTargets = func(_ *AerospikeEndpoint) []authTarget {
		return []authTarget{
			{authNodeKey: authNodeKey{nodeId: "A", ip: "10.0.0.1"}, host: &as.Host{Name: "10.0.0.1"}},
			{authNodeKey: authNodeKey{nodeId: "B", ip: "10.0.0.2"}, host: &as.Host{Name: "10.0.0.2"}},
			{authNodeKey: authNodeKey{nodeId: "C", ip: "10.0.0.3"}, host: &as.Host{Name: "10.0.0.3"}},
		}
	}
	freshLogin = func(_ *AerospikeEndpoint, host *as.Host) (string, error) {
		switch host.Name {
		case "10.0.0.2":
			return authStatusAuthFail, errors.New("invalid credentials")
		case "10.0.0.3":
			return authStatusConnError, errors.New("dial timeout")
		default:
			return authStatusSuccess, nil
		}
	}

	err := AuthCheck(e)
	if err == nil {
		t.Fatal("expected AuthCheck to fail when a node rejects authentication")
	}

	if got := testutil.ToFloat64(authCheckTotal.WithLabelValues(cluster, "10.0.0.1", "A", authStatusSuccess)); got != 1 {
		t.Errorf("node A: expected 1 success, got %v", got)
	}
	if got := testutil.ToFloat64(authCheckTotal.WithLabelValues(cluster, "10.0.0.2", "B", authStatusAuthFail)); got != 1 {
		t.Errorf("node B: expected 1 auth_failure, got %v", got)
	}
	if got := testutil.ToFloat64(authCheckTotal.WithLabelValues(cluster, "10.0.0.3", "C", authStatusConnError)); got != 1 {
		t.Errorf("node C: expected 1 connection_error, got %v", got)
	}
	// Exactly the three probed nodes remain (one status series each); the stale Z is gone.
	if got := countAuthCheckSeries(t, cluster); got != 3 {
		t.Errorf("expected 3 series after cleanup of the departed node, got %d", got)
	}
}

// TestAuthCheckConnectionErrorNotFailure verifies that a pure connectivity failure is
// recorded but does not fail the check (so the scheduler signal stays auth-specific).
func TestAuthCheckConnectionErrorNotFailure(t *testing.T) {
	cluster := authTestCluster(t)
	e := &AerospikeEndpoint{
		ClusterConfig: &AerospikeClientConfig{
			clusterName:   cluster,
			authEnabled:   true,
			genericConfig: &AerospikeEndpointConfig{},
		},
		Logger: log.NewNopLogger(),
	}

	origTargets, origLogin := authTargets, freshLogin
	defer func() { authTargets, freshLogin = origTargets, origLogin }()

	authTargets = func(_ *AerospikeEndpoint) []authTarget {
		return []authTarget{{authNodeKey: authNodeKey{nodeId: "A", ip: "10.1.0.1"}, host: &as.Host{Name: "10.1.0.1"}}}
	}
	freshLogin = func(_ *AerospikeEndpoint, _ *as.Host) (string, error) {
		return authStatusConnError, errors.New("dial timeout")
	}

	if err := AuthCheck(e); err != nil {
		t.Fatalf("connection errors must not fail the auth check, got %v", err)
	}
	if got := testutil.ToFloat64(authCheckTotal.WithLabelValues(cluster, "10.1.0.1", "A", authStatusConnError)); got != 1 {
		t.Errorf("expected 1 connection_error, got %v", got)
	}
}

func TestAuthCheckDisabled(t *testing.T) {
	e := &AerospikeEndpoint{ClusterConfig: &AerospikeClientConfig{clusterName: authTestCluster(t), authEnabled: false}, Logger: log.NewNopLogger()}

	called := false
	origTargets := authTargets
	defer func() { authTargets = origTargets }()
	authTargets = func(_ *AerospikeEndpoint) []authTarget {
		called = true
		return nil
	}

	if err := AuthCheck(e); err != nil {
		t.Fatalf("expected nil error when auth is disabled, got %v", err)
	}
	if called {
		t.Fatal("expected AuthCheck to short-circuit before probing when auth is disabled")
	}
}

func TestAuthCheckParallelism(t *testing.T) {
	for _, tc := range []struct {
		name    string
		targets int
		want    int
	}{
		{name: "none", targets: 0, want: 1},
		{name: "one", targets: 1, want: 1},
		{name: "three", targets: 3, want: 3},
		{name: "many", targets: maxAuthCheckParallelism + 3, want: maxAuthCheckParallelism},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := authCheckParallelism(tc.targets); got != tc.want {
				t.Fatalf("expected parallelism %d, got %d", tc.want, got)
			}
		})
	}
}

func TestAuthCheckHonorsParallelism(t *testing.T) {
	cluster := authTestCluster(t)
	e := &AerospikeEndpoint{
		ClusterConfig: &AerospikeClientConfig{
			clusterName:   cluster,
			authEnabled:   true,
			genericConfig: &AerospikeEndpointConfig{},
		},
		Logger: log.NewNopLogger(),
	}

	origTargets, origLogin := authTargets, freshLogin
	defer func() { authTargets, freshLogin = origTargets, origLogin }()

	targets := make([]authTarget, 0, maxAuthCheckParallelism+3)
	for i := 0; i < maxAuthCheckParallelism+3; i++ {
		host := fmt.Sprintf("10.2.0.%d", i)
		targets = append(targets, authTarget{
			authNodeKey: authNodeKey{nodeId: fmt.Sprintf("N%d", i), ip: host},
			host:        &as.Host{Name: host},
		})
	}
	authTargets = func(_ *AerospikeEndpoint) []authTarget {
		return targets
	}

	var active int32
	var maxActive int32
	freshLogin = func(_ *AerospikeEndpoint, _ *as.Host) (string, error) {
		current := atomic.AddInt32(&active, 1)
		for {
			max := atomic.LoadInt32(&maxActive)
			if current <= max || atomic.CompareAndSwapInt32(&maxActive, max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return authStatusSuccess, nil
	}

	if err := AuthCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := atomic.LoadInt32(&maxActive); got > maxAuthCheckParallelism {
		t.Fatalf("expected at most %d active auth checks, got %d", maxAuthCheckParallelism, got)
	}
}
