package aerospike

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func authTestCluster(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

// countClusterSeries counts the series currently exported by c for a given cluster.
func countClusterSeries(t *testing.T, c prometheus.Collector, cluster string) int {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
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

// countAuthCheckSeries counts the number of auth_check_total series currently exported for a given cluster.
func countAuthCheckSeries(t *testing.T, cluster string) int {
	t.Helper()
	return countClusterSeries(t, authCheckTotal, cluster)
}

func observeLatencyMetric(e *AerospikeEndpoint, operation, namespace string, target opNodeKey) {
	ObserveOpLatency(func() error { return nil }, opMetricLabels(e, operation, namespace, target))
}

// opLatencyCount returns the number of observations of an existing op_latency series. It must
// only be called for a series expected to exist: looking one up creates it.
func opLatencyCount(t *testing.T, labels []string) uint64 {
	t.Helper()
	observer, err := opLatency.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("unexpected error getting op_latency series: %v", err)
	}
	metric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatalf("expected the histogram to implement prometheus.Metric, got %T", observer)
	}
	var dm dto.Metric
	if err := metric.Write(&dm); err != nil {
		t.Fatalf("unexpected error writing op_latency series: %v", err)
	}
	return dm.GetHistogram().GetSampleCount()
}

func TestForEachNamespace(t *testing.T) {
	e := &AerospikeEndpoint{Namespaces: []string{"a", "b", "c"}}

	var mu sync.Mutex
	seen := map[string]int{}
	indexes := map[string]int{}
	err := forEachNamespace(e, 2, func(index int, ns string) error {
		mu.Lock()
		seen[ns]++
		indexes[ns] = index
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(seen) != 3 || seen["a"] != 1 || seen["b"] != 1 || seen["c"] != 1 {
		t.Fatalf("expected each namespace probed exactly once, got %v", seen)
	}
	if indexes["a"] != 0 || indexes["b"] != 1 || indexes["c"] != 2 {
		t.Errorf("unexpected namespace indexes: %v", indexes)
	}
}

// TestForEachNamespaceErrorIsolation locks in the guarantee that an error on one namespace
// does not prevent the others from being probed.
func TestForEachNamespaceErrorIsolation(t *testing.T) {
	e := &AerospikeEndpoint{Namespaces: []string{"a", "b", "c"}}

	var ran int32
	err := forEachNamespace(e, 2, func(_ int, ns string) error {
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
	err := forEachNamespace(e, 2, func(_ int, ns string) error {
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
	err := forEachNamespace(e, 2, func(_ int, ns string) error {
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
	previous := runtime.GOMAXPROCS(3)
	defer runtime.GOMAXPROCS(previous)

	for _, tc := range []struct {
		name       string
		namespaces int
		want       int
	}{
		{name: "none", namespaces: 0, want: 1},
		{name: "one", namespaces: 1, want: 1},
		{name: "two", namespaces: 2, want: 2},
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

func TestNamespaceCheckParallelismSingleCPU(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)

	if got := namespaceCheckParallelism(4); got != 1 {
		t.Fatalf("expected parallelism 1 with one GOMAXPROCS, got %d", got)
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
	observeAuthResult(e, authTarget{nodeId: "Z", ip: "10.9.9.9"}, authStatusSuccess)
	otherCluster := cluster + "-other"
	otherEndpoint := &AerospikeEndpoint{ClusterConfig: &AerospikeClientConfig{clusterName: otherCluster}}
	observeAuthResult(otherEndpoint, authTarget{nodeId: "Z", ip: "10.9.9.9"}, authStatusSuccess)

	origTargets, origLogin := authTargets, freshLogin
	defer func() { authTargets, freshLogin = origTargets, origLogin }()

	authTargets = func(_ *AerospikeEndpoint) []authTarget {
		return []authTarget{
			{nodeId: "A", ip: "10.0.0.1", host: &as.Host{Name: "10.0.0.1"}},
			{nodeId: "B", ip: "10.0.0.2", host: &as.Host{Name: "10.0.0.2"}},
			{nodeId: "C", ip: "10.0.0.3", host: &as.Host{Name: "10.0.0.3"}},
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
	if got := countAuthCheckSeries(t, otherCluster); got != 1 {
		t.Errorf("expected the other cluster's series to be kept, got %d", got)
	}
}

func TestAuthCheckSkipsCleanupWithoutTargets(t *testing.T) {
	cluster := authTestCluster(t)
	e := &AerospikeEndpoint{
		ClusterConfig: &AerospikeClientConfig{clusterName: cluster, authEnabled: true},
		Logger:        log.NewNopLogger(),
	}
	observeAuthResult(e, authTarget{nodeId: "Z", ip: "10.9.9.9"}, authStatusSuccess)

	origTargets := authTargets
	defer func() { authTargets = origTargets }()
	authTargets = func(_ *AerospikeEndpoint) []authTarget { return nil }

	if err := AuthCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := countAuthCheckSeries(t, cluster); got != 1 {
		t.Errorf("expected existing auth series to remain, got %d", got)
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
		return []authTarget{{nodeId: "A", ip: "10.1.0.1", host: &as.Host{Name: "10.1.0.1"}}}
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
		{name: "two", targets: 2, want: 2},
		{name: "three", targets: 3, want: 2},
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
			nodeId: fmt.Sprintf("N%d", i),
			ip:     host,
			host:   &as.Host{Name: host},
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

func TestLatencyCheckRemovesStaleSeries(t *testing.T) {
	cluster := authTestCluster(t)
	current := opNodeKey{endpoint: "10.0.0.1", node: "node-1.example.com", pod: "aerospike-0", nodeId: "A"}
	departed := opNodeKey{endpoint: "10.0.0.9", node: "node-9.example.com", pod: "aerospike-9", nodeId: "Z"}
	unknown := opNodeKey{endpoint: current.endpoint, node: "unknown", pod: "unknown", nodeId: current.nodeId}
	e := &AerospikeEndpoint{
		Namespaces:    []string{"ns1"},
		ClusterConfig: &AerospikeClientConfig{clusterName: cluster},
		Logger:        log.NewNopLogger(),
	}
	observeLatencyMetric(e, "put", "ns1", departed)
	observeLatencyMetric(e, "put", "ns1", unknown)

	origCheck := latencyCheckNamespace
	defer func() { latencyCheckNamespace = origCheck }()
	latencyCheckNamespace = func(e *AerospikeEndpoint, namespace string) ([]opNodeKey, error) {
		observeLatencyMetric(e, "put", namespace, current)
		return []opNodeKey{current}, nil
	}

	if err := LatencyCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := countClusterSeries(t, opLatency, cluster); got != 1 {
		t.Errorf("expected only the current op_latency series to remain, got %d", got)
	}
	if got := countClusterSeries(t, opFailuresTotal, cluster); got != 1 {
		t.Errorf("expected only the current op_latency_failures series to remain, got %d", got)
	}
	if got := opLatencyCount(t, opMetricLabels(e, "put", "ns1", current)); got != 1 {
		t.Errorf("current op_latency series should keep its observation, got %d", got)
	}
}

func TestLatencyCheckKeepsEffectiveTargetSeries(t *testing.T) {
	cluster := authTestCluster(t)
	snapshotTarget := opNodeKey{endpoint: "10.0.0.1", node: "unknown", pod: "unknown", nodeId: "A"}
	effectiveTarget := opNodeKey{endpoint: "10.0.0.1", node: "node-1.example.com", pod: "aerospike-0", nodeId: "A"}
	e := &AerospikeEndpoint{
		Namespaces:    []string{"ns1"},
		ClusterConfig: &AerospikeClientConfig{clusterName: cluster},
		Logger:        log.NewNopLogger(),
	}

	origCheck := latencyCheckNamespace
	defer func() { latencyCheckNamespace = origCheck }()
	latencyCheckNamespace = func(e *AerospikeEndpoint, namespace string) ([]opNodeKey, error) {
		observeLatencyMetric(e, "put", namespace, effectiveTarget)
		return []opNodeKey{snapshotTarget, effectiveTarget}, nil
	}

	if err := LatencyCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := countClusterSeries(t, opLatency, cluster); got != 1 {
		t.Errorf("expected the effective target's op_latency series to remain, got %d", got)
	}
	if got := countClusterSeries(t, opFailuresTotal, cluster); got != 1 {
		t.Errorf("expected the effective target's op_latency_failures series to remain, got %d", got)
	}
}

func TestLatencyCheckSkipsCleanupWithoutTargets(t *testing.T) {
	cluster := authTestCluster(t)
	stale := opNodeKey{endpoint: "10.0.0.9", node: "node-9.example.com", pod: "aerospike-9", nodeId: "Z"}
	e := &AerospikeEndpoint{
		Namespaces:    []string{"ns1"},
		ClusterConfig: &AerospikeClientConfig{clusterName: cluster},
		Logger:        log.NewNopLogger(),
	}
	observeLatencyMetric(e, "put", "ns1", stale)

	origCheck := latencyCheckNamespace
	defer func() { latencyCheckNamespace = origCheck }()
	latencyCheckNamespace = func(_ *AerospikeEndpoint, _ string) ([]opNodeKey, error) {
		return nil, nil
	}

	if err := LatencyCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := countClusterSeries(t, opLatency, cluster); got != 1 {
		t.Errorf("expected existing op_latency series to remain, got %d", got)
	}
	if got := countClusterSeries(t, opFailuresTotal, cluster); got != 1 {
		t.Errorf("expected existing op_latency_failures series to remain, got %d", got)
	}
}

func TestDurabilityCheckRemovesStaleSeries(t *testing.T) {
	cluster := authTestCluster(t)
	e := &AerospikeEndpoint{
		Name:          "cluster-a",
		Namespaces:    []string{"live"},
		ClusterConfig: &AerospikeClientConfig{clusterName: cluster},
	}
	publishDurabilityMetrics(e, "stale", 3, 2, 1)

	origCheck := durabilityCheckNamespace
	defer func() { durabilityCheckNamespace = origCheck }()
	durabilityCheckNamespace = func(e *AerospikeEndpoint, namespace string) error {
		publishDurabilityMetrics(e, namespace, 3, 2, 1)
		return nil
	}

	if err := DurabilityCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	for _, metric := range []prometheus.Collector{
		durabilityExpectedItems,
		durabilityFoundItems,
		durabilityCorruptedItems,
	} {
		if got := countClusterSeries(t, metric, cluster); got != 1 {
			t.Errorf("expected only the live namespace series to remain, got %d", got)
		}
	}
}

func TestDurabilityCheckRemovesAllSeriesWithoutNamespaces(t *testing.T) {
	cluster := authTestCluster(t)
	e := &AerospikeEndpoint{
		Name:          "cluster-a",
		ClusterConfig: &AerospikeClientConfig{clusterName: cluster},
	}
	publishDurabilityMetrics(e, "stale", 3, 2, 1)

	if err := DurabilityCheck(e); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	for _, metric := range []prometheus.Collector{
		durabilityExpectedItems,
		durabilityFoundItems,
		durabilityCorruptedItems,
	} {
		if got := countClusterSeries(t, metric, cluster); got != 0 {
			t.Errorf("expected all durability series to be removed, got %d", got)
		}
	}
}

func TestNodeInfoForFallsBackToUnknown(t *testing.T) {
	cache := common.NewNodeInfoCache()
	cache.Replace(map[string]*common.ClusterNodeInfo{
		"10.0.0.1": {NodeName: "10.0.0.1", PodName: "aerospike-0", NodeFqdn: "node-1.example.com"},
	})
	e := &AerospikeEndpoint{ClusterConfig: &AerospikeClientConfig{nodeInfoCache: cache}}

	known := nodeInfoFor(e, "10.0.0.1")
	if known.PodName != "aerospike-0" || known.NodeFqdn != "node-1.example.com" {
		t.Errorf("unexpected node info for a known address: %+v", known)
	}

	unknown := nodeInfoFor(e, "10.0.0.2")
	if unknown.NodeName != "10.0.0.2" || unknown.PodName != "unknown" || unknown.NodeFqdn != "unknown" {
		t.Errorf("unexpected node info for an unknown address: %+v", unknown)
	}
}
