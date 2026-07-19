package scheduler

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/go-kit/log"
)

type testEndpoint struct {
	topology.DummyEndpoint
	deadline         time.Time
	CheckCallCount   int
	RefreshCallCount int
	// Used to track asynchronous changes
	UpdatedChan   chan bool
	FailOnConnect bool
}

func (te *testEndpoint) Refresh() error {
	te.RefreshCallCount += 1
	if te.RefreshCallCount%2 == 0 {
		return errors.New("fake err")
	}
	return nil
}

func (te *testEndpoint) Connect() error {
	if te.FailOnConnect {
		return errors.New("fake connect error")
	}
	te.Connected = true
	return nil
}

func TestWorkerWorks(t *testing.T) {
	controlChan := make(chan struct{})
	doneChan := make(chan struct{})
	testChan := make(chan bool, 1)
	var stopOnce sync.Once

	checkFn := func(p topology.ProbeableEndpoint) error {
		e := p.(*testEndpoint)
		e.CheckCallCount += 1
		if e.CheckCallCount == 100 || e.deadline.Before(time.Now()) {
			// Ask the probe to stop right away
			stopOnce.Do(func() {
				close(controlChan)
				testChan <- true
			})
		}
		if e.CheckCallCount%2 == 0 {
			return errors.New("fake err")
		}
		return nil
	}
	c := Check{
		Name:       "latency_check",
		PrepareFn:  Noop,
		CheckFn:    checkFn,
		TeardownFn: Noop,
		Interval:   1 * time.Millisecond,
	}

	te := testEndpoint{
		// Run the test for 200ms max
		deadline:         time.Now().Add(200 * time.Millisecond),
		CheckCallCount:   0,
		RefreshCallCount: 0,
	}

	w := ProberWorker{
		logger:          log.NewNopLogger(),
		endpoint:        &te,
		checks:          []Check{c},
		controlChan:     controlChan,
		done:            doneChan,
		refreshInterval: 10 * time.Millisecond,
	}
	go w.startProbing()
	<-testChan
	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Fatal("Worker did not stop after receiving control signal")
	}
	if te.CheckCallCount != 100 {
		t.Errorf("Check wasn't called the correct number of time: %d", te.CheckCallCount)
	}
	if te.RefreshCallCount < 5 {
		t.Errorf("Refresh wasn't called the correct number of time: %d", te.RefreshCallCount)
	}
}

func TestWorkerStopIfNoChecks(t *testing.T) {
	controlChan := make(chan struct{})
	w := ProberWorker{
		logger:          log.NewNopLogger(),
		endpoint:        &topology.DummyEndpoint{},
		checks:          []Check{},
		controlChan:     controlChan,
		refreshInterval: 10 * time.Millisecond,
	}
	w.startProbing()
}

// Checks are different between cluster endpoints and node endpoints
// Test if the proper checks are executed for each types

func TestNodeChecksCalledOnlyOnNodeEndpoints(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	nodeCheckCalled := false
	fakeNodeCheck := Check{
		Name: "fakenodecheck",
		PrepareFn: func(topology.ProbeableEndpoint) error {
			nodeCheckCalled = true
			return nil
		},
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}

	clusterCheckCalled := false
	fakeClusterCheck := Check{
		Name: "fakeclustercheck",
		PrepareFn: func(topology.ProbeableEndpoint) error {
			clusterCheckCalled = true
			return nil
		},
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}

	ps.RegisterNewClusterCheck(fakeClusterCheck)
	ps.RegisterNewNodeCheck(fakeNodeCheck)

	fakeEndoint := testEndpoint{}
	fakeEndoint.Name = "foo1"
	// Endpoint is node
	fakeEndoint.Cluster = false

	clusterMap := topology.NewClusterMap()
	clusterMap.AppendCluster(topology.NewCluster(&fakeEndoint))

	topologyUpdateChan <- clusterMap
	ps.ManageProbes()

	if !nodeCheckCalled || clusterCheckCalled {
		t.Fatalf("Checks for a node not properly called ")
	}
}

func TestClusterChecksCalledOnlyOnClusterEndpoints(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	nodeCheckCalled := false
	fakeNodeCheck := Check{
		Name: "fakenodecheck",
		PrepareFn: func(topology.ProbeableEndpoint) error {
			nodeCheckCalled = true
			return nil
		},
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}

	clusterCheckCalled := false
	fakeClusterCheck := Check{
		Name: "fakeclustercheck",
		PrepareFn: func(topology.ProbeableEndpoint) error {
			clusterCheckCalled = true
			return nil
		},
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}

	ps.RegisterNewClusterCheck(fakeClusterCheck)
	ps.RegisterNewNodeCheck(fakeNodeCheck)

	fakeEndoint := testEndpoint{}
	fakeEndoint.Name = "foo1"
	// Endpoint is cluster
	fakeEndoint.Cluster = true

	clusterMap := topology.NewClusterMap()
	clusterMap.AppendCluster(topology.NewCluster(&fakeEndoint))

	topologyUpdateChan <- clusterMap
	ps.ManageProbes()

	if nodeCheckCalled || !clusterCheckCalled {
		t.Fatalf("Checks for a cluster not properly called ")
	}
}

// Helper function to track the execution of teardown on an endpoint
func DummyTeardown(endpoint topology.ProbeableEndpoint) error {
	e, _ := endpoint.(*testEndpoint)
	e.UpdatedChan <- true
	return nil
}

func DummyAlwaysFail(endpoint topology.ProbeableEndpoint) error {
	return errors.New("dummy test failure")
}

func TestWorkerEndpointProperlyClosed(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)
	// Use a chan to have a synchronization point as probe worker is async
	updateChan := make(chan bool, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	fakeCheck := Check{
		Name:       "fakecheck",
		PrepareFn:  Noop,
		CheckFn:    Noop,
		TeardownFn: DummyTeardown,
		Interval:   time.Hour,
	}
	ps.RegisterNewClusterCheck(fakeCheck)
	ps.RegisterNewNodeCheck(fakeCheck)

	fakeEndoint := testEndpoint{UpdatedChan: updateChan}
	fakeEndoint.Name = "foo1"
	fakeEndoint.Cluster = true

	clusterMap := topology.NewClusterMap()
	clusterMap.AppendCluster(topology.NewCluster(&fakeEndoint))

	// Create a new probe worker
	topologyUpdateChan <- clusterMap
	ps.ManageProbes()

	// Should stop the new probe
	clusterMap = topology.NewClusterMap()
	topologyUpdateChan <- clusterMap
	ps.ManageProbes()

	// Wait for teardown to execute
	<-updateChan

	if fakeEndoint.Closed != true {
		t.Fatalf("Endpoint not closed after being removed from topology")
	}
}

func TestWorkerCloseEndpointOnStartFailure(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	fakeCheck := Check{
		Name:       "fakecheck",
		PrepareFn:  Noop,
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}
	ps.RegisterNewClusterCheck(fakeCheck)
	ps.RegisterNewNodeCheck(fakeCheck)

	fakeEndoint1 := testEndpoint{}
	fakeEndoint1.Name = "foo1"
	fakeEndoint1.Cluster = true
	fakeEndoint1.FailOnConnect = true

	clusterMap := topology.NewClusterMap()
	clusterMap.AppendCluster(topology.NewCluster(&fakeEndoint1))

	// Test failure on connect
	topologyUpdateChan <- clusterMap
	ps.ManageProbes()

	if fakeEndoint1.Closed != true {
		t.Fatalf("Endpoint not closed after being removed from topology")
	}

	// Test failure on prepare fn
	fakeEndoint2 := testEndpoint{}
	fakeEndoint2.Name = "foo2"
	fakeEndoint2.Cluster = true
	fakeEndoint2.FailOnConnect = false

	clusterMap = topology.NewClusterMap()
	clusterMap.AppendCluster(topology.NewCluster(&fakeEndoint2))
	topologyUpdateChan <- clusterMap

	fakeCheck.PrepareFn = DummyAlwaysFail
	ps.RegisterNewClusterCheck(fakeCheck)
	ps.ManageProbes()

	if !fakeEndoint2.Connected || !fakeEndoint2.Closed {
		t.Fatalf("Endpoint not closed after being removed from topology")
	}
}

func TestStartNewWorkerEarlyReturns(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	fakeCheck := Check{
		Name:       "fakecheck",
		PrepareFn:  Noop,
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}
	ps.RegisterNewClusterCheck(fakeCheck)

	// Create a test endpoint
	fakeEndpoint := testEndpoint{}
	fakeEndpoint.Name = "foo1"
	fakeEndpoint.Hash = "foo1"
	fakeEndpoint.Cluster = true

	// First call to startNewWorker should succeed
	err, ok := ps.startNewWorker(&fakeEndpoint, ps.clusterChecks)
	if err != nil {
		t.Fatalf("First call to startNewWorker failed: %v", err)
	}
	if !ok {
		t.Fatalf("First call to startNewWorker should have started a worker")
	}
	t.Cleanup(func() {
		if _, exists := ps.workerControlChans[fakeEndpoint.GetHash()]; exists {
			ps.stopWorkerForEndpoint(&fakeEndpoint)
		}
	})

	// Number of workers should be 1
	if len(ps.workerControlChans) != 1 {
		t.Fatalf("Expected 1 worker, got %d", len(ps.workerControlChans))
	}

	// Second call to startNewWorker with the same endpoint should early return
	err, ok = ps.startNewWorker(&fakeEndpoint, ps.clusterChecks)
	if err != nil {
		t.Fatalf("Second call to startNewWorker failed: %v", err)
	}
	if ok {
		t.Fatalf("Second call to startNewWorker should have early returned but didn't")
	}

	// Number of workers should still be 1 and with the same channel
	if len(ps.workerControlChans) != 1 {
		t.Fatalf("Expected 1 worker after second call, got %d", len(ps.workerControlChans))
	}
}

type atomicTestEndpoint struct {
	topology.DummyEndpoint
	refreshCount atomic.Int32
	closeCount   atomic.Int32
}

func (te *atomicTestEndpoint) Refresh() error {
	count := te.refreshCount.Add(1)
	if count%2 == 0 {
		return errors.New("fake refresh error")
	}
	return nil
}

func (te *atomicTestEndpoint) Close() error {
	te.closeCount.Add(1)
	return te.DummyEndpoint.Close()
}

func TestRunChecksIndependently(t *testing.T) {
	ps := NewProbingScheduler(log.NewNopLogger(), make(chan topology.ClusterMap, 1))

	ps.RunChecksIndependently()

	if !ps.independentChecks {
		t.Fatal("RunChecksIndependently did not enable independent checks")
	}
}

func TestIndependentProbingRunsChecksConcurrentlyAndStopsCleanly(t *testing.T) {
	controlChan := make(chan struct{})
	doneChan := make(chan struct{})
	slowStarted := make(chan struct{}, 1)
	fastRan := make(chan struct{}, 1)
	var teardownCount atomic.Int32

	signal := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	slowCheck := Check{
		Name:      "slow_check",
		PrepareFn: Noop,
		CheckFn: func(topology.ProbeableEndpoint) error {
			signal(slowStarted)
			time.Sleep(80 * time.Millisecond)
			return nil
		},
		TeardownFn: func(topology.ProbeableEndpoint) error {
			teardownCount.Add(1)
			return nil
		},
		Interval: time.Millisecond,
	}
	fastCheck := Check{
		Name:      "fast_check",
		PrepareFn: Noop,
		CheckFn: func(topology.ProbeableEndpoint) error {
			signal(fastRan)
			return nil
		},
		TeardownFn: func(topology.ProbeableEndpoint) error {
			teardownCount.Add(1)
			return nil
		},
		Interval: 2 * time.Millisecond,
	}
	endpoint := atomicTestEndpoint{}
	endpoint.Name = "independent"
	endpoint.Hash = "independent"
	endpoint.Cluster = true

	w := ProberWorker{
		logger:          log.NewNopLogger(),
		endpoint:        &endpoint,
		checks:          []Check{slowCheck, fastCheck},
		controlChan:     controlChan,
		done:            doneChan,
		refreshInterval: 2 * time.Millisecond,
	}

	go w.startIndependentProbing()

	select {
	case <-slowStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Slow check did not start")
	}
	select {
	case <-fastRan:
	case <-time.After(40 * time.Millisecond):
		t.Fatal("Fast check did not run while slow check was still executing")
	}
	for start := time.Now(); endpoint.refreshCount.Load() < 2; {
		if time.Since(start) > 100*time.Millisecond {
			t.Fatalf("Refresh was not called enough times: %d", endpoint.refreshCount.Load())
		}
		time.Sleep(time.Millisecond)
	}

	close(controlChan)

	select {
	case <-doneChan:
	case <-time.After(time.Second):
		t.Fatal("Independent worker did not stop")
	}
	if teardownCount.Load() != 2 {
		t.Fatalf("Expected teardown to run for both checks, got %d", teardownCount.Load())
	}
	if endpoint.closeCount.Load() != 1 || !endpoint.Closed {
		t.Fatalf("Endpoint was not closed exactly once, close_count=%d closed=%v", endpoint.closeCount.Load(), endpoint.Closed)
	}
}

func TestManageProbesKeepsCurrentWorkersAndRollsBackNewWorkersOnUpdateFailure(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)
	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	fakeCheck := Check{
		Name:       "fakecheck",
		PrepareFn:  Noop,
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}
	ps.RegisterNewClusterCheck(fakeCheck)
	ps.RegisterNewNodeCheck(fakeCheck)

	oldEndpoint := testEndpoint{}
	oldEndpoint.Name = "old"
	oldEndpoint.Hash = "old"
	oldEndpoint.Cluster = true
	oldMap := topology.NewClusterMap()
	oldMap.AppendCluster(topology.NewCluster(&oldEndpoint))

	topologyUpdateChan <- oldMap
	ps.ManageProbes()
	t.Cleanup(func() {
		if _, exists := ps.workerControlChans[oldEndpoint.GetHash()]; exists {
			ps.stopWorkerForEndpoint(&oldEndpoint)
		}
	})

	newClusterEndpoint := testEndpoint{}
	newClusterEndpoint.Name = "new-cluster"
	newClusterEndpoint.Hash = "new-cluster"
	newClusterEndpoint.Cluster = true
	failingNodeEndpoint := testEndpoint{}
	failingNodeEndpoint.Name = "failing-node"
	failingNodeEndpoint.Hash = "failing-node"
	failingNodeEndpoint.FailOnConnect = true

	newCluster := topology.NewCluster(&newClusterEndpoint)
	newCluster.AddEndpoint(&failingNodeEndpoint)
	newMap := topology.NewClusterMap()
	newMap.AppendCluster(newCluster)

	topologyUpdateChan <- newMap
	ps.ManageProbes()

	if oldEndpoint.Closed {
		t.Fatal("Existing worker was stopped even though topology update failed")
	}
	if !newClusterEndpoint.Closed {
		t.Fatal("New worker started during failed topology update was not rolled back")
	}
	if !failingNodeEndpoint.Closed {
		t.Fatal("Endpoint was not closed after start failure")
	}
	if _, exists := ps.currentTopology.Clusters[oldEndpoint.GetHash()]; !exists {
		t.Fatal("Current topology changed after failed update")
	}
	if _, exists := ps.workerControlChans[oldEndpoint.GetHash()]; !exists {
		t.Fatal("Existing worker was removed after failed update")
	}
	if _, exists := ps.workerControlChans[newClusterEndpoint.GetHash()]; exists {
		t.Fatal("Rolled back worker is still registered")
	}
}
