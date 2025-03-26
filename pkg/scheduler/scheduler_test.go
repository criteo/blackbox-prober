package scheduler

import (
	"errors"
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
	controlChan := make(chan bool, 1)
	testChan := make(chan bool, 1)

	checkFn := func(p topology.ProbeableEndpoint) error {
		e := p.(*testEndpoint)
		e.CheckCallCount += 1
		if e.CheckCallCount == 100 || e.deadline.Before(time.Now()) {
			// Ask the probe to stop right away
			controlChan <- true
			testChan <- true
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
		refreshInterval: 10 * time.Millisecond,
	}
	go w.StartProbing()
	<-testChan
	if te.CheckCallCount != 100 {
		t.Errorf("Check wasn't called the correct number of time: %d", te.CheckCallCount)
	}
	if te.RefreshCallCount < 5 {
		t.Errorf("Refresh wasn't called the correct number of time: %d", te.RefreshCallCount)
	}
}

func TestWorkerStopIfNoChecks(t *testing.T) {
	controlChan := make(chan bool, 1)
	w := ProberWorker{
		logger:          log.NewNopLogger(),
		endpoint:        &topology.DummyEndpoint{},
		checks:          []Check{},
		controlChan:     controlChan,
		refreshInterval: 10 * time.Millisecond,
	}
	w.StartProbing()
}

// Checks are different between cluster endpoints and node endpoints
// Test if the proper checks are executed for each types

func TestNodeChecksCalledOnlyOnNodeEndpoints(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	nodeCheckCalled := false
	fakeNodeCheck := Check{
		Name:       "fakenodecheck",
		PrepareFn:  func(topology.ProbeableEndpoint) error { *(&nodeCheckCalled) = true; return nil },
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}

	clusterCheckCalled := false
	fakeClusterCheck := Check{
		Name:       "fakeclustercheck",
		PrepareFn:  func(topology.ProbeableEndpoint) error { *(&clusterCheckCalled) = true; return nil },
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

	if nodeCheckCalled != true && clusterCheckCalled != false {
		t.Fatalf("Checks for a node not properly called ")
	}
}

func TestClusterChecksCalledOnlyOnClusterEndpoints(t *testing.T) {
	topologyUpdateChan := make(chan topology.ClusterMap, 1)

	ps := NewProbingScheduler(log.NewNopLogger(), topologyUpdateChan)
	nodeCheckCalled := false
	fakeNodeCheck := Check{
		Name:       "fakenodecheck",
		PrepareFn:  func(topology.ProbeableEndpoint) error { *(&nodeCheckCalled) = true; return nil },
		CheckFn:    Noop,
		TeardownFn: Noop,
		Interval:   time.Hour,
	}

	clusterCheckCalled := false
	fakeClusterCheck := Check{
		Name:       "fakeclustercheck",
		PrepareFn:  func(topology.ProbeableEndpoint) error { *(&clusterCheckCalled) = true; return nil },
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

	if nodeCheckCalled != false && clusterCheckCalled != true {
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

	if fakeEndoint2.Connected != true && fakeEndoint2.Closed != true {
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
	fakeEndpoint.Cluster = true

	// First call to startNewWorker should succeed
	err, ok := ps.startNewWorker(&fakeEndpoint, ps.clusterChecks)
	if err != nil {
		t.Fatalf("First call to startNewWorker failed: %v", err)
	}

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
