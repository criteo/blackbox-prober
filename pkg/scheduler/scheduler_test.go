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
}

func (te *testEndpoint) Refresh() error {
	te.RefreshCallCount += 1
	if te.RefreshCallCount%2 == 0 {
		return errors.New("fake err")
	}
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

func TestWorkeStopIfNoChecks(t *testing.T) {
	controlChan := make(chan bool, 1)
	w := ProberWorker{
		logger:          log.NewNopLogger(),
		endpoint:        topology.DummyEndpoint{},
		checks:          []Check{},
		controlChan:     controlChan,
		refreshInterval: 10 * time.Millisecond,
	}
	w.StartProbing()
}
