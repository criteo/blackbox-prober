package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
)

var SchedulerFailureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: utils.MetricSuffix + "_scheduler_failure",
	Help: "Total number of failures during scheduling",
}, []string{"endpoint_name"})

var EndpointFailureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: utils.MetricSuffix + "_scheduler_endpoint_failure",
	Help: "Total number of failures during scheduling for an endpoint",
}, []string{"func", "endpoint_name"})

var EndpointSuccessTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: utils.MetricSuffix + "_scheduler_endpoint_success",
	Help: "Total number of successful operations during scheduling for an endpoint",
}, []string{"func", "endpoint_name"})

var CheckSuccessTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: utils.MetricSuffix + "_scheduler_check_success",
	Help: "Total number of successful checks call during scheduling",
}, []string{"func", "endpoint_name", "check_name"})

var CheckFailureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: utils.MetricSuffix + "_scheduler_check_failure",
	Help: "Total number of check failures during scheduling",
}, []string{"func", "endpoint_name", "check_name"})

type Check struct {
	// Name of the check
	Name string
	// Prepare function called once after the endpoint has been init
	// Used to prepare the database the check (creating the monitoring keyspaces/buckets... etc)
	PrepareFn func(topology.ProbeableEndpoint) error
	// Check function called every Interval
	// Used to monitor the endpoint, this should produce metrics for SLXs
	CheckFn func(topology.ProbeableEndpoint) error
	// Teardown function called just before terminating/closing an endpoint
	// Used to clean the database if needed
	TeardownFn func(topology.ProbeableEndpoint) error
	// Interval at which the CheckFn is performed
	Interval time.Duration
}

type CheckConfig struct {
	Enable   bool          `yaml:"enable,omitempty"`
	Interval time.Duration `yaml:"interval,omitempty"`
}

// Noop do nothing. It is a noop function to use in a check when there is nothing to do
func Noop(topology.ProbeableEndpoint) error {
	return nil
}

// workerHandle is the scheduler's control over one endpoint's worker: stop is closed to ask
// the worker to terminate; done is closed by the worker once it has fully stopped (all checks
// torn down and the endpoint closed), so stopping can be synchronous.
type workerHandle struct {
	stop chan struct{}
	done chan struct{}
}

type ProbingScheduler struct {
	logger             log.Logger
	currentTopology    topology.ClusterMap
	topologyUpdateChan chan topology.ClusterMap
	workerControlChans map[string]workerHandle
	clusterChecks      []Check
	nodeChecks         []Check
	// independentChecks, when true, runs each check of an endpoint in its own goroutine (on
	// its own cadence) instead of sequentially in a single shared worker, so a slow check
	// never delays another. Opt-in via RunChecksIndependently.
	independentChecks bool
}

func NewProbingScheduler(logger log.Logger, topologyUpdateChan chan topology.ClusterMap) ProbingScheduler {
	return ProbingScheduler{
		logger:             logger,
		currentTopology:    topology.NewClusterMap(),
		topologyUpdateChan: topologyUpdateChan,
		workerControlChans: make(map[string]workerHandle),
		clusterChecks:      []Check{},
		nodeChecks:         []Check{},
	}
}

// RunChecksIndependently makes the scheduler run each registered check in its own goroutine
// per endpoint, rather than sequentially in one shared worker. A slow check (e.g. a long
// durability sweep) then never delays the others. Opt-in: schedulers that don't call this
// keep the default sequential behavior.
func (ps *ProbingScheduler) RunChecksIndependently() {
	ps.independentChecks = true
}

// RegisterNewClusterCheck add a new check at the cluster level
// It will be executed once per cluster every check interval
func (ps *ProbingScheduler) RegisterNewClusterCheck(check Check) {
	ps.clusterChecks = append(ps.clusterChecks, check)
}

// RegisterNewClusterCheck add a new check level
// It will be executed once for each nodes every check interval
func (ps *ProbingScheduler) RegisterNewNodeCheck(check Check) {
	ps.nodeChecks = append(ps.nodeChecks, check)
}

func (ps *ProbingScheduler) Start() {
	for {
		ps.ManageProbes()
	}
}

// - listen for topology changes
// - start and stop probes
func (ps *ProbingScheduler) ManageProbes() {
	newTopology := <-ps.topologyUpdateChan

	level.Info(ps.logger).Log("msg", "New topology received, updating...")

	toStopEndpoints, toAddEndpoints := ps.currentTopology.Diff(&newTopology)
	any_failed_update := false
	startedEndpoints := []topology.ProbeableEndpoint{}
	for _, endpoint := range toAddEndpoints {
		var checks []Check
		var endpoint_type string
		if endpoint.IsCluster() {
			checks = ps.clusterChecks
			endpoint_type = "cluster"
		} else {
			checks = ps.nodeChecks
			endpoint_type = "node"
		}

		if len(checks) > 0 {
			err, started := ps.startNewWorker(endpoint, checks)
			if err != nil {
				level.Error(ps.logger).Log("msg", "Probe start failure", "err", err)
				SchedulerFailureTotal.WithLabelValues(endpoint.GetName()).Inc()
				any_failed_update = true
				continue
			}
			if started {
				startedEndpoints = append(startedEndpoints, endpoint)
			}
		} else {
			level.Debug(ps.logger).Log("msg", fmt.Sprintf("Skipped probing on %s: no %s checks defined", endpoint.GetName(), endpoint_type))
		}
	}
	// Only update the topology if all probes successfully started
	if !any_failed_update {
		for _, endpoint := range toStopEndpoints {
			ps.stopWorkerForEndpoint(endpoint)
		}
		ps.currentTopology = newTopology
	} else {
		for _, endpoint := range startedEndpoints {
			ps.stopWorkerForEndpoint(endpoint)
		}
	}
}

func (ps *ProbingScheduler) stopWorkerForEndpoint(endpoint topology.ProbeableEndpoint) {
	handle, ok := ps.workerControlChans[endpoint.GetHash()]
	if !ok {
		level.Error(ps.logger).Log("msg", fmt.Sprintf("Couldn't stop probing on %s (%s). The probe is not registered", endpoint.GetName(), endpoint.GetHash()))
		return
	}

	level.Info(ps.logger).Log("msg", fmt.Sprintf("Stopping probing on %s", endpoint.GetName()))
	close(handle.stop) // Terminate worker(s): broadcasts to every goroutine waiting on it
	<-handle.done      // Wait until the worker has torn down and closed the endpoint
	delete(ps.workerControlChans, endpoint.GetHash())
}

func (ps *ProbingScheduler) startNewWorker(endpoint topology.ProbeableEndpoint, checks []Check) (error, bool) {
	// If the worker is already started, do not start it again (avoid leaking or flapping)
	_, ok := ps.workerControlChans[endpoint.GetHash()]
	if ok {
		level.Info(ps.logger).Log("msg", fmt.Sprintf("Probe already started for %s", endpoint.GetName()))
		return nil, false
	}

	handle := workerHandle{stop: make(chan struct{}), done: make(chan struct{})}
	w := ProberWorker{logger: log.With(ps.logger, "endpoint_name", endpoint.GetName(), "endpoint_hash", endpoint.GetHash()),
		endpoint: endpoint, checks: checks,
		controlChan: handle.stop, done: handle.done, refreshInterval: 30 * time.Second}

	// Checking if the probe will work properly once it is in its own goroutine. It is easier to validate the endpoint
	// now than after the probe is started. If it fails here, the topology update will fail and be scheduled at next
	// run.

	// Make sure the endpoint is connectable
	err := w.endpoint.Connect()
	if err != nil {
		w.endpoint.Close()
		return errors.Wrapf(err, "Init failure during connection to endpoint %s", w.endpoint.GetHash()), false
	}

	// Make sure the probe is able to prepare the endpoint
	err = w.prepareProbing()
	if err != nil {
		w.endpoint.Close()
		return errors.Wrapf(err, "Init failure during preparation of endpoint %s", w.endpoint.GetHash()), false
	}

	ps.workerControlChans[endpoint.GetHash()] = handle
	if ps.independentChecks {
		go w.startIndependentProbing()
	} else {
		go w.startProbing()
	}
	return nil, true
}

type ProberWorker struct {
	logger          log.Logger
	endpoint        topology.ProbeableEndpoint
	refreshInterval time.Duration
	checks          []Check
	controlChan     <-chan struct{}
	// done is closed once the worker has fully stopped (endpoint closed), letting the
	// scheduler stop synchronously. It may be nil when a ProberWorker is used directly (tests).
	done chan struct{}
}

// signalDone closes the done channel (if set) to tell the scheduler the worker has fully
// stopped. Must run after the endpoint has been closed.
func (pw *ProberWorker) signalDone() {
	if pw.done != nil {
		close(pw.done)
	}
}

// runAllPendingChecks run all the checks that should be run according to the last time they were run
// It returns the duration to wait for the soonest next check
func (pw *ProberWorker) runAllPendingChecks(lastChecks []time.Time) time.Duration {
	soonestCheck := lastChecks[0].Add(pw.checks[0].Interval)
	for i, check := range pw.checks {
		nextCheckTime := lastChecks[i].Add(check.Interval)
		if nextCheckTime.Before(time.Now()) {
			pw.runCheck(check)
			lastChecks[i] = time.Now()
		}
		if soonestCheck.After(nextCheckTime) {
			soonestCheck = nextCheckTime
		}
	}

	return time.Until(soonestCheck)
}

func (pw *ProberWorker) prepareProbing() error {
	for _, check := range pw.checks {
		err := check.PrepareFn(pw.endpoint)
		if err != nil {
			level.Error(pw.logger).Log("msg", fmt.Sprintf("Error while preparing %s", check.Name), "err", err)
			return err
		}
	}
	return nil
}

func (pw *ProberWorker) runCheck(check Check) {
	level.Debug(pw.logger).Log("msg", fmt.Sprintf("Performing check %s", check.Name))
	if err := check.CheckFn(pw.endpoint); err != nil {
		CheckFailureTotal.WithLabelValues(check.Name, pw.endpoint.GetName(), check.Name).Inc()
		level.Error(pw.logger).Log("msg", "Error while probing", "err", err)
	} else {
		CheckSuccessTotal.WithLabelValues(check.Name, pw.endpoint.GetName(), check.Name).Inc()
	}
}

func (pw *ProberWorker) teardownCheck(check Check) {
	if err := check.TeardownFn(pw.endpoint); err != nil {
		level.Error(pw.logger).Log("msg", "Error while tearingdown", "err", err)
		CheckFailureTotal.WithLabelValues("teardown", pw.endpoint.GetName(), check.Name).Inc()
	} else {
		CheckSuccessTotal.WithLabelValues("teardown", pw.endpoint.GetName(), check.Name).Inc()
	}
}

func (pw *ProberWorker) refreshEndpoint() {
	level.Debug(pw.logger).Log("msg", "Refreshing probe endpoint")
	if err := pw.endpoint.Refresh(); err != nil {
		level.Error(pw.logger).Log("msg", "Error while refreshing", "err", err)
		EndpointFailureTotal.WithLabelValues("refresh", pw.endpoint.GetName()).Inc()
	} else {
		EndpointSuccessTotal.WithLabelValues("refresh", pw.endpoint.GetName()).Inc()
	}
}

func (pw *ProberWorker) startProbing() {
	level.Info(pw.logger).Log("msg", "starting probing")
	defer pw.signalDone()     // Runs after Close() (LIFO): tells the scheduler we've fully stopped
	defer pw.endpoint.Close() // Make sure the client is closed if the probe is stopped

	if len(pw.checks) < 1 {
		level.Error(pw.logger).Log("msg", "Probe not started no checks registered")
		return
	}

	lastChecks := make([]time.Time, len(pw.checks))

	// Find when is the earliest check to execute
	nextCheckWaitTime := pw.checks[0].Interval
	for i, check := range pw.checks {
		lastChecks[i] = time.Now()
		if nextCheckWaitTime > check.Interval {
			nextCheckWaitTime = check.Interval
		}
	}

	checkTicker := time.After(nextCheckWaitTime)
	refreshTicker := time.NewTicker(pw.refreshInterval)

	for {
		select {
		// If we receive something on the control chan we terminate
		// otherwise we continue to perform checks
		case <-pw.controlChan:
			level.Info(pw.logger).Log("msg", "Probe terminated by the scheduler")
			for _, check := range pw.checks {
				pw.teardownCheck(check)
			}
			return
		case <-refreshTicker.C:
			pw.refreshEndpoint()
		case <-checkTicker:
			level.Debug(pw.logger).Log("msg", "Checking for work")
			nextCheckWaitTime := pw.runAllPendingChecks(lastChecks)
			checkTicker = time.After(nextCheckWaitTime)
		}
	}
}

// startIndependentProbing runs each check in its own goroutine on its own cadence, so a slow
// check never delays another. It mirrors StartProbing's lifecycle (a refresh loop and
// teardown-on-stop) but with no shared per-endpoint check loop. The control channel is closed
// by the scheduler to stop, which broadcasts to every loop; the client is closed only once
// all loops have exited.
func (pw *ProberWorker) startIndependentProbing() {
	level.Info(pw.logger).Log("msg", "starting independent probing")
	defer pw.signalDone()     // Runs after Close() (LIFO): tells the scheduler we've fully stopped
	defer pw.endpoint.Close() // Close the client once every loop has stopped

	if len(pw.checks) < 1 {
		level.Error(pw.logger).Log("msg", "Probe not started no checks registered")
		return
	}

	var wg sync.WaitGroup
	for _, check := range pw.checks {
		wg.Add(1)
		go func(check Check) {
			defer wg.Done()
			pw.runCheckLoop(check)
		}(check)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		pw.runRefreshLoop()
	}()

	<-pw.controlChan // block until the scheduler closes the channel to stop
	level.Info(pw.logger).Log("msg", "Probe terminated by the scheduler")
	wg.Wait() // let every loop observe the stop and run its teardown before we Close()
}

// runCheckLoop runs a single check to completion, then waits its Interval, repeatedly. Because
// it runs the check fully before waiting, a check can never overlap itself (no guard needed),
// while different checks run in parallel in their own loops.
func (pw *ProberWorker) runCheckLoop(check Check) {
	for {
		select {
		case <-pw.controlChan:
			pw.teardownCheck(check)
			return
		case <-time.After(check.Interval):
			pw.runCheck(check)
		}
	}
}

// runRefreshLoop periodically refreshes the endpoint state, mirroring StartProbing's refresh
// branch. Independent checks each have their own loop, so refresh gets its own too.
func (pw *ProberWorker) runRefreshLoop() {
	for {
		select {
		case <-pw.controlChan:
			return
		case <-time.After(pw.refreshInterval):
			pw.refreshEndpoint()
		}
	}
}
