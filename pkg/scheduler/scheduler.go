package scheduler

import (
	"fmt"
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

type ProbingScheduler struct {
	logger             log.Logger
	currentTopology    topology.ClusterMap
	topologyUpdateChan chan topology.ClusterMap
	workerControlChans map[string]chan bool
	clusterChecks      []Check
	nodeChecks         []Check
}

func NewProbingScheduler(logger log.Logger, topologyUpdateChan chan topology.ClusterMap) ProbingScheduler {
	currentTopology := topology.NewClusterMap()
	workerControlChans := make(map[string]chan bool)
	clusterChecks := []Check{}
	nodeChecks := []Check{}
	return ProbingScheduler{logger, currentTopology, topologyUpdateChan, workerControlChans, clusterChecks, nodeChecks}
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
	for _, endpoint := range toStopEndpoints {
		ps.stopWorkerForEndpoint(endpoint)
	}

	any_failed_update := false
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
			err := ps.startNewWorker(endpoint, checks)
			if err != nil {
				level.Error(ps.logger).Log("msg", "Probe start failure", "err", err)
				SchedulerFailureTotal.WithLabelValues(endpoint.GetName()).Inc()
				any_failed_update = true
				continue
			}
		} else {
			level.Debug(ps.logger).Log("msg", fmt.Sprintf("Skipped probing on %s: no %s checks defined", endpoint.GetName(), endpoint_type))
		}
	}
	// Only update the topology if all probes successfully started
	if !any_failed_update {
		ps.currentTopology = newTopology
	}
}

func (ps *ProbingScheduler) stopWorkerForEndpoint(endpoint topology.ProbeableEndpoint) {
	workerChan, ok := ps.workerControlChans[endpoint.GetHash()]
	if !ok {
		level.Error(ps.logger).Log("msg", fmt.Sprintf("Couldn't stop probing on %s (%s). The probe is not registered", endpoint.GetName(), endpoint.GetHash()))
		return
	}

	level.Info(ps.logger).Log("msg", fmt.Sprintf("Stopping probing on %s", endpoint.GetName()))
	workerChan <- false // Terminate workers
	delete(ps.workerControlChans, endpoint.GetHash())
}

func (ps *ProbingScheduler) startNewWorker(endpoint topology.ProbeableEndpoint, checks []Check) error {
	// If the worker is already started, do not start it again (avoid leaking or flapping)
	_, ok := ps.workerControlChans[endpoint.GetHash()]
	if ok {
		level.Info(ps.logger).Log("msg", fmt.Sprintf("Probe already started for %s", endpoint.GetName()))
	}

	wc := make(chan bool, 1)
	w := ProberWorker{logger: log.With(ps.logger, "endpoint_name", endpoint.GetName(), "endpoint_hash", endpoint.GetHash()),
		endpoint: endpoint, checks: checks,
		controlChan: wc, refreshInterval: 30 * time.Second}

	// Checking if the probe will work properly once it is in its own goroutine. It is easier to validate the endpoint
	// now than after the probe is started. If it fails here, the topology update will fail and be scheduled at next
	// run.

	// Make sure the endpoint is connectable
	err := w.endpoint.Connect()
	if err != nil {
		w.endpoint.Close()
		return errors.Wrapf(err, "Init failure during connection to endpoint %s", w.endpoint.GetHash())
	}

	// Make sure the probe is able to prepare the endpoint
	err = w.PrepareProbing()
	if err != nil {
		w.endpoint.Close()
		return errors.Wrapf(err, "Init failure during preparation of endpoint %s", w.endpoint.GetHash())
	}

	ps.workerControlChans[endpoint.GetHash()] = wc
	go w.StartProbing()
	return nil
}

type ProberWorker struct {
	logger          log.Logger
	endpoint        topology.ProbeableEndpoint
	refreshInterval time.Duration
	checks          []Check
	controlChan     chan bool
}

// runAllPendingChecks run all the checks that should be run according to the last time they were run
// It returns the duration to wait for the soonest next check
func (pw *ProberWorker) runAllPendingChecks(lastChecks []time.Time) time.Duration {
	soonestCheck := lastChecks[0].Add(pw.checks[0].Interval)
	for i, check := range pw.checks {
		nextCheckTime := lastChecks[i].Add(check.Interval)
		if nextCheckTime.Before(time.Now()) {
			level.Debug(pw.logger).Log("msg", fmt.Sprintf("Performing check %s", check.Name))
			err := check.CheckFn(pw.endpoint)
			if err != nil {
				CheckFailureTotal.WithLabelValues(check.Name, pw.endpoint.GetName(), check.Name).Inc()
				level.Error(pw.logger).Log("msg", "Error while probing", "err", err)
			} else {
				CheckSuccessTotal.WithLabelValues(check.Name, pw.endpoint.GetName(), check.Name).Inc()
			}
			lastChecks[i] = time.Now()
		}
		if soonestCheck.After(nextCheckTime) {
			soonestCheck = nextCheckTime
		}
	}

	return time.Until(soonestCheck)
}

func (pw *ProberWorker) PrepareProbing() error {
	for _, check := range pw.checks {
		err := check.PrepareFn(pw.endpoint)
		if err != nil {
			level.Error(pw.logger).Log("msg", fmt.Sprintf("Error while preparing %s", check.Name), "err", err)
			return err
		}
	}
	return nil
}

func (pw *ProberWorker) StartProbing() {
	level.Info(pw.logger).Log("msg", "starting probing")
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
				err := check.TeardownFn(pw.endpoint)
				if err != nil {
					level.Error(pw.logger).Log("msg", "Error while tearingdown", "err", err)
					CheckFailureTotal.WithLabelValues("teardown", pw.endpoint.GetName(), check.Name).Inc()
				} else {
					CheckSuccessTotal.WithLabelValues("teardown", pw.endpoint.GetName(), check.Name).Inc()
				}
			}
			return
		case <-refreshTicker.C:
			level.Debug(pw.logger).Log("msg", "Refreshing probe endpoint")
			err := pw.endpoint.Refresh()
			if err != nil {
				level.Error(pw.logger).Log("msg", "Error while refreshing", "err", err)
				EndpointFailureTotal.WithLabelValues("refresh", pw.endpoint.GetName()).Inc()
			} else {
				EndpointSuccessTotal.WithLabelValues("refresh", pw.endpoint.GetName()).Inc()
			}
		case <-checkTicker:
			level.Debug(pw.logger).Log("msg", "Checking for work")
			nextCheckWaitTime := pw.runAllPendingChecks(lastChecks)
			checkTicker = time.After(nextCheckWaitTime)
		}
	}
}
