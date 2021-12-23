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
	workerControlChans map[topology.ProbeableEndpoint]chan bool
	clusterChecks      []Check
	nodeChecks         []Check
}

func NewProbingScheduler(logger log.Logger, topologyUpdateChan chan topology.ClusterMap) ProbingScheduler {
	currentTopology := topology.NewClusterMap()
	workerControlChans := make(map[topology.ProbeableEndpoint]chan bool)
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

// Start the probing scheduler:
// - listen for topology changes
// - start and stop probes
func (ps *ProbingScheduler) Start() {
	for {
		newTopology := <-ps.topologyUpdateChan

		level.Info(ps.logger).Log("msg", "New topology received, updating...")

		toStopEndpoints, toAddEndpoints := ps.currentTopology.Diff(&newTopology)
		for _, endpoint := range toStopEndpoints {
			ps.stopWorkerForEndpoint(endpoint)
		}

		for _, endpoint := range toAddEndpoints {
			if endpoint.IsCluster() {
				if len(ps.clusterChecks) > 0 {
					err := ps.startNewWorker(endpoint, ps.clusterChecks)
					if err != nil {
						level.Error(ps.logger).Log("msg", "Probe start failure", "err", err)
						SchedulerFailureTotal.WithLabelValues(endpoint.GetName()).Inc()
						continue
					}
				} else {
					level.Debug(ps.logger).Log("msg", fmt.Sprintf("Skipped probing on %s: no cluster checks defined", endpoint.GetName()))
				}
			} else {
				if len(ps.nodeChecks) > 0 {
					err := ps.startNewWorker(endpoint, ps.nodeChecks)
					if err != nil {
						level.Error(ps.logger).Log("msg", "Probe start failure", "err", err)
						SchedulerFailureTotal.WithLabelValues(endpoint.GetName()).Inc()
						continue
					}
				} else {
					level.Debug(ps.logger).Log("msg", fmt.Sprintf("Skipped probing on %s: no node checks defined", endpoint.GetName()))
				}
			}
		}
		ps.currentTopology = newTopology
	}
}

func (ps *ProbingScheduler) stopWorkerForEndpoint(endpoint topology.ProbeableEndpoint) {
	workerChan, ok := ps.workerControlChans[endpoint]
	if ok {
		level.Info(ps.logger).Log("msg", fmt.Sprintf("Stopping probing on %s", endpoint.GetName()))
		workerChan <- false // Terminate workers
		delete(ps.workerControlChans, endpoint)
	}
}

func (ps *ProbingScheduler) startNewWorker(endpoint topology.ProbeableEndpoint, checks []Check) error {
	wc := make(chan bool)
	w := ProberWorker{logger: log.With(ps.logger, "endpoint_name", endpoint.GetName()),
		endpoint: endpoint, checks: checks,
		controlChan: wc, refreshInterval: 30 * time.Second}

	err := w.endpoint.Connect()
	if err != nil {
		return errors.Wrapf(err, "Init failure during connection to endpoint %s", w.endpoint.GetHash())
	}

	ps.workerControlChans[endpoint] = wc
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

func (pw *ProberWorker) findWork(lastChecks []time.Time) {
	for {
		check_performed := false
		for i, check := range pw.checks {
			if lastChecks[i].Add(check.Interval).Before(time.Now()) {
				level.Debug(pw.logger).Log("msg", fmt.Sprintf("Performing check %s", check.Name))
				err := check.CheckFn(pw.endpoint)
				if err != nil {
					CheckFailureTotal.WithLabelValues(check.Name, pw.endpoint.GetName(), check.Name)
					level.Error(pw.logger).Log("msg", "Error while probing", "err", err)
				} else {
					CheckSuccessTotal.WithLabelValues(check.Name, pw.endpoint.GetName(), check.Name)
				}
				lastChecks[i] = time.Now()
				check_performed = true
			}
		}
		// If we have performed a check it means we lost some time
		// We should check for available work before going to sleep again
		if !check_performed {
			break
		}
	}
}

func (pw *ProberWorker) StartProbing() {
	level.Info(pw.logger).Log("msg", "starting probing")

	if len(pw.checks) < 1 {
		level.Error(pw.logger).Log("msg", "Probe not started no checks registered")
		return
	}

	lastChecks := make([]time.Time, len(pw.checks))
	// shortestInterval will be used as ticker for all checks.
	// findWork will then check if we have a check that is ready to be processed.
	// (that we waited enough time since last check run)
	// It means that the interval between each check might not be exact.
	// This is done because in Go we cannot wait against an arbitrary number of ticker
	shortestInterval := pw.checks[0].Interval

	for i, check := range pw.checks {
		lastChecks[i] = time.Now()
		check.PrepareFn(pw.endpoint)
		if shortestInterval > check.Interval {
			shortestInterval = check.Interval
		}
	}

	checkTicker := time.NewTicker(shortestInterval)
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
					CheckFailureTotal.WithLabelValues("teardown", pw.endpoint.GetName(), check.Name)
				} else {
					CheckSuccessTotal.WithLabelValues("teardown", pw.endpoint.GetName(), check.Name)
				}
			}
			return
		case <-refreshTicker.C:
			level.Debug(pw.logger).Log("msg", "Probe endpoint refreshed")
			err := pw.endpoint.Refresh()
			if err != nil {
				level.Error(pw.logger).Log("msg", "Error while refreshing", "err", err)
				EndpointFailureTotal.WithLabelValues("refresh", pw.endpoint.GetName())
			} else {
				EndpointSuccessTotal.WithLabelValues("refresh", pw.endpoint.GetName())
			}
		case <-checkTicker.C:
			level.Debug(pw.logger).Log("msg", "Checking for work")
			pw.findWork(lastChecks)
		}
	}
}
