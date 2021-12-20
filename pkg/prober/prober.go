package prober

import (
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/criteo/blackbox-prober/pkg/topology"
)

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
		select {
		case newTopology := <-ps.topologyUpdateChan:
			level.Info(ps.logger).Log("msg", "New topology received, updating...")
			// Flush all current probes and recreate everything
			// this is very naive way of doing it that should be improved in the future
			allEndpoints := []topology.ProbeableEndpoint{}
			for _, cluster := range ps.currentTopology.GetAllClusters() {
				allEndpoints = append(allEndpoints, cluster.ClusterEndpoint)
				allEndpoints = append(allEndpoints, cluster.GetAllEndpoints()...)
			}
			for _, endpoint := range allEndpoints {
				ps.stopWorkerForEndpoint(endpoint)
			}

			for _, cluster := range newTopology.GetAllClusters() {
				if len(ps.clusterChecks) > 0 {
					ps.startNewWorker(cluster.ClusterEndpoint, ps.clusterChecks)
				}
				for _, endpoint := range cluster.GetAllEndpoints() {
					if len(ps.nodeChecks) > 0 {
						ps.startNewWorker(endpoint, ps.nodeChecks)
					}
				}
			}
			ps.currentTopology = newTopology
		}
	}
}

func (ps *ProbingScheduler) stopWorkerForEndpoint(endpoint topology.ProbeableEndpoint) {
	workerChan, ok := ps.workerControlChans[endpoint]
	if ok {
		level.Info(ps.logger).Log("msg", fmt.Sprintf("Stopping probing on %s\n", endpoint.GetName()))
		workerChan <- false // Terminate workers
		delete(ps.workerControlChans, endpoint)
	}
}

func (ps *ProbingScheduler) startNewWorker(endpoint topology.ProbeableEndpoint, checks []Check) {
	wc := make(chan bool)
	w := ProberWorker{logger: log.With(ps.logger, "component", "probe_worker", "name", endpoint.GetName()), endpoint: endpoint, checks: checks, controlChan: wc}
	ps.workerControlChans[endpoint] = wc
	go w.StartProbing()
}

type ProberWorker struct {
	logger      log.Logger
	endpoint    topology.ProbeableEndpoint
	checks      []Check
	controlChan chan bool
}

func (pw *ProberWorker) StartProbing() {
	level.Info(pw.logger).Log("msg", "starting probing")

	if len(pw.checks) < 1 {
		level.Error(pw.logger).Log("msg", "Probe not started no checks registered")
		return
	}

	lastChecks := make([]time.Time, len(pw.checks))
	shortestInterval := pw.checks[0].Interval

	err := pw.endpoint.Connect()
	if err != nil {
		level.Error(pw.logger).Log("msg", "Probe failure during connection", "err", err)
		return
	}

	for i, check := range pw.checks {
		lastChecks[i] = time.Now()
		check.PrepareFn(pw.endpoint)
		if shortestInterval > check.Interval {
			shortestInterval = check.Interval
		}
	}

	checkTicker := time.NewTicker(shortestInterval)
	refreshTicker := time.NewTicker(30 * time.Second)

	for {
		select {
		// If we receive something on the control chan we terminate
		// otherwise we continue to perform checks
		case <-pw.controlChan:
			level.Info(pw.logger).Log("msg", "Probe terminated by the scheduler")
			for _, check := range pw.checks {
				check.TeardownFn(pw.endpoint)
			}
			return
		case <-refreshTicker.C:
			level.Debug(pw.logger).Log("msg", "Probe endpoint refreshed")
			pw.endpoint.Refresh()
		case <-checkTicker.C:
			level.Debug(pw.logger).Log("msg", "Checking for work")

			for {
				check_performed := false
				for i, check := range pw.checks {
					if lastChecks[i].Add(check.Interval).Before(time.Now()) {
						level.Debug(pw.logger).Log("msg", fmt.Sprintf("Performing check %s", check.Name))
						err = check.CheckFn(pw.endpoint)
						if err != nil {
							level.Error(pw.logger).Log("msg", "Error while probing", "err", err)
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
	}
}
