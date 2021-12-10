package prober

import (
	"log"
	"time"

	"github.com/criteo/blackbox-prober/pkg/topology"
)

type Check struct {
	Name       string
	PrepareFn  func(topology.ProbeableEndpoint) error
	CheckFn    func(topology.ProbeableEndpoint) error
	TeardownFn func(topology.ProbeableEndpoint) error
	Interval   time.Duration
}

// Noop do nothing. It is a noop function to use in a check when there is nothing to do
func Noop(topology.ProbeableEndpoint) error {
	return nil
}

type ProbingScheduler struct {
	currentTopology    topology.ClusterMap
	topologyUpdateChan chan topology.ClusterMap
	workerControlChans map[topology.ProbeableEndpoint]chan bool
	clusterChecks      []Check
	nodeChecks         []Check
}

func NewProbingScheduler(topologyUpdateChan chan topology.ClusterMap) ProbingScheduler {
	currentTopology := topology.NewClusterMap()
	workerControlChans := make(map[topology.ProbeableEndpoint]chan bool)
	clusterChecks := []Check{}
	nodeChecks := []Check{}
	return ProbingScheduler{currentTopology, topologyUpdateChan, workerControlChans, clusterChecks, nodeChecks}
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
			log.Printf("New topology received, updating...")
			// Flush all current probes and recreate everything
			// this is very naive way of doing it that should be improved in the future
			allEndpoints := []topology.ProbeableEndpoint{}
			for _, cluster := range ps.currentTopology.GetAllClusters() {
				allEndpoints = append(allEndpoints, cluster.Cluster)
				allEndpoints = append(allEndpoints, cluster.GetAllEndpoints()...)
			}
			for _, endpoint := range allEndpoints {
				ps.stopWorkerForEndpoint(endpoint)
			}

			for _, cluster := range newTopology.GetAllClusters() {
				ps.startNewWorker(cluster.Cluster, ps.clusterChecks)
				for _, endpoint := range cluster.GetAllEndpoints() {
					ps.startNewWorker(endpoint, ps.nodeChecks)
				}
			}
			ps.currentTopology = newTopology
		}
	}
}

func (ps *ProbingScheduler) stopWorkerForEndpoint(endpoint topology.ProbeableEndpoint) {
	workerChan, ok := ps.workerControlChans[endpoint]
	if ok {
		workerChan <- false // Terminate workers
		delete(ps.workerControlChans, endpoint)
	}
}

func (ps *ProbingScheduler) startNewWorker(endpoint topology.ProbeableEndpoint, checks []Check) {
	wc := make(chan bool)
	w := ProberWorker{endpoint: endpoint, checks: ps.clusterChecks, controlChan: wc}
	ps.workerControlChans[endpoint] = wc
	go w.StartProbing()
}

type ProberWorker struct {
	endpoint    topology.ProbeableEndpoint
	checks      []Check
	controlChan chan bool
}

func (pw *ProberWorker) StartProbing() error {
	log.Printf("Starting probing on %s\n", pw.endpoint.GetName())

	if len(pw.checks) < 1 {
		log.Printf("Probe not started for %s: no checks registered\n", pw.endpoint.GetName())
		return nil
	}

	lastChecks := make([]time.Time, len(pw.checks))
	shortestInterval := pw.checks[0].Interval

	for i, check := range pw.checks {
		lastChecks[i] = time.Now()
		check.PrepareFn(pw.endpoint)
		if shortestInterval > check.Interval {
			shortestInterval = check.Interval
		}
	}

	ticker := time.NewTicker(shortestInterval)

	for {
		select {
		// If we receive something on the control chan we terminate
		// otherwise we continue to perform checks
		case <-pw.controlChan:
			log.Printf("Terminating probing on %s\n", pw.endpoint.GetName())
			for _, check := range pw.checks {
				check.TeardownFn(pw.endpoint)
			}
			return nil
		case <-ticker.C:
			log.Printf("Checking for work on %s\n", pw.endpoint.GetName())

			for {
				check_performed := false
				for i, check := range pw.checks {
					if lastChecks[i].Add(check.Interval).Before(time.Now()) {
						log.Printf("Performing check %s on %s\n", check.Name, pw.endpoint.GetName())
						check.CheckFn(pw.endpoint)
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
