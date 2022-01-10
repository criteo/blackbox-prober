package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	ASSuffix = utils.MetricSuffix + "_aerospike"
)

// TODO: add timeouts
func main() {
	// CLI Flags
	cfg := common.ProbeConfig{
		LogConfig: promlog.Config{},
	}
	a := kingpin.New(filepath.Base(os.Args[0]), "Aerospike blackbox probe").UsageWriter(os.Stdout)
	common.AddFlags(a, &cfg)
	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	// Init loggger
	logger := cfg.GetLogger()

	// Parse config file
	config := AerospikeProbeConfig{}
	err = cfg.ParseConfigFile(&config)
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during parsing of config file", "err", err)
		os.Exit(2)
	}

	// Metrics/pprof server
	cfg.StartHttpServer()

	// DISCO stuff
	topo := make(chan topology.ClusterMap, 1)
	discoverer, err := discovery.NewConsulDiscoverer(log.With(logger), config.DiscoveryConfig.ConsulConfig, topo, config.generateTopologyBuilder())
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during init of service discovery", "err", err)
		os.Exit(2)
	}
	go discoverer.Start()

	// Scheduler stuff
	p := scheduler.NewProbingScheduler(log.With(logger), topo)

	if config.AerospikeChecksConfigs.LatencyCheckConfig.Enable {
		p.RegisterNewNodeCheck(scheduler.Check{
			Name:       "latency_check",
			PrepareFn:  scheduler.Noop,
			CheckFn:    LatencyCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.AerospikeChecksConfigs.LatencyCheckConfig.Interval,
		})
	}
	if config.AerospikeChecksConfigs.DurabilityCheckConfig.Enable {
		p.RegisterNewClusterCheck(scheduler.Check{
			Name:       "durability_check",
			PrepareFn:  DurabilityPrepare,
			CheckFn:    DurabilityCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.AerospikeChecksConfigs.DurabilityCheckConfig.Interval,
		})
	}

	p.Start()
}
