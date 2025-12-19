package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/criteo/blackbox-prober/pkg/aerospike"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"

	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/alecthomas/kingpin/v2"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
)

// TODO: add timeouts
func main() {
	// CLI Flags
	commonCfg := common.ProbeConfig{
		LogConfig: promlog.Config{},
	}

	aerospikeCfg := aerospike.AerospikeProbeCommandLine{}

	a := kingpin.New(filepath.Base(os.Args[0]), "Aerospike blackbox probe").UsageWriter(os.Stdout)
	common.AddFlags(a, &commonCfg)
	aerospike.AddFlags(a, &aerospikeCfg)
	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	// Init loggger
	logger := commonCfg.GetLogger()
	aslLevel, err := aerospike.GetLevel(aerospikeCfg.AerospikeLogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		a.Usage(os.Args[1:])
		os.Exit(2)
	}
	asl.Logger.SetLevel(aslLevel)

	// Parse config file
	config := aerospike.AerospikeProbeConfig{}
	err = commonCfg.ParseConfigFile(&config)
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during parsing of config file", "err", err)
		os.Exit(2)
	}

	// Metrics/pprof server
	commonCfg.StartHttpServer()

	// DISCO stuff
	topo := make(chan topology.ClusterMap, 1)
	discoverer, err := discovery.NewConsulDiscoverer(log.With(logger), config.DiscoveryConfig.ConsulConfig, topo, config.DiscoverClusters)
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during init of service discovery", "err", err)
		os.Exit(2)
	}
	go discoverer.Start()

	// Scheduler stuff
	p := scheduler.NewProbingScheduler(log.With(logger), topo)

	if config.AerospikeChecksConfigs.LatencyCheckConfig.Enable {
		p.RegisterNewClusterCheck(scheduler.Check{
			Name:       "latency_check",
			PrepareFn:  scheduler.Noop,
			CheckFn:    aerospike.LatencyCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.AerospikeChecksConfigs.LatencyCheckConfig.Interval,
		})
	}
	if config.AerospikeChecksConfigs.DurabilityCheckConfig.Enable {
		p.RegisterNewClusterCheck(scheduler.Check{
			Name:       "durability_check",
			PrepareFn:  aerospike.DurabilityPrepare,
			CheckFn:    aerospike.DurabilityCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.AerospikeChecksConfigs.DurabilityCheckConfig.Interval,
		})
	}
	if config.AerospikeChecksConfigs.AvailabilityCheckConfig.Enable {
		p.RegisterNewNodeCheck(scheduler.Check{
			Name:       "availability_check",
			PrepareFn:  scheduler.Noop,
			CheckFn:    aerospike.AvailabilityCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.AerospikeChecksConfigs.AvailabilityCheckConfig.Interval,
		})
	}
	p.Start()
}
