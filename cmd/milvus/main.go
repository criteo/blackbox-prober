package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/alecthomas/kingpin/v2"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/milvus"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
)

func main() {
	// CLI Flags
	commonCfg := common.ProbeConfig{
		LogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "Milvus blackbox probe").UsageWriter(os.Stdout)
	common.AddFlags(a, &commonCfg)
	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	// Init loggger
	logger := commonCfg.GetLogger()

	// Parse config file
	config := milvus.MilvusProbeConfig{}
	err = commonCfg.ParseConfigFile(&config)
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during parsing of config file", "err", err)
		os.Exit(2)
	}

	// Metrics/pprof server
	commonCfg.StartHttpServer()

	// DISCO stuff
	topo := make(chan topology.ClusterMap, 1)
	discoverer, err := discovery.NewConsulDiscoverer(log.With(logger), config.DiscoveryConfig.ConsulConfig, topo, config.NamespacedTopologyBuilder())
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during init of service discovery", "err", err)
		os.Exit(2)
	}
	go discoverer.Start()

	// Scheduler stuff
	p := scheduler.NewProbingScheduler(log.With(logger), topo)

	if config.MilvusChecksConfigs.LatencyCheckConfig.Enable {
		p.RegisterNewClusterCheck(scheduler.Check{
			Name:       "latency_check",
			PrepareFn:  milvus.LatencyPrepare,
			CheckFn:    milvus.LatencyCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.MilvusChecksConfigs.LatencyCheckConfig.Interval,
		})
	}
	if config.MilvusChecksConfigs.DurabilityCheckConfig.Enable {
		p.RegisterNewClusterCheck(scheduler.Check{
			Name:       "durability_check",
			PrepareFn:  milvus.DurabilityPrepare,
			CheckFn:    milvus.DurabilityCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.MilvusChecksConfigs.DurabilityCheckConfig.Interval,
		})
	}

	p.Start()
}
