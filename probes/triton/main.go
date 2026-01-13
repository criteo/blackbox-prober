package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/alecthomas/kingpin/v2"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/triton"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
)

func main() {
	// CLI Flags
	commonCfg := common.ProbeConfig{
		LogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "Triton Inference Server blackbox probe").UsageWriter(os.Stdout)
	common.AddFlags(a, &commonCfg)
	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	// Init logger
	logger := commonCfg.GetLogger()

	// Parse config file
	config := triton.TritonProbeConfig{}
	err = commonCfg.ParseConfigFile(&config)
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during parsing of config file", "err", err)
		os.Exit(2)
	}

	// Metrics/pprof server
	commonCfg.StartHttpServer()

	// Discovery: Consul service discovery
	topo := make(chan topology.ClusterMap, 1)
	discoverer, err := discovery.NewConsulDiscoverer(log.With(logger), config.DiscoveryConfig.ConsulConfig, topo, config.TopologyBuilder())
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during init of service discovery", "err", err)
		os.Exit(2)
	}
	go discoverer.Start()

	// Scheduler: register checks and start probing
	p := scheduler.NewProbingScheduler(log.With(logger), topo)

	//Register latency check as a NODE check (runs on each Triton server instance)
	if config.TritonChecksConfigs.LatencyCheckConfig.Enable {
		p.RegisterNewNodeCheck(scheduler.Check{
			Name:       "latency_check",
			PrepareFn:  scheduler.Noop,
			CheckFn:    triton.LatencyCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.TritonChecksConfigs.LatencyCheckConfig.Interval,
		})
	}

	p.Start()
}
