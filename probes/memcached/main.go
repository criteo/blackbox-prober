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
	"github.com/criteo/blackbox-prober/pkg/memcached"
	"github.com/criteo/blackbox-prober/pkg/scheduler"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
	"gopkg.in/alecthomas/kingpin.v2"
)

// TODO: add timeouts
func main() {
	// CLI Flags
	commonCfg := common.ProbeConfig{
		LogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "Memcached blackbox probe").UsageWriter(os.Stdout)
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
	config := memcached.MemcachedProbeConfig{}
	err = commonCfg.ParseConfigFile(&config)

	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during parsing of config file", "err", err)
		os.Exit(2)
	}

	// Metrics/pprof server
	commonCfg.StartHttpServer()

	// DISCO stuff
	topo := make(chan topology.ClusterMap, 1)
	discoverer, err := discovery.NewConsulDiscoverer(log.With(logger), config.DiscoveryConfig.ConsulConfig, topo, config.GenerateTopologyBuilder())
	if err != nil {
		level.Error(logger).Log("msg", "Fatal: error during init of service discovery", "err", err)
		os.Exit(2)
	}
	go discoverer.Start()

	// Scheduler stuff
	p := scheduler.NewProbingScheduler(log.With(logger), topo)

	if config.MemcachedChecksConfigs.LatencyCheckConfig.Enable {
		p.RegisterNewNodeCheck(scheduler.Check{
			Name:       "latency_check",
			PrepareFn:  scheduler.Noop,
			CheckFn:    memcached.LatencyCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.MemcachedChecksConfigs.LatencyCheckConfig.Interval,
		})
	}

	p.Start()
}
