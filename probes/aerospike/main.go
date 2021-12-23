package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/promlog"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	ASSuffix = utils.MetricSuffix + "_aerospike"
)

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    ASSuffix + "_op_latency",
	Help:    "Latency for operation",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

var opCountTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: ASSuffix + "_op_total",
	Help: "Total number of operation (successful + error)",
}, []string{"operation", "endpoint", "namespace", "cluster"})

func LatencyCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}

	for namespace := range e.namespaces {
		// TODO configurable set
		key, err := as.NewKey(namespace, "monitoring", utils.RandomHex(20))
		if err != nil {
			return err
		}
		val := as.BinMap{
			"val": utils.RandomHex(1024),
		}
		policy := as.NewWritePolicy(0, 0)

		partition, err := as.PartitionForWrite(e.Client.Cluster(), &policy.BasePolicy, key)
		if err != nil {
			return err
		}
		node, err := partition.GetNodeWrite(e.Client.Cluster())
		if err != nil {
			return err
		}

		start := time.Now()
		err = e.Client.Put(policy, key, val)
		if err != nil {
			return fmt.Errorf("record put failed for: namespace=%s set=%s key=%v: %s", key.Namespace(), key.SetName(), key.Value(), err)
		}
		opLatency.WithLabelValues("put", node.GetHost().Name, namespace, e.GetName(), node.GetName()).Observe(time.Since(start).Seconds())

		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record put: namespace=%s set=%s key=%v", key.Namespace(), key.SetName(), key.Value()))
	}
	return nil
}

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
		p.RegisterNewClusterCheck(scheduler.Check{
			Name:       "latency_check",
			PrepareFn:  scheduler.Noop,
			CheckFn:    LatencyCheck,
			TeardownFn: scheduler.Noop,
			Interval:   config.AerospikeChecksConfigs.LatencyCheckConfig.Interval,
		})
	}

	p.Start()
}
