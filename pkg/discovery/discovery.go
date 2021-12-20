package discovery

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
)

var DiscoveryFailureTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: utils.MetricSuffix + "_discovery_failure",
	Help: "Total number of failures during discovery",
})

type ServiceEntry struct {
	Service string
	Tags    []string
	Meta    map[string]string
	Port    int
	Address string
}

// Contains the keys/tags to use during the topology generation
type GenericDiscoveryConfig struct {
	// Key for the cluster name
	MetaClusterKey string `yaml:"meta_cluster_key,omitempty"`
	// Specific configuration consul
	ConsulConfig ConsulConfig `yaml:"consul_sd_config,omitempty"`
}

var (
	defaultDiscoveryConfig = GenericDiscoveryConfig{
		MetaClusterKey: "CLUSTER",
	}
)

func (conf GenericDiscoveryConfig) GetGenericTopologyBuilder(
	ClusterFn func(log.Logger, []ServiceEntry) (topology.ProbeableEndpoint, error),
	NodeFn func(log.Logger, ServiceEntry) (topology.ProbeableEndpoint, error)) func(log.Logger, []ServiceEntry) (topology.ClusterMap, error) {

	return func(logger log.Logger, entries []ServiceEntry) (topology.ClusterMap, error) {
		clusterMap := topology.NewClusterMap()
		clusterEntries := conf.GroupNodesByCluster(logger, entries)
		for clusterName, entries := range clusterEntries {
			clusterEndpoint, err := ClusterFn(logger, entries)
			if err != nil {
				return clusterMap, err
			}
			cluster := topology.NewCluster(clusterEndpoint)
			for _, entry := range entries {
				nodeEndpoint, err := NodeFn(logger, entry)
				if err != nil {
					return clusterMap, err
				}
				cluster.AddEndpoint(nodeEndpoint)
			}
			clusterMap.Clusters[clusterName] = cluster
		}
		return clusterMap, nil
	}
}

func (conf GenericDiscoveryConfig) GroupNodesByCluster(logger log.Logger, entries []ServiceEntry) map[string][]ServiceEntry {
	clusterEntries := make(map[string][]ServiceEntry)

	for _, entry := range entries {
		clusterName, ok := entry.Meta[conf.MetaClusterKey]
		if !ok {
			level.Warn(logger).Log("msg", fmt.Sprintf("Skipped %s (in %s), missing cluster key: %s", entry.Address, entry.Service, conf.MetaClusterKey))
			continue
		}

		if _, ok = clusterEntries[clusterName]; !ok {
			clusterEntries[clusterName] = []ServiceEntry{}
		}
		clusterEntries[clusterName] = append(clusterEntries[clusterName], entry)
	}
	return clusterEntries
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *GenericDiscoveryConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultDiscoveryConfig
	type plain GenericDiscoveryConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	return nil
}
