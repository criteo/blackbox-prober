package discovery

import (
	"log"

	"github.com/criteo/blackbox-prober/pkg/topology"
)

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
	ClusterFn func([]ServiceEntry) (topology.ProbeableEndpoint, error),
	NodeFn func(ServiceEntry) (topology.ProbeableEndpoint, error)) func([]ServiceEntry) topology.ClusterMap {

	return func(entries []ServiceEntry) topology.ClusterMap {
		clusterMap := topology.NewClusterMap()
		clusterEntries := conf.GroupNodesByCluster(entries)
		for clusterName, entries := range clusterEntries {
			clusterEndpoint, _ := ClusterFn(entries)
			cluster := topology.NewCluster(clusterEndpoint)
			for _, entry := range entries {
				nodeEndpoint, _ := NodeFn(entry)
				cluster.AddEndpoint(nodeEndpoint)
			}
			clusterMap.Clusters[clusterName] = cluster
		}
		return clusterMap
	}
}

func (conf GenericDiscoveryConfig) GroupNodesByCluster(entries []ServiceEntry) map[string][]ServiceEntry {
	clusterEntries := make(map[string][]ServiceEntry)

	for _, entry := range entries {
		clusterName, ok := entry.Meta[conf.MetaClusterKey]
		if !ok {
			log.Printf("Skipped %s (in %s), missing cluster key: %s", entry.Address, entry.Service, conf.MetaClusterKey)
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
