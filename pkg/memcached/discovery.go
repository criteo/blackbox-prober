package memcached

import (
	"fmt"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type MemcachedProbeConfig struct {
	// Generic consul configurations
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Generic probe configurations
	MemcachedEndpointConfig MemcachedEndpointConfig `yaml:"client_config,omitempty"`
	// Check configurations
	MemcachedChecksConfigs MemcachedChecksConfigs `yaml:"checks_configs,omitempty"`
}

type MemcachedChecksConfigs struct {
	LatencyCheckConfig scheduler.CheckConfig `yaml:"latency_check,omitempty"`
}

func (conf MemcachedProbeConfig) generateNodeFromEntry(logger log.Logger, entry discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	return conf.generateMemcachedEndpointFromEntry(logger, entry)
}

func (conf MemcachedProbeConfig) generateClusterFromEntries(logger log.Logger, entries []discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	// there are no cluster level check now
	clusterName, ok := entries[0].Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		level.Warn(logger).Log("msg", "Cluster name not found, replacing it with hostname")
		clusterName = entries[0].Address
	}
	return topology.DummyEndpoint{Name: clusterName, Hash: clusterName, Cluster: true}, nil
}

func (conf MemcachedProbeConfig) GenerateTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return conf.DiscoveryConfig.GetGenericTopologyBuilder(conf.generateClusterFromEntries, conf.generateNodeFromEntry)
}

func (conf *MemcachedProbeConfig) generateMemcachedEndpointFromEntry(logger log.Logger, entry discovery.ServiceEntry) (*MemcachedEndpoint, error) {
	name := fmt.Sprintf("%s:%d", entry.Address, entry.Port)

	clusterName, ok := entry.Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		level.Warn(logger).Log("msg", "Cluster name not found, replacing it with hostname")
		clusterName = name
	}

	return &MemcachedEndpoint{
		Name:        name,
		ClusterName: clusterName,
		Config:      *conf,
		Servers:     []string{name},
		Logger:      log.With(logger, "endpoint_name", name),
	}, nil
}
