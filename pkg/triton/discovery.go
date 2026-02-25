package triton

import (
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/go-kit/log"
)

// TopologyBuilder returns a function that builds a ClusterMap from Consul service entries.
// Services are grouped by service name, with each service becoming a "cluster".
// The first endpoint in each cluster serves as the ClusterEndpoint (required by the framework),
// but since we only register node checks, the scheduler will skip cluster-level probing.
func (conf *TritonProbeConfig) TopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return func(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
		clusterMap := topology.NewClusterMap()

		// Group entries by service name (each service = one cluster)
		clusterEntries := make(map[string][]discovery.ServiceEntry)
		for _, entry := range entries {
			clusterName := entry.Service
			if _, ok := clusterEntries[clusterName]; !ok {
				clusterEntries[clusterName] = []discovery.ServiceEntry{}
			}
			clusterEntries[clusterName] = append(clusterEntries[clusterName], entry)
		}

		// Build topology for each cluster
		for clusterName, entries := range clusterEntries {
			// Create endpoints for all nodes
			var endpoints []*TritonEndpoint
			for _, entry := range entries {
				endpoint := NewTritonEndpoint(logger, clusterName, entry, &conf.TritonEndpointConfig)
				endpoints = append(endpoints, endpoint)
			}

			// First endpoint serves as ClusterEndpoint (framework requirement)
			// All endpoints are added as NodeEndpoints
			cluster := topology.NewCluster(endpoints[0])
			for _, endpoint := range endpoints {
				cluster.AddEndpoint(endpoint)
			}
			clusterMap.AppendCluster(cluster)
		}

		return clusterMap, nil
	}
}
