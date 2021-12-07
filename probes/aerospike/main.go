package main

import (
	"time"

	"github.com/criteo/blackbox-prober/pkg/prober"
	"github.com/criteo/blackbox-prober/pkg/topology"
)

type AerospikeClusterEndpoint struct {
	name string
}

func (e AerospikeClusterEndpoint) Hash() string {
	return e.name
}

func (e AerospikeClusterEndpoint) GetName() string {
	return e.name
}

func main() {
	topo := make(chan topology.ClusterMap, 1)
	cm := topology.NewClusterMap()

	cm.Clusters["test"] = topology.Cluster{Cluster: AerospikeClusterEndpoint{name: "test"}, Nodes: make(map[string]topology.ProbeableEndpoint)}
	topo <- cm
	p := prober.NewProbingScheduler(topo)
	check := prober.Check{"noop check", prober.Noop, prober.Noop, prober.Noop, 1 * time.Second}
	p.RegisterNewClusterCheck(check)
	p.Start()
}
