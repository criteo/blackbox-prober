package main

import (
	"io/ioutil"
	"log"
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/prober"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"gopkg.in/yaml.v2"
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

type AerospikeProbeConfig struct {
	ConsulConfig discovery.ConsulConfig `yaml:"consul_sd_config,omitempty"`
	// Will include check configs
}

func main() {
	configData, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	config := AerospikeProbeConfig{}
	yaml.Unmarshal(configData, &config)

	log.Println(config)

	// DISCO stuff
	topo := make(chan topology.ClusterMap, 1)
	discoverer, err := discovery.NewConsulDiscoverer(config.ConsulConfig, topo)
	if err != nil {
		log.Fatalln("Error during init of service discovery:", err)
	}
	err = discoverer.Start()
	if err != nil {
		log.Fatalln("Error during init of service discovery:", err)
	}
	cm := topology.NewClusterMap()

	cm.Clusters["test"] = topology.Cluster{Cluster: AerospikeClusterEndpoint{name: "test"}, Nodes: make(map[string]topology.ProbeableEndpoint)}
	topo <- cm

	// Scheduler stuff
	p := prober.NewProbingScheduler(topo)
	check := prober.Check{"noop check", prober.Noop, prober.Noop, prober.Noop, 1 * time.Second}
	p.RegisterNewClusterCheck(check)
	p.Start()
}
