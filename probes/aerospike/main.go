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

type AerospikeEndpoint struct {
	name string
}

func (e AerospikeEndpoint) Hash() string {
	return e.name
}

func (e AerospikeEndpoint) GetName() string {
	return e.name
}

type AerospikeProbeConfig struct {
	// Generic consul configurations
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Client configuration
	// AerospikeClientConfig AerospikeClientConfig `yaml:"client_config,omitempty"`
	// Will include check configs
}

func generateNodeFromEntry(entry discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	// consul.ServiceEntry
	return AerospikeEndpoint{name: entry.Address}, nil
}

func generateClusterFromEntries(entry []discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	// consul.ServiceEntry
	return AerospikeEndpoint{name: entry[0].Address}, nil
}

func (conf AerospikeProbeConfig) generateTopologyBuilder() func([]discovery.ServiceEntry) topology.ClusterMap {
	return conf.DiscoveryConfig.GetGenericTopologyBuilder(generateClusterFromEntries, generateNodeFromEntry)
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
	discoverer, err := discovery.NewConsulDiscoverer(config.DiscoveryConfig.ConsulConfig, topo, config.generateTopologyBuilder())
	if err != nil {
		log.Fatalln("Error during init of service discovery:", err)
	}
	err = discoverer.Start()
	if err != nil {
		log.Fatalln("Error during init of service discovery:", err)
	}

	// Scheduler stuff
	p := prober.NewProbingScheduler(topo)
	check := prober.Check{"noop check", prober.Noop, prober.Noop, prober.Noop, 1 * time.Second}
	p.RegisterNewClusterCheck(check)
	p.Start()
}
