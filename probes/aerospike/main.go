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
	check := prober.Check{"noop check", prober.Noop, prober.Noop, prober.Noop, 1 * time.Minute}
	p.RegisterNewClusterCheck(check)
	p.Start()
}
