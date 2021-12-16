package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/prober"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"gopkg.in/yaml.v2"
)

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
		fmt.Println(node.GetHost(), node.GetName(), node.GetAliases())

		err = e.Client.Put(policy, key, val)
		if err != nil {
			return fmt.Errorf("record put failed for: namespace=%s set=%s key=%v: %s", key.Namespace(), key.SetName(), key.Value(), err)
		}
		log.Printf("record put: namespace=%s set=%s key=%v", key.Namespace(), key.SetName(), key.Value())
	}
	return nil
}

// func (e topology.ProbeableEndpoint)

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
	check := prober.Check{"Cluster check", prober.Noop, LatencyCheck, prober.Noop, 10 * time.Second}
	p.RegisterNewClusterCheck(check)
	p.Start()
}
