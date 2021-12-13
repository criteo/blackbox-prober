package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/prober"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"gopkg.in/yaml.v2"
)

func (conf AerospikeProbeConfig) generateAerospikeEndpointFromEntry(entry discovery.ServiceEntry) (AerospikeEndpoint, error) {
	authEnabled := conf.AerospikeEndpointConfig.AuthEnabled
	var (
		username    string
		password    string
		tlsHostname string
		ok          bool
	)
	if authEnabled {
		username, ok = os.LookupEnv(conf.AerospikeEndpointConfig.UsernameEnv)
		log.Println(username)
		if !ok {
			return AerospikeEndpoint{}, fmt.Errorf("error: username not found in env (%s)", conf.AerospikeEndpointConfig.UsernameEnv)
		}
		password, ok = os.LookupEnv(conf.AerospikeEndpointConfig.PasswordEnv)
		if !ok {
			return AerospikeEndpoint{}, fmt.Errorf("error: password not found in env (%s)", conf.AerospikeEndpointConfig.PasswordEnv)
		}
	}

	tlsEnabled := utils.Contains(entry.Tags, conf.AerospikeEndpointConfig.TLSTag)
	if tlsEnabled {
		hostname, ok := entry.Meta[conf.AerospikeEndpointConfig.TLSHostnameMetaKey]
		if ok {
			tlsHostname = hostname
		}
	}

	return AerospikeEndpoint{Name: entry.Address, Config: AerospikeClientConfig{
		// auth
		authEnabled:  authEnabled,
		authExternal: conf.AerospikeEndpointConfig.AuthExternal,
		username:     username,
		password:     password,
		// tls
		tlsEnabled:    tlsEnabled,
		tlsHostname:   tlsHostname,
		tlsSkipVerify: conf.AerospikeEndpointConfig.TLSSkipVerify,
		// Contact point
		host: as.Host{Name: entry.Address, TLSName: tlsHostname, Port: entry.Port},
	}}, nil
}

func (conf AerospikeProbeConfig) generateNodeFromEntry(entry discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	return conf.generateAerospikeEndpointFromEntry(entry)
}

func (conf AerospikeProbeConfig) generateClusterFromEntries(entries []discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	return conf.generateAerospikeEndpointFromEntry(entries[0])
}

func (conf AerospikeProbeConfig) generateTopologyBuilder() func([]discovery.ServiceEntry) (topology.ClusterMap, error) {
	return conf.DiscoveryConfig.GetGenericTopologyBuilder(conf.generateClusterFromEntries, conf.generateNodeFromEntry)
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
	check := prober.Check{"noop check", prober.Noop, prober.Noop, prober.Noop, 1 * time.Minute}
	p.RegisterNewClusterCheck(check)
	p.Start()
}
