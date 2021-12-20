package main

import (
	"fmt"
	"os"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"

	"github.com/go-kit/log"
)

func (conf AerospikeProbeConfig) generateAerospikeEndpointFromEntry(logger log.Logger, entry discovery.ServiceEntry) (*AerospikeEndpoint, error) {
	authEnabled := conf.AerospikeEndpointConfig.AuthEnabled
	var (
		username    string
		password    string
		tlsHostname string
		ok          bool
	)
	if authEnabled {
		username, ok = os.LookupEnv(conf.AerospikeEndpointConfig.UsernameEnv)
		if !ok {
			return nil, fmt.Errorf("error: username not found in env (%s)", conf.AerospikeEndpointConfig.UsernameEnv)
		}
		password, ok = os.LookupEnv(conf.AerospikeEndpointConfig.PasswordEnv)
		if !ok {
			return nil, fmt.Errorf("error: password not found in env (%s)", conf.AerospikeEndpointConfig.PasswordEnv)
		}
	}

	tlsEnabled := utils.Contains(entry.Tags, conf.AerospikeEndpointConfig.TLSTag)
	if tlsEnabled {
		hostname, ok := entry.Meta[conf.AerospikeEndpointConfig.TLSHostnameMetaKey]
		if ok {
			tlsHostname = hostname
		}
	}

	return &AerospikeEndpoint{Name: entry.Address, Config: AerospikeClientConfig{
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
		host: as.Host{Name: entry.Address, TLSName: tlsHostname, Port: entry.Port}},
		Logger: log.With(logger, "component", "endpoint", "name", entry.Address),
	}, nil
}

func (conf AerospikeProbeConfig) generateNodeFromEntry(logger log.Logger, entry discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	return conf.generateAerospikeEndpointFromEntry(logger, entry)
}

func (conf AerospikeProbeConfig) generateClusterFromEntries(logger log.Logger, entries []discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	endpoint, err := conf.generateAerospikeEndpointFromEntry(logger, entries[0])
	if err != nil {
		return endpoint, err
	}
	endpoint.Name = entries[0].Meta[conf.DiscoveryConfig.MetaClusterKey]
	endpoint.Logger = log.With(logger, "component", "endpoint", "name", endpoint.Name)
	return endpoint, nil
}

func (conf AerospikeProbeConfig) generateTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return conf.DiscoveryConfig.GetGenericTopologyBuilder(conf.generateClusterFromEntries, conf.generateNodeFromEntry)
}
