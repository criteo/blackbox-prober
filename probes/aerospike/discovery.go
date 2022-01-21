package main

import (
	"fmt"
	"os"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func (conf *AerospikeProbeConfig) generateAerospikeEndpointFromEntry(logger log.Logger, entry discovery.ServiceEntry) (*AerospikeEndpoint, error) {
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

	clusterName, ok := entry.Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		level.Warn(logger).Log("msg", "Cluster name not found, replacing it with hostname")
		clusterName = entry.Address
	}

	namespaces := make(map[string]struct{})
	autoDiscoverNamespaces := true

	if conf.AerospikeEndpointConfig.NamespaceMetaKey != "" {
		nsString, ok := entry.Meta[conf.AerospikeEndpointConfig.NamespaceMetaKey]
		if ok {
			nsFromDiscovery := strings.Split(nsString, ";")
			for _, ns := range nsFromDiscovery {
				namespaces[ns] = struct{}{}
			}
			autoDiscoverNamespaces = false
		}
	}

	return &AerospikeEndpoint{Name: entry.Address,
		ClusterName:            clusterName,
		namespaces:             namespaces,
		AutoDiscoverNamespaces: autoDiscoverNamespaces,
		Config: AerospikeClientConfig{
			// auth
			authEnabled: authEnabled,
			username:    username,
			password:    password,
			// tls
			tlsEnabled:  tlsEnabled,
			tlsHostname: tlsHostname,
			// conf
			genericConfig: &conf.AerospikeEndpointConfig,
			// Contact point
			host: as.Host{Name: entry.Address, TLSName: tlsHostname, Port: entry.Port}},
		Logger: log.With(logger, "endpoint_name", entry.Address),
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
	endpoint.ClusterName = entries[0].Meta[conf.DiscoveryConfig.MetaClusterKey]
	endpoint.Logger = log.With(logger, "endpoint_name", endpoint.Name)
	endpoint.clusterLevel = true
	return endpoint, nil
}

func (conf AerospikeProbeConfig) generateTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return conf.DiscoveryConfig.GetGenericTopologyBuilder(conf.generateClusterFromEntries, conf.generateNodeFromEntry)
}
