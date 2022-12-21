package aerospike

import (
	"fmt"
	"os"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func (conf *AerospikeProbeConfig) generateNamespacedEndpointsFromEntry(logger log.Logger, entry discovery.ServiceEntry) ([]*AerospikeEndpoint, error) {
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

	namespaces := conf.getNamespacesFromEntry(entry)

	var endpoints []*AerospikeEndpoint
	for namespace := range namespaces {
		e := &AerospikeEndpoint{Name: clusterName,
			ClusterName:  clusterName,
			Namespace:    namespace,
			ClusterLevel: true,
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
		}
		endpoints = append(endpoints, e)
	}

	return endpoints, nil
}

func (conf AerospikeProbeConfig) getNamespacesFromEntry(entry discovery.ServiceEntry) map[string]struct{} {
	namespaces := make(map[string]struct{})
	nsString, ok := entry.Meta[conf.AerospikeEndpointConfig.NamespaceMetaKey]
	if ok {
		nsFromDiscovery := strings.Split(nsString, ";")
		for _, ns := range nsFromDiscovery {
			namespaces[ns] = struct{}{}
		}
	}
	return namespaces
}

func (conf AerospikeProbeConfig) NamespacedTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return func(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
		clusterMap := topology.NewClusterMap()
		clusterEntries := conf.DiscoveryConfig.GroupNodesByCluster(logger, entries)
		for _, entries := range clusterEntries {
			endpoints, err := conf.generateNamespacedEndpointsFromEntry(logger, entries[0])
			if err != nil {
				return clusterMap, err
			}

			for _, endpoint := range endpoints {
				cluster := topology.NewCluster(endpoint)
				clusterMap.AppendCluster(cluster)
			}

		}
		return clusterMap, nil
	}
}
