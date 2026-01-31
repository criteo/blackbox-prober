package aerospike

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func (conf *AerospikeProbeConfig) buildClusterClientConfig(logger log.Logger, entries []discovery.ServiceEntry) (*AerospikeClientConfig, error) {
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

	tlsEnabled := utils.Contains(entries[0].Tags, conf.AerospikeEndpointConfig.TLSTag)
	if tlsEnabled {
		hostname, ok := entries[0].Meta[conf.AerospikeEndpointConfig.TLSHostnameMetaKey]
		if ok {
			tlsHostname = hostname
		}
	}

	clusterName, ok := entries[0].Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		level.Warn(logger).Log("msg", "Cluster name not found, replacing it with hostname")
		clusterName = entries[0].Address
	}

	nodeInfoCache := map[string]*AerospikeNodeInfo{}
	for _, entry := range entries {
		nodeInfoCache[entry.Address] = &AerospikeNodeInfo{
			NodeName: entry.Address,
			PodName:  entry.PodName,
			NodeFqdn: entry.NodeFqdn,
		}
	}

	clusterConfig := AerospikeClientConfig{
		clusterName: clusterName,
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
		host: as.Host{Name: entries[0].Address, TLSName: tlsHostname, Port: entries[0].Port},
		// node info cache
		nodeInfoCache: nodeInfoCache,
	}

	return &clusterConfig, nil
}

func (conf AerospikeProbeConfig) getNamespacesFromEntry(logger log.Logger, entry discovery.ServiceEntry) map[string]struct{} {
	namespaces := make(map[string]struct{})

	for metaKey, metaValue := range entry.Meta {
		if !strings.HasPrefix(metaKey, conf.AerospikeEndpointConfig.NamespaceMetaKeyPrefix) {
			continue
		}
		ready, err := strconv.ParseBool(metaValue)
		if err != nil {
			level.Error(logger).Log("msg", fmt.Sprintf("Fail to parse boolean value from MetaData %s. Fallbacking to deprecated method.", metaKey), "err", err)
			continue
		}
		if !ready {
			continue
		}
		// MetaKey is like : "aerospike-monitoring-foo"
		ns := metaKey[len(conf.AerospikeEndpointConfig.NamespaceMetaKeyPrefix):]
		if len(ns) > 0 {
			namespaces[ns] = struct{}{}
		}
	}

	return namespaces
}

func (conf *AerospikeProbeConfig) generateNamespacedEndpointsFromEntry(logger log.Logger, entry discovery.ServiceEntry, clusterConfig *AerospikeClientConfig) []*AerospikeEndpoint {
	namespaces := conf.getNamespacesFromEntry(logger, entry)

	var endpoints []*AerospikeEndpoint
	for namespace := range namespaces {
		e := &AerospikeEndpoint{Name: clusterConfig.clusterName,
			Namespace:     namespace,
			ClusterLevel:  true,
			ClusterConfig: clusterConfig,
			Logger:        log.With(logger, "endpoint_name", entry.Address),
		}
		endpoints = append(endpoints, e)
	}

	return endpoints
}

func (conf AerospikeProbeConfig) NamespacedTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return func(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
		clusterMap := topology.NewClusterMap()

		clusterEntries := conf.DiscoveryConfig.GroupNodesByCluster(logger, entries)
		for _, entries := range clusterEntries {
			clusterConfig, err := conf.buildClusterClientConfig(logger, entries)
			if err != nil {
				return clusterMap, err
			}

			endpoints := conf.generateNamespacedEndpointsFromEntry(logger, entries[0], clusterConfig)
			for _, endpoint := range endpoints {
				cluster := topology.NewCluster(endpoint)
				clusterMap.AppendCluster(cluster)
			}

		}
		return clusterMap, nil
	}
}
