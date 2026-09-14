package aerospike

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// nodeInfoCacheFor returns the live node info cache of a cluster, creating it on first sight.
// Endpoints keep a pointer to it, so refreshing its content also refreshes the metric labels of
// the probes already running against that cluster. Only the discovery goroutine builds
// topologies, so the map itself needs no lock; the cache content is lock-protected for the
// concurrent check readers.
func (conf *AerospikeProbeConfig) nodeInfoCacheFor(clusterName string) *common.NodeInfoCache {
	if conf.nodeInfoCaches == nil {
		conf.nodeInfoCaches = map[string]*common.NodeInfoCache{}
	}
	cache, found := conf.nodeInfoCaches[clusterName]
	if !found {
		cache = common.NewNodeInfoCache()
		conf.nodeInfoCaches[clusterName] = cache
	}
	return cache
}

// pruneNodeInfoCaches drops the caches of clusters that are no longer discovered, so a
// decommissioned cluster does not keep its node metadata alive for the lifetime of the probe.
func (conf *AerospikeProbeConfig) pruneNodeInfoCaches(discovered map[string]struct{}) {
	for clusterName := range conf.nodeInfoCaches {
		if _, found := discovered[clusterName]; !found {
			delete(conf.nodeInfoCaches, clusterName)
		}
	}
}

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

	nodeInfo := make(map[string]*common.ClusterNodeInfo, len(entries))
	hosts := make([]*as.Host, 0, len(entries))
	for _, entry := range entries {
		nodeInfo[entry.Address] = &common.ClusterNodeInfo{
			NodeName: entry.Address,
			PodName:  entry.PodName,
			NodeFqdn: entry.NodeFqdn,
		}
		hosts = append(hosts, &as.Host{Name: entry.Address, TLSName: tlsHostname, Port: entry.Port})
	}
	// Update the cluster cache in place so probes already running against this cluster pick up
	// the new addresses (pod restart, rescheduling) instead of labelling them "unknown".
	nodeInfoCache := conf.nodeInfoCacheFor(clusterName)
	nodeInfoCache.Replace(nodeInfo)

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
		// Contact points (seeds)
		hosts: hosts,
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

func (conf *AerospikeProbeConfig) shouldSkipNamespace(namespace, cluster string) bool {
	notReadyNamespaces, found := conf.DiscoveryConfig.NotReadyNamespaces[cluster]
	return found && slices.Contains(notReadyNamespaces, namespace)
}

// generateEndpointFromEntry builds the single endpoint that covers a whole cluster.
// TODO: we should use a consul dns seed
func (conf *AerospikeProbeConfig) generateEndpointFromEntry(logger log.Logger, entry discovery.ServiceEntry, clusterConfig *AerospikeClientConfig) *AerospikeEndpoint {
	namespaceSet := conf.getNamespacesFromEntry(logger, entry)
	namespaces := make([]string, 0, len(namespaceSet))
	for namespace := range namespaceSet {
		if conf.shouldSkipNamespace(namespace, clusterConfig.clusterName) {
			level.Info(logger).Log("msg", fmt.Sprintf("Skipping namespace %s on cluster %s as it is not ready for monitoring.", namespace, clusterConfig.clusterName))
		} else {
			namespaces = append(namespaces, namespace)
		}
	}
	// Keep sorted so GetHash is stable regardless of map iteration order.
	sort.Strings(namespaces)

	return &AerospikeEndpoint{
		Name:          clusterConfig.clusterName,
		Namespaces:    namespaces,
		ClusterLevel:  true,
		ClusterConfig: clusterConfig,
		Logger:        log.With(logger, "endpoint_name", entry.Address),
	}
}

func (conf *AerospikeProbeConfig) BuildTopology(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
	clusterMap := topology.NewClusterMap()

	clusterEntries := conf.DiscoveryConfig.GroupNodesByCluster(logger, entries)
	discoveredClusters := make(map[string]struct{}, len(clusterEntries))
	for _, clusterGroup := range clusterEntries {
		clusterConfig, err := conf.buildClusterClientConfig(logger, clusterGroup)
		if err != nil {
			return clusterMap, err
		}
		discoveredClusters[clusterConfig.clusterName] = struct{}{}

		endpoint := conf.generateEndpointFromEntry(logger, clusterGroup[0], clusterConfig)
		if len(endpoint.Namespaces) == 0 {
			level.Debug(logger).Log("msg", fmt.Sprintf("Skipped probing on %s: no Aerospike namespaces discovered", endpoint.GetName()))
			continue
		}
		cluster := topology.NewCluster(endpoint)
		clusterMap.AppendCluster(cluster)
	}
	conf.pruneNodeInfoCaches(discoveredClusters)
	return clusterMap, nil
}
