package aerospike

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func (conf *AerospikeProbeConfig) addCluster(clusterConfig *AerospikeClusterConfig, clusterServices []discovery.ServiceEntry, clusterMap topology.ClusterMap, logger log.Logger) error {
	// generate one endpoint per cluster per namespace for latency & durability checks (cluster checks)
	for _, namespace := range clusterConfig.namespaces {
		cluster := topology.NewCluster(&AerospikeEndpoint{
			Name:          fmt.Sprintf("%s/%s", clusterConfig.clusterName, namespace),
			Namespace:     namespace,
			Seed:          as.Host{Name: clusterServices[0].Address, Port: clusterServices[0].Port, TLSName: clusterConfig.tlsHostname},
			ClusterLevel:  true,
			ClusterConfig: clusterConfig,
			Logger:        log.With(logger, "endpoint_name", clusterServices[0].Address),
		})

		clusterMap.AppendCluster(cluster)
	}

	// generate one endpoint per cluster per node for availability check (node checks)
	for _, clusterService := range clusterServices {
		cluster := topology.NewCluster(&AerospikeEndpoint{
			Name:          fmt.Sprintf("%s/%s", clusterConfig.clusterName, clusterService.Address),
			Namespace:     "",
			Seed:          as.Host{Name: clusterService.Address, Port: clusterService.Port, TLSName: clusterConfig.tlsHostname},
			ClusterLevel:  false, // ClusterLevel flag is used
			ClusterConfig: clusterConfig,
			Logger:        log.With(logger, "endpoint_name", clusterService.Address),
		})

		clusterMap.AppendCluster(cluster)
	}

	return nil
}

func (conf *AerospikeProbeConfig) getNamespacesFromEntry(logger log.Logger, clusterService *discovery.ServiceEntry) map[string]struct{} {
	namespaces := make(map[string]struct{})

	for metaKey, metaValue := range clusterService.Meta {
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

func (conf *AerospikeProbeConfig) buildClusterConfig(clusterName string, clusterService *discovery.ServiceEntry, logger log.Logger) (*AerospikeClusterConfig, error) {
	authEnabled := conf.AerospikeEndpointConfig.AuthEnabled
	var username, password string
	var ok bool
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

	tlsEnabled := utils.Contains(clusterService.Tags, conf.AerospikeEndpointConfig.TLSTag)
	var tlsHostname string
	if tlsEnabled {
		tlsHostname, ok = clusterService.Meta[conf.AerospikeEndpointConfig.TLSHostnameMetaKey]
		if !ok {
			return nil, fmt.Errorf("unable to determine tls hostname from consul service meta %s", conf.AerospikeEndpointConfig.TLSHostnameMetaKey)
		}
	}

	namespaces := []string{}
	for ns := range conf.getNamespacesFromEntry(logger, clusterService) {
		namespaces = append(namespaces, ns)
	}

	return &AerospikeClusterConfig{
		clusterName: clusterName,
		namespaces:  namespaces,
		// auth
		authEnabled: authEnabled,
		username:    username,
		password:    password,
		// tls
		tlsEnabled:  tlsEnabled,
		tlsHostname: tlsHostname,
		// conf
		genericConfig: &conf.AerospikeEndpointConfig,
	}, nil
}

func (conf *AerospikeProbeConfig) DiscoverClusters(logger log.Logger, aerospikeServices []discovery.ServiceEntry) (topology.ClusterMap, error) {
	clusterMap := topology.NewClusterMap()

	aerospikeServicesPerCluster := conf.DiscoveryConfig.GroupNodesByCluster(logger, aerospikeServices)

	for clusterName, clusterServices := range aerospikeServicesPerCluster {
		clusterConfig, err := conf.buildClusterConfig(clusterName, &clusterServices[0], logger)
		if err != nil {
			return clusterMap, err
		}

		err = conf.addCluster(clusterConfig, clusterServices, clusterMap, logger)
		if err != nil {
			return clusterMap, err
		}
	}

	return clusterMap, nil
}
