package milvus

import (
	"errors"
	"fmt"
	"os"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	mv "github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func (conf *MilvusProbeConfig) buildAddress(tlsEnabled bool, addressUrl string) string {
	proto := "http"
	if tlsEnabled {
		proto = "https"
	}

	return fmt.Sprintf("%s://%s", proto, addressUrl)
}

func (conf *MilvusProbeConfig) generateClusterEndpointsFromEntry(logger log.Logger, entry discovery.ServiceEntry) ([]*MilvusEndpoint, error) {
	authEnabled := conf.MilvusEndpointConfig.AuthEnabled
	var (
		username string
		password string
		ok       bool
	)

	if authEnabled {
		username, ok = os.LookupEnv(conf.MilvusEndpointConfig.UsernameEnv)
		if !ok {
			return nil, fmt.Errorf("error: username not found in env (%s)", conf.MilvusEndpointConfig.UsernameEnv)
		}
		password, ok = os.LookupEnv(conf.MilvusEndpointConfig.PasswordEnv)
		if !ok {
			return nil, fmt.Errorf("error: password not found in env (%s)", conf.MilvusEndpointConfig.PasswordEnv)
		}
	}
	tlsEnabled := utils.Contains(entry.Tags, conf.MilvusEndpointConfig.TLSTag)
	addressUrl, ok := entry.Meta[conf.MilvusEndpointConfig.AddressMetaKey]
	if !ok {
		msg := fmt.Sprintf("%s not found in consul meta key for service %s", conf.MilvusEndpointConfig.AddressMetaKey, entry.Service)
		level.Warn(logger).Log("msg", msg)
		return nil, errors.New(msg)
	}

	clusterName, ok := entry.Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		msg := fmt.Sprintf("ClusterName meta key not found. Ignoring service %s.", entry.Service)
		level.Error(logger).Log("msg", msg)
		return nil, errors.New(msg)
	}
	address := conf.buildAddress(tlsEnabled, addressUrl)

	endpoint := &MilvusEndpoint{Name: clusterName,
		ClusterName:  clusterName,
		ClusterLevel: true,
		ClientConfig: mv.ClientConfig{
			// auth
			Username: username,
			Password: password,
			// tls
			Address: address,
			// conf
			RetryRateLimit: &mv.RetryRateLimitOption{
				MaxRetry:   conf.MilvusEndpointConfig.MaxRetry,
				MaxBackoff: conf.MilvusEndpointConfig.MaxBackoff,
			},
		},
		Config: conf.MilvusEndpointConfig,
		Logger: log.With(logger, "endpoint_name", entry.Address),
	}

	return []*MilvusEndpoint{endpoint}, nil
}

func (conf MilvusProbeConfig) NamespacedTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return func(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
		clusterMap := topology.NewClusterMap()
		clusterEntries := conf.DiscoveryConfig.GroupNodesByCluster(logger, entries)
		for _, entries := range clusterEntries {
			endpoints, err := conf.generateClusterEndpointsFromEntry(logger, entries[0])
			if err != nil {
				return clusterMap, err
			}

			for _, endpoint := range endpoints {
				level.Debug(logger).Log("msg", "Adding cluster", "cluster", endpoint.Name, "address", endpoint.ClientConfig.Address)

				cluster := topology.NewCluster(endpoint)
				clusterMap.AppendCluster(cluster)
			}

		}
		return clusterMap, nil
	}
}
