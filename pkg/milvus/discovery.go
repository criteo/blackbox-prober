package milvus

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func (conf *MilvusProbeConfig) buildAddress(tlsEnabled bool, addressUrl string) string {
	proto := "http"
	if tlsEnabled {
		proto = "https"
	}
	// Address like "https://milvus-milvuss99.da1.preprod.crto.in:19531"
	return fmt.Sprintf("%s://%s", proto, addressUrl)
}

func (conf *MilvusProbeConfig) generateDatabaseEndpointsFromEntry(logger log.Logger, entry discovery.ServiceEntry) ([]*MilvusEndpoint, error) {
	var (
		username string
		password string
		ok       bool
	)

	username, ok = os.LookupEnv(conf.MilvusEndpointConfig.UsernameEnv)
	if !ok {
		return nil, fmt.Errorf("error: username not found in env (%s)", conf.MilvusEndpointConfig.UsernameEnv)
	}
	password, ok = os.LookupEnv(conf.MilvusEndpointConfig.PasswordEnv)
	if !ok {
		return nil, fmt.Errorf("error: password not found in env (%s)", conf.MilvusEndpointConfig.PasswordEnv)
	}

	var endpoints []*MilvusEndpoint

	tlsEnabled := utils.Contains(entry.Tags, conf.MilvusEndpointConfig.TLSTag)
	addressUrl, ok := entry.Meta[conf.MilvusEndpointConfig.AddressMetaKey]
	if !ok {
		msg := fmt.Sprintf("%s not found in consul meta key for service %s", conf.MilvusEndpointConfig.AddressMetaKey, entry.Service)
		level.Warn(logger).Log("msg", msg)
		return endpoints, errors.New(msg)
	}

	clusterName, ok := entry.Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		msg := fmt.Sprintf("ClusterName meta key not found. Ignoring service %s.", entry.Service)
		level.Error(logger).Log("msg", msg)
		return endpoints, errors.New(msg)
	}

	databases := conf.getDatabasesFromEntry(logger, entry)
	address := conf.buildAddress(tlsEnabled, addressUrl)

	// TODO d.amsallem 08/25/2025: It may be worth to have only 1 MilvusEndpoint per cluster instead of one per db
	for database := range databases {
		e := &MilvusEndpoint{Name: clusterName,
			ClusterName:  clusterName,
			Database:     database,
			ClusterLevel: true,
			Config: MilvusClientConfig{
				// auth
				Username: username,
				Password: password,
				DBName:   database,
				// tls
				EnableTLSAuth: tlsEnabled,
				Address:       address,
				// conf
				RetryRateLimit: &milvusclient.RetryRateLimitOption{
					MaxRetry:   conf.MilvusEndpointConfig.MaxRetry,
					MaxBackoff: conf.MilvusEndpointConfig.MaxBackoff,
				},
			},
			Logger: log.With(logger, "endpoint_name", entry.Address),
		}
		endpoints = append(endpoints, e)
	}

	return endpoints, nil
}

func (conf MilvusProbeConfig) getDatabasesFromEntry(logger log.Logger, entry discovery.ServiceEntry) map[string]struct{} {
	databases := make(map[string]struct{})

	for metaKey, metaValue := range entry.Meta {
		if !strings.HasPrefix(metaKey, conf.MilvusEndpointConfig.DatabaseMetaKeyPrefix) {
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
		// MetaKey is like : "milvus-monitoring-foo"
		ns := metaKey[len(conf.MilvusEndpointConfig.DatabaseMetaKeyPrefix):]
		if len(ns) > 0 {
			databases[ns] = struct{}{}
		}
	}

	return databases
}

func (conf MilvusProbeConfig) NamespacedTopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return func(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
		clusterMap := topology.NewClusterMap()
		clusterEntries := conf.DiscoveryConfig.GroupNodesByCluster(logger, entries)
		for _, entries := range clusterEntries {
			endpoints, err := conf.generateDatabaseEndpointsFromEntry(logger, entries[0])
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
