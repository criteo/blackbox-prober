package opensearch

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func (conf *OpenSearchProbeConfig) buildAddress(tlsEnabled bool, entry discovery.ServiceEntry) string {
	proto := "http"
	if tlsEnabled {
		proto = "https"
	}

	return fmt.Sprintf("%s://%s", proto, fmt.Sprintf("%s:%d", entry.Address, entry.Port))
}

func (conf *OpenSearchProbeConfig) buildOpenSearchEndpoint(logger log.Logger, clusterName string, entry discovery.ServiceEntry) (*OpenSearchEndpoint, error) {
	// Init Client Config
	tlsEnabled := utils.Contains(entry.Tags, conf.OpenSearchEndpointConfig.TLSTag)
	clientConfig := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{conf.buildAddress(tlsEnabled, entry)},
		},
	}

	// Auth Enabled?
	authEnabled := conf.OpenSearchEndpointConfig.AuthEnabled
	var (
		username string
		password string
		ok       bool
	)

	// Insecure Skip Verify
	if tlsEnabled && conf.OpenSearchEndpointConfig.InsecureSkipVerify {
		clientConfig.Client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	if authEnabled {
		username, ok = os.LookupEnv(conf.OpenSearchEndpointConfig.UsernameEnv)
		if !ok {
			return nil, fmt.Errorf("error: username not found in env (%s)", conf.OpenSearchEndpointConfig.UsernameEnv)
		}
		password, ok = os.LookupEnv(conf.OpenSearchEndpointConfig.PasswordEnv)
		if !ok {
			return nil, fmt.Errorf("error: password not found in env (%s)", conf.OpenSearchEndpointConfig.PasswordEnv)
		}

		clientConfig.Client.Username = username
		clientConfig.Client.Password = password
	}

	endpoint := &OpenSearchEndpoint{
		Name:         entry.Address,
		PodName:      entry.Meta["k8s_pod"],
		NodeFqdn:     entry.NodeFqdn,
		ClusterName:  conf.valueFromTags("cluster_name", entry.Tags),
		ClusterLevel: true,
		ClientConfig: clientConfig,
		Config:       conf.OpenSearchEndpointConfig,
		Logger:       log.With(logger, "endpoint_name", entry.Address),
	}

	return endpoint, nil
}

func (conf *OpenSearchProbeConfig) generateClusterEndpointFromEntries(logger log.Logger, entries []discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("no entries provided")
	}

	entry := entries[0]

	clusterName, ok := entry.Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		return nil, fmt.Errorf("cluster name not found in meta key: %s", conf.DiscoveryConfig.MetaClusterKey)
	}

	return conf.buildOpenSearchEndpoint(logger, clusterName, entry)
}

func (conf *OpenSearchProbeConfig) generateNodeEndpointFromEntry(logger log.Logger, entry discovery.ServiceEntry) (topology.ProbeableEndpoint, error) {
	clusterName, ok := entry.Meta[conf.DiscoveryConfig.MetaClusterKey]
	if !ok {
		clusterName = entry.Address
	}
	return conf.buildOpenSearchEndpoint(logger, clusterName, entry)
}

func (conf *OpenSearchProbeConfig) valueFromTags(prefix string, serviceTags []string) string {
	for _, tag := range serviceTags {
		splitted := strings.SplitN(tag, "-", 2)
		if splitted[0] == prefix {
			return splitted[1]
		}
	}
	return ""
}

func (conf *OpenSearchProbeConfig) TopologyBuilder() func(log.Logger, []discovery.ServiceEntry) (topology.ClusterMap, error) {
	return conf.DiscoveryConfig.GetGenericTopologyBuilder(
		conf.generateClusterEndpointFromEntries,
		conf.generateNodeEndpointFromEntry,
	)
}
