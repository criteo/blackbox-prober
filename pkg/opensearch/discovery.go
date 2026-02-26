package opensearch

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func (conf *OpenSearchProbeConfig) buildAddress(entry discovery.ServiceEntry) string {
	proto := "http"
	if utils.Contains(entry.Tags, conf.OpenSearchEndpointConfig.TLSTag) {
		proto = "https"
	}

	return fmt.Sprintf("%s://%s", proto, fmt.Sprintf("%s:%d", entry.Address, entry.Port))
}

func (conf *OpenSearchProbeConfig) buildOpenSearchEndpoint(logger log.Logger, entries []discovery.ServiceEntry) (*OpenSearchEndpoint, error) {
	// Init Client Config
	tlsEnabled := false

	nodeInfoCache := map[string]*common.ClusterNodeInfo{} // a map keeping information about nodes to enrich metrics
	seeds := []string{}
	for _, entry := range entries {
		contactPoint := conf.buildAddress(entry)
		seeds = append(seeds, contactPoint)
		if strings.HasPrefix(contactPoint, "https") {
			tlsEnabled = true
		}

		nodeInfoCache[entry.PodName] = &common.ClusterNodeInfo{
			NodeIP:   entry.Address,
			PodName:  entry.PodName,
			NodeFqdn: entry.NodeFqdn,
		}
	}

	clientConfig := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses:             seeds,
			DiscoverNodesOnStart:  true, // sniffing enabled to retrieve topology updates before consul
			DiscoverNodesInterval: 30 * time.Second,
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

	clusterName := conf.valueFromTags("cluster_name", entries[0].Tags)
	endpoint := &OpenSearchEndpoint{
		Name:          clusterName,
		ClusterName:   clusterName,
		ClusterLevel:  true,
		ClientConfig:  clientConfig,
		Config:        conf.OpenSearchEndpointConfig,
		Logger:        log.With(logger, "endpoint_name", clusterName),
		nodeInfoCache: nodeInfoCache,
	}

	return endpoint, nil
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

func (conf *OpenSearchProbeConfig) BuildTopology(logger log.Logger, entries []discovery.ServiceEntry) (topology.ClusterMap, error) {
	clusterMap := topology.NewClusterMap()
	clusterEntries := conf.DiscoveryConfig.GroupNodesByCluster(logger, entries)
	for serviceName, entries := range clusterEntries {
		if len(entries) == 0 {
			return clusterMap, fmt.Errorf("no service instances found for %s", serviceName)
		}

		clusterEndpoint, err := conf.buildOpenSearchEndpoint(logger, entries)
		if err != nil {
			return clusterMap, err
		}
		cluster := topology.NewCluster(clusterEndpoint)
		clusterMap.AppendCluster(cluster)
	}
	return clusterMap, nil
}
