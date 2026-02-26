package opensearch

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/go-kit/log"
)

func TestBuildAddress(t *testing.T) {
	conf := OpenSearchProbeConfig{
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			TLSTag: "tls",
		},
	}

	tests := []struct {
		name     string
		entry    discovery.ServiceEntry
		expected string
	}{
		{
			name: "NoTLSTag",
			entry: discovery.ServiceEntry{
				Address: "10.0.0.1",
				Port:    9200,
				Tags:    []string{"cluster_name-mycluster"},
			},
			expected: "http://10.0.0.1:9200",
		},
		{
			name: "WithTLSTag",
			entry: discovery.ServiceEntry{
				Address: "10.0.0.1",
				Port:    9200,
				Tags:    []string{"tls", "cluster_name-mycluster"},
			},
			expected: "https://10.0.0.1:9200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := conf.buildAddress(tt.entry)
			if got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestBuildOpenSearchEndpoint(t *testing.T) {
	conf := OpenSearchProbeConfig{
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: false,
			TLSTag:      "tls",
		},
	}

	entries := []discovery.ServiceEntry{
		{
			Address:  "10.0.0.1",
			Port:     9200,
			Tags:     []string{"cluster_name-mycluster"},
			PodName:  "pod-1",
			NodeFqdn: "node1.example.com",
		},
		{
			Address:  "10.0.0.2",
			Port:     9200,
			Tags:     []string{"cluster_name-mycluster"},
			PodName:  "pod-2",
			NodeFqdn: "node2.example.com",
		},
	}

	endpoint, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Cluster name from tags
	if endpoint.ClusterName != "mycluster" {
		t.Fatalf("expected cluster name %q, got %q", "mycluster", endpoint.ClusterName)
	}
	if endpoint.Name != "mycluster" {
		t.Fatalf("expected name %q, got %q", "mycluster", endpoint.Name)
	}
	if !endpoint.ClusterLevel {
		t.Fatal("expected endpoint to be cluster level")
	}

	// nodeInfoCache should contain both pods
	if len(endpoint.nodeInfoCache) != 2 {
		t.Fatalf("expected 2 entries in nodeInfoCache, got %d", len(endpoint.nodeInfoCache))
	}
	ni1 := endpoint.nodeInfoCache["pod-1"]
	if ni1 == nil {
		t.Fatal("expected pod-1 in nodeInfoCache")
	}
	if ni1.NodeIP != "10.0.0.1" || ni1.NodeFqdn != "node1.example.com" || ni1.PodName != "pod-1" {
		t.Fatalf("unexpected nodeInfoCache entry for pod-1: %+v", ni1)
	}
	ni2 := endpoint.nodeInfoCache["pod-2"]
	if ni2 == nil {
		t.Fatal("expected pod-2 in nodeInfoCache")
	}
	if ni2.NodeIP != "10.0.0.2" || ni2.NodeFqdn != "node2.example.com" || ni2.PodName != "pod-2" {
		t.Fatalf("unexpected nodeInfoCache entry for pod-2: %+v", ni2)
	}

	// Seeds should have both addresses
	addresses := endpoint.ClientConfig.Client.Addresses
	if len(addresses) != 2 {
		t.Fatalf("expected 2 seed addresses, got %d", len(addresses))
	}
	if addresses[0] != "http://10.0.0.1:9200" {
		t.Fatalf("expected first seed %q, got %q", "http://10.0.0.1:9200", addresses[0])
	}
	if addresses[1] != "http://10.0.0.2:9200" {
		t.Fatalf("expected second seed %q, got %q", "http://10.0.0.2:9200", addresses[1])
	}

	// Sniffing config
	if !endpoint.ClientConfig.Client.DiscoverNodesOnStart {
		t.Fatal("expected DiscoverNodesOnStart to be true")
	}
	if endpoint.ClientConfig.Client.DiscoverNodesInterval != 30*time.Second {
		t.Fatalf("expected DiscoverNodesInterval 30s, got %v", endpoint.ClientConfig.Client.DiscoverNodesInterval)
	}
}

func TestBuildOpenSearchEndpointWithTLS(t *testing.T) {
	conf := OpenSearchProbeConfig{
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled:        false,
			TLSTag:             "tls",
			InsecureSkipVerify: true,
		},
	}

	entries := []discovery.ServiceEntry{
		{
			Address:  "10.0.0.1",
			Port:     9200,
			Tags:     []string{"tls", "cluster_name-securecluster"},
			PodName:  "pod-1",
			NodeFqdn: "node1.example.com",
		},
	}

	endpoint, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if endpoint.ClientConfig.Client.Addresses[0] != "https://10.0.0.1:9200" {
		t.Fatalf("expected https address, got %q", endpoint.ClientConfig.Client.Addresses[0])
	}

	// Transport should be configured for InsecureSkipVerify
	if endpoint.ClientConfig.Client.Transport == nil {
		t.Fatal("expected Transport to be set for TLS with InsecureSkipVerify")
	}
}

func TestBuildOpenSearchEndpointWithAuth(t *testing.T) {
	conf := OpenSearchProbeConfig{
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: true,
			UsernameEnv: "OS_TEST_USER",
			PasswordEnv: "OS_TEST_PASS",
			TLSTag:      "tls",
		},
	}

	t.Setenv("OS_TEST_USER", "admin")
	t.Setenv("OS_TEST_PASS", "secret")

	entries := []discovery.ServiceEntry{
		{
			Address:  "10.0.0.1",
			Port:     9200,
			Tags:     []string{"cluster_name-authcluster"},
			PodName:  "pod-1",
			NodeFqdn: "node1.example.com",
		},
	}

	endpoint, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if endpoint.ClientConfig.Client.Username != "admin" {
		t.Fatalf("expected username %q, got %q", "admin", endpoint.ClientConfig.Client.Username)
	}
	if endpoint.ClientConfig.Client.Password != "secret" {
		t.Fatalf("expected password %q, got %q", "secret", endpoint.ClientConfig.Client.Password)
	}
}

func TestBuildOpenSearchEndpointAuthMissingEnv(t *testing.T) {
	usernameEnv := fmt.Sprintf("OS_TEST_USER_%d", time.Now().UnixNano())
	passwordEnv := fmt.Sprintf("OS_TEST_PASS_%d", time.Now().UnixNano())

	conf := OpenSearchProbeConfig{
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: true,
			UsernameEnv: usernameEnv,
			PasswordEnv: passwordEnv,
			TLSTag:      "tls",
		},
	}

	_ = os.Unsetenv(usernameEnv)
	_ = os.Unsetenv(passwordEnv)

	entries := []discovery.ServiceEntry{
		{
			Address:  "10.0.0.1",
			Port:     9200,
			Tags:     []string{"cluster_name-mycluster"},
			PodName:  "pod-1",
			NodeFqdn: "node1.example.com",
		},
	}

	_, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), entries)
	if err == nil {
		t.Fatal("expected error when auth env vars are missing")
	}

	expected := fmt.Sprintf("error: username not found in env (%s)", usernameEnv)
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestBuildTopology(t *testing.T) {
	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: false,
			TLSTag:      "tls",
		},
	}

	entries := []discovery.ServiceEntry{
		{
			Service:  "opensearch",
			Address:  "10.0.0.1",
			Port:     9200,
			Tags:     []string{"cluster_name-cluster1"},
			PodName:  "pod-1",
			NodeFqdn: "node1.example.com",
			Meta:     map[string]string{"CLUSTER": "cluster1"},
		},
		{
			Service:  "opensearch",
			Address:  "10.0.0.2",
			Port:     9200,
			Tags:     []string{"cluster_name-cluster1"},
			PodName:  "pod-2",
			NodeFqdn: "node2.example.com",
			Meta:     map[string]string{"CLUSTER": "cluster1"},
		},
	}

	clusterMap, err := conf.BuildTopology(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(clusterMap.Clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusterMap.Clusters))
	}

	var ep *OpenSearchEndpoint
	for _, cluster := range clusterMap.Clusters {
		ep = cluster.ClusterEndpoint.(*OpenSearchEndpoint)
	}
	if ep.ClusterName != "cluster1" {
		t.Fatalf("expected cluster name %q, got %q", "cluster1", ep.ClusterName)
	}
	if len(ep.nodeInfoCache) != 2 {
		t.Fatalf("expected 2 entries in nodeInfoCache, got %d", len(ep.nodeInfoCache))
	}
}
