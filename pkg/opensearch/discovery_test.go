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
	conf := OpenSearchProbeConfig{}

	tests := []struct {
		name     string
		tls      bool
		entry    discovery.ServiceEntry
		expected string
	}{
		{
			name: "TLSDisabled",
			tls:  false,
			entry: discovery.ServiceEntry{
				Address: "opensearch.local",
				Port:    9200,
			},
			expected: "http://opensearch.local:9200",
		},
		{
			name: "TLSEnabled",
			tls:  true,
			entry: discovery.ServiceEntry{
				Address: "opensearch.local",
				Port:    9200,
			},
			expected: "https://opensearch.local:9200",
		},
		{
			name: "DifferentPort",
			tls:  false,
			entry: discovery.ServiceEntry{
				Address: "localhost",
				Port:    9300,
			},
			expected: "http://localhost:9300",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := (&conf).buildAddress(tt.tls, tt.entry)
			if got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestValueFromTags(t *testing.T) {
	conf := OpenSearchProbeConfig{}

	tests := []struct {
		name     string
		prefix   string
		tags     []string
		expected string
	}{
		{
			name:     "ClusterNameFound",
			prefix:   "cluster_name",
			tags:     []string{"cluster_name-prod-cluster", "env-production"},
			expected: "prod-cluster",
		},
		{
			name:     "ClusterNameNotFound",
			prefix:   "cluster_name",
			tags:     []string{"env-production", "region-us-east"},
			expected: "",
		},
		{
			name:     "EmptyTags",
			prefix:   "cluster_name",
			tags:     []string{},
			expected: "",
		},
		{
			name:     "TagWithMultipleDashes",
			prefix:   "cluster_name",
			tags:     []string{"cluster_name-my-prod-cluster"},
			expected: "my-prod-cluster",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := (&conf).valueFromTags(tt.prefix, tt.tags)
			if got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestBuildOpenSearchEndpoint(t *testing.T) {
	baseConfig := OpenSearchProbeConfig{
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			TLSTag:             "tls",
			InsecureSkipVerify: true,
		},
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
	}

	entryTemplate := discovery.ServiceEntry{
		Service: "opensearch",
		Address: "10.0.0.1",
		Port:    9200,
		Tags:    []string{"tls", "cluster_name-prod-cluster"},
		Meta: map[string]string{
			"CLUSTER":  "opensearch-prod",
			"k8s_pod":  "opensearch-pod-1",
		},
	}

	tests := []struct {
		name        string
		authEnabled bool
		username    string
		password    string
		tlsEnabled  bool
	}{
		{
			name:        "AuthDisabled_TLSDisabled",
			authEnabled: false,
			username:    "",
			password:    "",
			tlsEnabled:  false,
		},
		{
			name:        "AuthEnabled_TLSEnabled",
			authEnabled: true,
			username:    "admin",
			password:    "secret123",
			tlsEnabled:  true,
		},
		{
			name:        "AuthEnabled_TLSDisabled",
			authEnabled: true,
			username:    "user",
			password:    "pass",
			tlsEnabled:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			conf := baseConfig
			conf.OpenSearchEndpointConfig.AuthEnabled = tt.authEnabled
			entry := entryTemplate

			if !tt.tlsEnabled {
				entry.Tags = []string{"cluster_name-prod-cluster"}
			}

			if tt.authEnabled {
				conf.OpenSearchEndpointConfig.UsernameEnv = "OPENSEARCH_TEST_USERNAME"
				conf.OpenSearchEndpointConfig.PasswordEnv = "OPENSEARCH_TEST_PASSWORD"
				t.Setenv(conf.OpenSearchEndpointConfig.UsernameEnv, tt.username)
				t.Setenv(conf.OpenSearchEndpointConfig.PasswordEnv, tt.password)
			}

			endpoint, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), "prod-cluster", entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if endpoint.Name != entry.Address {
				t.Fatalf("unexpected endpoint name: %s", endpoint.Name)
			}
			if endpoint.ClusterName != "prod-cluster" {
				t.Fatalf("unexpected cluster name: %s", endpoint.ClusterName)
			}
			if endpoint.PodName != "opensearch-pod-1" {
				t.Fatalf("unexpected pod name: %s", endpoint.PodName)
			}
			if !endpoint.ClusterLevel {
				t.Fatal("expected endpoint to be marked as cluster level")
			}

			expectedProto := "http"
			if tt.tlsEnabled {
				expectedProto = "https"
			}
			expectedAddr := fmt.Sprintf("%s://%s:%d", expectedProto, entry.Address, entry.Port)
			if endpoint.ClientConfig.Client.Addresses[0] != expectedAddr {
				t.Fatalf("expected address %q, got %q", expectedAddr, endpoint.ClientConfig.Client.Addresses[0])
			}

			if tt.authEnabled {
				if endpoint.ClientConfig.Client.Username != tt.username {
					t.Fatalf("expected username %q, got %q", tt.username, endpoint.ClientConfig.Client.Username)
				}
				if endpoint.ClientConfig.Client.Password != tt.password {
					t.Fatalf("expected password %q, got %q", tt.password, endpoint.ClientConfig.Client.Password)
				}
			} else if endpoint.ClientConfig.Client.Username != "" || endpoint.ClientConfig.Client.Password != "" {
				t.Fatalf("expected empty credentials when auth disabled, got %q/%q",
					endpoint.ClientConfig.Client.Username, endpoint.ClientConfig.Client.Password)
			}

			if endpoint.Config != conf.OpenSearchEndpointConfig {
				t.Fatal("expected endpoint config to match OpenSearchEndpointConfig")
			}
		})
	}
}

func TestBuildOpenSearchEndpointAuthMissingEnv(t *testing.T) {
	usernameEnv := fmt.Sprintf("OPENSEARCH_TEST_USERNAME_%d", time.Now().UnixNano())
	passwordEnv := fmt.Sprintf("OPENSEARCH_TEST_PASSWORD_%d", time.Now().UnixNano())

	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: true,
			UsernameEnv: usernameEnv,
			PasswordEnv: passwordEnv,
			TLSTag:      "tls",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "opensearch",
		Address: "10.0.0.1",
		Port:    9200,
		Tags:    []string{"tls"},
		Meta: map[string]string{
			"CLUSTER": "opensearch-prod",
		},
	}

	_ = os.Unsetenv(usernameEnv)
	_ = os.Unsetenv(passwordEnv)

	_, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), "cluster", entry)
	if err == nil {
		t.Fatal("expected error when auth env vars are missing")
	}

	expected := fmt.Sprintf("error: username not found in env (%s)", usernameEnv)
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestBuildOpenSearchEndpointPasswordMissingEnv(t *testing.T) {
	usernameEnv := fmt.Sprintf("OPENSEARCH_TEST_USERNAME_%d", time.Now().UnixNano())
	passwordEnv := fmt.Sprintf("OPENSEARCH_TEST_PASSWORD_%d", time.Now().UnixNano())

	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: true,
			UsernameEnv: usernameEnv,
			PasswordEnv: passwordEnv,
			TLSTag:      "tls",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "opensearch",
		Address: "10.0.0.1",
		Port:    9200,
		Tags:    []string{"tls"},
		Meta: map[string]string{
			"CLUSTER": "opensearch-prod",
		},
	}

	t.Setenv(usernameEnv, "testuser")
	_ = os.Unsetenv(passwordEnv)

	_, err := conf.buildOpenSearchEndpoint(log.NewNopLogger(), "cluster", entry)
	if err == nil {
		t.Fatal("expected error when password env var is missing")
	}

	expected := fmt.Sprintf("error: password not found in env (%s)", passwordEnv)
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGenerateClusterEndpointFromEntries(t *testing.T) {
	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: false,
			TLSTag:      "tls",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "opensearch",
		Address: "10.0.0.1",
		Port:    9200,
		Tags:    []string{"cluster_name-test-cluster"},
		Meta: map[string]string{
			"CLUSTER": "opensearch-test",
		},
	}

	endpoint, err := conf.generateClusterEndpointFromEntries(log.NewNopLogger(), []discovery.ServiceEntry{entry})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if endpoint.GetName() != entry.Address {
		t.Fatalf("expected name %q, got %q", entry.Address, endpoint.GetName())
	}
}

func TestGenerateClusterEndpointFromEntriesNoEntries(t *testing.T) {
	conf := OpenSearchProbeConfig{}

	_, err := conf.generateClusterEndpointFromEntries(log.NewNopLogger(), []discovery.ServiceEntry{})
	if err == nil {
		t.Fatal("expected error when no entries provided")
	}

	expected := "no entries provided"
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestGenerateClusterEndpointFromEntriesMissingClusterMeta(t *testing.T) {
	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: false,
			TLSTag:      "tls",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "opensearch",
		Address: "10.0.0.1",
		Port:    9200,
		Tags:    []string{},
		Meta:    map[string]string{},
	}

	_, err := conf.generateClusterEndpointFromEntries(log.NewNopLogger(), []discovery.ServiceEntry{entry})
	if err == nil {
		t.Fatal("expected error when cluster meta is missing")
	}

	expected := fmt.Sprintf("cluster name not found in meta key: %s", conf.DiscoveryConfig.MetaClusterKey)
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestGenerateNodeEndpointFromEntry(t *testing.T) {
	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: false,
			TLSTag:      "tls",
		},
	}

	tests := []struct {
		name                string
		entry               discovery.ServiceEntry
		expectedClusterName string
	}{
		{
			name: "WithClusterMeta",
			entry: discovery.ServiceEntry{
				Service: "opensearch",
				Address: "10.0.0.1",
				Port:    9200,
				Tags:    []string{"cluster_name-node-cluster"},
				Meta: map[string]string{
					"CLUSTER": "opensearch-node",
				},
			},
			expectedClusterName: "node-cluster", // ClusterName comes from valueFromTags, not Meta
		},
		{
			name: "WithoutClusterMeta",
			entry: discovery.ServiceEntry{
				Service: "opensearch",
				Address: "10.0.0.2",
				Port:    9200,
				Tags:    []string{},
				Meta:    map[string]string{},
			},
			expectedClusterName: "", // No cluster_name tag means empty string
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			endpoint, err := conf.generateNodeEndpointFromEntry(log.NewNopLogger(), tt.entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			osEndpoint, ok := endpoint.(*OpenSearchEndpoint)
			if !ok {
				t.Fatal("expected OpenSearchEndpoint type")
			}

			if osEndpoint.ClusterName != tt.expectedClusterName {
				t.Fatalf("expected cluster name %q, got %q", tt.expectedClusterName, osEndpoint.ClusterName)
			}

			if osEndpoint.Name != tt.entry.Address {
				t.Fatalf("expected name %q, got %q", tt.entry.Address, osEndpoint.Name)
			}
		})
	}
}

func TestTopologyBuilder(t *testing.T) {
	conf := OpenSearchProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		OpenSearchEndpointConfig: OpenSearchEndpointConfig{
			AuthEnabled: false,
			TLSTag:      "tls",
		},
	}

	builder := conf.TopologyBuilder()
	if builder == nil {
		t.Fatal("expected non-nil topology builder function")
	}

	// Test that the builder function is callable
	entries := []discovery.ServiceEntry{
		{
			Service: "opensearch",
			Address: "10.0.0.1",
			Port:    9200,
			Tags:    []string{"cluster_name-test"},
			Meta: map[string]string{
				"CLUSTER": "opensearch-test",
			},
		},
	}

	clusterMap, err := builder(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("topology builder failed: %v", err)
	}

	if len(clusterMap.Clusters) == 0 {
		t.Fatal("expected non-empty cluster map")
	}
}
