package opensearch

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func TestOpenSearchEndpointGetHash(t *testing.T) {
	tests := []struct {
		name         string
		clusterName  string
		endpointName string
		expected     string
	}{
		{
			name:         "SimpleHash",
			clusterName:  "prod-cluster",
			endpointName: "node-1",
			expected:     "prod-cluster/node-1",
		},
		{
			name:         "EmptyClusterName",
			clusterName:  "",
			endpointName: "node-1",
			expected:     "/node-1",
		},
		{
			name:         "EmptyEndpointName",
			clusterName:  "cluster",
			endpointName: "",
			expected:     "cluster/",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &OpenSearchEndpoint{
				ClusterName: tt.clusterName,
				Name:        tt.endpointName,
			}

			got := endpoint.GetHash()
			if got != tt.expected {
				t.Fatalf("expected hash %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestOpenSearchEndpointGetName(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "SimpleName",
			expected: "opensearch-node-1",
		},
		{
			name:     "IPAddress",
			expected: "10.0.0.1",
		},
		{
			name:     "EmptyName",
			expected: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &OpenSearchEndpoint{
				Name: tt.expected,
			}

			got := endpoint.GetName()
			if got != tt.expected {
				t.Fatalf("expected name %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestOpenSearchEndpointIsCluster(t *testing.T) {
	tests := []struct {
		name         string
		clusterLevel bool
	}{
		{
			name:         "ClusterLevel",
			clusterLevel: true,
		},
		{
			name:         "NodeLevel",
			clusterLevel: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &OpenSearchEndpoint{
				ClusterLevel: tt.clusterLevel,
			}

			got := endpoint.IsCluster()
			if got != tt.clusterLevel {
				t.Fatalf("expected IsCluster %v, got %v", tt.clusterLevel, got)
			}
		})
	}
}

func TestOpenSearchEndpointConnect(t *testing.T) {
	tests := []struct {
		name        string
		config      opensearchapi.Config
		expectError bool
	}{
		{
			name: "ValidConfig",
			config: opensearchapi.Config{
				Client: opensearch.Config{
					Addresses: []string{"http://localhost:9200"},
				},
			},
			expectError: false,
		},
		{
			name: "EmptyAddresses",
			config: opensearchapi.Config{
				Client: opensearch.Config{
					Addresses: []string{},
				},
			},
			expectError: false, // OpenSearch client allows empty addresses
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &OpenSearchEndpoint{
				ClientConfig: tt.config,
				Logger:       log.NewNopLogger(),
			}

			err := endpoint.Connect()
			if tt.expectError && err == nil {
				t.Fatal("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !tt.expectError && endpoint.Client == nil {
				t.Fatal("expected client to be initialized")
			}
		})
	}
}

func TestOpenSearchEndpointRefresh(t *testing.T) {
	endpoint := &OpenSearchEndpoint{
		Logger: log.NewNopLogger(),
	}

	// Refresh is a no-op, should always return nil
	err := endpoint.Refresh()
	if err != nil {
		t.Fatalf("unexpected error from Refresh: %v", err)
	}
}

func TestOpenSearchEndpointClose(t *testing.T) {
	endpoint := &OpenSearchEndpoint{
		Logger: log.NewNopLogger(),
	}

	// Close is a no-op, should always return nil
	err := endpoint.Close()
	if err != nil {
		t.Fatalf("unexpected error from Close: %v", err)
	}
}

func TestCheckIndexExistsNilClient(t *testing.T) {
	endpoint := &OpenSearchEndpoint{
		Client: nil,
		Logger: log.NewNopLogger(),
	}

	exists, err := endpoint.checkIndexExists("test-index")
	if err == nil {
		t.Fatal("expected error when client is nil")
	}
	if exists {
		t.Fatal("expected exists to be false when client is nil")
	}

	expectedError := "opensearch endpoint client not initialized"
	if err.Error() != expectedError {
		t.Fatalf("expected error %q, got %q", expectedError, err.Error())
	}
}

func TestCheckIndexExistsNilEndpoint(t *testing.T) {
	var endpoint *OpenSearchEndpoint

	exists, err := endpoint.checkIndexExists("test-index")
	if err == nil {
		t.Fatal("expected error when endpoint is nil")
	}
	if exists {
		t.Fatal("expected exists to be false when endpoint is nil")
	}
}

func TestOpenSearchEndpointStructFields(t *testing.T) {
	config := OpenSearchEndpointConfig{
		AuthEnabled: true,
		UsernameEnv: "TEST_USER",
		PasswordEnv: "TEST_PASS",
	}

	clientConfig := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{"http://localhost:9200"},
		},
	}

	endpoint := &OpenSearchEndpoint{
		Name:         "test-node",
		ClusterLevel: true,
		ClusterName:  "test-cluster",
		PodName:      "test-pod-1",
		ClientConfig: clientConfig,
		Config:       config,
		Logger:       log.NewNopLogger(),
	}

	if endpoint.Name != "test-node" {
		t.Fatalf("expected Name to be 'test-node', got %q", endpoint.Name)
	}
	if !endpoint.ClusterLevel {
		t.Fatal("expected ClusterLevel to be true")
	}
	if endpoint.ClusterName != "test-cluster" {
		t.Fatalf("expected ClusterName to be 'test-cluster', got %q", endpoint.ClusterName)
	}
	if endpoint.PodName != "test-pod-1" {
		t.Fatalf("expected PodName to be 'test-pod-1', got %q", endpoint.PodName)
	}
	if endpoint.Config.AuthEnabled != config.AuthEnabled {
		t.Fatal("expected Config to match")
	}
	if len(endpoint.ClientConfig.Client.Addresses) != 1 {
		t.Fatalf("expected 1 address, got %d", len(endpoint.ClientConfig.Client.Addresses))
	}
}
