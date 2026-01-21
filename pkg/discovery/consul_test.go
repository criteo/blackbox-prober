package discovery

import (
	"testing"

	consul "github.com/hashicorp/consul/api"
)

func TestToServiceEntryNodeFqdnResolution(t *testing.T) {
	tests := []struct {
		name             string
		entry            *consul.ServiceEntry
		expectedNodeFqdn string
	}{
		{
			name: "fqdn from node meta",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{
					Meta: map[string]string{"fqdn": "node.example.com"},
				},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{"tag1"},
					Meta:    map[string]string{"key": "value"},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedNodeFqdn: "node.example.com",
		},
		{
			name: "fallback to external-k8s-node-name",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{
					Meta: map[string]string{},
				},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{"tag1"},
					Meta:    map[string]string{"external-k8s-node-name": "k8s-node.example.com"},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedNodeFqdn: "k8s-node.example.com",
		},
		{
			name: "fqdn takes priority over external-k8s-node-name",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{
					Meta: map[string]string{"fqdn": "node.example.com"},
				},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{"tag1"},
					Meta:    map[string]string{"external-k8s-node-name": "k8s-node.example.com"},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedNodeFqdn: "node.example.com",
		},
		{
			name: "empty when neither present",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{
					Meta: map[string]string{},
				},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{},
					Meta:    map[string]string{},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedNodeFqdn: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toServiceEntry(tt.entry)

			if result.NodeFqdn != tt.expectedNodeFqdn {
				t.Errorf("NodeFqdn = %q, want %q", result.NodeFqdn, tt.expectedNodeFqdn)
			}
			if result.Service != tt.entry.Service.Service {
				t.Errorf("Service = %q, want %q", result.Service, tt.entry.Service.Service)
			}
			if result.Port != tt.entry.Service.Port {
				t.Errorf("Port = %d, want %d", result.Port, tt.entry.Service.Port)
			}
			if result.Address != tt.entry.Service.Address {
				t.Errorf("Address = %q, want %q", result.Address, tt.entry.Service.Address)
			}
		})
	}
}

func TestToServiceEntryPodNameResolution(t *testing.T) {
	tests := []struct {
		name            string
		entry           *consul.ServiceEntry
		expectedPodName string
	}{
		{
			name: "pod name from k8s_pod service meta",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{"tag1"},
					Meta:    map[string]string{"key": "value", "k8s_pod": "test-pod"},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedPodName: "test-pod",
		},
		{
			name: "fallback to external-k8s-ref-name",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{"tag1"},
					Meta:    map[string]string{"key": "value", "external-k8s-ref-name": "test-pod"},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedPodName: "test-pod",
		},
		{
			name: "fqdn takes priority over external-k8s-node-name",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{"tag1"},
					Meta:    map[string]string{"k8s_pod": "test-pod", "external-k8s-ref-name": "test-pod"},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedPodName: "test-pod",
		},
		{
			name: "empty when neither present",
			entry: &consul.ServiceEntry{
				Node: &consul.Node{},
				Service: &consul.AgentService{
					Service: "my-service",
					Tags:    []string{},
					Meta:    map[string]string{},
					Port:    8080,
					Address: "10.0.0.1",
				},
			},
			expectedPodName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toServiceEntry(tt.entry)

			if result.PodName != tt.expectedPodName {
				t.Errorf("NodeFqdn = %q, want %q", result.NodeFqdn, tt.expectedPodName)
			}
		})
	}
}
