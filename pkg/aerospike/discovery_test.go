package aerospike

import (
	"reflect"
	"testing"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/go-kit/log"
)

func TestGetNamespacesFromEntry(t *testing.T) {
	entry_Valid := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-monitoring-test1": "true",
			"aerospike-monitoring-test2": "true",
			"aerospike-monitoring-test3": "false",
		},
	}
	expected_Valid := map[string]struct{}{
		"test1": {},
		"test2": {},
	}

	entry_OneInvalid := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-monitoring-test1": "true",
			"aerospike-monitoring-test2": "foo",
			"aerospike-monitoring-test3": "false",
		},
	}
	expected_OneInvalid := map[string]struct{}{
		"test1": {},
	}

	entry_Invalid := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-monitoring-test1": "bar",
			"aerospike-monitoring-test2": "foo",
		},
	}
	expected_Invalid := map[string]struct{}{}

	entry_empty := discovery.ServiceEntry{
		Meta: map[string]string{},
	}
	expected_empty := map[string]struct{}{}

	// Minimum config for getNamespacesFromEntry tests
	config := AerospikeProbeConfig{}
	config.AerospikeEndpointConfig = AerospikeEndpointConfig{
		NamespaceMetaKey:       "aerospike-namespaces",
		NamespaceMetaKeyPrefix: "aerospike-monitoring-",
	}

	namespaces := config.getNamespacesFromEntry(log.NewNopLogger(), entry_Valid)
	if !reflect.DeepEqual(namespaces, expected_Valid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_Valid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_OneInvalid)
	if !reflect.DeepEqual(namespaces, expected_OneInvalid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_OneInvalid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_Invalid)
	if !reflect.DeepEqual(namespaces, expected_Invalid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_Invalid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_empty)
	if !reflect.DeepEqual(namespaces, expected_empty) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_empty'.")
	}
}

func TestShouldSkipNamespace(t *testing.T) {
	tests := []struct {
		name               string
		notReadyNamespaces map[string][]string
		namespace          string
		cluster            string
		want               bool
	}{
		{
			name:      "nil configuration",
			namespace: "namespace-1",
			cluster:   "cluster-1",
		},
		{
			name:               "empty configuration",
			notReadyNamespaces: map[string][]string{},
			namespace:          "namespace-1",
			cluster:            "cluster-1",
		},
		{
			name: "cluster is not configured",
			notReadyNamespaces: map[string][]string{
				"cluster-2": {"namespace-1"},
			},
			namespace: "namespace-1",
			cluster:   "cluster-1",
		},
		{
			name: "cluster has no not-ready namespaces",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {},
			},
			namespace: "namespace-1",
			cluster:   "cluster-1",
		},
		{
			name: "namespace is not configured for cluster",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {"namespace-2"},
			},
			namespace: "namespace-1",
			cluster:   "cluster-1",
		},
		{
			name: "namespace is configured for cluster",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {"namespace-1", "namespace-2"},
			},
			namespace: "namespace-1",
			cluster:   "cluster-1",
			want:      true,
		},
		{
			name: "same namespace is only skipped on configured cluster",
			notReadyNamespaces: map[string][]string{
				"cluster-2": {"namespace-1"},
			},
			namespace: "namespace-1",
			cluster:   "cluster-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := AerospikeProbeConfig{
				DiscoveryConfig: discovery.GenericDiscoveryConfig{
					NotReadyNamespaces: tt.notReadyNamespaces,
				},
			}

			if got := config.shouldSkipNamespace(tt.namespace, tt.cluster); got != tt.want {
				t.Errorf("shouldSkipNamespace(%q, %q) = %t, want %t", tt.namespace, tt.cluster, got, tt.want)
			}
		})
	}
}

func TestGenerateNamespacedEndpointsSkipsNotReadyNamespaces(t *testing.T) {
	entry := discovery.ServiceEntry{
		Address: "node-1",
		Meta: map[string]string{
			"aerospike-monitoring-namespace-1": "true",
			"aerospike-monitoring-namespace-2": "true",
		},
	}

	tests := []struct {
		name               string
		notReadyNamespaces map[string][]string
		wantNamespaces     map[string]struct{}
	}{
		{
			name: "no skip configuration",
			wantNamespaces: map[string]struct{}{
				"namespace-1": {},
				"namespace-2": {},
			},
		},
		{
			name: "skip one namespace",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {"namespace-1"},
			},
			wantNamespaces: map[string]struct{}{
				"namespace-2": {},
			},
		},
		{
			name: "do not skip namespace configured for another cluster",
			notReadyNamespaces: map[string][]string{
				"cluster-2": {"namespace-1"},
			},
			wantNamespaces: map[string]struct{}{
				"namespace-1": {},
				"namespace-2": {},
			},
		},
		{
			name: "skip all namespaces",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {"namespace-1", "namespace-2"},
			},
			wantNamespaces: map[string]struct{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := AerospikeProbeConfig{
				DiscoveryConfig: discovery.GenericDiscoveryConfig{
					NotReadyNamespaces: tt.notReadyNamespaces,
				},
				AerospikeEndpointConfig: AerospikeEndpointConfig{
					NamespaceMetaKeyPrefix: "aerospike-monitoring-",
				},
			}
			clusterConfig := &AerospikeClientConfig{clusterName: "cluster-1"}

			endpoints := config.generateNamespacedEndpointsFromEntry(log.NewNopLogger(), entry, clusterConfig)
			gotNamespaces := make(map[string]struct{}, len(endpoints))
			for _, endpoint := range endpoints {
				gotNamespaces[endpoint.Namespace] = struct{}{}
			}

			if !reflect.DeepEqual(gotNamespaces, tt.wantNamespaces) {
				t.Errorf("generated namespaces = %v, want %v", gotNamespaces, tt.wantNamespaces)
			}
		})
	}
}
