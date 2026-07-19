package aerospike

import (
	"reflect"
	"testing"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
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

func testProbeConfig() AerospikeProbeConfig {
	return AerospikeProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		AerospikeEndpointConfig: AerospikeEndpointConfig{
			AuthEnabled:            false,
			NamespaceMetaKeyPrefix: "aerospike-monitoring-",
		},
	}
}

func TestBuildTopologySkipsClusterWithoutNamespaces(t *testing.T) {
	config := testProbeConfig()
	entries := []discovery.ServiceEntry{
		{
			Address: "10.0.0.1",
			Port:    3000,
			Meta: map[string]string{
				"CLUSTER": "cluster-a",
			},
		},
	}

	clusterMap, err := config.BuildTopology(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(clusterMap.Clusters) != 0 {
		t.Fatalf("expected no cluster endpoint without discovered namespaces, got %d", len(clusterMap.Clusters))
	}
}

func TestBuildTopologyCreatesSingleEndpointWithSortedNamespaces(t *testing.T) {
	config := testProbeConfig()
	entries := []discovery.ServiceEntry{
		{
			Address: "10.0.0.1",
			Port:    3000,
			Meta: map[string]string{
				"CLUSTER":                 "cluster-a",
				"aerospike-monitoring-z":  "true",
				"aerospike-monitoring-aa": "true",
			},
		},
	}

	clusterMap, err := config.BuildTopology(log.NewNopLogger(), entries)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(clusterMap.Clusters) != 1 {
		t.Fatalf("expected one cluster endpoint, got %d", len(clusterMap.Clusters))
	}

	var endpoint topology.ProbeableEndpoint
	for _, cluster := range clusterMap.Clusters {
		endpoint = cluster.ClusterEndpoint
	}
	aerospikeEndpoint, ok := endpoint.(*AerospikeEndpoint)
	if !ok {
		t.Fatalf("expected AerospikeEndpoint, got %T", endpoint)
	}
	expectedNamespaces := []string{"aa", "z"}
	if !reflect.DeepEqual(aerospikeEndpoint.Namespaces, expectedNamespaces) {
		t.Fatalf("expected sorted namespaces %v, got %v", expectedNamespaces, aerospikeEndpoint.Namespaces)
	}
}
