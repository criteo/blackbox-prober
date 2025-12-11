package aerospike

import (
	"reflect"
	"testing"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/go-kit/log"
)

func TestDiscoverCluster(t *testing.T) {
	meta := map[string]string{
		"CLUSTER":                  "aerospikesXX",
		"aerospike-monitoring-ns1": "true",
		"aerospike-monitoring-ns2": "true",
	}
	aerospikeServices := []discovery.ServiceEntry{
		{Address: "1.1.1.1", Port: 4333, Service: "aerospike-aerospikesXX", Meta: meta},
		{Address: "2.2.2.2", Port: 4333, Service: "aerospike-aerospikesXX", Meta: meta},
		{Address: "3.3.3.3", Port: 4333, Service: "aerospike-aerospikesXX", Meta: meta},
	}

	config := AerospikeProbeConfig{}
	config.DiscoveryConfig.MetaClusterKey = "CLUSTER"
	config.AerospikeEndpointConfig.NamespaceMetaKeyPrefix = "aerospike-monitoring-"
	clusterMap, err := config.DiscoverClusters(log.NewNopLogger(), aerospikeServices)

	if err != nil {
		t.Error(err)
	}
	if len(clusterMap.Clusters) != 5 {
		t.Errorf("Expecting 5 clusters but got %d", len(clusterMap.Clusters))
	}
	cluster, ok := clusterMap.Clusters["aerospikesXX/ns1"]
	if !ok {
		t.Errorf("Missing cluster aerospikesXX/ns1")
	}
	if !cluster.ClusterEndpoint.IsCluster() {
		t.Errorf("Expecting aerospikesXX/ns1 endpoint to be flagged as cluster endpoint")
	}
	cluster, ok = clusterMap.Clusters["aerospikesXX/ns2"]
	if !ok {
		t.Errorf("Missing cluster aerospikesXX/ns2")
	}
	if !cluster.ClusterEndpoint.IsCluster() {
		t.Errorf("Expecting aerospikesXX/ns2 endpoint to be flagged as cluster endpoint")
	}
	cluster, ok = clusterMap.Clusters["aerospikesXX/1.1.1.1"]
	if !ok {
		t.Errorf("Missing cluster aerospikesXX/1.1.1.1")
	}
	if cluster.ClusterEndpoint.IsCluster() {
		t.Errorf("Expecting aerospikesXX/1.1.1.1 endpoint to be flagged as node endpoint")
	}
	cluster, ok = clusterMap.Clusters["aerospikesXX/2.2.2.2"]
	if !ok {
		t.Errorf("Missing cluster aerospikesXX/2.2.2.2")
	}
	if cluster.ClusterEndpoint.IsCluster() {
		t.Errorf("Expecting aerospikesXX//2.2.2.2 endpoint to be flagged as node endpoint")
	}
	cluster, ok = clusterMap.Clusters["aerospikesXX/2.2.2.2"]
	if !ok {
		t.Errorf("Missing cluster aerospikesXX/3.3.3.3")
	}
	if cluster.ClusterEndpoint.IsCluster() {
		t.Errorf("Expecting aerospikesXX/3.3.3.3 endpoint to be flagged as node endpoint")
	}
}

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

	namespaces := config.getNamespacesFromEntry(log.NewNopLogger(), &entry_Valid)
	if !reflect.DeepEqual(namespaces, expected_Valid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_Valid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), &entry_OneInvalid)
	if !reflect.DeepEqual(namespaces, expected_OneInvalid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_OneInvalid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), &entry_Invalid)
	if !reflect.DeepEqual(namespaces, expected_Invalid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_Invalid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), &entry_empty)
	if !reflect.DeepEqual(namespaces, expected_empty) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_empty'.")
	}
}
