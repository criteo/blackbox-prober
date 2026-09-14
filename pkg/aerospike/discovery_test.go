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

func TestGenerateEndpointSkipsNotReadyNamespaces(t *testing.T) {
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
		wantNamespaces     []string
	}{
		{
			name:           "no skip configuration",
			wantNamespaces: []string{"namespace-1", "namespace-2"},
		},
		{
			name: "skip one namespace",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {"namespace-1"},
			},
			wantNamespaces: []string{"namespace-2"},
		},
		{
			name: "do not skip namespace configured for another cluster",
			notReadyNamespaces: map[string][]string{
				"cluster-2": {"namespace-1"},
			},
			wantNamespaces: []string{"namespace-1", "namespace-2"},
		},
		{
			name: "skip all namespaces",
			notReadyNamespaces: map[string][]string{
				"cluster-1": {"namespace-1", "namespace-2"},
			},
			wantNamespaces: []string{},
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

			endpoint := config.generateEndpointFromEntry(log.NewNopLogger(), entry, clusterConfig)
			if !reflect.DeepEqual(endpoint.Namespaces, tt.wantNamespaces) {
				t.Errorf("generated namespaces = %v, want %v", endpoint.Namespaces, tt.wantNamespaces)
			}
		})
	}
}

func singleClusterEndpoint(t *testing.T, clusterMap topology.ClusterMap) *AerospikeEndpoint {
	t.Helper()
	if len(clusterMap.Clusters) != 1 {
		t.Fatalf("expected one cluster endpoint, got %d", len(clusterMap.Clusters))
	}
	for _, cluster := range clusterMap.Clusters {
		endpoint, ok := cluster.ClusterEndpoint.(*AerospikeEndpoint)
		if !ok {
			t.Fatalf("expected AerospikeEndpoint, got %T", cluster.ClusterEndpoint)
		}
		return endpoint
	}
	return nil
}

// The scheduler keeps probing with the endpoint built by the first discovery run as long as its
// hash is unchanged (a pod restart changes no namespace), so that endpoint must see the node
// metadata of later runs. Otherwise a pod coming back with a new IP is labelled "unknown".
func TestBuildTopologyRefreshesNodeInfoCacheOfRunningEndpoint(t *testing.T) {
	config := testProbeConfig()
	meta := map[string]string{
		"CLUSTER":                  "cluster-a",
		"aerospike-monitoring-ns1": "true",
	}

	clusterMap, err := config.BuildTopology(log.NewNopLogger(), []discovery.ServiceEntry{
		{Address: "10.0.0.1", Port: 3000, PodName: "aerospike-0", NodeFqdn: "node-1.example.com", Meta: meta},
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	runningEndpoint := singleClusterEndpoint(t, clusterMap)

	// The pod restarts with a new address, on another physical node.
	_, err = config.BuildTopology(log.NewNopLogger(), []discovery.ServiceEntry{
		{Address: "10.0.0.2", Port: 3000, PodName: "aerospike-0", NodeFqdn: "node-2.example.com", Meta: meta},
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	info, found := runningEndpoint.ClusterConfig.nodeInfoCache.Get("10.0.0.2")
	if !found {
		t.Fatal("expected the running endpoint to resolve the new address of the restarted pod")
	}
	if info.PodName != "aerospike-0" || info.NodeFqdn != "node-2.example.com" {
		t.Fatalf("unexpected node info: %+v", info)
	}
	if _, found := runningEndpoint.ClusterConfig.nodeInfoCache.Get("10.0.0.1"); found {
		t.Error("expected the address of the pod before restart to be dropped")
	}
}

func TestBuildTopologyPrunesNodeInfoCacheOfGoneClusters(t *testing.T) {
	config := testProbeConfig()
	entry := func(address, cluster string) discovery.ServiceEntry {
		return discovery.ServiceEntry{
			Address: address,
			Port:    3000,
			Meta: map[string]string{
				"CLUSTER":                  cluster,
				"aerospike-monitoring-ns1": "true",
			},
		}
	}

	_, err := config.BuildTopology(log.NewNopLogger(), []discovery.ServiceEntry{
		entry("10.0.0.1", "cluster-a"),
		entry("10.0.1.1", "cluster-b"),
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(config.nodeInfoCaches) != 2 {
		t.Fatalf("expected 2 node info caches, got %d", len(config.nodeInfoCaches))
	}

	// cluster-b is decommissioned: its cache must not outlive its discovery.
	_, err = config.BuildTopology(log.NewNopLogger(), []discovery.ServiceEntry{
		entry("10.0.0.1", "cluster-a"),
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(config.nodeInfoCaches) != 1 {
		t.Fatalf("expected 1 node info cache, got %d", len(config.nodeInfoCaches))
	}
	if _, found := config.nodeInfoCaches["cluster-a"]; !found {
		t.Error("expected the cache of the still discovered cluster to be kept")
	}
}
